# pipeline.py — Deterministic 480×360 field-math and tracking primitives.
#
# Rationale: CONVERGED.md §5c deliberately chooses a light, explainable
# normalize→guard→motion→segment→track pipeline.  NumPy accelerates field
# arithmetic when present; every operation has a Pillow implementation so the
# exact core remains replayable on razorback and HA.  ERRATA.md ERR-06 makes
# distinct-content credit, rather than call cadence, the persistence clock.

"""Image normalization, guards, segmentation, and stationarity tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
import math
from typing import Any, Sequence

from PIL import Image, ImageChops, ImageFilter

from .config import DetectorConfig
from .envelopes import FrameEnvelope

try:  # Optional by design; ImportError is the supported Pillow-only path.
    import numpy as _np
except ImportError:  # pragma: no cover - exercised by a separate acceptance run
    _np = None


NUMPY_AVAILABLE = _np is not None


@dataclass(frozen=True, slots=True)
class GrayFrame:
    """Decoded working-resolution image plus source-coordinate mapping."""

    image: Image.Image
    source_size: tuple[int, int]

    @property
    def scale_x(self) -> float:
        return self.source_size[0] / self.image.width

    @property
    def scale_y(self) -> float:
        return self.source_size[1] / self.image.height


@dataclass(frozen=True, slots=True)
class Component:
    """One structural change component in working-resolution coordinates."""

    bbox: tuple[int, int, int, int]
    bbox_norm: tuple[float, float, float, float]
    area: int
    area_frac: float
    centroid: tuple[float, float]
    polarity: str
    edge_gain: float
    pixels: tuple[int, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class SegmentationResult:
    normalized: Image.Image
    ratio: float
    illumination_fraction: float
    change_mask: Image.Image
    components: tuple[Component, ...]
    rejected_shadows: int


@dataclass(slots=True)
class Track:
    track_id: int
    component: Component
    first_seen_frame: str
    first_seen_mono_ms: int
    first_sha256: str
    last_frame: str
    last_sha256: str
    stable_count: int = 0
    sightings: int = 1
    stability_sad: float = math.inf
    emitted: bool = False
    frame_sha256s: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConfirmedCandidate:
    track: Track
    component: Component
    confirmed_frame: str
    confirmed_mono_ms: int
    confirmed_sha256: str
    confirm_count: int
    stability_sad: float


def backend_name() -> str:
    return "numpy" if NUMPY_AVAILABLE else "pillow"


def decode_gray(frame: FrameEnvelope, config: DetectorConfig) -> GrayFrame:
    """Decode an envelope to L mode and the configured working resolution."""

    if frame.jpeg_bytes is not None:
        with Image.open(BytesIO(frame.jpeg_bytes)) as source:
            source.load()
            image = source.convert("L")
    else:
        value = frame.gray_array
        if isinstance(value, Image.Image):
            image = value.convert("L")
        elif _np is not None and isinstance(value, _np.ndarray):
            array = _np.asarray(value)
            if array.ndim != 2:
                raise ValueError("gray_array numpy input must be two-dimensional")
            image = Image.fromarray(_np.clip(array, 0, 255).astype("uint8"), mode="L")
        else:
            rows = [list(row) for row in value]
            if not rows or not rows[0] or any(len(row) != len(rows[0]) for row in rows):
                raise ValueError("gray_array rows must form a non-empty rectangle")
            image = Image.new("L", (len(rows[0]), len(rows)))
            image.putdata([max(0, min(255, int(pixel))) for row in rows for pixel in row])

    source_size = image.size
    if image.size != config.working_size:
        image = image.resize(config.working_size, Image.Resampling.BILINEAR)
    return GrayFrame(image=image, source_size=source_size)


def roi_box(size: tuple[int, int], roi_norm: Sequence[float]) -> tuple[int, int, int, int]:
    width, height = size
    x0, y0, x1, y1 = roi_norm
    return (
        max(0, min(width - 1, int(math.floor(x0 * width)))),
        max(0, min(height - 1, int(math.floor(y0 * height)))),
        max(1, min(width, int(math.ceil(x1 * width)))),
        max(1, min(height, int(math.ceil(y1 * height)))),
    )


def _roi_mask(
    size: tuple[int, int], box: tuple[int, int, int, int], exclusion: Image.Image | None = None
) -> Image.Image:
    mask = Image.new("L", size, 0)
    mask.paste(255, box)
    if exclusion is not None:
        inverse = exclusion.convert("L").point(lambda value: 0 if value else 255)
        mask = ImageChops.multiply(mask, inverse)
    return mask


def _masked_median(image: Image.Image, mask: Image.Image) -> float:
    histogram = image.histogram(mask=mask)
    count = sum(histogram)
    if count == 0:
        return 0.0
    midpoint = (count - 1) // 2
    running = 0
    for value, frequency in enumerate(histogram):
        running += frequency
        if running > midpoint:
            return float(value)
    return 0.0


def photometric_normalize(
    current: Image.Image,
    baseline: Image.Image,
    config: DetectorConfig,
    exclusion_mask: Image.Image | None = None,
) -> tuple[Image.Image, float]:
    """Median-ratio normalization over ROI reference pixels, clamped 0.5–2.0."""

    if current.size != baseline.size:
        raise ValueError("normalization images must have equal dimensions")
    box = roi_box(current.size, config.roi_norm)
    mask = _roi_mask(current.size, box, exclusion_mask)

    if _np is not None:
        current_array = _np.asarray(current, dtype=_np.float32)
        baseline_array = _np.asarray(baseline, dtype=_np.float32)
        mask_array = _np.asarray(mask, dtype=_np.uint8) != 0
        if mask_array.any():
            current_median = float(_np.median(current_array[mask_array]))
            baseline_median = float(_np.median(baseline_array[mask_array]))
        else:
            current_median = baseline_median = 0.0
    else:
        current_median = _masked_median(current, mask)
        baseline_median = _masked_median(baseline, mask)

    if current_median <= 0.0:
        ratio = 1.0
    else:
        ratio = baseline_median / current_median
    ratio = max(config.normalization_ratio_min, min(config.normalization_ratio_max, ratio))

    if _np is not None:
        normalized = _np.rint(_np.clip(_np.asarray(current, dtype=_np.float32) * ratio, 0, 255))
        return Image.fromarray(normalized.astype("uint8"), mode="L"), ratio
    lookup = [max(0, min(255, int(round(value * ratio)))) for value in range(256)]
    return current.point(lookup), ratio


def difference_mask(first: Image.Image, second: Image.Image, threshold: int) -> Image.Image:
    """Return a binary mask for absolute grayscale deltas strictly above threshold."""

    if _np is not None:
        delta = _np.abs(
            _np.asarray(first, dtype=_np.int16) - _np.asarray(second, dtype=_np.int16)
        )
        return Image.fromarray(_np.where(delta > threshold, 255, 0).astype("uint8"), mode="L")
    delta = ImageChops.difference(first, second)
    lookup = [255 if value > threshold else 0 for value in range(256)]
    return delta.point(lookup)


def mask_fraction(mask: Image.Image, box: tuple[int, int, int, int] | None = None) -> float:
    field = mask.crop(box) if box is not None else mask
    histogram = field.histogram()
    total = field.width * field.height
    return (total - histogram[0]) / total if total else 0.0


def illumination_change_fraction(
    normalized: Image.Image, baseline: Image.Image, config: DetectorConfig
) -> float:
    mask = difference_mask(normalized, baseline, config.illumination_delta)
    return mask_fraction(mask, roi_box(normalized.size, config.roi_norm))


def motion_map(
    current: Image.Image, previous: Image.Image | None, config: DetectorConfig
) -> tuple[Image.Image, float]:
    """Raw frame-to-frame >12 map, dilated by a five-pixel radius."""

    if previous is None:
        return Image.new("L", current.size, 0), 0.0
    mask = difference_mask(current, previous, config.motion_delta)
    if config.motion_dilation_px:
        size = config.motion_dilation_px * 2 + 1
        mask = mask.filter(ImageFilter.MaxFilter(size))
    return mask, mask_fraction(mask, roi_box(current.size, config.roi_norm))


def _morphology(mask: Image.Image, config: DetectorConfig) -> Image.Image:
    opened = mask.filter(ImageFilter.MinFilter(config.morph_open_px))
    opened = opened.filter(ImageFilter.MaxFilter(config.morph_open_px))
    closed = opened.filter(ImageFilter.MaxFilter(config.morph_close_px))
    return closed.filter(ImageFilter.MinFilter(config.morph_close_px))


def _connected_pixels(mask: Image.Image) -> list[tuple[tuple[int, int, int, int], tuple[int, ...]]]:
    width, height = mask.size
    values = mask.tobytes()
    visited = bytearray(width * height)
    groups: list[tuple[tuple[int, int, int, int], tuple[int, ...]]] = []
    for origin, value in enumerate(values):
        if value == 0 or visited[origin]:
            continue
        visited[origin] = 1
        stack = [origin]
        pixels: list[int] = []
        min_x = max_x = origin % width
        min_y = max_y = origin // width
        while stack:
            index = stack.pop()
            pixels.append(index)
            x = index % width
            y = index // width
            min_x = min(min_x, x)
            max_x = max(max_x, x)
            min_y = min(min_y, y)
            max_y = max(max_y, y)
            for ny in range(max(0, y - 1), min(height, y + 2)):
                row = ny * width
                for nx in range(max(0, x - 1), min(width, x + 2)):
                    neighbour = row + nx
                    if values[neighbour] and not visited[neighbour]:
                        visited[neighbour] = 1
                        stack.append(neighbour)
        groups.append(((min_x, min_y, max_x + 1, max_y + 1), tuple(pixels)))
    return groups


def _sobel_is_edge(image: Image.Image, x: int, y: int, threshold: int) -> bool:
    if x <= 0 or y <= 0 or x >= image.width - 1 or y >= image.height - 1:
        return False
    pixel = image.load()
    gx = (
        -pixel[x - 1, y - 1]
        + pixel[x + 1, y - 1]
        - 2 * pixel[x - 1, y]
        + 2 * pixel[x + 1, y]
        - pixel[x - 1, y + 1]
        + pixel[x + 1, y + 1]
    )
    gy = (
        -pixel[x - 1, y - 1]
        - 2 * pixel[x, y - 1]
        - pixel[x + 1, y - 1]
        + pixel[x - 1, y + 1]
        + 2 * pixel[x, y + 1]
        + pixel[x + 1, y + 1]
    )
    return abs(gx) + abs(gy) > threshold * 4


def _edge_budget(
    current: Image.Image,
    baseline: Image.Image,
    bbox: tuple[int, int, int, int],
    pixels: Sequence[int],
    threshold: int,
) -> tuple[str, float]:
    width = current.width
    added_edges = 0
    removed_edges = 0
    for index in pixels:
        x = index % width
        y = index // width
        current_edge = _sobel_is_edge(current, x, y, threshold)
        baseline_edge = _sobel_is_edge(baseline, x, y, threshold)
        added_edges += int(current_edge and not baseline_edge)
        removed_edges += int(baseline_edge and not current_edge)
    x0, y0, x1, y1 = bbox
    perimeter = max(1, 2 * ((x1 - x0) + (y1 - y0)))
    edge_gain = max(added_edges, removed_edges) / perimeter
    if added_edges == removed_edges:
        current_values = current.load()
        baseline_values = baseline.load()
        signed = sum(
            current_values[index % width, index // width]
            - baseline_values[index % width, index // width]
            for index in pixels
        )
        polarity = "added" if signed >= 0 else "removed"
    else:
        polarity = "added" if added_edges > removed_edges else "removed"
    return polarity, edge_gain


def segment_changes(
    current: Image.Image,
    baseline: Image.Image,
    config: DetectorConfig,
    exclusion_mask: Image.Image | None = None,
) -> SegmentationResult:
    """Normalize, guard, morphologically segment, and describe structural changes."""

    # Candidate regions are not known until a first pass.  Recompute the
    # median ratio after excluding both caller-supplied motion and that first
    # candidate map, which implements §5c-1's reference-pixel definition
    # without introducing a circular dependency.
    initial_normalized, _ = photometric_normalize(
        current, baseline, config, exclusion_mask
    )
    initial_candidates = _morphology(
        difference_mask(initial_normalized, baseline, config.change_delta), config
    )
    reference_exclusion = (
        initial_candidates
        if exclusion_mask is None
        else ImageChops.lighter(initial_candidates, exclusion_mask.convert("L"))
    )
    normalized, ratio = photometric_normalize(
        current, baseline, config, reference_exclusion
    )
    illumination_fraction = illumination_change_fraction(normalized, baseline, config)
    change_mask = _morphology(difference_mask(normalized, baseline, config.change_delta), config)
    width, height = current.size
    roi = roi_box(current.size, config.roi_norm)
    components: list[Component] = []
    rejected = 0
    for bbox, pixels in _connected_pixels(change_mask):
        if not pixels:
            continue
        x0, y0, x1, y1 = bbox
        # Ignore components wholly outside the configured delivery field.
        if x1 <= roi[0] or y1 <= roi[1] or x0 >= roi[2] or y0 >= roi[3]:
            continue
        polarity, edge_gain = _edge_budget(
            normalized, baseline, bbox, pixels, config.sobel_edge_threshold
        )
        if edge_gain < config.shadow_edge_gain_min:
            rejected += 1
            continue
        x_sum = sum(index % width for index in pixels)
        y_sum = sum(index // width for index in pixels)
        area = len(pixels)
        components.append(
            Component(
                bbox=bbox,
                bbox_norm=(x0 / width, y0 / height, (x1 - x0) / width, (y1 - y0) / height),
                area=area,
                area_frac=area / (width * height),
                centroid=(x_sum / area, y_sum / area),
                polarity=polarity,
                edge_gain=edge_gain,
                pixels=pixels,
            )
        )
    return SegmentationResult(
        normalized=normalized,
        ratio=ratio,
        illumination_fraction=illumination_fraction,
        change_mask=change_mask,
        components=tuple(components),
        rejected_shadows=rejected,
    )


def bbox_iou(first: Sequence[int], second: Sequence[int]) -> float:
    x0 = max(first[0], second[0])
    y0 = max(first[1], second[1])
    x1 = min(first[2], second[2])
    y1 = min(first[3], second[3])
    intersection = max(0, x1 - x0) * max(0, y1 - y0)
    first_area = max(0, first[2] - first[0]) * max(0, first[3] - first[1])
    second_area = max(0, second[2] - second[0]) * max(0, second[3] - second[1])
    union = first_area + second_area - intersection
    return intersection / union if union else 0.0


def component_overlaps_mask(component: Component, mask: Image.Image) -> bool:
    values = mask.tobytes()
    return any(values[index] != 0 for index in component.pixels)


def component_sad(current: Image.Image, previous: Image.Image, component: Component) -> float:
    current_values = current.tobytes()
    previous_values = previous.tobytes()
    return sum(abs(current_values[index] - previous_values[index]) for index in component.pixels) / max(
        1, component.area
    )


def centroid_in_roi(component: Component, config: DetectorConfig, size: tuple[int, int]) -> bool:
    x0, y0, x1, y1 = roi_box(size, config.roi_norm)
    x, y = component.centroid
    return x0 <= x < x1 and y0 <= y < y1


def full_resolution_bbox(component: Component, frame: GrayFrame) -> tuple[int, int, int, int]:
    x0, y0, x1, y1 = component.bbox
    return (
        int(round(x0 * frame.scale_x)),
        int(round(y0 * frame.scale_y)),
        int(round(x1 * frame.scale_x)),
        int(round(y1 * frame.scale_y)),
    )


class StationarityTracker:
    """IoU/SAD/motion tracker with distinct-frame confirmation credit."""

    def __init__(self, config: DetectorConfig) -> None:
        self.config = config
        self._tracks: list[Track] = []
        self._next_track_id = 1

    @property
    def tracks(self) -> tuple[Track, ...]:
        return tuple(self._tracks)

    def reset(self) -> None:
        self._tracks.clear()
        self._next_track_id = 1

    def update(
        self,
        components: Sequence[Component],
        current: Image.Image,
        previous: Image.Image | None,
        motion: Image.Image,
        frame: FrameEnvelope,
    ) -> list[ConfirmedCandidate]:
        unmatched_tracks = set(range(len(self._tracks)))
        unmatched_components = set(range(len(components)))
        matches: list[tuple[float, int, int]] = []
        for track_index, track in enumerate(self._tracks):
            for component_index, component in enumerate(components):
                if track.component.polarity != component.polarity:
                    continue
                overlap = bbox_iou(track.component.bbox, component.bbox)
                if overlap >= self.config.track_iou_min:
                    matches.append((overlap, track_index, component_index))
        chosen: list[tuple[int, int, float]] = []
        for overlap, track_index, component_index in sorted(matches, reverse=True):
            if track_index in unmatched_tracks and component_index in unmatched_components:
                unmatched_tracks.remove(track_index)
                unmatched_components.remove(component_index)
                chosen.append((track_index, component_index, overlap))

        confirmed: list[ConfirmedCandidate] = []
        for track_index, component_index, overlap in chosen:
            track = self._tracks[track_index]
            component = components[component_index]
            sad = component_sad(current, previous, component) if previous is not None else math.inf
            stable = (
                overlap >= self.config.confirm_iou_min
                and sad <= self.config.stability_sad_max
                and not component_overlaps_mask(component, motion)
            )
            if stable:
                track.stable_count += 1
                track.sightings += 1
                track.frame_sha256s[frame.frame_id] = frame.sha256
            else:
                track.first_seen_frame = frame.frame_id
                track.first_seen_mono_ms = frame.at_mono_ms
                track.first_sha256 = frame.sha256
                track.stable_count = 0
                track.sightings = 1
                track.frame_sha256s = {frame.frame_id: frame.sha256}
            track.component = component
            track.last_frame = frame.frame_id
            track.last_sha256 = frame.sha256
            track.stability_sad = sad
            if stable and track.stable_count >= self.config.confirm_k and not track.emitted:
                confirmed.append(
                    ConfirmedCandidate(
                        track=track,
                        component=component,
                        confirmed_frame=frame.frame_id,
                        confirmed_mono_ms=frame.at_mono_ms,
                        confirmed_sha256=frame.sha256,
                        confirm_count=track.sightings,
                        stability_sad=sad,
                    )
                )

        # Non-emitted candidates require consecutive sightings.  Emitted tracks
        # persist while their baseline-relative component persists, preventing
        # a stationary package from being re-published on every frame.
        survivors = [
            track
            for index, track in enumerate(self._tracks)
            if index not in unmatched_tracks or track.emitted
        ]
        self._tracks = survivors
        for component_index in sorted(unmatched_components):
            component = components[component_index]
            track = Track(
                track_id=self._next_track_id,
                component=component,
                first_seen_frame=frame.frame_id,
                first_seen_mono_ms=frame.at_mono_ms,
                first_sha256=frame.sha256,
                last_frame=frame.frame_id,
                last_sha256=frame.sha256,
                frame_sha256s={frame.frame_id: frame.sha256},
            )
            self._next_track_id += 1
            self._tracks.append(track)
        return confirmed


def _edge_profiles(image: Image.Image) -> tuple[list[float], list[float]]:
    """Return Sobel-like vertical/horizontal edge projections."""

    if _np is not None:
        values = _np.asarray(image, dtype=_np.int16)
        gx = _np.abs(values[:, 2:] - values[:, :-2])
        gy = _np.abs(values[2:, :] - values[:-2, :])
        x_profile = [0.0] + gx.sum(axis=0).astype(float).tolist() + [0.0]
        y_profile = [0.0] + gy.sum(axis=1).astype(float).tolist() + [0.0]
        return x_profile, y_profile
    pixels = image.load()
    x_profile = [0.0] * image.width
    y_profile = [0.0] * image.height
    for y in range(1, image.height - 1):
        for x in range(1, image.width - 1):
            x_profile[x] += abs(pixels[x + 1, y] - pixels[x - 1, y])
            y_profile[y] += abs(pixels[x, y + 1] - pixels[x, y - 1])
    return x_profile, y_profile


def _correlation(first: Sequence[float], second: Sequence[float], shift: int) -> float:
    if shift >= 0:
        left = first[: len(first) - shift or None]
        right = second[shift:]
    else:
        left = first[-shift:]
        right = second[: len(second) + shift]
    if len(left) < 3:
        return -1.0
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    left_energy = sum((a - left_mean) ** 2 for a in left)
    right_energy = sum((b - right_mean) ** 2 for b in right)
    denominator = math.sqrt(left_energy * right_energy)
    return numerator / denominator if denominator else -1.0


def _best_profile_shift(
    first: Sequence[float], second: Sequence[float], search_px: int
) -> tuple[int, float]:
    candidates = [
        (shift, _correlation(first, second, shift))
        for shift in range(-search_px, search_px + 1)
    ]
    return max(candidates, key=lambda item: (item[1], -abs(item[0])))


def estimate_camera_shift(
    baseline: Image.Image, current: Image.Image, search_px: int = 12
) -> tuple[int, int, float]:
    """Estimate translation by edge-profile correlation on the REBASE path."""

    baseline_x, baseline_y = _edge_profiles(baseline)
    current_x, current_y = _edge_profiles(current)
    dx, x_score = _best_profile_shift(baseline_x, current_x, search_px)
    dy, y_score = _best_profile_shift(baseline_y, current_y, search_px)
    return dx, dy, min(x_score, y_score)


def global_stability_fraction(
    current: Image.Image, previous: Image.Image, config: DetectorConfig
) -> float:
    mask = difference_mask(current, previous, config.rebase_stable_delta)
    return mask_fraction(mask, roi_box(current.size, config.roi_norm))
