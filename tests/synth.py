# synth.py — Deterministic package-camera scene and sequence primitives.
#
# Rationale: CONVERGED.md §5i requires full temporal sequences rather than
# selected stills.  These 480×360 Pillow scenes cover structural objects,
# moving couriers, soft shadows, exposure/IR changes, and camera translation;
# every pseudo-random choice is driven by an explicit fixed seed.

"""Synthetic grayscale package-camera imagery for offline scenario replay."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import random
from typing import Iterable, Sequence

from PIL import Image, ImageDraw, ImageOps

from homeassistant.package_fast.core.envelopes import FrameEnvelope, SignalEnvelope


WIDTH = 480
HEIGHT = 360
FIXED_WALL = datetime(2026, 8, 27, 16, 0, 0, tzinfo=timezone.utc)
BOX_A = (112, 245, 174, 294)
BOX_B = (278, 238, 346, 292)
BOX_C = (360, 250, 418, 300)


def scene(seed: int = 7703, objects: Sequence[tuple[int, int, int, int]] = ()) -> Image.Image:
    """Create a textured but static porch view with optional resident objects."""

    rng = random.Random(seed)
    image = Image.new("L", (WIDTH, HEIGHT), 102)
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, WIDTH - 1, 194), fill=118)
    draw.rectangle((18, 20, 186, 194), fill=72, outline=156, width=4)  # door
    draw.rectangle((52, 62, 148, 154), fill=88, outline=145, width=3)
    draw.line((100, 62, 100, 154), fill=142, width=2)
    draw.line((52, 108, 148, 108), fill=142, width=2)
    draw.rectangle((360, 38, 438, 184), fill=105, outline=148, width=3)
    draw.ellipse((382, 70, 416, 104), fill=82, outline=160, width=2)
    draw.rectangle((0, 195, WIDTH - 1, HEIGHT - 1), fill=91)
    for y in range(206, HEIGHT, 31):
        draw.line((0, y, WIDTH, y), fill=98, width=2)
    for x in range(-20, WIDTH, 48):
        draw.line((x, 195, x + 34, HEIGHT), fill=86, width=1)
    draw.rectangle((186, 236, 278, 314), fill=78, outline=111, width=3)  # mat
    for _ in range(42):
        x = rng.randrange(WIDTH)
        y = rng.randrange(198, HEIGHT)
        shade = rng.choice((84, 87, 95, 99))
        draw.point((x, y), fill=shade)
    for index, bbox in enumerate(objects):
        draw_package(image, bbox, variant=index)
    return image


def draw_package(
    image: Image.Image,
    bbox: tuple[int, int, int, int],
    *,
    variant: int = 0,
) -> Image.Image:
    """Draw a high-structure parcel in-place and return the image."""

    draw = ImageDraw.Draw(image)
    x0, y0, x1, y1 = bbox
    fill = 168 + (variant % 3) * 8
    draw.rectangle(bbox, fill=fill, outline=226, width=3)
    draw.line((x0 + 4, y0 + 9, x1 - 4, y0 + 9), fill=118, width=2)
    draw.line(((x0 + x1) // 2, y0 + 3, (x0 + x1) // 2, y1 - 3), fill=214, width=2)
    draw.rectangle((x0 + 8, y0 + 15, min(x1 - 7, x0 + 28), min(y1 - 7, y0 + 31)), outline=104, width=2)
    if variant % 2:
        draw.line((x0 + 5, y1 - 9, x1 - 5, y0 + 13), fill=132, width=2)
    return image


def with_objects(
    base: Image.Image, objects: Sequence[tuple[int, int, int, int]]
) -> Image.Image:
    image = base.copy()
    for index, bbox in enumerate(objects):
        draw_package(image, bbox, variant=index)
    return image


def courier(
    base: Image.Image,
    *,
    center_x: int,
    foot_y: int = 322,
    carrying: bool = True,
) -> Image.Image:
    """Overlay a moving, package-carrying courier silhouette."""

    image = base.copy()
    draw = ImageDraw.Draw(image)
    draw.ellipse((center_x - 19, foot_y - 190, center_x + 19, foot_y - 152), fill=48)
    draw.rounded_rectangle(
        (center_x - 34, foot_y - 154, center_x + 34, foot_y - 56),
        radius=12,
        fill=52,
        outline=35,
        width=2,
    )
    draw.polygon(
        ((center_x - 25, foot_y - 58), (center_x - 5, foot_y - 58), (center_x - 16, foot_y)),
        fill=42,
    )
    draw.polygon(
        ((center_x + 6, foot_y - 58), (center_x + 27, foot_y - 58), (center_x + 18, foot_y)),
        fill=42,
    )
    if carrying:
        draw.rectangle(
            (center_x - 48, foot_y - 126, center_x + 48, foot_y - 78),
            fill=174,
            outline=226,
            width=3,
        )
        draw.line((center_x, foot_y - 124, center_x, foot_y - 80), fill=122, width=2)
    return image


def shadow_sweep(
    base: Image.Image,
    *,
    center_x: int,
    half_width: int = 105,
    darkness: int = 34,
) -> Image.Image:
    """Apply a broad, soft luma-only shadow band without structural edges."""

    source = base.load()
    image = base.copy()
    target = image.load()
    for y in range(HEIGHT):
        for x in range(WIDTH):
            distance = abs(x - center_x)
            if distance >= half_width:
                continue
            weight = (1.0 - distance / half_width) ** 2
            target[x, y] = max(0, int(round(source[x, y] - darkness * weight)))
    return image


def illumination_ramp(base: Image.Image, factor: float) -> Image.Image:
    lookup = [max(0, min(255, int(round(value * factor)))) for value in range(256)]
    return base.point(lookup)


def ir_flip(base: Image.Image) -> Image.Image:
    """Simulate a day/night IR polarity and tonal-curve discontinuity."""

    inverted = ImageOps.invert(base)
    return inverted.point([max(0, min(255, int(value * 0.82 + 18))) for value in range(256)])


def camera_shift(base: Image.Image, dx: int, dy: int, fill: int = 102) -> Image.Image:
    """Translate framing without wraparound."""

    return base.transform(
        base.size,
        Image.Transform.AFFINE,
        (1, 0, -dx, 0, 1, -dy),
        resample=Image.Resampling.BILINEAR,
        fillcolor=fill,
    )


def variant(base: Image.Image, serial: int) -> Image.Image:
    """Add sub-threshold sensor variation so equal scenes have distinct hashes."""

    image = base.copy()
    pixels = image.load()
    for offset in range(6):
        x = 4 + offset
        delta = ((serial * 3 + offset) % 9) - 4
        pixels[x, 3] = max(0, min(255, pixels[x, 3] + delta))
    return image


def frame(
    image: Image.Image,
    frame_id: str,
    at_mono_ms: int,
    *,
    wall_origin_mono_ms: int = 0,
) -> FrameEnvelope:
    at_wall = FIXED_WALL + timedelta(milliseconds=at_mono_ms - wall_origin_mono_ms)
    sha256 = hashlib.sha256(image.tobytes()).hexdigest()
    return FrameEnvelope(
        frame_id=frame_id,
        gray_array=image.copy(),
        at_wall=at_wall.isoformat(timespec="milliseconds"),
        at_mono_ms=at_mono_ms,
        sha256=sha256,
    )


def signal(
    kind: str,
    at_mono_ms: int,
    *,
    event_id: str | None = None,
    wall_origin_mono_ms: int = 0,
) -> SignalEnvelope:
    at_wall = FIXED_WALL + timedelta(milliseconds=at_mono_ms - wall_origin_mono_ms)
    meta = {"event_id": event_id} if event_id is not None else {}
    return SignalEnvelope(
        kind=kind,  # type: ignore[arg-type]
        at_wall=at_wall.isoformat(timespec="milliseconds"),
        at_mono_ms=at_mono_ms,
        meta=meta,
    )


def distinct_frames(
    images: Iterable[Image.Image], start_mono_ms: int, spacing_ms: int = 500
) -> list[FrameEnvelope]:
    return [
        frame(variant(image, index), f"f_{index:03d}", start_mono_ms + index * spacing_ms)
        for index, image in enumerate(images)
    ]

