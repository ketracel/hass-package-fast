"""Pure policy helpers for the package-fast Home Assistant shell.

This module intentionally imports only the Python standard library.  Home
Assistant-coupled orchestration lives in ``runtime.py``; these failure-policy
decisions remain unit-testable in the same offline environment as the core.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
import json
import math
from typing import Any, Iterable, Mapping, MutableMapping, Sequence


BBox = tuple[float, float, float, float]


def percentile(values: Iterable[float], quantile: float) -> float:
    """Return a deterministic nearest-rank percentile, or zero when empty."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if not 0.0 <= quantile <= 1.0:
        raise ValueError("quantile must be in [0, 1]")
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def bounded_cache_put(
    cache: MutableMapping[str, Any],
    order: deque[str],
    key: str,
    value: Any,
    *,
    limit: int,
) -> None:
    """Insert one bounded-cache value without duplicating its eviction key."""

    if limit <= 0:
        raise ValueError("cache limit must be positive")
    if key not in cache:
        order.append(key)
    cache[key] = value
    while len(order) > limit:
        cache.pop(order.popleft(), None)


def heartbeat_can_advance(detector_state: Any) -> bool:
    """Return false while a suspended detector must expose a stale heartbeat."""

    value = getattr(detector_state, "value", detector_state)
    return str(value).upper() != "SUSPENDED"


@dataclass(frozen=True, slots=True)
class ParsedSolDecision:
    """Normalized result parsed from the existing last-decision helper."""

    label: str
    confidence: float
    test: bool


def parse_sol_decision(value: str, lane: str) -> ParsedSolDecision | None:
    """Parse an early/final Sol helper value after counter-based lane inference."""

    parts = [part.strip() for part in str(value).split("|")]
    if lane == "early":
        if len(parts) < 3 or parts[0] not in {
            "FRONT EARLY",
            "FRONT EARLY TEST",
        }:
            return None
        label_index = 1
        test = parts[0].endswith(" TEST")
    elif lane == "final":
        if not parts or parts[0].startswith(("GARAGE", "FRONT EARLY")):
            return None
        test = parts[0] == "TEST"
        label_index = 1 if test else 0
        if len(parts) <= label_index + 1:
            return None
    else:
        raise ValueError(f"unsupported Sol lane: {lane}")

    label = parts[label_index].lower()
    confidence_text = parts[label_index + 1]
    if not label or not confidence_text.endswith("%"):
        return None
    try:
        confidence = float(confidence_text[:-1]) / 100.0
    except ValueError:
        return None
    if not 0.0 <= confidence <= 1.0:
        return None
    return ParsedSolDecision(label=label, confidence=confidence, test=test)


def _iso_epoch_ms(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return int(
            datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            * 1_000
        )
    except (TypeError, ValueError, OverflowError):
        return None


def match_sol_episode(
    episodes: Mapping[str, Any],
    decided_at_wall: str,
    *,
    maximum_age_ms: int,
) -> str | None:
    """Match a Sol decision to an open interval or the newest recent episode."""

    if maximum_age_ms <= 0:
        raise ValueError("maximum Sol join age must be positive")
    decided_ms = _iso_epoch_ms(decided_at_wall)
    if decided_ms is None:
        return None
    exact: list[tuple[int, str]] = []
    recent: list[tuple[int, str]] = []
    for episode_id, episode in episodes.items():
        if not isinstance(episode_id, str) or not isinstance(episode, Mapping):
            continue
        opened_ms = _iso_epoch_ms(episode.get("opened_at_wall"))
        if opened_ms is None or opened_ms > decided_ms:
            continue
        closed_value = episode.get("closed_at_wall")
        closed_ms = _iso_epoch_ms(closed_value)
        if closed_value is None or (
            closed_ms is not None and decided_ms <= closed_ms
        ):
            exact.append((opened_ms, episode_id))
        if decided_ms - opened_ms <= maximum_age_ms:
            recent.append((opened_ms, episode_id))
    candidates = exact or recent
    return max(candidates)[1] if candidates else None


@dataclass(frozen=True, slots=True)
class SLOLimits:
    """The measured Phase-0 envelope used by the runtime circuit breaker."""

    fetch_p95_limit_ms: float = 900.0
    poll_gap_limit_ms: float = 1_500.0
    # Phase-0 measured ~1.0 distinct FPS on 2026-08-27/28; threshold = measured - margin.
    minimum_distinct_fps: float = 0.8
    maximum_error_rate: float = 0.005
    maximum_gap_rate: float = 0.005
    window_ms: int = 120_000
    minimum_samples: int = 20
    minimum_armed_span_ms: int = 10_000
    error_budget_interval_ms: int = 500

    def __post_init__(self) -> None:
        if min(
            self.fetch_p95_limit_ms,
            self.poll_gap_limit_ms,
            self.minimum_distinct_fps,
            self.window_ms,
            self.minimum_samples,
            self.minimum_armed_span_ms,
            self.error_budget_interval_ms,
        ) <= 0:
            raise ValueError("positive SLO limits are required")
        if not 0.0 <= self.maximum_error_rate <= 1.0:
            raise ValueError("maximum_error_rate must be in [0, 1]")
        if not 0.0 <= self.maximum_gap_rate <= 1.0:
            raise ValueError("maximum_gap_rate must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class _SLOSample:
    at_mono_ms: int
    fetch_ms: float
    gap_ms: float
    success: bool
    content_hash: str | None
    armed: bool
    duplicate: bool


@dataclass(frozen=True, slots=True)
class SLOSnapshot:
    sample_count: int
    fetch_p95_ms: float
    poll_gap_count: int
    duplicate_count: int
    distinct_fps: float | None
    error_rate: float
    gap_rate: float
    last_duplicate: bool
    qualified: bool
    violations: tuple[str, ...]

    @property
    def healthy(self) -> bool:
        """Whether a complete window is inside every enforced bound."""

        return self.qualified and not self.violations


class SlidingSLOMonitor:
    """Measure fetch, spacing, errors, and consecutive-content freshness."""

    def __init__(self, limits: SLOLimits | None = None) -> None:
        self.limits = limits or SLOLimits()
        self._samples: deque[_SLOSample] = deque()
        self._last_success_hash: str | None = None

    def reset(self) -> None:
        """Start a new qualification window after entering suspension."""

        self._samples.clear()
        self._last_success_hash = None

    def record(
        self,
        *,
        at_mono_ms: int,
        fetch_ms: float,
        gap_ms: float,
        success: bool,
        content_hash: str | None,
        armed: bool,
    ) -> SLOSnapshot:
        if fetch_ms < 0 or gap_ms < 0:
            raise ValueError("fetch and gap times cannot be negative")
        if success and not content_hash:
            raise ValueError("successful observations require a content hash")
        if not success and content_hash is not None:
            raise ValueError("failed observations cannot carry a content hash")

        duplicate = bool(
            success
            and self._last_success_hash is not None
            and content_hash == self._last_success_hash
        )
        if success:
            self._last_success_hash = content_hash
        self._samples.append(
            _SLOSample(
                at_mono_ms=at_mono_ms,
                fetch_ms=float(fetch_ms),
                gap_ms=float(gap_ms),
                success=success,
                content_hash=content_hash,
                armed=armed,
                duplicate=duplicate,
            )
        )
        cutoff = at_mono_ms - self.limits.window_ms
        while self._samples and self._samples[0].at_mono_ms < cutoff:
            self._samples.popleft()
        return self.snapshot()

    def snapshot(self) -> SLOSnapshot:
        samples = tuple(self._samples)
        count = len(samples)
        fetch_p95 = percentile((sample.fetch_ms for sample in samples), 0.95)
        armed_count = sum(sample.armed for sample in samples)
        gap_count = sum(
            sample.armed and sample.gap_ms > self.limits.poll_gap_limit_ms
            for sample in samples
        )
        duplicate_count = sum(sample.duplicate for sample in samples)
        errors = sum(not sample.success for sample in samples)
        # Error budget is normalized to the fixed 2 Hz Phase-0 observation
        # window, not the number of samples that happened to arrive.  That
        # keeps one transient inside a 120 s idle window below the 0.5% gate.
        error_budget_samples = (
            self.limits.window_ms / self.limits.error_budget_interval_ms
        )
        error_rate = errors / error_budget_samples
        # The 1.5 s gap boundary qualifies the 2 Hz armed path.  A designed
        # 0.5 Hz idle sample is two seconds apart and is not itself a gap.
        gap_rate = gap_count / armed_count if armed_count else 0.0

        armed_elapsed_ms = 0
        distinct_transitions = 0
        for previous, current in zip(samples, samples[1:]):
            if not (previous.armed and current.armed):
                continue
            elapsed = max(0, current.at_mono_ms - previous.at_mono_ms)
            armed_elapsed_ms += elapsed
            if (
                previous.success
                and current.success
                and previous.content_hash != current.content_hash
            ):
                distinct_transitions += 1
        distinct_fps = (
            distinct_transitions / (armed_elapsed_ms / 1_000.0)
            if armed_elapsed_ms >= self.limits.minimum_armed_span_ms
            else None
        )

        qualified = count >= self.limits.minimum_samples
        violations: list[str] = []
        if qualified and fetch_p95 > self.limits.fetch_p95_limit_ms:
            violations.append("fetch_p95")
        if qualified and error_rate >= self.limits.maximum_error_rate:
            violations.append("fetch_error_rate")
        if qualified and gap_rate > self.limits.maximum_gap_rate:
            violations.append("poll_gap_rate")
        if (
            qualified
            and distinct_fps is not None
            and distinct_fps < self.limits.minimum_distinct_fps
        ):
            violations.append("distinct_fps")

        return SLOSnapshot(
            sample_count=count,
            fetch_p95_ms=fetch_p95,
            poll_gap_count=gap_count,
            duplicate_count=duplicate_count,
            distinct_fps=distinct_fps,
            error_rate=error_rate,
            gap_rate=gap_rate,
            last_duplicate=bool(samples and samples[-1].duplicate),
            qualified=qualified,
            violations=tuple(violations),
        )


@dataclass(frozen=True, slots=True)
class FeedObservation:
    streak: int
    duplicate: bool
    suspect: bool


class FeedSuspectMonitor:
    """Detect a fully static feed while a recent person edge says it changed.

    This is deliberately a *consecutive* identical-hash streak.  The frozen
    core also grants distinctness relative to the immediately preceding
    distinct hash, so an A,B,A stale alternation is not caught by this check;
    failure mode 2 is the fully frozen/hash-static case.  That accepted WO-3a
    nuance is documented here rather than silently broadening the core rule.
    """

    def __init__(self, *, identical_frames: int, person_window_ms: int) -> None:
        if identical_frames < 2 or person_window_ms <= 0:
            raise ValueError("feed thresholds must be positive")
        self.identical_frames = identical_frames
        self.person_window_ms = person_window_ms
        self._last_hash: str | None = None
        self._streak = 0
        self._person_edges: deque[int] = deque()
        self._person_hash: str | None = None
        self._person_streak = 0

    def note_person_edge(self, at_mono_ms: int) -> None:
        self._person_edges.append(at_mono_ms)
        self._person_hash = None
        self._person_streak = 0
        self._prune(at_mono_ms)

    def _prune(self, at_mono_ms: int) -> None:
        cutoff = at_mono_ms - self.person_window_ms
        while self._person_edges and self._person_edges[0] < cutoff:
            self._person_edges.popleft()
        if not self._person_edges:
            self._person_hash = None
            self._person_streak = 0

    def observe(self, content_hash: str, at_mono_ms: int) -> FeedObservation:
        if not content_hash:
            raise ValueError("content_hash is required")
        duplicate = content_hash == self._last_hash
        if duplicate:
            self._streak += 1
        else:
            self._last_hash = content_hash
            self._streak = 1
        self._prune(at_mono_ms)
        if self._person_edges:
            if content_hash == self._person_hash:
                self._person_streak += 1
            else:
                self._person_hash = content_hash
                self._person_streak = 1
        return FeedObservation(
            streak=self._person_streak if self._person_edges else self._streak,
            duplicate=duplicate,
            suspect=(
                bool(self._person_edges)
                and self._person_streak >= self.identical_frames
            ),
        )


@dataclass(frozen=True, slots=True)
class RetentionEntry:
    path: str
    modified_ms: int
    size_bytes: int
    keep: bool = False

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("retention size cannot be negative")


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    delete_paths: tuple[str, ...]
    reclaimed_bytes: int
    bytes_after_write: int
    allow_write: bool


def plan_retention(
    entries: Sequence[RetentionEntry],
    *,
    now_ms: int,
    max_bytes: int,
    max_age_ms: int,
    incoming_bytes: int = 0,
) -> RetentionPlan:
    """Plan age/cap pruning, oldest first, without ever deleting ``keep``."""

    if max_bytes < 0 or max_age_ms < 0 or incoming_bytes < 0:
        raise ValueError("retention limits cannot be negative")
    if len({entry.path for entry in entries}) != len(entries):
        raise ValueError("retention paths must be unique")

    ordered = sorted(entries, key=lambda entry: (entry.modified_ms, entry.path))
    deleted: list[RetentionEntry] = []
    deleted_paths: set[str] = set()
    age_cutoff = now_ms - max_age_ms
    for entry in ordered:
        if not entry.keep and entry.modified_ms < age_cutoff:
            deleted.append(entry)
            deleted_paths.add(entry.path)

    current_bytes = sum(entry.size_bytes for entry in entries)
    reclaimed = sum(entry.size_bytes for entry in deleted)
    for entry in ordered:
        if current_bytes - reclaimed + incoming_bytes <= max_bytes:
            break
        if entry.keep or entry.path in deleted_paths:
            continue
        deleted.append(entry)
        deleted_paths.add(entry.path)
        reclaimed += entry.size_bytes

    bytes_after = current_bytes - reclaimed + incoming_bytes
    return RetentionPlan(
        delete_paths=tuple(entry.path for entry in deleted),
        reclaimed_bytes=reclaimed,
        bytes_after_write=bytes_after,
        allow_write=bytes_after <= max_bytes,
    )


def bbox_iou(first: BBox, second: BBox) -> float:
    """Intersection-over-union for normalized ``x, y, width, height`` boxes."""

    ax, ay, aw, ah = first
    bx, by, bw, bh = second
    left = max(ax, bx)
    top = max(ay, by)
    right = min(ax + aw, bx + bw)
    bottom = min(ay + ah, by + bh)
    intersection = max(0.0, right - left) * max(0.0, bottom - top)
    union = aw * ah + bw * bh - intersection
    return intersection / union if union > 0 else 0.0


def _valid_bbox(bbox: Sequence[float]) -> BBox:
    if len(bbox) != 4:
        raise ValueError("bbox must contain x, y, width, height")
    x, y, width, height = (float(value) for value in bbox)
    if not (
        0.0 <= x < 1.0
        and 0.0 <= y < 1.0
        and width > 0.0
        and height > 0.0
        and x + width <= 1.0
        and y + height <= 1.0
    ):
        raise ValueError("bbox must be normalized and remain inside the frame")
    return (x, y, width, height)


@dataclass(slots=True)
class _MaskHistory:
    bbox: BBox
    hits_ms: list[int]


@dataclass(frozen=True, slots=True)
class MaskCreation:
    bbox: BBox
    expires_ms: int


class SuppressionMaskPolicy:
    """Create masks only from repeated, non-announceable shadow flicker."""

    def __init__(
        self,
        *,
        hits_required: int,
        window_ms: int,
        ttl_ms: int,
        iou_threshold: float,
    ) -> None:
        if hits_required < 2 or min(window_ms, ttl_ms) <= 0:
            raise ValueError("mask counts and windows must be positive")
        if not 0.0 < iou_threshold <= 1.0:
            raise ValueError("iou_threshold must be in (0, 1]")
        self.hits_required = hits_required
        self.window_ms = window_ms
        self.ttl_ms = ttl_ms
        self.iou_threshold = iou_threshold
        self._histories: list[_MaskHistory] = []
        self._protected: list[MaskCreation] = []
        self._active: list[MaskCreation] = []

    def _prune(self, at_ms: int) -> None:
        cutoff = at_ms - self.window_ms
        retained: list[_MaskHistory] = []
        for history in self._histories:
            history.hits_ms[:] = [hit for hit in history.hits_ms if hit >= cutoff]
            if history.hits_ms:
                retained.append(history)
        self._histories = retained
        self._protected = [item for item in self._protected if item.expires_ms > at_ms]
        self._active = [item for item in self._active if item.expires_ms > at_ms]

    def observe(
        self,
        bbox: Sequence[float],
        *,
        at_ms: int,
        announce_eligible: bool,
        person_context_present: bool = False,
    ) -> MaskCreation | None:
        """Observe one durable detection and perhaps return a new mask."""

        normalized = _valid_bbox(bbox)
        self._prune(at_ms)

        mask_seed_eligible = (
            not announce_eligible and not person_context_present
        )
        if not mask_seed_eligible:
            # Any person-context region is never allowed to seed a mask.  It
            # also protects the region for a full policy window so two prior
            # weak hits plus a later weak hit cannot indirectly mask it.
            self._histories = [
                item
                for item in self._histories
                if bbox_iou(item.bbox, normalized) < self.iou_threshold
            ]
            self._protected.append(
                MaskCreation(normalized, at_ms + self.window_ms)
            )
            return None

        if any(
            bbox_iou(item.bbox, normalized) >= self.iou_threshold
            for item in self._protected
        ):
            return None

        history = next(
            (
                item
                for item in self._histories
                if bbox_iou(item.bbox, normalized) >= self.iou_threshold
            ),
            None,
        )
        if history is None:
            history = _MaskHistory(normalized, [])
            self._histories.append(history)
        else:
            count = len(history.hits_ms)
            history.bbox = tuple(
                round((old * count + new) / (count + 1), 9)
                for old, new in zip(history.bbox, normalized)
            )  # type: ignore[assignment]
        history.hits_ms.append(at_ms)

        if len(history.hits_ms) < self.hits_required:
            return None
        creation = MaskCreation(history.bbox, at_ms + self.ttl_ms)
        self._histories.remove(history)
        self._active.append(creation)
        return creation

    def active_masks(self, at_ms: int) -> tuple[MaskCreation, ...]:
        self._prune(at_ms)
        return tuple(self._active)

    def clear(self) -> None:
        """Invalidate histories and active masks after a camera reframe."""

        self._histories.clear()
        self._protected.clear()
        self._active.clear()

    def to_state(self) -> dict[str, Any]:
        """Return JSON-compatible state so the 24-hour policy survives restart."""

        return {
            "version": 1,
            "histories": [
                {"bbox": list(item.bbox), "hits_ms": list(item.hits_ms)}
                for item in self._histories
            ],
            "protected": [
                {"bbox": list(item.bbox), "expires_ms": item.expires_ms}
                for item in self._protected
            ],
            "active": [
                {"bbox": list(item.bbox), "expires_ms": item.expires_ms}
                for item in self._active
            ],
        }

    def restore_state(self, state: Mapping[str, Any], *, now_ms: int) -> None:
        """Restore only valid, unexpired policy state from disk."""

        histories: list[_MaskHistory] = []
        protected: list[MaskCreation] = []
        active: list[MaskCreation] = []
        try:
            for value in state.get("histories", []):
                bbox = _valid_bbox(value["bbox"])
                hits = [int(hit) for hit in value.get("hits_ms", [])]
                histories.append(_MaskHistory(bbox, hits))
            for key, target in (("protected", protected), ("active", active)):
                for value in state.get(key, []):
                    target.append(
                        MaskCreation(
                            _valid_bbox(value["bbox"]), int(value["expires_ms"])
                        )
                    )
        except (KeyError, TypeError, ValueError):
            return
        self._histories = histories
        self._protected = protected
        self._active = active
        self._prune(now_ms)


def relevant_system_log(data: Mapping[str, Any]) -> bool:
    """Select package-fast and event-loop blocking warnings for ERR-11."""

    text = json.dumps(data, default=str, ensure_ascii=True).lower()
    return "package_fast" in text or any(
        marker in text
        for marker in ("blocking call", "blocking-call", "detected blocking")
    )


__all__ = [
    "FeedObservation",
    "FeedSuspectMonitor",
    "MaskCreation",
    "ParsedSolDecision",
    "RetentionEntry",
    "RetentionPlan",
    "SLOLimits",
    "SLOSnapshot",
    "SlidingSLOMonitor",
    "SuppressionMaskPolicy",
    "bbox_iou",
    "bounded_cache_put",
    "heartbeat_can_advance",
    "match_sol_episode",
    "parse_sol_decision",
    "percentile",
    "plan_retention",
    "relevant_system_log",
]
