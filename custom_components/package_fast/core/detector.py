# detector.py — Pure synchronous package-fast FSM and decision pipeline.
#
# Rationale: CONVERGED.md §Architecture freezes the states and §5c freezes the
# deterministic pipeline.  ERRATA.md ERR-06 makes monotonic receive time and
# distinct hashes normative; ERR-07 keeps this module deliberately unable to
# publish.  step() stages immutable envelopes and the shell must pass them to
# Journal.commit() before emitting anything.

"""Stateful but I/O-free detector with a synchronous ``step`` API."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from PIL import Image

from .config import DetectorConfig
from .envelopes import DetectionEnvelope, FrameEnvelope, SignalEnvelope
from .journal import Journal
from .pipeline import (
    Component,
    ConfirmedCandidate,
    GrayFrame,
    StationarityTracker,
    centroid_in_roi,
    decode_gray,
    estimate_camera_shift,
    full_resolution_bbox,
    global_stability_fraction,
    motion_map,
    segment_changes,
)


class DetectorState(str, Enum):
    DISABLED = "DISABLED"
    IDLE = "IDLE"
    ARMED = "ARMED"
    EPISODE_OPEN = "EPISODE_OPEN"
    DETECTED = "DETECTED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    REBASE = "REBASE"
    SUSPENDED = "SUSPENDED"


VETO_NO_PERSON_CONTEXT = 1 << 0
VETO_WEAK_MARGIN = 1 << 1
VETO_REMOVAL = 1 << 2
VETO_MOVED_OBJECT = 1 << 3
VETO_TEST = 1 << 4

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _parse_wall_ms(at_wall: str) -> int:
    try:
        parsed = datetime.fromisoformat(at_wall.replace("Z", "+00:00"))
        return max(0, int(parsed.timestamp() * 1000))
    except (TypeError, ValueError, OverflowError):
        return 0


def make_ulid(at_wall: str, entropy: str) -> str:
    """Create a deterministic, valid 26-character ULID for replay."""

    timestamp = _parse_wall_ms(at_wall) & ((1 << 48) - 1)
    random_bits = int.from_bytes(hashlib.sha256(entropy.encode("utf-8")).digest()[:10], "big")
    value = (timestamp << 80) | random_bits
    encoded = ["0"] * 26
    for index in range(25, -1, -1):
        encoded[index] = _CROCKFORD[value & 31]
        value >>= 5
    return "".join(encoded)


@dataclass(frozen=True, slots=True)
class _QuietFrame:
    image: Image.Image
    envelope: FrameEnvelope
    source_size: tuple[int, int]


@dataclass(frozen=True, slots=True)
class SuppressionMask:
    bbox_norm: tuple[float, float, float, float]
    expires_mono_ms: int


class Detector:
    """Pure package detector; journal composition is optional and explicit."""

    def __init__(
        self,
        config: DetectorConfig | None = None,
        *,
        journal: Journal | None = None,
        journal_dir: str | Path | None = None,
        id_factory: Callable[[str, str], str] | None = None,
    ) -> None:
        if journal is not None and journal_dir is not None:
            raise ValueError("pass journal or journal_dir, not both")
        self.config = config or DetectorConfig()
        self.journal = journal or (Journal(journal_dir) if journal_dir is not None else None)
        self._id_factory = id_factory or make_ulid

        self.state = DetectorState.IDLE
        self.episode_substate = "QUIET"
        self.state_history: list[DetectorState] = [self.state]
        self.suspension_reason: str | None = None

        self._ring: deque[_QuietFrame] = deque(maxlen=self.config.ring_frames)
        self._rolling_baseline: _QuietFrame | None = None
        self._episode_baseline: _QuietFrame | None = None
        self._disturbance_baseline: _QuietFrame | None = None
        self._tracker = StationarityTracker(self.config)

        self._last_received_mono_ms: int | None = None
        self._last_distinct_sha256: str | None = None
        self._last_distinct_image: Image.Image | None = None
        self._last_gray_frame: GrayFrame | None = None
        self._last_frame_envelope: FrameEnvelope | None = None

        self._episode_id: str | None = None
        self._episode_opened_mono_ms: int | None = None
        self._episode_opened_wall: str | None = None
        self._episode_test = False
        self._episode_detection_count = 0
        self._episode_deposit_count = 0
        self._detection_serial = 0
        self._fast_result_staged = False
        self._quiet_since_mono_ms: int | None = None
        self._g4_active = False
        self._g6_active = False
        self._person_on_mono_ms: deque[int] = deque()
        self._person_context_wall: dict[str, str | None] = {
            "g4_on_at": None,
            "g4_off_at": None,
            "g6_on_at": None,
            "g6_off_at": None,
        }
        self._g4_event_ids: list[str] = []

        self._armed_activity_mono_ms: int | None = None
        self._pending_disturbance_frames = 0

        self._rebase_pretrip_baseline: _QuietFrame | None = None
        self._rebase_prior_state = DetectorState.IDLE
        self._rebase_stable_since_mono_ms: int | None = None
        self._pending_rebase_events: list[tuple[str, FrameEnvelope, dict[str, Any]]] = []
        self._pending_rebase_frames = 0

        self._suppression_masks: list[SuppressionMask] = []
        self.suppression_masks_invalidated = 0
        self._exception_times_ms: deque[int] = deque()

        self.frames_received = 0
        self.distinct_frames = 0
        self.duplicate_frames = 0
        self.illum_guard_trips = 0
        self.camera_shift_events = 0

    @property
    def episode_id(self) -> str | None:
        return self._episode_id

    @property
    def active_suppression_masks(self) -> int:
        return len(self._suppression_masks)

    @property
    def baseline_sha256(self) -> str | None:
        baseline = self._episode_baseline or self._rolling_baseline
        return baseline.envelope.sha256 if baseline is not None else None

    def _transition(self, state: DetectorState) -> None:
        if self.state != state:
            self.state = state
            self.state_history.append(state)

    def add_suppression_mask(
        self,
        bbox_norm: Sequence[float],
        at_mono_ms: int,
        ttl_seconds: float | None = None,
    ) -> None:
        x, y, width, height = (float(value) for value in bbox_norm)
        if not (0 <= x < 1 and 0 <= y < 1 and width > 0 and height > 0):
            raise ValueError("suppression bbox must be normalized x/y/width/height")
        ttl = self.config.suppression_decay_seconds if ttl_seconds is None else ttl_seconds
        self._suppression_masks.append(
            SuppressionMask((x, y, width, height), at_mono_ms + int(ttl * 1000))
        )

    def suspend(self, reason: str, at_wall: str, at_mono_ms: int) -> list[DetectionEnvelope]:
        staged: list[DetectionEnvelope] = []
        if self._episode_id is not None:
            staged.append(self._close_episode(at_wall, at_mono_ms, "suspended"))
        self.suspension_reason = reason
        self._transition(DetectorState.SUSPENDED)
        return staged

    def step(
        self, frame: FrameEnvelope, signals: list[SignalEnvelope]
    ) -> list[DetectionEnvelope]:
        """Advance the detector once and return non-durable staged records."""

        staged: list[DetectionEnvelope] = []
        self.frames_received += 1

        if self._last_received_mono_ms is not None and frame.at_mono_ms < self._last_received_mono_ms:
            if self._episode_id is not None:
                staged.append(
                    self._close_episode(frame.at_wall, frame.at_mono_ms, "timing_quarantine")
                )
            self.suspension_reason = "timing_quarantine"
            self._transition(DetectorState.SUSPENDED)
            return staged

        if self.state == DetectorState.CLOSED and self._episode_id is None:
            self._transition(DetectorState.IDLE)

        for signal in sorted(signals, key=lambda item: item.at_mono_ms):
            staged.extend(self._apply_signal(signal))

        is_duplicate = frame.sha256 == self._last_distinct_sha256
        if is_duplicate:
            self.duplicate_frames += 1
            self._last_received_mono_ms = frame.at_mono_ms
            staged.extend(self._run_timeouts(frame.at_wall, frame.at_mono_ms))
            return staged

        if self.state in {DetectorState.DISABLED, DetectorState.SUSPENDED}:
            self._last_received_mono_ms = frame.at_mono_ms
            self._last_distinct_sha256 = frame.sha256
            return staged

        try:
            gray = decode_gray(frame, self.config)
        except Exception:
            self._last_received_mono_ms = frame.at_mono_ms
            staged.extend(self._record_exception(frame.at_wall, frame.at_mono_ms))
            return staged

        self.distinct_frames += 1
        current = gray.image

        if self._rolling_baseline is None:
            quiet = _QuietFrame(current.copy(), frame, gray.source_size)
            self._rolling_baseline = quiet
            self._ring.append(quiet)

        if self.state == DetectorState.REBASE:
            staged.extend(self._step_rebase(frame, gray))
            self._remember_distinct(frame, gray)
            staged.extend(self._run_timeouts(frame.at_wall, frame.at_mono_ms))
            return staged

        previous = self._last_distinct_image
        motion, motion_fraction = motion_map(current, previous, self.config)
        analysis_baseline = (
            self._episode_baseline
            if self._episode_id is not None
            else self._disturbance_baseline or self._rolling_baseline
        )
        if analysis_baseline is None:
            self._remember_distinct(frame, gray)
            staged.extend(self._run_timeouts(frame.at_wall, frame.at_mono_ms))
            return staged

        try:
            segmented = segment_changes(
                current, analysis_baseline.image, self.config, exclusion_mask=motion
            )
        except Exception:
            self._last_received_mono_ms = frame.at_mono_ms
            staged.extend(self._record_exception(frame.at_wall, frame.at_mono_ms))
            return staged

        if segmented.illumination_fraction > self.config.illumination_fraction:
            staged.extend(self._enter_rebase(frame, gray, analysis_baseline))
            self._remember_distinct(frame, gray)
            staged.extend(self._run_timeouts(frame.at_wall, frame.at_mono_ms))
            return staged

        components = tuple(
            component
            for component in segmented.components
            if centroid_in_roi(component, self.config, current.size)
        )
        disturbance = any(
            self.config.area_frac_min <= component.area_frac <= self.config.area_frac_max
            for component in components
        )
        confirmed = self._tracker.update(components, current, previous, motion, frame)

        if self._episode_id is None:
            if disturbance:
                if self._pending_disturbance_frames == 0:
                    self._disturbance_baseline = self._select_baseline(frame.at_mono_ms)
                    self._pending_disturbance_frames = 1
                    self._armed_activity_mono_ms = frame.at_mono_ms
                    self._transition(DetectorState.ARMED)
                else:
                    self._pending_disturbance_frames += 1
                if self._pending_disturbance_frames >= self.config.disturbance_confirm_frames:
                    staged.append(
                        self._open_episode(
                            frame.at_wall,
                            frame.at_mono_ms,
                            "roi_disturbance",
                            test=False,
                            baseline=self._disturbance_baseline,
                            preserve_tracker=True,
                        )
                    )
            else:
                self._pending_disturbance_frames = 0
                self._disturbance_baseline = None
                self._tracker.reset()
                if motion_fraction <= self.config.motion_fraction:
                    self._remember_quiet(frame, gray)

        if self._episode_id is not None:
            self.episode_substate = (
                "OCCLUDED" if motion_fraction > self.config.motion_fraction else "QUIET"
            )
            if self.episode_substate == "QUIET":
                staged.extend(self._decide_candidates(confirmed, frame, gray))
            if self.state == DetectorState.DETECTED and not any(
                envelope.kind == "deposit" for envelope in staged
            ):
                self._transition(DetectorState.EPISODE_OPEN)

        self._remember_distinct(frame, gray)
        staged.extend(self._run_timeouts(frame.at_wall, frame.at_mono_ms))
        return staged

    def _apply_signal(self, signal: SignalEnvelope) -> list[DetectionEnvelope]:
        staged: list[DetectionEnvelope] = []
        if signal.kind == "master_off":
            if self._episode_id is not None:
                staged.append(self._close_episode(signal.at_wall, signal.at_mono_ms, "suspended"))
            self._transition(DetectorState.DISABLED)
            self.suspension_reason = None
            return staged
        if signal.kind == "master_on":
            if self.state == DetectorState.DISABLED:
                self._reset_scene_state()
                self._transition(DetectorState.IDLE)
            return staged
        if self.state in {DetectorState.DISABLED, DetectorState.SUSPENDED}:
            return staged

        if signal.kind in {"g4_person_on", "g6_person_on"}:
            camera = signal.kind[:2]
            self._person_on_mono_ms.append(signal.at_mono_ms)
            self._prune_person_context(signal.at_mono_ms)
            self._person_context_wall[f"{camera}_on_at"] = signal.at_wall
            self._armed_activity_mono_ms = signal.at_mono_ms
            if self.state == DetectorState.IDLE:
                self._transition(DetectorState.ARMED)
            if camera == "g4":
                self._g4_active = True
                self._quiet_since_mono_ms = None
                event_id = signal.meta.get("event_id")
                if isinstance(event_id, str) and event_id and event_id not in self._g4_event_ids:
                    self._g4_event_ids.append(event_id)
                if self._episode_id is None:
                    staged.append(
                        self._open_episode(
                            signal.at_wall, signal.at_mono_ms, "g4_person", test=False
                        )
                    )
            else:
                self._g6_active = True
        elif signal.kind in {"g4_person_off", "g6_person_off"}:
            camera = signal.kind[:2]
            self._person_context_wall[f"{camera}_off_at"] = signal.at_wall
            if camera == "g4":
                self._g4_active = False
                if self._episode_id is not None:
                    self._quiet_since_mono_ms = signal.at_mono_ms
            else:
                self._g6_active = False
        elif signal.kind == "manual_test":
            self._armed_activity_mono_ms = signal.at_mono_ms
            if self._episode_id is None:
                staged.append(
                    self._open_episode(
                        signal.at_wall, signal.at_mono_ms, "manual_test", test=True
                    )
                )
        return staged

    def _select_baseline(self, at_mono_ms: int) -> _QuietFrame | None:
        cutoff = at_mono_ms - int(self.config.baseline_min_age_seconds * 1000)
        eligible = [quiet for quiet in self._ring if quiet.envelope.at_mono_ms <= cutoff]
        if eligible:
            return eligible[-1]
        return None

    def _new_episode_id(self, at_wall: str, at_mono_ms: int) -> str:
        entropy = f"{at_wall}|{at_mono_ms}|{self.frames_received}|{len(self.state_history)}"
        return self._id_factory(at_wall, entropy)

    @staticmethod
    def _frames_dir(at_wall: str, episode_id: str) -> str:
        try:
            date = datetime.fromisoformat(at_wall.replace("Z", "+00:00")).date()
            parts = (f"{date.year:04d}", f"{date.month:02d}", f"{date.day:02d}")
        except (TypeError, ValueError):
            parts = ("1970", "01", "01")
        return "/".join(("episodes", *parts, episode_id))

    def _open_episode(
        self,
        at_wall: str,
        at_mono_ms: int,
        opened_by: str,
        *,
        test: bool,
        baseline: _QuietFrame | None = None,
        preserve_tracker: bool = False,
    ) -> DetectionEnvelope:
        if self._episode_id is not None:
            raise RuntimeError("episode already open")
        self._episode_id = self._new_episode_id(at_wall, at_mono_ms)
        self._episode_opened_mono_ms = at_mono_ms
        self._episode_opened_wall = at_wall
        self._episode_test = test
        self._episode_detection_count = 0
        self._episode_deposit_count = 0
        self._detection_serial = 0
        self._fast_result_staged = False
        self._episode_baseline = baseline or self._select_baseline(at_mono_ms)
        self._quiet_since_mono_ms = None if self._g4_active else at_mono_ms
        if not preserve_tracker:
            self._tracker.reset()
        if self.state != DetectorState.REBASE:
            self._transition(DetectorState.EPISODE_OPEN)

        baseline_payload: dict[str, Any] | None = None
        if self._episode_baseline is not None:
            envelope = self._episode_baseline.envelope
            baseline_payload = {
                "frame_ids": [envelope.frame_id],
                "sha256": envelope.sha256,
                "captured_at_wall": envelope.at_wall,
                "age_ms_at_open": max(0, at_mono_ms - envelope.at_mono_ms),
            }
        payload = {
            "opened_by": opened_by,
            "test": test,
            "g4_event_ids": list(self._g4_event_ids),
            "person_context": dict(self._person_context_wall),
            "baseline": baseline_payload,
            "frames_dir": self._frames_dir(at_wall, self._episode_id),
            "poll_config": {
                "idle_rate_hz": self.config.idle_rate_hz,
                "armed_rate_hz": self.config.armed_rate_hz,
                "fetch_budget_seconds": self.config.fetch_budget_seconds,
                "fetch_retries": self.config.fetch_retries,
            },
            "detector": {
                "algorithm_version": self.config.algorithm_version,
                "config_digest": self.config.config_digest,
                "model_digest": None,
            },
        }
        return DetectionEnvelope(
            record_type="episode_opened",
            episode_id=self._episode_id,
            at_wall=at_wall,
            at_mono_ms=at_mono_ms,
            lifecycle_payload=payload,
        )

    def _close_episode(
        self, at_wall: str, at_mono_ms: int, close_reason: str
    ) -> DetectionEnvelope:
        if self._episode_id is None:
            raise RuntimeError("no episode to close")
        episode_id = self._episode_id
        self._transition(DetectorState.CLOSING)
        payload = {
            "close_reason": close_reason,
            "poll": {
                "rate_hz": self.config.armed_rate_hz,
                "frames_received": self.frames_received,
                "distinct_frames": self.distinct_frames,
                "duplicate_frames": self.duplicate_frames,
            },
            "metrics": {
                "detections": self._episode_detection_count,
                "deposits": self._episode_deposit_count,
                "illum_guard_trips": self.illum_guard_trips,
                "camera_shift_events": self.camera_shift_events,
            },
        }
        closed = DetectionEnvelope(
            record_type="episode_closed",
            episode_id=episode_id,
            at_wall=at_wall,
            at_mono_ms=at_mono_ms,
            lifecycle_payload=payload,
        )
        self._episode_id = None
        self._episode_opened_mono_ms = None
        self._episode_opened_wall = None
        self._episode_baseline = None
        self._disturbance_baseline = None
        self._pending_disturbance_frames = 0
        self._quiet_since_mono_ms = None
        self._tracker.reset()
        self._transition(DetectorState.CLOSED)
        return closed

    def _remember_quiet(self, frame: FrameEnvelope, gray: GrayFrame) -> None:
        quiet = _QuietFrame(gray.image.copy(), frame, gray.source_size)
        self._rolling_baseline = quiet
        self._ring.append(quiet)

    def _remember_distinct(self, frame: FrameEnvelope, gray: GrayFrame) -> None:
        self._last_received_mono_ms = frame.at_mono_ms
        self._last_distinct_sha256 = frame.sha256
        self._last_distinct_image = gray.image.copy()
        self._last_gray_frame = gray
        self._last_frame_envelope = frame

    def _reset_scene_state(self) -> None:
        self._ring.clear()
        self._rolling_baseline = None
        self._episode_baseline = None
        self._disturbance_baseline = None
        self._tracker.reset()
        self._last_distinct_image = None
        self._last_distinct_sha256 = None
        self._last_gray_frame = None
        self._pending_disturbance_frames = 0

    def _record_exception(self, at_wall: str, at_mono_ms: int) -> list[DetectionEnvelope]:
        cutoff = at_mono_ms - int(self.config.exception_window_seconds * 1000)
        self._exception_times_ms.append(at_mono_ms)
        while self._exception_times_ms and self._exception_times_ms[0] < cutoff:
            self._exception_times_ms.popleft()
        if len(self._exception_times_ms) >= self.config.exception_limit:
            return self.suspend("exception_budget", at_wall, at_mono_ms)
        return []

    def _run_timeouts(self, at_wall: str, at_mono_ms: int) -> list[DetectionEnvelope]:
        staged: list[DetectionEnvelope] = []
        if (
            self._episode_id is not None
            and self._quiet_since_mono_ms is not None
            and at_mono_ms - self._quiet_since_mono_ms
            >= int(self.config.episode_quiet_seconds * 1000)
        ):
            staged.append(self._close_episode(at_wall, at_mono_ms, "quiet_75s"))
            return staged
        if (
            self._episode_id is None
            and self.state == DetectorState.ARMED
            and self._armed_activity_mono_ms is not None
            and at_mono_ms - self._armed_activity_mono_ms
            >= int(self.config.armed_timeout_seconds * 1000)
        ):
            self._pending_disturbance_frames = 0
            self._disturbance_baseline = None
            self._tracker.reset()
            self._transition(DetectorState.IDLE)
        return staged

    def _prune_person_context(self, at_mono_ms: int) -> None:
        cutoff = at_mono_ms - int(self.config.person_context_seconds * 1000)
        while self._person_on_mono_ms and self._person_on_mono_ms[0] < cutoff:
            self._person_on_mono_ms.popleft()

    def _person_context_present(self, at_mono_ms: int) -> bool:
        self._prune_person_context(at_mono_ms)
        window = int(self.config.person_context_seconds * 1000)
        return any(abs(at_mono_ms - timestamp) <= window for timestamp in self._person_on_mono_ms)

    def _inside_suppression(self, component: Component, at_mono_ms: int) -> bool:
        self._suppression_masks = [
            mask for mask in self._suppression_masks if mask.expires_mono_ms > at_mono_ms
        ]
        x = component.centroid[0] / self.config.working_width
        y = component.centroid[1] / self.config.working_height
        for mask in self._suppression_masks:
            left, top, width, height = mask.bbox_norm
            if left <= x <= left + width and top <= y <= top + height:
                return True
        return False

    def _strong_margins(self, candidate: ConfirmedCandidate) -> bool:
        component = candidate.component
        margin = self.config.strong_area_margin_frac
        return (
            component.edge_gain >= self.config.strong_edge_gain_min
            and component.area_frac >= self.config.area_frac_min + margin
            and component.area_frac <= self.config.area_frac_max - margin
            and candidate.stability_sad <= self.config.strong_stability_sad_max
        )

    def _next_detection_id(self) -> str:
        self._detection_serial += 1
        return f"d_{self._detection_serial:02d}"

    def _candidate_envelope(
        self,
        candidate: ConfirmedCandidate,
        kind: str,
        frame: FrameEnvelope,
        gray: GrayFrame,
        *,
        veto_bits: int,
        announce_eligible: bool,
        extra_scores: Mapping[str, Any] | None = None,
    ) -> DetectionEnvelope:
        assert self._episode_id is not None
        scores = {
            "edge_gain": round(candidate.component.edge_gain, 6),
            "stability_sad": round(candidate.stability_sad, 6),
            "illum_guard": "clear",
        }
        if extra_scores:
            scores.update(extra_scores)
        return DetectionEnvelope(
            record_type="detection",
            episode_id=self._episode_id,
            at_wall=frame.at_wall,
            at_mono_ms=frame.at_mono_ms,
            detection_id=self._next_detection_id(),
            kind=kind,
            bbox_norm=candidate.component.bbox_norm,
            bbox_full=full_resolution_bbox(candidate.component, gray),
            area_frac=candidate.component.area_frac,
            polarity=candidate.component.polarity,
            first_seen_frame=candidate.track.first_seen_frame,
            confirmed_frame=candidate.confirmed_frame,
            confirm_count=candidate.confirm_count,
            frame_sha256s=dict(candidate.track.frame_sha256s),
            scores=scores,
            veto_bits=veto_bits,
            latency_ms_from_first_visible=max(
                0, candidate.confirmed_mono_ms - candidate.track.first_seen_mono_ms
            ),
            announce_eligible=announce_eligible,
            person_context_present=self._person_context_present(frame.at_mono_ms),
            announced=False,
            shadow=True,
        )

    def _decide_candidates(
        self,
        confirmed: Sequence[ConfirmedCandidate],
        frame: FrameEnvelope,
        gray: GrayFrame,
    ) -> list[DetectionEnvelope]:
        staged: list[DetectionEnvelope] = []
        added = [item for item in confirmed if item.component.polarity == "added"]
        removed = [item for item in confirmed if item.component.polarity == "removed"]
        paired_added: set[int] = set()
        paired_removed: set[int] = set()

        for add_index, add in enumerate(added):
            for remove_index, remove in enumerate(removed):
                if remove_index in paired_removed:
                    continue
                ratio = add.component.area / max(1, remove.component.area)
                if self.config.moved_area_ratio_min <= ratio <= self.config.moved_area_ratio_max:
                    paired_added.add(add_index)
                    paired_removed.add(remove_index)
                    add.track.emitted = True
                    remove.track.emitted = True
                    context = self._person_context_present(frame.at_mono_ms)
                    veto = VETO_MOVED_OBJECT | (0 if context else VETO_NO_PERSON_CONTEXT)
                    envelope = self._candidate_envelope(
                        add,
                        "moved_object",
                        frame,
                        gray,
                        veto_bits=veto,
                        announce_eligible=False,
                        extra_scores={
                            "paired_removed_bbox_norm": list(remove.component.bbox_norm),
                            "area_ratio": round(ratio, 6),
                        },
                    )
                    staged.append(envelope)
                    self._episode_detection_count += 1
                    break

        remaining: list[ConfirmedCandidate] = [
            item for index, item in enumerate(added) if index not in paired_added
        ] + [item for index, item in enumerate(removed) if index not in paired_removed]

        for candidate in remaining:
            component = candidate.component
            if not (
                self.config.area_frac_min <= component.area_frac <= self.config.area_frac_max
            ):
                candidate.track.emitted = True
                continue
            if self._inside_suppression(component, frame.at_mono_ms):
                candidate.track.emitted = True
                continue
            candidate.track.emitted = True
            context = self._person_context_present(frame.at_mono_ms)
            if component.polarity == "removed":
                veto = VETO_REMOVAL | (0 if context else VETO_NO_PERSON_CONTEXT)
                staged.append(
                    self._candidate_envelope(
                        candidate,
                        "removal",
                        frame,
                        gray,
                        veto_bits=veto,
                        announce_eligible=False,
                    )
                )
                self._episode_detection_count += 1
                continue

            strong = self._strong_margins(candidate)
            announce = context and strong and not self._episode_test
            veto = 0
            if not context:
                veto |= VETO_NO_PERSON_CONTEXT
            if not strong:
                veto |= VETO_WEAK_MARGIN
            if self._episode_test:
                veto |= VETO_TEST
            staged.append(
                self._candidate_envelope(
                    candidate,
                    "deposit",
                    frame,
                    gray,
                    veto_bits=veto,
                    announce_eligible=announce,
                )
            )
            self._episode_detection_count += 1
            self._episode_deposit_count += 1
            self._transition(DetectorState.DETECTED)
            if not self._fast_result_staged:
                assert self._episode_id is not None
                staged.append(
                    DetectionEnvelope(
                        record_type="fast_result",
                        episode_id=self._episode_id,
                        at_wall=frame.at_wall,
                        at_mono_ms=frame.at_mono_ms,
                        lifecycle_payload={
                            "label": "delivery",
                            "decided_at_wall": frame.at_wall,
                            "decided_at_mono_ms": frame.at_mono_ms,
                        },
                    )
                )
                self._fast_result_staged = True
        return staged

    def _operational_envelope(
        self,
        kind: str,
        frame: FrameEnvelope,
        scores: Mapping[str, Any],
    ) -> DetectionEnvelope:
        assert self._episode_id is not None
        self._episode_detection_count += 1
        return DetectionEnvelope(
            record_type="detection",
            episode_id=self._episode_id,
            at_wall=frame.at_wall,
            at_mono_ms=frame.at_mono_ms,
            detection_id=self._next_detection_id(),
            kind=kind,
            bbox_norm=None,
            bbox_full=None,
            area_frac=0.0,
            polarity="n/a",
            first_seen_frame=frame.frame_id,
            confirmed_frame=frame.frame_id,
            confirm_count=1,
            frame_sha256s={frame.frame_id: frame.sha256},
            scores=dict(scores),
            veto_bits=VETO_WEAK_MARGIN,
            latency_ms_from_first_visible=0,
            announce_eligible=False,
            person_context_present=self._person_context_present(frame.at_mono_ms),
            announced=False,
            shadow=True,
        )

    def _enter_rebase(
        self, frame: FrameEnvelope, gray: GrayFrame, baseline: _QuietFrame
    ) -> list[DetectionEnvelope]:
        self.illum_guard_trips += 1
        self._rebase_prior_state = self.state
        self._rebase_pretrip_baseline = baseline
        self._rebase_stable_since_mono_ms = None
        self._pending_rebase_frames = 1
        self._tracker.reset()
        self._transition(DetectorState.REBASE)

        dx, dy, correlation = estimate_camera_shift(
            baseline.image, gray.image, self.config.camera_shift_search_px
        )
        rebase_scores = {
            "illum_guard": "tripped",
            "camera_shift_dx": dx,
            "camera_shift_dy": dy,
            "edge_profile_correlation": round(correlation, 6),
        }
        events: list[tuple[str, FrameEnvelope, dict[str, Any]]] = [
            ("rebase", frame, rebase_scores)
        ]
        if max(abs(dx), abs(dy)) > self.config.camera_shift_threshold_px:
            invalidated = len(self._suppression_masks)
            self._suppression_masks.clear()
            self.suppression_masks_invalidated += invalidated
            self.camera_shift_events += 1
            events.append(
                (
                    "camera_shift",
                    frame,
                    {
                        **rebase_scores,
                        "suppression_masks_invalidated": invalidated,
                    },
                )
            )

        if self._episode_id is None:
            self._pending_rebase_events = events
            return []
        return [self._operational_envelope(kind, event_frame, scores) for kind, event_frame, scores in events]

    def _step_rebase(
        self, frame: FrameEnvelope, gray: GrayFrame
    ) -> list[DetectionEnvelope]:
        staged: list[DetectionEnvelope] = []
        self._pending_rebase_frames += 1
        if (
            self._episode_id is None
            and self._pending_rebase_events
            and self._pending_rebase_frames >= self.config.disturbance_confirm_frames
        ):
            staged.append(
                self._open_episode(
                    frame.at_wall,
                    frame.at_mono_ms,
                    "roi_disturbance",
                    test=False,
                    baseline=self._rebase_pretrip_baseline,
                    preserve_tracker=True,
                )
            )
            staged.extend(
                self._operational_envelope(kind, event_frame, scores)
                for kind, event_frame, scores in self._pending_rebase_events
            )
            self._pending_rebase_events = []

        previous = self._last_distinct_image
        if previous is not None:
            changed = global_stability_fraction(gray.image, previous, self.config)
            if changed <= self.config.rebase_stable_fraction:
                if self._rebase_stable_since_mono_ms is None:
                    self._rebase_stable_since_mono_ms = frame.at_mono_ms
                elif (
                    frame.at_mono_ms - self._rebase_stable_since_mono_ms
                    >= int(self.config.rebase_stability_seconds * 1000)
                ):
                    quiet = _QuietFrame(gray.image.copy(), frame, gray.source_size)
                    self._rolling_baseline = quiet
                    self._ring.clear()
                    self._ring.append(quiet)
                    if self._episode_id is not None:
                        self._episode_baseline = quiet
                        self._transition(DetectorState.EPISODE_OPEN)
                    else:
                        self._transition(DetectorState.IDLE)
                    self._rebase_pretrip_baseline = None
                    self._rebase_stable_since_mono_ms = None
                    self._pending_rebase_events = []
                    self._pending_rebase_frames = 0
            else:
                self._rebase_stable_since_mono_ms = None
        return staged


__all__ = [
    "Detector",
    "DetectorState",
    "SuppressionMask",
    "make_ulid",
]
