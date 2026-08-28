"""Home Assistant-coupled polling, journaling, and publication shell."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import partial
import hashlib
import logging
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping, Sequence

from homeassistant.components import camera
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_START, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    CONF_ARMED_RATE_HZ,
    CONF_CAMERA_ENTITY,
    CONF_IDLE_RATE_HZ,
    CONF_MASK_HITS,
    CONF_MASK_IOU,
    CONF_MASK_TTL_HOURS,
    CONF_MASK_WINDOW_HOURS,
    CONF_MAX_AGE_DAYS,
    CONF_MAX_STORAGE_MB,
    CONF_PERSIST_FRAMES,
    DEFAULT_CAMERA_ENTITY,
    DEFAULT_MASK_HITS,
    DEFAULT_MASK_IOU,
    DEFAULT_MASK_TTL_HOURS,
    DEFAULT_MASK_WINDOW_HOURS,
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_MAX_STORAGE_MB,
    DEFAULT_PERSIST_FRAMES,
    EVENT_CONFIRMED,
    EVENT_SHADOW,
    EVENT_SYSTEM_LOG,
    FEED_PERSON_WINDOW_SECONDS,
    FEED_STATIC_FRAMES,
    FETCH_P95_LIMIT_MS,
    FRAME_MOMENTARY_SECONDS,
    G4_PERSON_ENTITY,
    G6_PERSON_ENTITY,
    MASTER_ENTITY,
    MAX_ERROR_RATE,
    MAX_GAP_RATE,
    MEDIA_ROOT,
    METRICS_FLUSH_SECONDS,
    MIN_DISTINCT_FPS,
    POLL_GAP_LIMIT_MS,
    PROMOTED_ENTITY,
    QUEUE_JOIN_TIMEOUT_SECONDS,
    RETENTION_SWEEP_SECONDS,
    SENSOR_UPDATE_SECONDS,
    SLO_ERROR_BUDGET_INTERVAL_MS,
    SLO_MIN_ARMED_SPAN_SECONDS,
    SLO_MIN_SAMPLES,
    SLO_RESUME_CLEAN_SECONDS,
    SLO_WINDOW_SECONDS,
    SOL_DECISION_ENTITY,
    SOL_JOIN_MAX_SECONDS,
    SOL_LANE_COUNTERS,
    STARTUP_STABILIZATION_SECONDS,
)
from .core import (
    DetectionEnvelope,
    Detector,
    DetectorConfig,
    DetectorState,
    FrameEnvelope,
    Journal,
    SignalEnvelope,
)
from .core.journal import REDUCER_VERSION, load_records, reduce_records
from .paging import (
    HEALTH_DEFAULT_LIMIT,
    HEALTH_MAX_LIMIT,
    JOURNAL_DEFAULT_LIMIT,
    SUSPENSION_HISTORY_LIMIT,
    clamp_limit,
    read_journal_page,
    redact_source,
)
from .shell_logic import (
    FeedSuspectMonitor,
    SLOLimits,
    SLOSnapshot,
    SlidingSLOMonitor,
    SuppressionMaskPolicy,
    bounded_cache_put,
    heartbeat_can_advance,
    match_sol_episode,
    parse_sol_decision,
    percentile,
    relevant_system_log,
)
from .storage import (
    FramePersistenceError,
    ShellStateStore,
    SparseFrame,
    SparseFrameStore,
)


_LOGGER = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def _wall_epoch_ms(at_wall: str) -> int:
    try:
        return int(
            datetime.fromisoformat(at_wall.replace("Z", "+00:00")).timestamp()
            * 1_000
        )
    except (TypeError, ValueError, OverflowError):
        return int(time.time() * 1_000)


def _local_date(at_wall: str) -> str:
    try:
        parsed = datetime.fromisoformat(at_wall.replace("Z", "+00:00"))
        return parsed.astimezone().date().isoformat()
    except (TypeError, ValueError):
        return datetime.now().astimezone().date().isoformat()


@dataclass(frozen=True, slots=True)
class _RawFrame:
    frame_id: str
    at_wall: str
    at_mono_ms: int
    jpeg_bytes: bytes


@dataclass(frozen=True, slots=True)
class _WorkItem:
    mode: str
    at_wall: str
    at_mono_ms: int
    fetch_ms: float
    gap_ms: float
    armed: bool
    raw: _RawFrame | None = None
    signals: tuple[SignalEnvelope, ...] = ()
    effective_on: bool = True
    g4_on: bool = False
    g6_on: bool = False
    stabilizing: bool = False


@dataclass(frozen=True, slots=True)
class _Outcome:
    durable: tuple[DetectionEnvelope, ...]
    snapshot: Mapping[str, Any]
    suppress_confirmed: bool = False


@dataclass(frozen=True, slots=True)
class _PendingSolCall:
    lane: str
    at_mono_ms: int


class _DetectorBackend:
    """Serialize detector mutation and writes while keeping file reads lock-free."""

    def __init__(
        self,
        config: DetectorConfig,
        *,
        root: str | Path,
        persist_frames: bool,
        max_bytes: int,
        max_age_days: float,
        mask_hits: int,
        mask_window_hours: float,
        mask_ttl_hours: float,
        mask_iou: float,
    ) -> None:
        self.config = config
        self.root = Path(root)
        self._lock = threading.RLock()
        self.state_store = ShellStateStore(self.root)
        self.frame_store = SparseFrameStore(
            self.root, max_bytes=max_bytes, max_age_days=max_age_days
        )
        pre_recovery = reduce_records(load_records(self.root / "episodes.jsonl"))
        open_before_recovery = {
            episode_id
            for episode_id, episode in pre_recovery.get("episodes", {}).items()
            if isinstance(episode, Mapping)
            and episode.get("opened_at_wall") is not None
            and episode.get("closed_at_wall") is None
        }
        self._frame_persistence_error: FramePersistenceError | None = None
        # Journal construction performs ERR-07 recovery before Detector or the
        # poller can consume a frame.
        self.journal = Journal(
            self.root,
            frame_persistence_enabled=persist_frames,
            frame_persist_callback=self._persist_frame_callback,
        )
        recovered: list[DetectionEnvelope] = []
        for record in self.journal.records():
            payload = record.get("payload", {})
            if (
                record.get("episode_id") not in open_before_recovery
                or record.get("record") != "episode_closed"
                or not isinstance(payload, Mapping)
                or payload.get("close_reason") != "interrupted_restart"
                or not isinstance(record.get("at_wall"), str)
                or not isinstance(record.get("at_mono_ms"), int)
            ):
                continue
            recovered.append(
                DetectionEnvelope(
                    record_type="episode_closed",
                    episode_id=str(record["episode_id"]),
                    at_wall=str(record["at_wall"]),
                    at_mono_ms=int(record["at_mono_ms"]),
                    lifecycle_payload=dict(payload),
                )
            )
        self._startup_recovery = tuple(recovered)
        self.frame_store.bind_accounting(self.journal.account_external_bytes)
        reduced = self.journal.reduce()
        self.frame_store.restore_episode_dirs(reduced)
        self.detector = Detector(config, journal=self.journal)

        self.slo = SlidingSLOMonitor(
            SLOLimits(
                fetch_p95_limit_ms=FETCH_P95_LIMIT_MS,
                poll_gap_limit_ms=POLL_GAP_LIMIT_MS,
                minimum_distinct_fps=MIN_DISTINCT_FPS,
                maximum_error_rate=MAX_ERROR_RATE,
                maximum_gap_rate=MAX_GAP_RATE,
                window_ms=int(SLO_WINDOW_SECONDS * 1_000),
                minimum_samples=SLO_MIN_SAMPLES,
                minimum_armed_span_ms=int(SLO_MIN_ARMED_SPAN_SECONDS * 1_000),
                error_budget_interval_ms=SLO_ERROR_BUDGET_INTERVAL_MS,
            )
        )
        self.feed = FeedSuspectMonitor(
            identical_frames=FEED_STATIC_FRAMES,
            person_window_ms=int(FEED_PERSON_WINDOW_SECONDS * 1_000),
        )
        self.mask_policy = SuppressionMaskPolicy(
            hits_required=mask_hits,
            window_ms=int(mask_window_hours * 3_600_000),
            ttl_ms=int(mask_ttl_hours * 3_600_000),
            iou_threshold=mask_iou,
        )
        now_wall_ms = int(time.time() * 1_000)
        self.mask_policy.restore_state(
            self.state_store.load_masks(), now_ms=now_wall_ms
        )
        self._restore_core_masks(now_wall_ms, _mono_ms())

        self._frame_cache: dict[str, SparseFrame] = {}
        self._frame_order: deque[str] = deque()
        self._frame_cache_limit = max(48, config.ring_frames + 8)
        self._fetch_samples_ms: deque[float] = deque(maxlen=2_048)
        self._cpu_samples_ms: deque[float] = deque(maxlen=2_048)
        self._shell_exception_times_ms: deque[int] = deque()
        self._shell_suspension: str | None = None
        self._clean_since_ms: int | None = None
        self._feed_changed_after_suspend = True
        self._suspended_since_mono_ms: int | None = None
        self._last_metrics_flush_ms = _mono_ms()
        self._last_retention_sweep_ms = _mono_ms()
        self._poll_gap_serial = 0
        self._episode_poll_gap_baselines: dict[str, int] = {}
        self._last_slo = self.slo.snapshot()
        self._heartbeat_at = _utc_now()
        self._heartbeat_status = "starting"
        self._last_sensor_update_ms: int | None = None
        self._reported_fetch_p95_ms = 0.0
        self._reported_freshness_fps: float | None = None
        self._reported_cpu_p95_ms = 0.0
        self._date = datetime.now().astimezone().date().isoformat()
        self._metrics = self._new_metrics(self._date)
        self._metrics.update(self.state_store.load_daily(self._date))
        self._reconcile_journal_metrics()

    @staticmethod
    def _new_metrics(date: str) -> dict[str, Any]:
        return {
            "date": date,
            "frames_polled": 0,
            "fetch_p50_ms": 0.0,
            "fetch_p95_ms": 0.0,
            "fetch_errors": 0,
            "poll_gaps": 0,
            "poll_gaps_over_1500ms": 0,
            "duplicate_frames": 0,
            "freshness_fps_est": None,
            "episodes": 0,
            "detections": 0,
            "deposits_deposit_level": 0,
            "announce_eligible": 0,
            "would_announce": 0,
            "illum_guard_trips": 0,
            "camera_shift_events": 0,
            "suppression_mask_regions": 0,
            "shadow_write_skips": 0,
            "store_bytes": 0,
            "suspensions": 0,
            "suspended_minutes": 0.0,
            "interrupted_restarts": 0,
            "restarts": 0,
            "system_log_warnings": 0,
            "journal_anomalies": 0,
            "cpu_ms_per_frame_p95": 0.0,
            "reducer_version": REDUCER_VERSION,
        }

    def _persist_frame_callback(
        self, episode_id: str, role: str, frame: Any
    ) -> int | None:
        """Preserve Journal's skip contract while surfacing fatal disk faults."""

        try:
            return self.frame_store.persist(episode_id, role, frame)
        except FramePersistenceError as error:
            # Journal intentionally converts callback exceptions to safe skips.
            # Keep real filesystem failures out of that catch-all path, then
            # re-raise them after Journal.persist_frame returns.
            self._frame_persistence_error = error
            return None

    def _record_date(self, record: Mapping[str, Any]) -> str | None:
        at_wall = record.get("at_wall")
        return _local_date(at_wall) if isinstance(at_wall, str) else None

    def _reconcile_journal_metrics(self) -> None:
        records = self.journal.records()
        detections: set[tuple[str, str]] = set()
        episodes: set[str] = set()
        deposits = eligible = interruptions = 0
        for record in records:
            if self._record_date(record) != self._date:
                continue
            episode_id = record.get("episode_id")
            payload = record.get("payload", {})
            if record.get("record") == "episode_opened" and isinstance(episode_id, str):
                episodes.add(episode_id)
            elif record.get("record") == "detection" and isinstance(payload, Mapping):
                detection_id = payload.get("detection_id")
                if isinstance(episode_id, str) and isinstance(detection_id, str):
                    key = (episode_id, detection_id)
                    if key not in detections:
                        detections.add(key)
                        deposits += payload.get("kind") == "deposit"
                        eligible += bool(payload.get("announce_eligible"))
            elif (
                record.get("record") == "episode_closed"
                and isinstance(payload, Mapping)
                and payload.get("close_reason") == "interrupted_restart"
            ):
                interruptions += 1
        self._metrics["episodes"] = max(
            int(self._metrics.get("episodes", 0)), len(episodes)
        )
        self._metrics["detections"] = max(
            int(self._metrics.get("detections", 0)), len(detections)
        )
        self._metrics["deposits_deposit_level"] = max(
            int(self._metrics.get("deposits_deposit_level", 0)), deposits
        )
        self._metrics["announce_eligible"] = max(
            int(self._metrics.get("announce_eligible", 0)), eligible
        )
        self._metrics["would_announce"] = self._metrics["announce_eligible"]
        self._metrics["interrupted_restarts"] = max(
            int(self._metrics.get("interrupted_restarts", 0)), interruptions
        )

    def _restore_core_masks(self, now_wall_ms: int, now_mono_ms: int) -> None:
        for mask in self.mask_policy.active_masks(now_wall_ms):
            remaining_seconds = (mask.expires_ms - now_wall_ms) / 1_000.0
            if remaining_seconds > 0:
                self.detector.add_suppression_mask(
                    mask.bbox, now_mono_ms, ttl_seconds=remaining_seconds
                )

    def _recreate_detector(self, item: _WorkItem) -> list[SignalEnvelope]:
        self._finish_suspension(item.at_mono_ms)
        self.detector = Detector(self.config, journal=self.journal)
        self._restore_core_masks(_wall_epoch_ms(item.at_wall), item.at_mono_ms)
        bootstrap: list[SignalEnvelope] = [
            SignalEnvelope("master_on", item.at_wall, item.at_mono_ms)
        ]
        if item.g4_on:
            bootstrap.append(
                SignalEnvelope(
                    "g4_person_on", item.at_wall, item.at_mono_ms, {"recovered": True}
                )
            )
        if item.g6_on:
            bootstrap.append(
                SignalEnvelope(
                    "g6_person_on", item.at_wall, item.at_mono_ms, {"recovered": True}
                )
            )
        self._shell_suspension = None
        self._clean_since_ms = None
        self._feed_changed_after_suspend = True
        self.slo.reset()
        self._append_health(
            "auto_resume", item.at_wall, item.at_mono_ms, {"clean_window_s": SLO_RESUME_CLEAN_SECONDS}
        )
        return bootstrap

    def _finish_suspension(self, at_mono_ms: int) -> None:
        if self._suspended_since_mono_ms is None:
            return
        elapsed_minutes = max(
            0.0, (at_mono_ms - self._suspended_since_mono_ms) / 60_000.0
        )
        self._metrics["suspended_minutes"] = round(
            float(self._metrics.get("suspended_minutes", 0.0))
            + elapsed_minutes,
            6,
        )
        self._suspended_since_mono_ms = None

    def _cache_frame(self, frame: FrameEnvelope) -> SparseFrame:
        assert frame.jpeg_bytes is not None
        sparse = SparseFrame(
            frame_id=frame.frame_id,
            at_wall=frame.at_wall,
            at_mono_ms=frame.at_mono_ms,
            sha256=frame.sha256,
            jpeg_bytes=frame.jpeg_bytes,
        )
        bounded_cache_put(
            self._frame_cache,
            self._frame_order,
            frame.frame_id,
            sparse,
            limit=self._frame_cache_limit,
        )
        return sparse

    def _frame(self, frame_id: str | None, bbox: Sequence[int] | None = None) -> SparseFrame | None:
        if frame_id is None:
            return None
        value = self._frame_cache.get(frame_id)
        if value is None:
            return None
        if bbox is None:
            return value
        return replace(value, bbox_full=tuple(int(part) for part in bbox))

    def _append_health(
        self,
        kind: str,
        at_wall: str,
        at_mono_ms: int,
        payload: Mapping[str, Any],
    ) -> None:
        try:
            delta = self.state_store.append_health(
                {
                    "schema_version": 1,
                    "record": kind,
                    "at_wall": at_wall,
                    "at_mono_ms": at_mono_ms,
                    "payload": dict(payload),
                }
            )
        except OSError:
            return
        self.journal.account_external_bytes(delta)

    def _save_masks(self) -> None:
        try:
            delta = self.state_store.write_masks(self.mask_policy.to_state())
        except OSError:
            return
        self.journal.account_external_bytes(delta)

    def _roll_date(self, at_wall: str) -> None:
        date = _local_date(at_wall)
        if date == self._date:
            return
        self._flush_metrics(force=True)
        self._date = date
        self._metrics = self._new_metrics(date)
        self._fetch_samples_ms.clear()
        self._cpu_samples_ms.clear()

    def _flush_metrics(self, *, force: bool = False) -> None:
        now_mono = _mono_ms()
        if (
            not force
            and now_mono - self._last_metrics_flush_ms
            < int(METRICS_FLUSH_SECONDS * 1_000)
        ):
            return
        self._metrics.update(
            {
                "fetch_p50_ms": round(percentile(self._fetch_samples_ms, 0.50), 3),
                "fetch_p95_ms": round(percentile(self._fetch_samples_ms, 0.95), 3),
                "freshness_fps_est": (
                    round(self._last_slo.distinct_fps, 6)
                    if self._last_slo.distinct_fps is not None
                    else None
                ),
                "cpu_ms_per_frame_p95": round(
                    percentile(self._cpu_samples_ms, 0.95), 3
                ),
                "shadow_write_skips": self.journal.shadow_write_skips,
                "store_bytes": self.journal.store_bytes,
                "suppression_mask_regions": len(
                    self.mask_policy.active_masks(int(time.time() * 1_000))
                ),
                "journal_anomalies": self.journal.metrics_snapshot()[
                    "journal_anomalies"
                ],
                "reducer_version": REDUCER_VERSION,
            }
        )
        try:
            delta = self.state_store.write_daily(self._date, self._metrics)
        except OSError:
            return
        self.journal.account_external_bytes(delta)
        self._last_metrics_flush_ms = now_mono

    def _suspend(
        self,
        reason: str,
        at_wall: str,
        at_mono_ms: int,
        *,
        auto_resume: bool,
        details: Mapping[str, Any],
    ) -> list[DetectionEnvelope]:
        if self.detector.state == DetectorState.SUSPENDED:
            return []
        staged = self.detector.suspend(reason, at_wall, at_mono_ms)
        self._metrics["suspensions"] = int(self._metrics.get("suspensions", 0)) + 1
        self._suspended_since_mono_ms = at_mono_ms
        self._shell_suspension = reason if auto_resume else None
        self._clean_since_ms = None
        if reason == "feed_suspect":
            self._feed_changed_after_suspend = False
        self._append_health("suspension", at_wall, at_mono_ms, {"reason": reason, **details})
        if auto_resume:
            self.slo.reset()
        return staged

    def _prepare_journal_records(
        self, staged: Sequence[DetectionEnvelope]
    ) -> tuple[list[DetectionEnvelope], dict[str, int]]:
        """Add canonical per-episode poll-gap evidence before journal commit."""

        prepared: list[DetectionEnvelope] = []
        baselines = dict(self._episode_poll_gap_baselines)
        for envelope in staged:
            if envelope.record_type == "episode_opened":
                baselines.setdefault(envelope.episode_id, self._poll_gap_serial)
            elif envelope.record_type == "episode_closed":
                payload = dict(envelope.lifecycle_payload)
                poll = dict(payload.get("poll", {}))
                baseline = baselines.pop(
                    envelope.episode_id, self._poll_gap_serial
                )
                poll.update(
                    {
                        "gaps_over_1500ms": max(
                            0, self._poll_gap_serial - baseline
                        ),
                        "gap_limit_ms": POLL_GAP_LIMIT_MS,
                    }
                )
                payload["poll"] = poll
                envelope = replace(envelope, lifecycle_payload=payload)
            prepared.append(envelope)
        return prepared, baselines

    def _commit(self, staged: Sequence[DetectionEnvelope], item: _WorkItem) -> list[DetectionEnvelope]:
        if not staged:
            return []
        prepared, baselines = self._prepare_journal_records(staged)
        durable = self.journal.commit(prepared)
        if durable:
            self._episode_poll_gap_baselines = baselines
            return durable
        # The failed records are intentionally never published.  Closing the
        # in-memory episode through the public suspend path makes the lane
        # fail closed; journal recovery will close the still-durable open line.
        self._suspend(
            "journal_write_failure",
            item.at_wall,
            item.at_mono_ms,
            auto_resume=False,
            details={"records_lost": len(staged)},
        )
        return []

    def _persist_one(
        self, episode_id: str, role: str, frame: SparseFrame | None
    ) -> None:
        if frame is None:
            self.journal.note_shadow_write_skip()
            return
        self._frame_persistence_error = None
        self.journal.persist_frame(episode_id, role, frame)
        persistence_error = self._frame_persistence_error
        self._frame_persistence_error = None
        if persistence_error is not None:
            raise persistence_error

    def _persist_durable(
        self, durable: Sequence[DetectionEnvelope], current: SparseFrame
    ) -> None:
        if not self.journal.frame_persistence_enabled:
            for envelope in durable:
                if envelope.record_type == "episode_opened":
                    frames_dir = envelope.lifecycle_payload.get("frames_dir")
                    if isinstance(frames_dir, str):
                        self.frame_store.register_episode(envelope.episode_id, frames_dir)
            return

        for envelope in durable:
            if envelope.record_type == "episode_opened":
                frames_dir = envelope.lifecycle_payload.get("frames_dir")
                if isinstance(frames_dir, str):
                    self.frame_store.register_episode(envelope.episode_id, frames_dir)
                baseline = envelope.lifecycle_payload.get("baseline")
                baseline_id = None
                if isinstance(baseline, Mapping):
                    ids = baseline.get("frame_ids")
                    if isinstance(ids, list) and ids:
                        baseline_id = ids[0]
                if baseline_id is not None:
                    self._persist_one(
                        envelope.episode_id, "baseline", self._frame(baseline_id)
                    )
            elif envelope.record_type == "detection" and envelope.bbox_full is not None:
                first = self._frame(envelope.first_seen_frame)
                confirmed = self._frame(envelope.confirmed_frame)
                decision = self._frame(envelope.confirmed_frame, envelope.bbox_full)
                self._persist_one(envelope.episode_id, "first_seen", first)
                self._persist_one(envelope.episode_id, "confirm", confirmed)
                self._persist_one(envelope.episode_id, "decision", decision)
                if envelope.kind == "deposit":
                    self._persist_one(
                        envelope.episode_id, "per_trip", confirmed
                    )
            elif envelope.record_type == "episode_closed":
                self._persist_one(envelope.episode_id, "final", current)

    def _apply_mask_policy(self, durable: Sequence[DetectionEnvelope]) -> None:
        changed = False
        for envelope in durable:
            if envelope.record_type != "detection":
                continue
            if envelope.kind == "camera_shift":
                self.mask_policy.clear()
                changed = True
                continue
            if (
                envelope.kind not in {"deposit", "removal", "moved_object"}
                or envelope.bbox_norm is None
            ):
                continue
            creation = self.mask_policy.observe(
                envelope.bbox_norm,
                at_ms=_wall_epoch_ms(envelope.at_wall),
                announce_eligible=(
                    envelope.announce_eligible
                    or envelope.person_context_present
                ),
                person_context_present=envelope.person_context_present,
            )
            changed = True
            if creation is not None:
                self.detector.add_suppression_mask(
                    creation.bbox,
                    envelope.at_mono_ms,
                    ttl_seconds=(creation.expires_ms - _wall_epoch_ms(envelope.at_wall))
                    / 1_000.0,
                )
                self._append_health(
                    "suppression_mask_created",
                    envelope.at_wall,
                    envelope.at_mono_ms,
                    {
                        "bbox": list(creation.bbox),
                        "expires_ms": creation.expires_ms,
                        "source_kind": envelope.kind,
                    },
                )
        if changed:
            self._save_masks()

    def _account_durable(self, durable: Sequence[DetectionEnvelope]) -> None:
        for envelope in durable:
            if envelope.record_type == "episode_opened":
                self._metrics["episodes"] = int(self._metrics.get("episodes", 0)) + 1
            elif envelope.record_type == "detection":
                self._metrics["detections"] = int(self._metrics.get("detections", 0)) + 1
                if envelope.kind == "deposit":
                    self._metrics["deposits_deposit_level"] = int(
                        self._metrics.get("deposits_deposit_level", 0)
                    ) + 1
                if envelope.announce_eligible:
                    self._metrics["announce_eligible"] = int(
                        self._metrics.get("announce_eligible", 0)
                    ) + 1
                    self._metrics["would_announce"] = int(
                        self._metrics.get("would_announce", 0)
                    ) + 1
                if envelope.kind == "rebase":
                    self._metrics["illum_guard_trips"] = int(
                        self._metrics.get("illum_guard_trips", 0)
                    ) + 1
                if envelope.kind == "camera_shift":
                    self._metrics["camera_shift_events"] = int(
                        self._metrics.get("camera_shift_events", 0)
                    ) + 1

    def _public_snapshot(
        self, at_wall: str, status: str, *, touch_heartbeat: bool = True
    ) -> dict[str, Any]:
        now_mono_ms = _mono_ms()
        refresh_sensors = bool(
            touch_heartbeat
            and (
                self._last_sensor_update_ms is None
                or now_mono_ms - self._last_sensor_update_ms
                >= int(SENSOR_UPDATE_SECONDS * 1_000)
            )
        )
        if touch_heartbeat:
            self._heartbeat_status = status
            if refresh_sensors and heartbeat_can_advance(self.detector.state):
                self._heartbeat_at = at_wall
        if refresh_sensors:
            self._reported_fetch_p95_ms = round(
                self._last_slo.fetch_p95_ms, 3
            )
            self._reported_freshness_fps = self._last_slo.distinct_fps
            self._reported_cpu_p95_ms = round(
                percentile(self._cpu_samples_ms, 0.95), 3
            )
            self._last_sensor_update_ms = now_mono_ms
        return {
            "heartbeat": self._heartbeat_at,
            "heartbeat_status": self._heartbeat_status,
            "state": self.detector.state.value,
            "suspension_reason": self.detector.suspension_reason,
            "fetch_p95_ms": self._reported_fetch_p95_ms,
            "freshness_fps": self._reported_freshness_fps,
            "slo_qualified": self._last_slo.qualified,
            "slo_violations": list(self._last_slo.violations),
            "cpu_ms_per_frame_p95": self._reported_cpu_p95_ms,
            "daily_poll_gaps": int(self._metrics.get("poll_gaps", 0)),
            "daily_duplicates": int(self._metrics.get("duplicate_frames", 0)),
            "daily_detections": int(self._metrics.get("detections", 0)),
            "daily_suspensions": int(self._metrics.get("suspensions", 0)),
            "daily_interrupted_restarts": int(
                self._metrics.get("interrupted_restarts", 0)
            ),
            "daily_restarts": int(self._metrics.get("restarts", 0)),
            "daily_system_log_warnings": int(
                self._metrics.get("system_log_warnings", 0)
            ),
            "daily_shadow_write_skips": self.journal.shadow_write_skips,
            "active_suppression_masks": self.detector.active_suppression_masks,
            "journal_write_failures": self.journal.journal_write_failures,
            "store_bytes": self.journal.store_bytes,
        }

    def initial_snapshot(self) -> Mapping[str, Any]:
        with self._lock:
            return self._public_snapshot(_utc_now(), "starting")

    def read_journal(
        self,
        *,
        since_seq: int | None = None,
        episode_id: str | None = None,
        limit: int = JOURNAL_DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        """Read one append-safe journal page without blocking frame processing."""

        try:
            return read_journal_page(
                self.journal.path,
                since_seq=since_seq,
                episode_id=episode_id,
                limit=limit,
            )
        except (OSError, TypeError, ValueError) as error:
            raise HomeAssistantError(
                f"Unable to read package-fast journal: {error}"
            ) from error

    def read_health(
        self, *, limit: int = HEALTH_DEFAULT_LIMIT
    ) -> dict[str, Any]:
        """Read current diagnostics without pruning or recomputing state."""

        try:
            note_limit = clamp_limit(
                limit, default=HEALTH_DEFAULT_LIMIT, maximum=HEALTH_MAX_LIMIT
            )
            # The append-safe disk snapshot must never contend with process().
            (
                notes,
                suspensions,
                skipped,
                suspensions_complete,
            ) = self.state_store.read_health(
                note_limit=note_limit, suspension_limit=SUSPENSION_HISTORY_LIMIT
            )
        except (OSError, TypeError, ValueError) as error:
            raise HomeAssistantError(
                f"Unable to read package-fast health: {error}"
            ) from error

        now_ms = int(time.time() * 1_000)
        # Only copy mutable in-memory state while holding the detector lock.
        with self._lock:
            mask_state = self.mask_policy.to_state()
            mask_ttl_ms = self.mask_policy.ttl_ms
            mask_hits_required = self.mask_policy.hits_required
            detector_state = self.detector.state.value
            suspension_reason = self.detector.suspension_reason
            slo = self._last_slo
            latest_slo = {
                "sample_count": slo.sample_count,
                "fetch_p50_ms": round(slo.fetch_p50_ms, 3),
                "fetch_p95_ms": round(slo.fetch_p95_ms, 3),
                "distinct_fps": slo.distinct_fps,
                "error_rate": slo.error_rate,
                "poll_gaps": slo.poll_gap_count,
                "poll_gap_rate": slo.gap_rate,
                "qualified": slo.qualified,
                "healthy": slo.healthy,
                "violations": list(slo.violations),
            }

        masks: list[dict[str, Any]] = []
        for value in mask_state.get("active", []):
            try:
                bbox = [float(part) for part in value["bbox"]]
                expires_ms = int(value["expires_ms"])
            except (KeyError, TypeError, ValueError):
                continue
            if len(bbox) != 4 or expires_ms <= now_ms:
                continue
            created_ms = expires_ms - mask_ttl_ms
            masks.append(
                {
                    "bbox_norm": bbox,
                    "created_at": datetime.fromtimestamp(
                        created_ms / 1_000.0, tz=timezone.utc
                    ).isoformat(timespec="milliseconds"),
                    "hit_count": mask_hits_required,
                    "ttl_remaining_seconds": round(
                        (expires_ms - now_ms) / 1_000.0, 3
                    ),
                }
            )
        return {
            "state": detector_state,
            "suspension_reason": suspension_reason,
            "recent_suspensions": suspensions,
            "recent_suspensions_complete": suspensions_complete,
            "active_suppression_masks": masks,
            "latest_slo": latest_slo,
            "health_notes": notes,
            "health_notes_skipped": skipped,
        }

    def process(self, item: _WorkItem) -> _Outcome:
        with self._lock:
            self._roll_date(item.at_wall)
            if item.mode == "failure":
                return self._process_failure(item)
            if item.raw is None:
                return _Outcome((), self._public_snapshot(item.at_wall, "degraded"))
            return self._process_frame(item, control=item.mode == "control")

    def _process_failure(self, item: _WorkItem) -> _Outcome:
        self._fetch_samples_ms.append(item.fetch_ms)
        self._metrics["fetch_errors"] = int(self._metrics.get("fetch_errors", 0)) + 1
        self._last_slo = self.slo.record(
            at_mono_ms=item.at_mono_ms,
            fetch_ms=item.fetch_ms,
            gap_ms=item.gap_ms,
            success=False,
            content_hash=None,
            armed=item.armed,
        )
        if item.armed and item.gap_ms > self.config.poll_gap_seconds * 1_000:
            self._poll_gap_serial += 1
            self._metrics["poll_gaps"] = int(self._metrics.get("poll_gaps", 0)) + 1
            self._metrics["poll_gaps_over_1500ms"] = self._metrics["poll_gaps"]
            self._append_health(
                "poll_gap",
                item.at_wall,
                item.at_mono_ms,
                {"gap_ms": round(item.gap_ms, 3), "fetch_failed": True},
            )
        staged: list[DetectionEnvelope] = []
        if self._last_slo.violations and self.detector.state not in {
            DetectorState.DISABLED,
            DetectorState.SUSPENDED,
        }:
            staged.extend(
                self._suspend(
                    f"slo:{','.join(self._last_slo.violations)}",
                    item.at_wall,
                    item.at_mono_ms,
                    auto_resume=True,
                    details={"violations": list(self._last_slo.violations)},
                )
            )
        durable = self._commit(staged, item)
        self._flush_metrics()
        return _Outcome(
            tuple(durable), self._public_snapshot(item.at_wall, "degraded")
        )

    def _process_frame(self, item: _WorkItem, *, control: bool) -> _Outcome:
        assert item.raw is not None
        cpu_started = time.thread_time_ns()
        digest = hashlib.sha256(item.raw.jpeg_bytes).hexdigest()
        frame = FrameEnvelope(
            frame_id=item.raw.frame_id,
            at_wall=item.raw.at_wall,
            at_mono_ms=item.raw.at_mono_ms,
            sha256=digest,
            jpeg_bytes=item.raw.jpeg_bytes,
        )
        current = self._cache_frame(frame)
        staged: list[DetectionEnvelope] = []
        suppress_confirmed = item.stabilizing

        if not control:
            self._fetch_samples_ms.append(item.fetch_ms)
            self._metrics["frames_polled"] = int(self._metrics.get("frames_polled", 0)) + 1
            for signal in item.signals:
                if signal.kind in {"g4_person_on", "g6_person_on"}:
                    self.feed.note_person_edge(signal.at_mono_ms)
            feed = self.feed.observe(digest, item.at_mono_ms)
            if self._shell_suspension == "feed_suspect" and not feed.duplicate:
                self._feed_changed_after_suspend = True
            if feed.duplicate:
                self._metrics["duplicate_frames"] = int(
                    self._metrics.get("duplicate_frames", 0)
                ) + 1
            self._last_slo = self.slo.record(
                at_mono_ms=item.at_mono_ms,
                fetch_ms=item.fetch_ms,
                gap_ms=item.gap_ms,
                success=True,
                content_hash=digest,
                armed=item.armed,
            )
            if item.armed and item.gap_ms > self.config.poll_gap_seconds * 1_000:
                self._poll_gap_serial += 1
                self._metrics["poll_gaps"] = int(
                    self._metrics.get("poll_gaps", 0)
                ) + 1
                self._metrics["poll_gaps_over_1500ms"] = self._metrics["poll_gaps"]
                self._append_health(
                    "poll_gap",
                    item.at_wall,
                    item.at_mono_ms,
                    {"gap_ms": round(item.gap_ms, 3), "fetch_failed": False},
                )

            master_off = any(signal.kind == "master_off" for signal in item.signals)
            reason: str | None = None
            details: dict[str, Any] = {}
            if feed.suspect:
                reason = "feed_suspect"
                details = {"identical_hash_streak": feed.streak}
            elif self._last_slo.violations:
                reason = f"slo:{','.join(self._last_slo.violations)}"
                details = {"violations": list(self._last_slo.violations)}
            if (
                reason is not None
                and not master_off
                and self.detector.state
                not in {DetectorState.DISABLED, DetectorState.SUSPENDED}
            ):
                staged.extend(
                    self._suspend(
                        reason,
                        item.at_wall,
                        item.at_mono_ms,
                        auto_resume=True,
                        details=details,
                    )
                )

            signals = list(item.signals)
            if (
                self._shell_suspension
                and self.detector.state == DetectorState.SUSPENDED
                and not master_off
            ):
                current_slo = self.slo.snapshot()
                feed_recovered = (
                    self._shell_suspension != "feed_suspect"
                    or self._feed_changed_after_suspend
                )
                healthy = current_slo.healthy and not feed.suspect and feed_recovered
                if healthy:
                    if self._clean_since_ms is None:
                        self._clean_since_ms = item.at_mono_ms
                    elif (
                        item.at_mono_ms - self._clean_since_ms
                        >= int(SLO_RESUME_CLEAN_SECONDS * 1_000)
                    ):
                        signals = self._recreate_detector(item) + signals
                else:
                    self._clean_since_ms = None
        else:
            signals = list(item.signals)

        before_state = self.detector.state
        suspension_count_before_step = int(
            self._metrics.get("suspensions", 0)
        )
        if any(signal.kind == "master_off" for signal in signals):
            self._finish_suspension(item.at_mono_ms)
            self._shell_suspension = None
            self._clean_since_ms = None
            self._feed_changed_after_suspend = True
        try:
            staged.extend(self.detector.step(frame, signals))
        except Exception as error:  # defensive shell boundary around frozen core
            _LOGGER.exception("package_fast core step failed")
            staged.extend(
                self._suspend(
                    "shell_exception",
                    item.at_wall,
                    item.at_mono_ms,
                    auto_resume=False,
                    details={"error": type(error).__name__},
                )
            )
        if (
            before_state != DetectorState.SUSPENDED
            and self.detector.state == DetectorState.SUSPENDED
            and int(self._metrics.get("suspensions", 0))
            == suspension_count_before_step
        ):
            self._metrics["suspensions"] = int(self._metrics.get("suspensions", 0)) + 1
            self._suspended_since_mono_ms = item.at_mono_ms
            self._append_health(
                "core_suspension",
                item.at_wall,
                item.at_mono_ms,
                {"reason": self.detector.suspension_reason},
            )

        durable = self._commit(staged, item)
        if durable:
            persistence_error: FramePersistenceError | None = None
            try:
                self._persist_durable(durable, current)
            except FramePersistenceError as error:
                persistence_error = error
            self._apply_mask_policy(durable)
            self._account_durable(durable)
            if persistence_error is not None:
                suppress_confirmed = True
                close = self._suspend(
                    "frame_persistence_failure",
                    item.at_wall,
                    item.at_mono_ms,
                    auto_resume=False,
                    details={"error": type(persistence_error).__name__},
                )
                durable.extend(self._commit(close, item))

        if (
            self.journal.frame_persistence_enabled
            and item.at_mono_ms - self._last_retention_sweep_ms
            >= int(RETENTION_SWEEP_SECONDS * 1_000)
        ):
            try:
                self.frame_store.prune(
                    now_ms=_wall_epoch_ms(item.at_wall),
                    protect=self.frame_store.active_episode_path(
                        self.detector.episode_id
                    ),
                )
            except FramePersistenceError as error:
                suppress_confirmed = True
                close = self._suspend(
                    "frame_persistence_failure",
                    item.at_wall,
                    item.at_mono_ms,
                    auto_resume=False,
                    details={"error": type(error).__name__, "during": "retention"},
                )
                durable.extend(self._commit(close, item))
            self._last_retention_sweep_ms = item.at_mono_ms

        # Include the periodic metrics/reducer work in the co-residency CPU
        # sample; the persisted p95 may lag this newest sample by one flush.
        self._flush_metrics()
        cpu_ms = (time.thread_time_ns() - cpu_started) / 1_000_000.0
        if not control:
            self._cpu_samples_ms.append(cpu_ms)
        status = (
            "degraded"
            if self.detector.state == DetectorState.SUSPENDED
            else "stabilizing" if item.stabilizing else "ok"
        )
        return _Outcome(
            tuple(durable),
            self._public_snapshot(item.at_wall, status),
            suppress_confirmed=suppress_confirmed,
        )

    def record_system_log(
        self, data: Mapping[str, Any], at_wall: str, at_mono_ms: int
    ) -> _Outcome:
        with self._lock:
            if not relevant_system_log(data):
                return _Outcome(
                    (),
                    self._public_snapshot(
                        at_wall, "ok", touch_heartbeat=False
                    ),
                )
            self._roll_date(at_wall)
            self._metrics["system_log_warnings"] = int(
                self._metrics.get("system_log_warnings", 0)
            ) + 1
            summary: dict[str, str] = {
                key: str(data[key])[:1_024]
                for key in ("level", "name", "message")
                if key in data
            }
            if "source" in data:
                summary["source"] = redact_source(data["source"])
            self._append_health("system_log", at_wall, at_mono_ms, summary)
            self._flush_metrics()
            status = "degraded" if self.detector.state == DetectorState.SUSPENDED else "ok"
            return _Outcome(
                (),
                self._public_snapshot(
                    at_wall, status, touch_heartbeat=False
                ),
            )

    def record_executor_exception(
        self, item: _WorkItem, error_name: str
    ) -> _Outcome:
        """Wrap unexpected worker failures and suspend on the third strike."""

        with self._lock:
            cutoff = item.at_mono_ms - int(
                self.config.exception_window_seconds * 1_000
            )
            self._shell_exception_times_ms.append(item.at_mono_ms)
            while (
                self._shell_exception_times_ms
                and self._shell_exception_times_ms[0] < cutoff
            ):
                self._shell_exception_times_ms.popleft()
            strikes = len(self._shell_exception_times_ms)
            self._append_health(
                "executor_exception",
                item.at_wall,
                item.at_mono_ms,
                {"error": error_name, "strikes_in_window": strikes},
            )
            durable: list[DetectionEnvelope] = []
            if strikes >= self.config.exception_limit:
                staged = self._suspend(
                    "executor_exception_budget",
                    item.at_wall,
                    item.at_mono_ms,
                    auto_resume=False,
                    details={"error": error_name, "strikes_in_window": strikes},
                )
                durable.extend(self._commit(staged, item))
            self._flush_metrics()
            return _Outcome(
                tuple(durable),
                self._public_snapshot(item.at_wall, "degraded"),
                suppress_confirmed=True,
            )

    def take_startup_recovery(self) -> _Outcome:
        """Return restart-recovery closures exactly once for shadow publication."""

        with self._lock:
            durable = self._startup_recovery
            self._startup_recovery = ()
            return _Outcome(
                durable,
                self._public_snapshot(
                    _utc_now(), "starting", touch_heartbeat=False
                ),
                suppress_confirmed=True,
            )

    def record_homeassistant_start(
        self, at_wall: str, at_mono_ms: int
    ) -> _Outcome:
        """Count HA start events independently from interrupted episodes."""

        with self._lock:
            self._roll_date(at_wall)
            self._metrics["restarts"] = int(
                self._metrics.get("restarts", 0)
            ) + 1
            self._append_health(
                "homeassistant_start", at_wall, at_mono_ms, {}
            )
            self._flush_metrics(force=True)
            status = (
                "degraded"
                if self.detector.state == DetectorState.SUSPENDED
                else "ok"
            )
            return _Outcome(
                (),
                self._public_snapshot(
                    at_wall, status, touch_heartbeat=False
                ),
            )

    def record_sol_decision(
        self,
        *,
        lane: str,
        label: str,
        confidence: float,
        test: bool,
        decision_text: str,
        at_wall: str,
        at_mono_ms: int,
    ) -> _Outcome:
        """Timestamp-join one inferred Sol lane result to the canonical journal."""

        with self._lock:
            self._roll_date(at_wall)
            reduced = self.journal.reduce()
            episodes = reduced.get("episodes", {})
            episode_id = (
                match_sol_episode(
                    episodes,
                    at_wall,
                    maximum_age_ms=int(SOL_JOIN_MAX_SECONDS * 1_000),
                )
                if isinstance(episodes, Mapping)
                else None
            )
            if episode_id is None:
                self._append_health(
                    "sol_result_unmatched",
                    at_wall,
                    at_mono_ms,
                    {"lane": lane, "label": label},
                )
                return _Outcome(
                    (),
                    self._public_snapshot(
                        at_wall, "ok", touch_heartbeat=False
                    ),
                )

            envelope = DetectionEnvelope(
                record_type="sol_result",
                episode_id=episode_id,
                at_wall=at_wall,
                at_mono_ms=at_mono_ms,
                lifecycle_payload={
                    "lane": lane,
                    "label": label,
                    "confidence": confidence,
                    "decided_at_wall": at_wall,
                    "test": test,
                    "source": SOL_DECISION_ENTITY,
                    "decision_text": decision_text,
                },
            )
            durable = self.journal.commit([envelope])
            if not durable:
                self._suspend(
                    "journal_write_failure",
                    at_wall,
                    at_mono_ms,
                    auto_resume=False,
                    details={"records_lost": 1, "record": "sol_result"},
                )
            status = (
                "degraded"
                if self.detector.state == DetectorState.SUSPENDED
                else "ok"
            )
            return _Outcome(
                tuple(durable),
                self._public_snapshot(
                    at_wall, status, touch_heartbeat=False
                ),
                suppress_confirmed=True,
            )

    def stop(self, *, close_episode: bool) -> _Outcome:
        with self._lock:
            at_wall = _utc_now()
            at_mono_ms = _mono_ms()
            self._finish_suspension(at_mono_ms)
            durable: list[DetectionEnvelope] = []
            if close_episode and self.detector.episode_id is not None:
                staged = self.detector.suspend(
                    "integration_unload", at_wall, at_mono_ms
                )
                if staged:
                    prepared, baselines = self._prepare_journal_records(staged)
                    committed = self.journal.commit(prepared)
                    if committed:
                        self._episode_poll_gap_baselines = baselines
                        durable.extend(committed)
            self._save_masks()
            self._flush_metrics(force=True)
            return _Outcome(
                tuple(durable), self._public_snapshot(at_wall, "stopped"), True
            )


class PackageFastRuntime:
    """One config-entry runtime with a bounded one-frame processing queue."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        backend: _DetectorBackend,
        config: DetectorConfig,
        camera_entity: str,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.backend = backend
        self.config = config
        self.camera_entity = camera_entity
        self.enabled = True
        # async_start registers listeners before these inputs are sampled, so
        # a restore edge during platform forwarding cannot be lost.
        self.master_on = False
        self.g4_on = False
        self.g6_on = False
        self._sol_counter_values: dict[str, float | None] = {}
        self._pending_sol_calls: deque[_PendingSolCall] = deque()
        self._snapshot = dict(backend.initial_snapshot())
        self._snapshot.update(
            {"enabled": self.enabled, "master_on": self.master_on, "camera": camera_entity}
        )
        self._listeners: set[Callable[[], None]] = set()
        self._unsubs: list[Callable[[], None]] = []
        self._pending_signals: list[SignalEnvelope] = []
        self._frame_queue: asyncio.Queue[_WorkItem | None] = asyncio.Queue(maxsize=1)
        self._wake = asyncio.Event()
        self._stopping = False
        self._poll_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._aux_tasks: set[asyncio.Task[Any]] = set()
        self._last_raw: _RawFrame | None = None
        self._last_success_mono_ms: int | None = None
        self._last_success_was_armed = False
        self._last_poll_started: float | None = None
        self._startup_until: float | None = None
        self._stabilization_released = False
        self._frame_serial = 0
        self._deposit_handle: asyncio.TimerHandle | None = None
        self._last_projection_update_ms = 0

    @classmethod
    async def async_create(
        cls, hass: HomeAssistant, entry: ConfigEntry
    ) -> PackageFastRuntime:
        defaults = DetectorConfig()
        options = entry.options
        config = replace(
            defaults,
            idle_rate_hz=float(options.get(CONF_IDLE_RATE_HZ, defaults.idle_rate_hz)),
            armed_rate_hz=float(options.get(CONF_ARMED_RATE_HZ, defaults.armed_rate_hz)),
        )
        camera_entity = str(options.get(CONF_CAMERA_ENTITY, DEFAULT_CAMERA_ENTITY))
        backend = await hass.async_add_executor_job(
            partial(
                _DetectorBackend,
                config,
                root=MEDIA_ROOT,
                persist_frames=bool(
                    options.get(CONF_PERSIST_FRAMES, DEFAULT_PERSIST_FRAMES)
                ),
                max_bytes=int(
                    float(options.get(CONF_MAX_STORAGE_MB, DEFAULT_MAX_STORAGE_MB))
                    * 1_024
                    * 1_024
                ),
                max_age_days=float(
                    options.get(CONF_MAX_AGE_DAYS, DEFAULT_MAX_AGE_DAYS)
                ),
                mask_hits=int(options.get(CONF_MASK_HITS, DEFAULT_MASK_HITS)),
                mask_window_hours=float(
                    options.get(CONF_MASK_WINDOW_HOURS, DEFAULT_MASK_WINDOW_HOURS)
                ),
                mask_ttl_hours=float(
                    options.get(CONF_MASK_TTL_HOURS, DEFAULT_MASK_TTL_HOURS)
                ),
                mask_iou=float(options.get(CONF_MASK_IOU, DEFAULT_MASK_IOU)),
            )
        )
        return cls(hass, entry, backend, config, camera_entity)

    @property
    def snapshot(self) -> Mapping[str, Any]:
        return self._snapshot

    @callback
    def async_add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        self._listeners.add(listener)

        @callback
        def remove() -> None:
            self._listeners.discard(listener)

        return remove

    @callback
    def _notify_entities(self) -> None:
        for listener in tuple(self._listeners):
            listener()

    def _signal(self, kind: str, *, entity_id: str, state: State | None = None) -> None:
        at_wall = _utc_now()
        meta: dict[str, Any] = {"entity_id": entity_id}
        if state is not None:
            meta["state_last_changed"] = state.last_changed.isoformat()
        self._pending_signals.append(
            SignalEnvelope(kind, at_wall, _mono_ms(), meta)  # type: ignore[arg-type]
        )
        self._wake.set()

    def _drop_pending_person_signals(self) -> None:
        self._pending_signals = [
            signal
            for signal in self._pending_signals
            if signal.kind
            not in {"g4_person_on", "g4_person_off", "g6_person_on", "g6_person_off"}
        ]

    def _seed_active_person_signals(self) -> None:
        if self.g4_on:
            self._signal("g4_person_on", entity_id=G4_PERSON_ENTITY)
        if self.g6_on:
            self._signal("g6_person_on", entity_id=G6_PERSON_ENTITY)

    def _mark_starting_heartbeat(self) -> None:
        self._snapshot["heartbeat_status"] = "stabilizing"
        if heartbeat_can_advance(self._snapshot.get("state")):
            self._snapshot["heartbeat"] = _utc_now()

    def _stabilizing(self) -> bool:
        return (
            self._startup_until is None
            or time.monotonic() < self._startup_until
        )

    @staticmethod
    def _counter_value(state: State | None) -> float | None:
        if state is None:
            return None
        try:
            return float(state.state)
        except (TypeError, ValueError):
            return None

    def _resample_state_inputs(self) -> None:
        """Sample only after listeners exist, closing the setup race window."""

        self.master_on = self.hass.states.is_state(MASTER_ENTITY, "on")
        self.g4_on = self.hass.states.is_state(G4_PERSON_ENTITY, "on")
        self.g6_on = self.hass.states.is_state(G6_PERSON_ENTITY, "on")
        self._sol_counter_values = {
            entity_id: self._counter_value(self.hass.states.get(entity_id))
            for entity_id in SOL_LANE_COUNTERS
        }

    def _sol_counter_changed(
        self, entity_id: str, new_state: State | None
    ) -> None:
        previous = self._sol_counter_values.get(entity_id)
        current = self._counter_value(new_state)
        self._sol_counter_values[entity_id] = current
        if previous is None or current is None or current <= previous:
            return
        self._pending_sol_calls.append(
            _PendingSolCall(SOL_LANE_COUNTERS[entity_id], _mono_ms())
        )

    def _sol_decision_changed(self, new_state: State | None) -> None:
        if new_state is None:
            return
        now_mono_ms = _mono_ms()
        cutoff = now_mono_ms - int(SOL_JOIN_MAX_SECONDS * 1_000)
        self._pending_sol_calls = deque(
            call
            for call in self._pending_sol_calls
            if call.at_mono_ms >= cutoff
        )
        calls = list(self._pending_sol_calls)
        selected_index: int | None = None
        parsed = None
        for index in range(len(calls) - 1, -1, -1):
            candidate = parse_sol_decision(new_state.state, calls[index].lane)
            if candidate is not None:
                selected_index = index
                parsed = candidate
                break
        if selected_index is None or parsed is None:
            return
        call = calls.pop(selected_index)
        self._pending_sol_calls = deque(calls)
        task = self.hass.async_create_task(
            self._async_record_sol_decision(
                lane=call.lane,
                label=parsed.label,
                confidence=parsed.confidence,
                test=parsed.test,
                decision_text=new_state.state,
                at_wall=new_state.last_changed.isoformat(),
            ),
            "package_fast Sol-result join",
        )
        self._aux_tasks.add(task)
        task.add_done_callback(self._aux_tasks.discard)

    @callback
    def _state_changed(self, event: Event) -> None:
        entity_id = event.data["entity_id"]
        new_state: State | None = event.data.get("new_state")
        if entity_id in SOL_LANE_COUNTERS:
            self._sol_counter_changed(entity_id, new_state)
            return
        if entity_id == SOL_DECISION_ENTITY:
            self._sol_decision_changed(new_state)
            return
        # Unavailable/unknown/removal is fail-closed: master idles and person
        # context clears instead of retaining a stale on-state.
        is_on = new_state is not None and new_state.state == "on"
        if entity_id == MASTER_ENTITY:
            if is_on == self.master_on:
                return
            self.master_on = is_on
            if not is_on:
                self._drop_pending_person_signals()
                self._startup_until = None
                self._stabilization_released = False
            self._signal(
                "master_on" if is_on and self.enabled else "master_off",
                entity_id=entity_id,
                state=new_state,
            )
            if is_on and self.enabled and not self._stabilizing():
                self._seed_active_person_signals()
            if is_on and self.enabled:
                self._mark_starting_heartbeat()
        elif entity_id == G4_PERSON_ENTITY:
            if is_on == self.g4_on:
                return
            self.g4_on = is_on
            if self._effective_on() and not self._stabilizing():
                self._signal(
                    "g4_person_on" if is_on else "g4_person_off",
                    entity_id=entity_id,
                    state=new_state,
                )
        elif entity_id == G6_PERSON_ENTITY:
            if is_on == self.g6_on:
                return
            self.g6_on = is_on
            if self._effective_on() and not self._stabilizing():
                self._signal(
                    "g6_person_on" if is_on else "g6_person_off",
                    entity_id=entity_id,
                    state=new_state,
                )
        self._snapshot.update({"master_on": self.master_on})
        self._notify_entities()

    async def _async_record_sol_decision(self, **decision: Any) -> None:
        try:
            outcome = await self.hass.async_add_executor_job(
                partial(
                    self.backend.record_sol_decision,
                    **decision,
                    at_mono_ms=_mono_ms(),
                )
            )
            self._apply_outcome(outcome)
        except Exception:
            _LOGGER.exception("package_fast Sol-result join failed")

    @callback
    def _homeassistant_start_event(self, _event: Event) -> None:
        task = self.hass.async_create_task(
            self._async_record_homeassistant_start(),
            "package_fast Home Assistant start record",
        )
        self._aux_tasks.add(task)
        task.add_done_callback(self._aux_tasks.discard)

    async def _async_record_homeassistant_start(self) -> None:
        try:
            outcome = await self.hass.async_add_executor_job(
                self.backend.record_homeassistant_start,
                _utc_now(),
                _mono_ms(),
            )
            self._apply_outcome(outcome)
        except Exception:
            _LOGGER.exception("package_fast could not record HA start")

    async def _homeassistant_stop_event(self, _event: Event) -> None:
        await self.async_stop(close_episode=False)

    @callback
    def _system_log_event(self, event: Event) -> None:
        task = self.hass.async_create_task(
            self._async_record_system_log(dict(event.data)),
            "package_fast system-log soak record",
        )
        self._aux_tasks.add(task)
        task.add_done_callback(self._aux_tasks.discard)

    async def _async_record_system_log(self, data: Mapping[str, Any]) -> None:
        outcome = await self.hass.async_add_executor_job(
            self.backend.record_system_log, data, _utc_now(), _mono_ms()
        )
        snapshot = dict(outcome.snapshot)
        snapshot.pop("heartbeat", None)
        snapshot.pop("heartbeat_status", None)
        self._apply_outcome(replace(outcome, snapshot=snapshot))

    async def async_start(self) -> None:
        """Attach state/soak listeners, then start the bounded sequential loop."""

        self._unsubs.append(
            async_track_state_change_event(
                self.hass,
                (
                    MASTER_ENTITY,
                    G4_PERSON_ENTITY,
                    G6_PERSON_ENTITY,
                    SOL_DECISION_ENTITY,
                    *SOL_LANE_COUNTERS,
                ),
                self._state_changed,
            )
        )
        self._resample_state_inputs()
        self._unsubs.append(
            self.hass.bus.async_listen(EVENT_SYSTEM_LOG, self._system_log_event)
        )
        self._unsubs.append(
            self.hass.bus.async_listen(
                EVENT_HOMEASSISTANT_START, self._homeassistant_start_event
            )
        )
        self._unsubs.append(
            self.hass.bus.async_listen(
                EVENT_HOMEASSISTANT_STOP, self._homeassistant_stop_event
            )
        )
        startup = await self.hass.async_add_executor_job(
            self.backend.take_startup_recovery
        )
        self._apply_outcome(startup)
        self._signal(
            "master_on" if self._effective_on() else "master_off",
            entity_id=MASTER_ENTITY,
        )
        self._worker_task = self.hass.async_create_task(
            self._worker_loop(), "package_fast frame worker"
        )
        self._poll_task = self.hass.async_create_task(
            self._poll_loop(), "package_fast sequential poller"
        )

    async def async_set_enabled(self, enabled: bool) -> None:
        if enabled == self.enabled:
            return
        self.enabled = enabled
        if not enabled:
            self._drop_pending_person_signals()
            self._startup_until = None
            self._stabilization_released = False
        self._signal(
            "master_on" if enabled and self.master_on else "master_off",
            entity_id="switch.package_fast_detector",
        )
        if enabled and self.master_on and not self._stabilizing():
            self._seed_active_person_signals()
        if enabled and self.master_on:
            self._mark_starting_heartbeat()
        self._snapshot["enabled"] = enabled
        self._notify_entities()

    def _effective_on(self) -> bool:
        return self.enabled and self.master_on

    def _armed(self) -> bool:
        if self._stabilizing():
            return False
        state = str(self._snapshot.get("state", DetectorState.IDLE.value))
        suspension_reason = str(self._snapshot.get("suspension_reason") or "")
        if state == DetectorState.SUSPENDED.value and (
            suspension_reason == "feed_suspect"
            or "distinct_fps" in suspension_reason
        ):
            # A frozen-feed recovery window must itself prove 2 Hz freshness;
            # dropping to idle would make the Phase-0 distinct-fps gate vanish.
            return True
        return self.g4_on or self.g6_on or state not in {
            DetectorState.IDLE.value,
            DetectorState.CLOSED.value,
            DetectorState.DISABLED.value,
            DetectorState.SUSPENDED.value,
        }

    def _drain_signals(
        self, *, include_person: bool = True
    ) -> tuple[SignalEnvelope, ...]:
        if include_person:
            signals = tuple(self._pending_signals)
            self._pending_signals.clear()
            return signals
        selected = tuple(
            signal
            for signal in self._pending_signals
            if signal.kind in {"master_on", "master_off"}
        )
        self._pending_signals = [
            signal
            for signal in self._pending_signals
            if signal.kind not in {"master_on", "master_off"}
        ]
        return selected

    async def _fetch_image(self) -> tuple[bytes | None, float, Exception | None]:
        started = time.monotonic()
        error: Exception | None = None
        for _attempt in range(self.config.fetch_retries + 1):
            try:
                async with asyncio.timeout(self.config.fetch_budget_seconds):
                    image = await camera.async_get_image(
                        self.hass,
                        self.camera_entity,
                        timeout=self.config.fetch_budget_seconds,
                    )
                content = bytes(image.content)
                if not content:
                    raise ValueError("camera returned an empty image")
                return content, (time.monotonic() - started) * 1_000, None
            except asyncio.CancelledError:
                raise
            except Exception as caught:  # retry exactly once by frozen config
                error = caught
        return None, (time.monotonic() - started) * 1_000, error

    async def _respect_minimum_spacing(self) -> None:
        if self._last_poll_started is None:
            return
        deadline = self._last_poll_started + 0.5
        while not self._stopping:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=remaining)
                self._wake.clear()
            except TimeoutError:
                return

    async def _wait_or_wake(self, delay: float) -> None:
        if delay <= 0 or self._stopping:
            return
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=delay)
        except TimeoutError:
            return
        finally:
            self._wake.clear()

    async def _flush_control_signals(self) -> None:
        if not self._pending_signals:
            return
        signals = self._drain_signals(include_person=False)
        if not signals:
            return
        at_wall = _utc_now()
        at_mono_ms = _mono_ms()
        raw = self._last_raw or _RawFrame(
            frame_id=f"control_{at_mono_ms}",
            at_wall=at_wall,
            at_mono_ms=at_mono_ms,
            # master_off is applied before decode, so a cold disabled control
            # carrier never treats this placeholder as camera content.
            jpeg_bytes=b"",
        )
        item = _WorkItem(
            mode="control",
            at_wall=at_wall,
            at_mono_ms=at_mono_ms,
            fetch_ms=0.0,
            gap_ms=0.0,
            armed=False,
            raw=raw,
            signals=signals,
            effective_on=self._effective_on(),
            g4_on=self.g4_on,
            g6_on=self.g6_on,
        )
        await self._frame_queue.put(item)
        await self._frame_queue.join()

    async def _poll_loop(self) -> None:
        try:
            while not self._stopping:
                if not self._effective_on():
                    await self._flush_control_signals()
                    self._wake.clear()
                    if self._effective_on():
                        continue
                    await self._wake.wait()
                    self._wake.clear()
                    continue

                await self._respect_minimum_spacing()
                if self._stopping:
                    break
                loop_started = time.monotonic()
                self._last_poll_started = loop_started
                if self._startup_until is None:
                    self._startup_until = (
                        loop_started + STARTUP_STABILIZATION_SECONDS
                    )
                stabilizing = loop_started < self._startup_until
                if not stabilizing and not self._stabilization_released:
                    self._drop_pending_person_signals()
                    self._seed_active_person_signals()
                    self._stabilization_released = True
                poll_armed = self._armed()
                armed_interval = poll_armed and self._last_success_was_armed
                rate = (
                    self.config.armed_rate_hz
                    if poll_armed
                    else self.config.idle_rate_hz
                )
                content, fetch_ms, error = await self._fetch_image()
                at_wall = _utc_now()
                at_mono_ms = _mono_ms()
                gap_ms = (
                    1_000.0 / rate
                    if self._last_success_mono_ms is None
                    else max(0, at_mono_ms - self._last_success_mono_ms)
                )

                if content is None:
                    item = _WorkItem(
                        mode="failure",
                        at_wall=at_wall,
                        at_mono_ms=at_mono_ms,
                        fetch_ms=fetch_ms,
                        gap_ms=gap_ms,
                        armed=armed_interval,
                        effective_on=self._effective_on(),
                        g4_on=self.g4_on,
                        g6_on=self.g6_on,
                        stabilizing=stabilizing,
                    )
                    if error is not None:
                        _LOGGER.debug("package_fast image fetch failed: %s", error)
                else:
                    self._frame_serial += 1
                    raw = _RawFrame(
                        frame_id=f"f_{self._frame_serial:08d}",
                        at_wall=at_wall,
                        at_mono_ms=at_mono_ms,
                        jpeg_bytes=content,
                    )
                    self._last_raw = raw
                    self._last_success_mono_ms = at_mono_ms
                    self._last_success_was_armed = poll_armed
                    item = _WorkItem(
                        mode="frame",
                        at_wall=at_wall,
                        at_mono_ms=at_mono_ms,
                        fetch_ms=fetch_ms,
                        gap_ms=gap_ms,
                        armed=armed_interval,
                        raw=raw,
                        signals=self._drain_signals(
                            include_person=not stabilizing
                        ),
                        effective_on=self._effective_on(),
                        g4_on=self.g4_on,
                        g6_on=self.g6_on,
                        stabilizing=stabilizing,
                    )

                # Queue size is one and join completes before the next fetch:
                # fetches and executor jobs can never stack or overlap.
                await self._frame_queue.put(item)
                await self._frame_queue.join()
                elapsed = time.monotonic() - loop_started
                await self._wait_or_wake(max(0.0, 1.0 / rate - elapsed))
        except asyncio.CancelledError:
            raise

    async def _worker_loop(self) -> None:
        while True:
            item = await self._frame_queue.get()
            try:
                if item is None:
                    return
                try:
                    outcome = await self.hass.async_add_executor_job(
                        self.backend.process, item
                    )
                except Exception as error:
                    _LOGGER.exception("package_fast executor job failed")
                    try:
                        outcome = await self.hass.async_add_executor_job(
                            self.backend.record_executor_exception,
                            item,
                            type(error).__name__,
                        )
                    except Exception:
                        _LOGGER.exception(
                            "package_fast executor exception accounting failed"
                        )
                        continue
                self._apply_outcome(outcome)
            except Exception:
                # Entity listeners and event-bus consumers are outside the
                # worker's trust boundary; one bad callback cannot kill it.
                _LOGGER.exception("package_fast worker item failed")
            finally:
                self._frame_queue.task_done()

    @staticmethod
    def _event_data(envelope: DetectionEnvelope) -> dict[str, Any]:
        payload = envelope.payload
        return {
            **payload,
            "schema_version": 2,
            "record": envelope.record_type,
            "episode_id": envelope.episode_id,
            "at_wall": envelope.at_wall,
            "at_mono_ms": envelope.at_mono_ms,
            "payload": payload,
        }

    @callback
    def _clear_deposit(self) -> None:
        self._deposit_handle = None
        self._snapshot["deposit"] = False
        self._notify_entities()

    @callback
    def _apply_outcome(self, outcome: _Outcome) -> None:
        previous_state = self._snapshot.get("state")
        previous_reason = self._snapshot.get("suspension_reason")
        self._snapshot.update(outcome.snapshot)
        self._snapshot.update(
            {
                "enabled": self.enabled,
                "master_on": self.master_on,
                "camera": self.camera_entity,
            }
        )
        for envelope in outcome.durable:
            event_data = self._event_data(envelope)
            # This is the only HA publication path, and outcome.durable can
            # contain only envelopes returned by Journal.commit().
            self.hass.bus.async_fire(EVENT_SHADOW, event_data)
            if (
                envelope.record_type == "detection"
                and envelope.announce_eligible
                and not outcome.suppress_confirmed
                and self.enabled
                and self.master_on
                and self.hass.states.is_state(PROMOTED_ENTITY, "on")
            ):
                self.hass.bus.async_fire(EVENT_CONFIRMED, event_data)
            if envelope.record_type == "detection" and envelope.kind == "deposit":
                self._snapshot["deposit"] = True
                self._snapshot["latest_deposit"] = event_data
                if self._deposit_handle is not None:
                    self._deposit_handle.cancel()
                self._deposit_handle = self.hass.loop.call_later(
                    FRAME_MOMENTARY_SECONDS, self._clear_deposit
                )
        now_mono_ms = _mono_ms()
        urgent = bool(outcome.durable) or (
            previous_state != self._snapshot.get("state")
            or previous_reason != self._snapshot.get("suspension_reason")
        )
        if urgent or (
            now_mono_ms - self._last_projection_update_ms
            >= int(SENSOR_UPDATE_SECONDS * 1_000)
        ):
            self._last_projection_update_ms = now_mono_ms
            self._notify_entities()

    async def async_stop(self, *, close_episode: bool) -> None:
        if self._stopping:
            return
        self._stopping = True
        for unsubscribe in self._unsubs:
            unsubscribe()
        self._unsubs.clear()
        self._wake.set()

        if self._poll_task is not None:
            self._poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        worker = self._worker_task
        if worker is not None:
            join_timed_out = False
            if not worker.done():
                try:
                    await asyncio.wait_for(
                        self._frame_queue.join(),
                        timeout=QUEUE_JOIN_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    join_timed_out = True
                    _LOGGER.error(
                        "package_fast frame queue did not drain during stop"
                    )
            if join_timed_out:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            elif not worker.done():
                await self._frame_queue.put(None)
                await asyncio.gather(worker, return_exceptions=True)
            else:
                await asyncio.gather(worker, return_exceptions=True)
            self._worker_task = None
        if self._aux_tasks:
            await asyncio.gather(*tuple(self._aux_tasks), return_exceptions=True)
            self._aux_tasks.clear()

        outcome = await self.hass.async_add_executor_job(
            partial(self.backend.stop, close_episode=close_episode)
        )
        self._apply_outcome(outcome)
        if self._deposit_handle is not None:
            self._deposit_handle.cancel()
            self._deposit_handle = None


__all__ = ["PackageFastRuntime"]
