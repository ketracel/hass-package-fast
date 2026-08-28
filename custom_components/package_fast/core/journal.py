# journal.py — ERR-07 typed, append-only durability boundary and reducer.
#
# Rationale: a detection is not publishable merely because the pure detector
# found it.  ERRATA.md ERR-07 requires one canonical episodes.jsonl writer,
# flush+fsync before publication, deterministic reduction, and restart closure.
# meta.json is emitted only as a regenerable view; JPEG persistence remains an
# optional shell callback so ERR-08's privacy gate can leave it fully disabled.

"""Durable typed journal writer, restart recovery, and deterministic reducer."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

from .envelopes import DetectionEnvelope


SCHEMA_VERSION = 2
REDUCER_VERSION = "package-fast-reducer-v1"
RECORD_TYPES = {
    "episode_opened",
    "detection",
    "fast_result",
    "sol_result",
    "episode_closed",
}


def _forbidden_payload_key(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in {"ground_truth", "agreement"}:
                return str(key)
            found = _forbidden_payload_key(child)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for child in value:
            found = _forbidden_payload_key(child)
            if found is not None:
                return found
    return None


def _without_forbidden_payload_keys(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _without_forbidden_payload_keys(child)
            for key, child in value.items()
            if key not in {"ground_truth", "agreement"}
        }
    if isinstance(value, (list, tuple)):
        return [_without_forbidden_payload_keys(child) for child in value]
    return value


def _canonical_line(record: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def load_records(path: str | Path) -> list[dict[str, Any]]:
    """Load complete JSON objects; malformed lines are represented as anomalies."""

    records: list[dict[str, Any]] = []
    journal_path = Path(path)
    if not journal_path.exists():
        return records
    with journal_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                records.append({"_malformed_line": line_number})
                continue
            if isinstance(value, dict):
                records.append(value)
            else:
                records.append({"_malformed_line": line_number})
    return records


def _agreement(episode: Mapping[str, Any]) -> str:
    fast = episode.get("fast_result")
    sol = episode.get("sol_results", [])
    closed = episode.get("closed_at_wall") is not None
    if fast is None and not sol:
        return "both_silent" if closed else "pending"
    if fast is not None and not sol:
        return "fast_only" if closed else "pending"
    if fast is None and sol:
        return "sol_only"
    fast_label = str(fast.get("label", "")).lower()
    sol_labels = {str(item.get("label", "")).lower() for item in sol}
    return "agree" if fast_label in sol_labels else "disagree"


def reduce_records(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Fold schema-v2 records in per-episode sequence order.

    Set-once records are first-wins.  Detection and Sol results are idempotent
    by stable keys.  Input-order, schema, sequence, and conflicting duplicate
    defects are counted without making the reduced view nondeterministic.
    """

    grouped: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    anomalies = 0
    last_seen_seq: dict[str, int] = {}
    for index, record in enumerate(records):
        if record.get("_malformed_line") is not None:
            anomalies += 1
            continue
        if record.get("schema_version") != SCHEMA_VERSION:
            anomalies += 1
            continue
        episode_id = record.get("episode_id")
        seq = record.get("seq")
        if not isinstance(episode_id, str) or not episode_id or not isinstance(seq, int):
            anomalies += 1
            continue
        if episode_id in last_seen_seq and seq <= last_seen_seq[episode_id]:
            anomalies += 1
        last_seen_seq[episode_id] = seq
        grouped[episode_id].append((index, record))

    episodes: dict[str, dict[str, Any]] = {}
    unique_detection_keys: set[tuple[str, str]] = set()
    kind_counts: dict[str, int] = defaultdict(int)

    for episode_id in sorted(grouped):
        episode: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "episode_id": episode_id,
            "detections": [],
            "sol_results": [],
            "journal_anomalies": 0,
        }
        seen_sequences: set[int] = set()
        detections: dict[str, dict[str, Any]] = {}
        sol_results: dict[tuple[str, str], dict[str, Any]] = {}
        opened = fast = closed = False
        expected_seq: int | None = None

        ordered = sorted(grouped[episode_id], key=lambda item: (int(item[1]["seq"]), item[0]))
        for _, record in ordered:
            seq = int(record["seq"])
            if seq in seen_sequences:
                episode["journal_anomalies"] += 1
            seen_sequences.add(seq)
            if expected_seq is not None and seq > expected_seq:
                episode["journal_anomalies"] += 1
            expected_seq = max(expected_seq or seq, seq + 1)
            record_type = record.get("record")
            payload = record.get("payload")
            if record_type not in RECORD_TYPES or not isinstance(payload, dict):
                episode["journal_anomalies"] += 1
                continue
            if _forbidden_payload_key(payload) is not None:
                episode["journal_anomalies"] += 1
                payload = _without_forbidden_payload_keys(payload)
            episode["last_seq"] = max(seq, int(episode.get("last_seq", 0)))

            if record_type == "episode_opened":
                if opened:
                    episode["journal_anomalies"] += 1
                    continue
                opened = True
                episode["opened_at_wall"] = record.get("at_wall")
                episode["opened_at_mono_ms"] = record.get("at_mono_ms")
                episode.update(payload)
            elif record_type == "fast_result":
                if fast:
                    episode["journal_anomalies"] += 1
                    continue
                fast = True
                episode["fast_result"] = dict(payload)
            elif record_type == "episode_closed":
                if closed:
                    episode["journal_anomalies"] += 1
                    continue
                closed = True
                episode["closed_at_wall"] = record.get("at_wall")
                episode["closed_at_mono_ms"] = record.get("at_mono_ms")
                episode["close_reason"] = payload.get("close_reason")
                episode["poll"] = dict(payload.get("poll", {}))
                episode["metrics"] = dict(payload.get("metrics", {}))
            elif record_type == "detection":
                detection_id = payload.get("detection_id")
                if not isinstance(detection_id, str) or not detection_id:
                    episode["journal_anomalies"] += 1
                    continue
                if detection_id in detections:
                    if detections[detection_id] != payload:
                        episode["journal_anomalies"] += 1
                    continue
                detections[detection_id] = dict(payload)
                unique_detection_keys.add((episode_id, detection_id))
                kind_counts[str(payload.get("kind", "unknown"))] += 1
            elif record_type == "sol_result":
                key = (str(payload.get("lane", "")), str(payload.get("decided_at_wall", "")))
                if key in sol_results:
                    if sol_results[key] != payload:
                        episode["journal_anomalies"] += 1
                    continue
                sol_results[key] = dict(payload)

        episode["detections"] = list(detections.values())
        episode["sol_results"] = list(sol_results.values())
        episode["agreement"] = _agreement(episode)
        anomalies += int(episode["journal_anomalies"])
        episodes[episode_id] = episode

    metrics = {
        "episodes": len(episodes),
        "detections": len(unique_detection_keys),
        "unique_detection_keys": len(unique_detection_keys),
        "detection_kinds": dict(sorted(kind_counts.items())),
        "reducer_version": REDUCER_VERSION,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "reducer_version": REDUCER_VERSION,
        "episodes": episodes,
        "journal_anomalies": anomalies,
        "metrics": metrics,
    }


def reduce_journal(path: str | Path) -> dict[str, Any]:
    return reduce_records(load_records(path))


def _envelope_from_payload(
    record_type: str,
    episode_id: str,
    at_wall: str,
    at_mono_ms: int,
    payload: Mapping[str, Any],
) -> DetectionEnvelope:
    if record_type != "detection":
        return DetectionEnvelope(
            record_type=record_type,  # type: ignore[arg-type]
            episode_id=episode_id,
            at_wall=at_wall,
            at_mono_ms=at_mono_ms,
            lifecycle_payload=dict(payload),
        )
    bbox_norm = payload.get("bbox_norm")
    bbox_full = payload.get("bbox_full")
    return DetectionEnvelope(
        record_type="detection",
        episode_id=episode_id,
        at_wall=at_wall,
        at_mono_ms=at_mono_ms,
        detection_id=payload.get("detection_id"),
        kind=payload.get("kind"),
        bbox_norm=tuple(bbox_norm) if bbox_norm is not None else None,
        bbox_full=tuple(bbox_full) if bbox_full is not None else None,
        area_frac=payload.get("area_frac"),
        polarity=payload.get("polarity"),
        first_seen_frame=payload.get("first_seen_frame"),
        confirmed_frame=payload.get("confirmed_frame"),
        confirm_count=payload.get("confirm_count"),
        frame_sha256s=dict(payload.get("frame_sha256s", {})),
        scores=dict(payload.get("scores", {})),
        veto_bits=int(payload.get("veto_bits", 0)),
        latency_ms_from_first_visible=payload.get("latency_ms_from_first_visible"),
        announce_eligible=bool(payload.get("announce_eligible", False)),
        person_context_present=bool(payload.get("person_context_present", False)),
        announced=bool(payload.get("announced", False)),
        shadow=bool(payload.get("shadow", True)),
    )


class Journal:
    """The single schema-v2 writer and persistence-before-publication gate."""

    def __init__(
        self,
        root: str | Path,
        *,
        fault_hook: Callable[[str, Mapping[str, Any]], None] | None = None,
        frame_persistence_enabled: bool = False,
        frame_persist_callback: Callable[[str, str, Any], int | None] | None = None,
        store_bytes_hook: Callable[[int, int], None] | None = None,
        wall_clock: Callable[[], str] | None = None,
        mono_clock_ms: Callable[[], int] | None = None,
        recover: bool = True,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "episodes.jsonl"
        self.path.touch(exist_ok=True)
        self.recovery_tail_truncations = int(self._truncate_incomplete_tail())
        self._lock = threading.Lock()
        self._fault_hook = fault_hook
        self.frame_persistence_enabled = frame_persistence_enabled
        self._frame_persist_callback = frame_persist_callback
        self._store_bytes_hook = store_bytes_hook
        self._wall_clock = wall_clock or (
            lambda: datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        )
        self._mono_clock_ms = mono_clock_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self._seq_by_episode: dict[str, int] = defaultdict(int)
        self._store_bytes = self._measure_store_bytes()
        self._shadow_write_skips = 0
        self.journal_write_failures = 0
        self.derived_write_failures = 0
        self.last_error: Exception | None = None
        self.last_committed_records: list[dict[str, Any]] = []

        records = load_records(self.path)
        for record in records:
            episode_id = record.get("episode_id")
            seq = record.get("seq")
            if isinstance(episode_id, str) and isinstance(seq, int):
                self._seq_by_episode[episode_id] = max(self._seq_by_episode[episode_id], seq)
        if recover and records:
            self._recover_open_episodes(records)

    @property
    def store_bytes(self) -> int:
        return self._store_bytes

    @property
    def shadow_write_skips(self) -> int:
        return self._shadow_write_skips

    def _measure_store_bytes(self) -> int:
        return sum(path.stat().st_size for path in self.root.rglob("*") if path.is_file())

    def _truncate_incomplete_tail(self) -> bool:
        """Discard only a non-newline crash tail; committed lines always end in LF."""

        with self.path.open("r+b") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            if size == 0:
                return False
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) == b"\n":
                return False
            cursor = size
            newline_at = -1
            while cursor > 0 and newline_at < 0:
                chunk_size = min(65_536, cursor)
                cursor -= chunk_size
                handle.seek(cursor)
                chunk = handle.read(chunk_size)
                offset = chunk.rfind(b"\n")
                if offset >= 0:
                    newline_at = cursor + offset
            handle.truncate(newline_at + 1 if newline_at >= 0 else 0)
            handle.flush()
            os.fsync(handle.fileno())
            return True

    def _notify_store_bytes(self, delta: int) -> None:
        if delta == 0:
            return
        self._store_bytes += delta
        if self._store_bytes_hook is not None:
            try:
                self._store_bytes_hook(delta, self._store_bytes)
            except Exception:
                # Accounting observers never get to revoke a durable record.
                pass

    def account_external_bytes(self, delta: int) -> None:
        """Account shell-owned sparse frames without granting core write access."""

        self._notify_store_bytes(int(delta))

    def note_shadow_write_skip(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("skip count cannot be negative")
        self._shadow_write_skips += count

    def persist_frame(self, episode_id: str, role: str, frame: Any) -> bool:
        """Invoke a shell callback only when ERR-08's frame gate is enabled."""

        if not self.frame_persistence_enabled:
            return False
        if self._frame_persist_callback is None:
            self.note_shadow_write_skip()
            return False
        try:
            written = self._frame_persist_callback(episode_id, role, frame)
        except Exception:
            self.note_shadow_write_skip()
            return False
        if written:
            self.account_external_bytes(int(written))
        return True

    def _call_fault(self, stage: str, context: Mapping[str, Any]) -> None:
        if self._fault_hook is not None:
            self._fault_hook(stage, context)

    @staticmethod
    def _validate_envelope(envelope: DetectionEnvelope) -> None:
        if envelope.record_type not in RECORD_TYPES:
            raise ValueError(f"unsupported journal record: {envelope.record_type}")
        payload = envelope.payload
        forbidden = _forbidden_payload_key(payload)
        if forbidden == "ground_truth":
            raise ValueError("ground truth is forbidden in the HA journal")
        if forbidden == "agreement":
            raise ValueError("agreement is reducer-derived and cannot be journaled")
        if envelope.record_type == "detection" and not payload.get("detection_id"):
            raise ValueError("detection payload requires detection_id")

    def commit(self, envelopes: Iterable[DetectionEnvelope]) -> list[DetectionEnvelope]:
        """Append, flush, fsync, then return only the now-durable envelopes.

        Any pre-fsync error truncates the uncommitted tail and returns an empty
        list.  Callers must emit events exclusively from this return value.
        """

        staged = list(envelopes)
        if not staged:
            self.last_committed_records = []
            return []
        try:
            for envelope in staged:
                self._validate_envelope(envelope)
        except Exception as error:
            self.last_error = error
            self.journal_write_failures += 1
            self.last_committed_records = []
            return []

        with self._lock:
            tentative_seq = dict(self._seq_by_episode)
            records: list[dict[str, Any]] = []
            for envelope in staged:
                seq = tentative_seq.get(envelope.episode_id, 0) + 1
                tentative_seq[envelope.episode_id] = seq
                records.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "record": envelope.record_type,
                        "seq": seq,
                        "episode_id": envelope.episode_id,
                        "at_wall": envelope.at_wall,
                        "at_mono_ms": envelope.at_mono_ms,
                        "payload": envelope.payload,
                    }
                )

            before_size = self.path.stat().st_size
            try:
                with self.path.open("r+b") as handle:
                    handle.seek(0, os.SEEK_END)
                    start = handle.tell()
                    self._call_fault("before_write", {"records": records})
                    for record in records:
                        handle.write(_canonical_line(record))
                    self._call_fault("before_fsync", {"records": records})
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception as error:
                try:
                    with self.path.open("r+b") as rollback:
                        rollback.truncate(before_size)
                        rollback.flush()
                        os.fsync(rollback.fileno())
                except Exception:
                    pass
                self.last_error = error
                self.journal_write_failures += 1
                self.last_committed_records = []
                return []

            self._seq_by_episode = defaultdict(int, tentative_seq)
            self.last_error = None
            self.last_committed_records = records
            self._notify_store_bytes(self.path.stat().st_size - before_size)

        # Canonical durability is complete.  A derived-view failure is counted
        # but cannot make a successfully fsync'd record disappear.
        closed_ids = {
            envelope.episode_id
            for envelope in staged
            if envelope.record_type == "episode_closed"
        }
        for episode_id in closed_ids:
            try:
                self._write_derived_meta(episode_id)
            except Exception:
                self.derived_write_failures += 1
        return staged

    def append(
        self,
        record_type: str,
        episode_id: str,
        at_wall: str,
        at_mono_ms: int,
        payload: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Convenience append for shell-owned lifecycle/Sol records."""

        envelope = _envelope_from_payload(
            record_type, episode_id, at_wall, at_mono_ms, payload
        )
        if not self.commit([envelope]):
            return None
        return dict(self.last_committed_records[-1])

    def records(self) -> list[dict[str, Any]]:
        return load_records(self.path)

    def reduce(self) -> dict[str, Any]:
        return reduce_records(self.records())

    def metrics_snapshot(self) -> dict[str, Any]:
        reduced = self.reduce()
        return {
            **reduced["metrics"],
            "store_bytes": self.store_bytes,
            "shadow_write_skips": self.shadow_write_skips,
            "journal_anomalies": reduced["journal_anomalies"],
            "reducer_version": REDUCER_VERSION,
        }

    def _recover_open_episodes(self, records: Sequence[Mapping[str, Any]]) -> None:
        reduced = reduce_records(records)
        for episode_id, episode in sorted(reduced["episodes"].items()):
            if episode.get("opened_at_wall") is None or episode.get("closed_at_wall") is not None:
                continue
            self.append(
                "episode_closed",
                episode_id,
                self._wall_clock(),
                self._mono_clock_ms(),
                {
                    "close_reason": "interrupted_restart",
                    "poll": {},
                    "metrics": {},
                },
            )

    def _write_derived_meta(self, episode_id: str) -> None:
        reduced = self.reduce()
        episode = reduced["episodes"].get(episode_id)
        if episode is None:
            return
        frames_dir = episode.get("frames_dir")
        if not isinstance(frames_dir, str) or not frames_dir:
            return
        target_dir = (self.root / frames_dir).resolve()
        root = self.root.resolve()
        if not target_dir.is_relative_to(root):
            raise ValueError("frames_dir escapes journal root")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / "meta.json"
        temporary = target.with_suffix(".json.tmp")
        before_size = target.stat().st_size if target.exists() else 0
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(episode, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        self._notify_store_bytes(target.stat().st_size - before_size)


class JournalReducer:
    """Small object façade for callers that prefer a named reducer."""

    version = REDUCER_VERSION

    @staticmethod
    def fold(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        return reduce_records(records)
