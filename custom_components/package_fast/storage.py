"""Synchronous disk adapters used only from Home Assistant executor jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
import json
import os
from pathlib import Path
import shutil
from typing import Any, Callable, Mapping

from PIL import Image

from .shell_logic import RetentionEntry, plan_retention


class FrameWriteSkipped(RuntimeError):
    """Signal a privacy/cap-safe skipped frame write to ``Journal``."""


class FramePersistenceError(OSError):
    """Signal a real filesystem failure, distinct from a safe skipped write."""


@dataclass(frozen=True, slots=True)
class SparseFrame:
    """One full JPEG or decision crop offered to the sparse-frame callback."""

    frame_id: str
    at_wall: str
    at_mono_ms: int
    sha256: str
    jpeg_bytes: bytes
    bbox_full: tuple[int, int, int, int] | None = None


def _tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def _atomic_write(path: Path, content: bytes) -> int:
    """Atomically replace one file and return its size delta."""

    path.parent.mkdir(parents=True, exist_ok=True)
    before = path.stat().st_size if path.exists() else 0
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path.stat().st_size - before


class SparseFrameStore:
    """ERR-07 layout plus ERR-08-gated sparse JPEG retention."""

    def __init__(self, root: str | Path, *, max_bytes: int, max_age_days: float) -> None:
        if max_bytes <= 0 or max_age_days <= 0:
            raise ValueError("positive frame retention limits are required")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_bytes)
        self.max_age_ms = int(max_age_days * 86_400_000)
        self._episode_dirs: dict[str, str] = {}
        self._account_external: Callable[[int], None] | None = None

    def bind_accounting(self, callback: Callable[[int], None]) -> None:
        """Connect pruning deltas to the core journal's store-byte counter."""

        self._account_external = callback

    def register_episode(self, episode_id: str, frames_dir: str) -> None:
        """Register the core-provided relative frame directory after commit."""

        if not episode_id or not frames_dir:
            raise ValueError("episode id and frame directory are required")
        resolved = (self.root / frames_dir).resolve()
        root = self.root.resolve()
        episodes = (root / "episodes").resolve()
        if not resolved.is_relative_to(episodes):
            raise ValueError("frames_dir escapes the episodes root")
        self._episode_dirs[episode_id] = resolved.relative_to(root).as_posix()

    def restore_episode_dirs(self, reduced: Mapping[str, Any]) -> None:
        """Rebuild the episode/path cache from the canonical reduced journal."""

        episodes = reduced.get("episodes", {})
        if not isinstance(episodes, Mapping):
            return
        for episode_id, episode in episodes.items():
            if not isinstance(episode_id, str) or not isinstance(episode, Mapping):
                continue
            frames_dir = episode.get("frames_dir")
            if isinstance(frames_dir, str) and frames_dir:
                try:
                    self.register_episode(episode_id, frames_dir)
                except ValueError:
                    continue

    def _target_dir(self, episode_id: str) -> Path:
        relative = self._episode_dirs.get(episode_id)
        if relative is None:
            raise FrameWriteSkipped(f"unknown episode directory: {episode_id}")
        target = (self.root / relative).resolve()
        if not target.is_relative_to((self.root.resolve() / "episodes")):
            raise FrameWriteSkipped("episode directory escaped retention root")
        return target

    def active_episode_path(self, episode_id: str | None) -> Path | None:
        """Return a validated retention-protection path for an open episode."""

        if episode_id is None or episode_id not in self._episode_dirs:
            return None
        return self._target_dir(episode_id)

    @staticmethod
    def _index(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {"schema_version": 1, "frames": []}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {"schema_version": 1, "frames": []}
        if not isinstance(value, dict) or not isinstance(value.get("frames"), list):
            return {"schema_version": 1, "frames": []}
        return value

    @staticmethod
    def _frame_bytes(role: str, frame: SparseFrame) -> tuple[bytes, bool]:
        if role != "decision" or frame.bbox_full is None:
            return frame.jpeg_bytes, False
        try:
            with Image.open(BytesIO(frame.jpeg_bytes)) as image:
                left, top, right, bottom = frame.bbox_full
                left = max(0, min(image.width - 1, int(left)))
                top = max(0, min(image.height - 1, int(top)))
                right = max(left + 1, min(image.width, int(right)))
                bottom = max(top + 1, min(image.height, int(bottom)))
                crop = image.convert("RGB").crop((left, top, right, bottom))
                output = BytesIO()
                crop.save(output, format="JPEG", quality=92, optimize=True)
                return output.getvalue(), True
        except Exception as error:
            raise FrameWriteSkipped("decision crop could not be encoded") from error

    def _retention_entries(self, protect: Path | None) -> list[RetentionEntry]:
        episode_root = self.root / "episodes"
        episode_dirs = [
            path
            for path in episode_root.glob("*/*/*/*")
            if path.is_dir()
        ] if episode_root.exists() else []
        episode_size = 0
        entries: list[RetentionEntry] = []
        for path in episode_dirs:
            size = _tree_size(path)
            episode_size += size
            timestamps = [
                child.stat().st_mtime_ns // 1_000_000
                for child in path.rglob("*")
                if child.is_file()
            ]
            modified_ms = min(timestamps) if timestamps else path.stat().st_mtime_ns // 1_000_000
            keep = (path / "keep").exists() or (protect is not None and path == protect)
            entries.append(
                RetentionEntry(
                    path=path.relative_to(self.root).as_posix(),
                    modified_ms=modified_ms,
                    size_bytes=size,
                    keep=keep,
                )
            )
        fixed_size = max(0, _tree_size(self.root) - episode_size)
        entries.append(
            RetentionEntry(
                path="__fixed__",
                modified_ms=0,
                size_bytes=fixed_size,
                keep=True,
            )
        )
        return entries

    def prune(self, *, now_ms: int, incoming_bytes: int = 0, protect: Path | None = None) -> bool:
        """Apply oldest-first retention and report whether incoming bytes fit."""

        try:
            return self._prune(
                now_ms=now_ms,
                incoming_bytes=incoming_bytes,
                protect=protect,
            )
        except FramePersistenceError:
            raise
        except OSError as error:
            raise FramePersistenceError("frame retention pruning failed") from error

    def _prune(
        self,
        *,
        now_ms: int,
        incoming_bytes: int = 0,
        protect: Path | None = None,
    ) -> bool:
        """Unwrapped retention implementation used by the fatal-error boundary."""

        plan = plan_retention(
            self._retention_entries(protect),
            now_ms=now_ms,
            max_bytes=self.max_bytes,
            max_age_ms=self.max_age_ms,
            incoming_bytes=max(0, incoming_bytes),
        )
        root = self.root.resolve()
        for relative in plan.delete_paths:
            if relative == "__fixed__":
                continue
            target = (root / relative).resolve()
            if not target.is_relative_to(root / "episodes") or not target.is_dir():
                continue
            size = _tree_size(target)
            try:
                shutil.rmtree(target)
            except OSError as error:
                raise FramePersistenceError(
                    f"could not prune frame directory: {relative}"
                ) from error
            if size and self._account_external is not None:
                self._account_external(-size)
        return plan.allow_write

    def persist(self, episode_id: str, role: str, value: Any) -> int | None:
        """Journal callback for baseline/first/confirm/decision/trip/final roles."""

        try:
            return self._persist(episode_id, role, value)
        except (FrameWriteSkipped, FramePersistenceError):
            raise
        except OSError as error:
            raise FramePersistenceError("sparse frame persistence failed") from error

    def _persist(self, episode_id: str, role: str, value: Any) -> int | None:
        """Unwrapped sparse-frame implementation used by the error boundary."""

        if not isinstance(value, SparseFrame):
            raise TypeError("sparse frame callback requires SparseFrame")
        if role not in {"baseline", "first_seen", "confirm", "decision", "per_trip", "final"}:
            raise ValueError(f"unsupported sparse frame role: {role}")

        target_dir = self._target_dir(episode_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        index_path = target_dir / "frames.json"
        index = self._index(index_path)
        identity = {
            "role": role,
            "frame_id": value.frame_id,
            "bbox_full": list(value.bbox_full) if value.bbox_full is not None else None,
        }
        if any(
            all(entry.get(key) == expected for key, expected in identity.items())
            for entry in index["frames"]
            if isinstance(entry, dict)
        ):
            return 0

        sequence = 0 if role == "baseline" else 1 + max(
            (int(entry.get("seq", 0)) for entry in index["frames"] if isinstance(entry, dict)),
            default=0,
        )
        filename = (
            "baseline.jpg"
            if role == "baseline"
            else f"f_{sequence:04d}_{value.at_mono_ms}.jpg"
        )
        target = target_dir / filename
        if role == "baseline" and target.exists():
            return 0

        jpeg_bytes, cropped = self._frame_bytes(role, value)
        entry = {
            "seq": sequence,
            "role": role,
            "frame_id": value.frame_id,
            "at_wall": value.at_wall,
            "at_mono_ms": value.at_mono_ms,
            "sha256": value.sha256,
            "path": filename,
            "cropped": cropped,
            "bbox_full": list(value.bbox_full) if value.bbox_full is not None else None,
        }
        index["frames"].append(entry)
        index_bytes = (
            json.dumps(index, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("utf-8")
        old_target_size = target.stat().st_size if target.exists() else 0
        old_index_size = index_path.stat().st_size if index_path.exists() else 0
        incoming = max(
            0,
            len(jpeg_bytes) - old_target_size + len(index_bytes) - old_index_size,
        )
        now_ms = int(datetime.now().timestamp() * 1_000)
        if not self.prune(now_ms=now_ms, incoming_bytes=incoming, protect=target_dir):
            raise FrameWriteSkipped("frame store is at its keep-protected byte cap")

        wrote_target = False
        try:
            target_delta = _atomic_write(target, jpeg_bytes)
            wrote_target = True
            index_delta = _atomic_write(index_path, index_bytes)
        except Exception:
            if wrote_target and old_target_size == 0:
                target.unlink(missing_ok=True)
            raise
        return target_delta + index_delta


class ShellStateStore:
    """Derived daily metrics, mask state, and ERR-11 health-note journal."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.metrics_dir = self.root / "metrics"
        self.state_dir = self.root / "state"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.health_path = self.metrics_dir / "health.jsonl"
        self.health_path.touch(exist_ok=True)
        self.mask_path = self.state_dir / "suppression_masks.json"

    def load_daily(self, date: str) -> dict[str, Any]:
        path = self.metrics_dir / f"daily-{date}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def write_daily(self, date: str, metrics: Mapping[str, Any]) -> int:
        path = self.metrics_dir / f"daily-{date}.json"
        content = (
            json.dumps(dict(metrics), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("utf-8")
        return _atomic_write(path, content)

    def append_health(self, record: Mapping[str, Any]) -> int:
        content = (
            json.dumps(dict(record), sort_keys=True, separators=(",", ":"), default=str)
            + "\n"
        ).encode("utf-8")
        with self.health_path.open("ab") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        return len(content)

    def load_masks(self) -> dict[str, Any]:
        try:
            value = json.loads(self.mask_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def write_masks(self, state: Mapping[str, Any]) -> int:
        content = (
            json.dumps(dict(state), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("utf-8")
        return _atomic_write(self.mask_path, content)


__all__ = [
    "FramePersistenceError",
    "FrameWriteSkipped",
    "ShellStateStore",
    "SparseFrame",
    "SparseFrameStore",
]
