"""Bounded, append-safe helpers for package-fast read surfaces."""

from __future__ import annotations

import ast
from collections.abc import Iterator, Mapping
import json
from pathlib import Path
import re
from typing import Any, BinaryIO


JOURNAL_DEFAULT_LIMIT = 500
JOURNAL_MAX_LIMIT = 5_000
HEALTH_DEFAULT_LIMIT = 50
HEALTH_MAX_LIMIT = 500
SUSPENSION_HISTORY_LIMIT = 50

_REVERSE_READ_SIZE = 64 * 1_024


def clamp_limit(value: int, *, default: int, maximum: int) -> int:
    """Validate a positive integer limit and clamp it to a hard maximum."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("limit must be a positive integer")
    if default < 1 or maximum < default:
        raise ValueError("invalid limit bounds")
    return min(value, maximum)


def _journal_cursor(value: int | None) -> int:
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("since_seq must be a non-negative integer")
    return value


def _is_journal_envelope(value: Mapping[str, Any]) -> bool:
    """Return whether a value has the additive ERR-07 envelope structure."""

    schema_version = value.get("schema_version")
    seq = value.get("seq")
    at_mono_ms = value.get("at_mono_ms")
    return bool(
        isinstance(schema_version, int)
        and not isinstance(schema_version, bool)
        and isinstance(value.get("record"), str)
        and value.get("record")
        and isinstance(seq, int)
        and not isinstance(seq, bool)
        and isinstance(value.get("episode_id"), str)
        and value.get("episode_id")
        and isinstance(value.get("at_wall"), str)
        and value.get("at_wall")
        and isinstance(at_mono_ms, int)
        and not isinstance(at_mono_ms, bool)
        and isinstance(value.get("payload"), Mapping)
    )


def _last_complete_end(handle: BinaryIO, snapshot_size: int) -> int:
    """Return the byte just after the last newline in a size snapshot."""

    position = snapshot_size
    while position > 0:
        start = max(0, position - _REVERSE_READ_SIZE)
        handle.seek(start)
        chunk = handle.read(position - start)
        newline = chunk.rfind(b"\n")
        if newline >= 0:
            return start + newline + 1
        position = start
    return 0


def _snapshot_complete_end(path: Path, handle: BinaryIO) -> int:
    """Snapshot a path size and cap it to bytes visible through ``handle``."""

    snapshot_size = path.stat().st_size
    handle.seek(0, 2)
    readable_size = min(snapshot_size, handle.tell())
    return _last_complete_end(handle, readable_size)


def read_journal_page(
    path: str | Path,
    *,
    since_seq: int | None = None,
    episode_id: str | None = None,
    limit: int = JOURNAL_DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Read one bounded, complete-line page from an append-only journal.

    Core envelope ``seq`` values restart for every episode. Therefore the
    public ``since_seq``/``next_seq`` pair is an exclusive, one-based cursor
    over physical journal lines. Returned envelopes retain their original
    per-episode ``seq`` values verbatim.
    """

    cursor = _journal_cursor(since_seq)
    if episode_id is not None and (
        not isinstance(episode_id, str) or not episode_id
    ):
        raise ValueError("episode_id must be a non-empty string")
    page_limit = clamp_limit(
        limit, default=JOURNAL_DEFAULT_LIMIT, maximum=JOURNAL_MAX_LIMIT
    )

    journal_path = Path(path)
    with journal_path.open("rb") as handle:
        complete_end = _snapshot_complete_end(journal_path, handle)
        handle.seek(0)
        position = 0
        while position < cursor and handle.tell() < complete_end:
            encoded = handle.readline(complete_end - handle.tell())
            if not encoded.endswith(b"\n"):
                break
            position += 1

        if position < cursor:
            return {
                "records": [],
                "next_seq": cursor,
                "truncated": False,
                "skipped": 0,
                "cursor_stale": True,
            }

        records: list[dict[str, Any]] = []
        next_seq = cursor
        skipped = 0
        blocked = False
        candidates = 0
        while handle.tell() < complete_end and candidates < page_limit:
            encoded = handle.readline(complete_end - handle.tell())
            if not encoded.endswith(b"\n"):
                break
            candidates += 1
            candidate_position = position + 1
            try:
                value = json.loads(encoded)
            except (UnicodeDecodeError, json.JSONDecodeError):
                skipped += 1
                blocked = True
                break
            if not isinstance(value, Mapping) or not _is_journal_envelope(value):
                skipped += 1
                blocked = True
                break

            position = candidate_position
            next_seq = position
            if episode_id is not None and value.get("episode_id") != episode_id:
                continue
            records.append(dict(value))

        return {
            "records": records,
            "next_seq": next_seq,
            "truncated": blocked or handle.tell() < complete_end,
            "skipped": skipped,
            "cursor_stale": False,
        }


def _iter_lines_reverse(handle: BinaryIO, end: int) -> Iterator[bytes]:
    """Yield complete lines newest-first without materializing the file."""

    position = end
    remainder = b""
    first_chunk = True
    while position > 0:
        start = max(0, position - _REVERSE_READ_SIZE)
        handle.seek(start)
        chunk = handle.read(position - start)
        position = start
        parts = (chunk + remainder).split(b"\n")
        remainder = parts[0]
        complete = parts[1:]
        if first_chunk and complete and complete[-1] == b"":
            complete.pop()
        first_chunk = False
        yield from reversed(complete)
    if remainder or end > 0:
        yield remainder


def _basename_with_line(path_value: Any, line_value: Any = None) -> str:
    raw_path = str(path_value).strip().strip("'\"")
    basename = re.split(r"[/\\]", raw_path)[-1].strip().strip("'\"")
    if not basename:
        basename = "<unknown>"
    if isinstance(line_value, int) and not isinstance(line_value, bool):
        return f"{basename}:{line_value}"
    if isinstance(line_value, str) and line_value.isdigit():
        return f"{basename}:{line_value}"
    return basename


def redact_source(value: Any) -> str:
    """Reduce a Home Assistant source tuple or path to basename and line."""

    if isinstance(value, (tuple, list)) and value:
        line_value = value[1] if len(value) > 1 else None
        return _basename_with_line(value[0], line_value)

    text = str(value).strip()
    try:
        parsed = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        parsed = None
    if isinstance(parsed, (tuple, list)) and parsed:
        line_value = parsed[1] if len(parsed) > 1 else None
        return _basename_with_line(parsed[0], line_value)

    colon_match = re.fullmatch(r"(.+?):(\d+)", text)
    if colon_match is not None:
        return _basename_with_line(colon_match.group(1), colon_match.group(2))
    if "/" in text or "\\" in text:
        line_match = re.search(
            r"(?:[:,]\s*|\bline\s+)(\d+)\D*$", text, re.IGNORECASE
        )
        basename_match = re.match(
            r"[A-Za-z0-9_.-]+", re.split(r"[/\\]", text)[-1].lstrip("'\"<")
        )
        basename = basename_match.group(0) if basename_match else "<redacted>"
        return _basename_with_line(
            basename, line_match.group(1) if line_match is not None else None
        )
    return text[:1_024]


def _redact_health_record(value: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(value)
    if "source" in record:
        record["source"] = redact_source(record["source"])
    payload = record.get("payload")
    if isinstance(payload, Mapping) and "source" in payload:
        redacted_payload = dict(payload)
        redacted_payload["source"] = redact_source(payload["source"])
        record["payload"] = redacted_payload
    return record


def read_health_tail(
    path: str | Path, *, note_limit: int, suspension_limit: int
) -> tuple[list[dict[str, Any]], list[dict[str, str]], int, bool]:
    """Read a bounded newest-first window from the append-only health log."""

    if (
        isinstance(note_limit, bool)
        or not isinstance(note_limit, int)
        or note_limit < 1
        or isinstance(suspension_limit, bool)
        or not isinstance(suspension_limit, int)
        or suspension_limit < 1
    ):
        raise ValueError("health history limits must be positive integers")

    health_path = Path(path)
    newest_notes: list[dict[str, Any]] = []
    newest_suspensions: list[dict[str, str]] = []
    skipped = 0
    scan_limit = note_limit + suspension_limit
    more_history = False
    with health_path.open("rb") as handle:
        complete_end = _snapshot_complete_end(health_path, handle)
        for index, encoded in enumerate(_iter_lines_reverse(handle, complete_end)):
            if index >= scan_limit:
                more_history = True
                break
            try:
                value = json.loads(encoded)
            except (UnicodeDecodeError, json.JSONDecodeError):
                skipped += 1
                continue
            if not isinstance(value, Mapping):
                skipped += 1
                continue
            record = _redact_health_record(value)
            if len(newest_notes) < note_limit:
                newest_notes.append(record)
            if (
                len(newest_suspensions) < suspension_limit
                and record.get("record") in {"suspension", "core_suspension"}
            ):
                payload = record.get("payload")
                at_wall = record.get("at_wall")
                reason = payload.get("reason") if isinstance(payload, Mapping) else None
                if isinstance(reason, str) and isinstance(at_wall, str):
                    newest_suspensions.append(
                        {"reason": reason, "at_wall": at_wall}
                    )
            if (
                len(newest_notes) == note_limit
                and len(newest_suspensions) == suspension_limit
            ):
                break
    suspensions_complete = (
        len(newest_suspensions) == suspension_limit or not more_history
    )
    return (
        list(reversed(newest_notes)),
        list(reversed(newest_suspensions)),
        skipped,
        suspensions_complete,
    )


__all__ = [
    "HEALTH_DEFAULT_LIMIT",
    "HEALTH_MAX_LIMIT",
    "JOURNAL_DEFAULT_LIMIT",
    "JOURNAL_MAX_LIMIT",
    "SUSPENSION_HISTORY_LIMIT",
    "clamp_limit",
    "read_health_tail",
    "read_journal_page",
    "redact_source",
]
