"""Fixture-only tests for the authenticated package-fast read surfaces."""

from __future__ import annotations

import ast
import json
from types import SimpleNamespace
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from homeassistant.package_fast.custom_components.package_fast import paging
from homeassistant.package_fast.custom_components.package_fast.paging import (
    JOURNAL_MAX_LIMIT,
    read_journal_page,
)
from homeassistant.package_fast.custom_components.package_fast.storage import (
    ShellStateStore,
)


COMPONENT_ROOT = (
    Path(__file__).resolve().parents[1] / "custom_components/package_fast"
)
WEBSOCKET_PATH = COMPONENT_ROOT / "websocket.py"
RUNTIME_PATH = COMPONENT_ROOT / "runtime.py"


def journal_record(
    episode_id: str,
    seq: int,
    marker: str,
    *,
    record_type: str | None = None,
    schema_version: int = 2,
) -> dict:
    return {
        "schema_version": schema_version,
        "record": record_type or ("episode_opened" if seq == 1 else "episode_closed"),
        "seq": seq,
        "episode_id": episode_id,
        "at_wall": f"2026-08-28T12:00:{marker}+00:00",
        "at_mono_ms": int(marker) * 1_000,
        "payload": {"marker": marker},
    }


def write_journal(path: Path, records: list[dict], *, tail: bytes = b"") -> None:
    path.write_bytes(
        b"".join(
            (json.dumps(record, sort_keys=True) + "\n").encode("utf-8")
            for record in records
        )
        + tail
    )


def function_source(path: Path, name: str) -> tuple[str, ast.AST]:
    source = path.read_text(encoding="utf-8")
    matches = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one function {name}, found {len(matches)}")
    return source, matches[0]


def decorator_name(value: ast.AST) -> str:
    if isinstance(value, ast.Call):
        value = value.func
    parts = []
    while isinstance(value, ast.Attribute):
        parts.append(value.attr)
        value = value.value
    if isinstance(value, ast.Name):
        parts.append(value.id)
    return ".".join(reversed(parts))


def websocket_schema(function: ast.AST) -> ast.Dict:
    for decorator in function.decorator_list:  # type: ignore[attr-defined]
        if decorator_name(decorator) != "websocket_api.websocket_command":
            continue
        schema = decorator.args[0]  # type: ignore[attr-defined]
        if not isinstance(schema, ast.Dict):
            raise AssertionError("websocket schema is not a literal mapping")
        return schema
    raise AssertionError("websocket command decorator is missing")


def schema_key_name(key: ast.AST) -> str | None:
    if isinstance(key, ast.Call) and key.args:
        key = key.args[0]
    if isinstance(key, ast.Constant) and isinstance(key.value, str):
        return key.value
    return None


def websocket_schema_keys(function: ast.AST) -> set[str]:
    return {
        name
        for key in websocket_schema(function).keys
        if (name := schema_key_name(key)) is not None
    }


def websocket_schema_validator(function: ast.AST, key_name: str) -> ast.AST:
    schema = websocket_schema(function)
    for key, validator in zip(schema.keys, schema.values):
        if schema_key_name(key) == key_name:
            return validator
    raise AssertionError(f"schema key {key_name} is missing")


class JournalPagingTests(unittest.TestCase):
    def test_default_limit_is_500(self):
        records = [
            journal_record("episode-a", index, f"{index:02d}")
            for index in range(1, 502)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, records)
            page = read_journal_page(journal)
        self.assertEqual(len(page["records"]), 500)
        self.assertEqual(page["next_seq"], 500)
        self.assertIs(page["truncated"], True)
        self.assertEqual(page["skipped"], 0)

    def test_since_limit_next_seq_and_truncated_use_file_order(self):
        records = [
            journal_record("episode-a", 1, "01"),
            journal_record("episode-a", 2, "02"),
            journal_record("episode-b", 1, "03"),
            journal_record("episode-b", 2, "04"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, records)
            first = read_journal_page(journal, since_seq=0, limit=3)
            second = read_journal_page(
                journal, since_seq=first["next_seq"], limit=3
            )
        self.assertEqual(
            [(item["episode_id"], item["seq"]) for item in first["records"]],
            [("episode-a", 1), ("episode-a", 2), ("episode-b", 1)],
        )
        self.assertEqual(first["next_seq"], 3)
        self.assertIs(first["truncated"], True)
        self.assertEqual(
            [(item["episode_id"], item["seq"]) for item in second["records"]],
            [("episode-b", 2)],
        )
        self.assertEqual(second["next_seq"], 4)
        self.assertIs(second["truncated"], False)

    def test_episode_filter_advances_through_bounded_candidate_windows(self):
        records = [
            journal_record("episode-a", 1, "01"),
            journal_record("episode-b", 1, "02"),
            journal_record("episode-a", 2, "03"),
            journal_record("episode-b", 2, "04"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, records)
            first = read_journal_page(journal, episode_id="episode-a", limit=1)
            second = read_journal_page(
                journal,
                since_seq=first["next_seq"],
                episode_id="episode-a",
                limit=1,
            )
            third = read_journal_page(
                journal,
                since_seq=second["next_seq"],
                episode_id="episode-a",
                limit=1,
            )
            exhausted = read_journal_page(
                journal,
                since_seq=third["next_seq"],
                episode_id="episode-a",
                limit=1,
            )
        self.assertEqual(first["records"], [records[0]])
        self.assertEqual(first["next_seq"], 1)
        self.assertIs(first["truncated"], True)
        self.assertEqual(second["records"], [])
        self.assertEqual(second["next_seq"], 2)
        self.assertIs(second["truncated"], True)
        self.assertEqual(third["records"], [records[2]])
        self.assertEqual(third["next_seq"], 3)
        self.assertIs(third["truncated"], True)
        self.assertEqual(exhausted["records"], [])
        self.assertEqual(exhausted["next_seq"], 4)
        self.assertIs(exhausted["truncated"], False)

    def test_limit_is_clamped_at_5000_for_direct_reader_calls(self):
        records = [
            journal_record("episode-a", index, f"{index:02d}")
            for index in range(1, JOURNAL_MAX_LIMIT + 2)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, records)
            page = read_journal_page(journal, limit=JOURNAL_MAX_LIMIT * 2)
        self.assertEqual(len(page["records"]), JOURNAL_MAX_LIMIT)
        self.assertEqual(page["next_seq"], JOURNAL_MAX_LIMIT)
        self.assertIs(page["truncated"], True)

    def test_torn_final_line_is_not_consumed_or_advanced_past(self):
        first = journal_record("episode-a", 1, "01")
        second = journal_record("episode-a", 2, "02")
        second_line = (json.dumps(second, sort_keys=True) + "\n").encode("utf-8")
        split = len(second_line) // 2
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, [first], tail=second_line[:split])
            torn = read_journal_page(journal, limit=10)
            with journal.open("ab") as handle:
                handle.write(second_line[split:])
            completed = read_journal_page(
                journal, since_seq=torn["next_seq"], limit=10
            )
        self.assertEqual(torn["records"], [first])
        self.assertEqual(torn["next_seq"], 1)
        self.assertEqual(torn["skipped"], 0)
        self.assertIs(torn["truncated"], False)
        self.assertEqual(completed["records"], [second])
        self.assertEqual(completed["next_seq"], 2)

    def test_malformed_line_is_counted_and_blocks_cursor_advance(self):
        first = journal_record("episode-a", 1, "01")
        third = journal_record("episode-a", 3, "03")
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, [first], tail=b"not-json\n")
            with journal.open("ab") as handle:
                handle.write((json.dumps(third) + "\n").encode("utf-8"))
            page = read_journal_page(journal, limit=10)
            repeated = read_journal_page(
                journal, since_seq=page["next_seq"], limit=10
            )
        self.assertEqual(page["records"], [first])
        self.assertEqual(page["next_seq"], 1)
        self.assertEqual(page["skipped"], 1)
        self.assertIs(page["truncated"], True)
        self.assertEqual(repeated["next_seq"], 1)
        self.assertEqual(repeated["skipped"], 1)

    def test_malformed_line_marker_is_counted_not_silently_consumed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, [{"_malformed_line": 1}])
            page = read_journal_page(journal, limit=10)
        self.assertEqual(page["records"], [])
        self.assertEqual(page["next_seq"], 0)
        self.assertEqual(page["skipped"], 1)

    def test_unknown_record_type_and_schema_version_are_returned(self):
        future = journal_record(
            "episode-a",
            1,
            "01",
            record_type="future_additive_record",
            schema_version=3,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "episodes.jsonl"
            write_journal(journal, [future])
            page = read_journal_page(journal, limit=10)
        self.assertEqual(page["records"], [future])
        self.assertEqual(page["skipped"], 0)

    def test_json_decode_cost_does_not_grow_with_trailing_file_size(self):
        small = [journal_record("episode-a", i, f"{i:02d}") for i in range(1, 11)]
        large = [
            journal_record("episode-a", i, f"{i:02d}")
            for i in range(1, 20_001)
        ]
        original_loads = json.loads
        with tempfile.TemporaryDirectory() as temp_dir:
            small_path = Path(temp_dir) / "small.jsonl"
            large_path = Path(temp_dir) / "large.jsonl"
            write_journal(small_path, small)
            write_journal(large_path, large)
            counts = []
            for journal in (small_path, large_path):
                with mock.patch.object(
                    paging.json, "loads", wraps=original_loads
                ) as loads:
                    page = read_journal_page(
                        journal,
                        since_seq=2,
                        episode_id="does-not-match",
                        limit=3,
                    )
                counts.append(loads.call_count)
                self.assertEqual(page["records"], [])
                self.assertEqual(page["next_seq"], 5)
        self.assertEqual(counts, [3, 3])


class WebSocketContractTests(unittest.TestCase):
    def test_both_commands_are_admin_gated(self):
        for name in ("websocket_journal", "websocket_health"):
            with self.subTest(name=name):
                _, function = function_source(WEBSOCKET_PATH, name)
                self.assertEqual(
                    [decorator_name(item) for item in function.decorator_list],  # type: ignore[attr-defined]
                    [
                        "websocket_api.require_admin",
                        "websocket_api.async_response",
                        "websocket_api.websocket_command",
                    ],
                )

    def test_neither_command_accepts_a_path_like_parameter(self):
        _, journal = function_source(WEBSOCKET_PATH, "websocket_journal")
        _, health = function_source(WEBSOCKET_PATH, "websocket_health")
        self.assertEqual(
            websocket_schema_keys(journal),
            {"type", "since_seq", "episode_id", "limit"},
        )
        self.assertEqual(websocket_schema_keys(health), {"type", "limit"})
        for keys in (websocket_schema_keys(journal), websocket_schema_keys(health)):
            self.assertFalse(
                any("path" in key.lower() or "file" in key.lower() for key in keys)
            )

    def test_limit_true_is_rejected_and_schema_has_hard_maximum(self):
        _, reject_node = function_source(WEBSOCKET_PATH, "_reject_bool")

        class FixtureInvalid(Exception):
            pass

        namespace = {
            "Any": Any,
            "vol": SimpleNamespace(Invalid=FixtureInvalid),
        }
        module = ast.Module(body=[reject_node], type_ignores=[])
        ast.fix_missing_locations(module)
        exec(compile(module, str(WEBSOCKET_PATH), "exec"), namespace)
        with self.assertRaises(FixtureInvalid):
            namespace["_reject_bool"](True)

        expected_maximums = {
            "websocket_journal": "JOURNAL_MAX_LIMIT",
            "websocket_health": "HEALTH_MAX_LIMIT",
        }
        for function_name, maximum in expected_maximums.items():
            with self.subTest(function=function_name):
                _, function = function_source(WEBSOCKET_PATH, function_name)
                validator = websocket_schema_validator(function, "limit")
                self.assertIsInstance(validator, ast.Call)
                self.assertEqual(decorator_name(validator), "vol.All")
                self.assertIsInstance(validator.args[0], ast.Name)
                self.assertEqual(validator.args[0].id, "_reject_bool")
                range_call = validator.args[-1]
                self.assertIsInstance(range_call, ast.Call)
                self.assertEqual(decorator_name(range_call), "vol.Range")
                keywords = {item.arg: item.value for item in range_call.keywords}
                self.assertEqual(ast.literal_eval(keywords["min"]), 1)
                self.assertIsInstance(keywords["max"], ast.Name)
                self.assertEqual(keywords["max"].id, maximum)

    def test_handlers_dispatch_disk_reads_to_the_executor(self):
        source = WEBSOCKET_PATH.read_text(encoding="utf-8")
        self.assertEqual(source.count("await hass.async_add_executor_job("), 2)
        for name in ("read_journal", "read_health"):
            runtime_source, function = function_source(RUNTIME_PATH, name)
            body = ast.get_source_segment(runtime_source, function)
            self.assertNotIn(".reduce(", body)
            self.assertNotIn("active_masks(", body)
            self.assertNotIn("write_", body)

    def test_detector_lock_is_never_held_across_file_reads(self):
        _, journal = function_source(RUNTIME_PATH, "read_journal")
        journal_locks = [
            node
            for node in ast.walk(journal)
            if isinstance(node, ast.With)
            and any(
                isinstance(item.context_expr, ast.Attribute)
                and isinstance(item.context_expr.value, ast.Name)
                and item.context_expr.value.id == "self"
                and item.context_expr.attr == "_lock"
                for item in node.items
            )
        ]
        self.assertEqual(journal_locks, [])

        _, health = function_source(RUNTIME_PATH, "read_health")
        health_locks = [
            node
            for node in ast.walk(health)
            if isinstance(node, ast.With)
            and any(
                isinstance(item.context_expr, ast.Attribute)
                and isinstance(item.context_expr.value, ast.Name)
                and item.context_expr.value.id == "self"
                and item.context_expr.attr == "_lock"
                for item in node.items
            )
        ]
        self.assertEqual(len(health_locks), 1)
        locked_nodes = set(ast.walk(health_locks[0]))
        disk_reads = [
            node
            for node in ast.walk(health)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "read_health"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "state_store"
        ]
        self.assertEqual(len(disk_reads), 1)
        self.assertNotIn(disk_reads[0], locked_nodes)

    def test_manifest_and_runtime_version_are_0_2_1(self):
        manifest = (COMPONENT_ROOT / "manifest.json").read_text(encoding="utf-8")
        constants = (COMPONENT_ROOT / "const.py").read_text(encoding="utf-8")
        self.assertIn('"version": "0.2.1"', manifest)
        self.assertIn('INTEGRATION_VERSION = "0.2.1"', constants)

    def test_reserved_diagnostics_module_was_renamed(self):
        self.assertFalse((COMPONENT_ROOT / "diagnostics.py").exists())
        self.assertTrue((COMPONENT_ROOT / "paging.py").is_file())
        for path in COMPONENT_ROOT.glob("*.py"):
            self.assertNotIn(".diagnostics", path.read_text(encoding="utf-8"))

    def test_entry_setup_registers_both_commands(self):
        setup = (COMPONENT_ROOT / "__init__.py").read_text(encoding="utf-8")
        websocket = WEBSOCKET_PATH.read_text(encoding="utf-8")
        self.assertIn("async_register_commands(hass)", setup)
        self.assertIn(
            "websocket_api.async_register_command(hass, websocket_journal)",
            websocket,
        )
        self.assertIn(
            "websocket_api.async_register_command(hass, websocket_health)",
            websocket,
        )


class HealthHistoryTests(unittest.TestCase):
    @staticmethod
    def append(store: ShellStateStore, index: int, *, source: str | None = None) -> None:
        payload = {"reason": f"reason-{index}"}
        if source is not None:
            payload["source"] = source
        store.append_health(
            {
                "record": "suspension" if index % 2 else "poll_gap",
                "at_wall": f"2026-08-28T12:00:{index:02d}+00:00",
                "at_mono_ms": index * 1_000,
                "payload": payload,
            }
        )

    def test_health_reader_bounds_notes_and_suspension_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ShellStateStore(temp_dir)
            for index in range(5):
                self.append(store, index)
            notes, suspensions, skipped, complete = store.read_health(
                note_limit=2, suspension_limit=1
            )
        self.assertEqual([note["at_mono_ms"] for note in notes], [3_000, 4_000])
        self.assertEqual(
            suspensions,
            [{"reason": "reason-3", "at_wall": "2026-08-28T12:00:03+00:00"}],
        )
        self.assertEqual(skipped, 0)
        self.assertIs(complete, True)

    def test_health_tail_cost_is_bounded_by_requested_window(self):
        original_loads = json.loads
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ShellStateStore(temp_dir)
            for index in range(5_000):
                self.append(store, index)
            with mock.patch.object(
                paging.json, "loads", wraps=original_loads
            ) as loads:
                notes, _, _, complete = store.read_health(
                    note_limit=2, suspension_limit=1
                )
        self.assertEqual(len(notes), 2)
        self.assertLessEqual(loads.call_count, 3)
        self.assertIs(complete, True)

    def test_sparse_suspension_history_reports_bounded_window_incomplete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ShellStateStore(temp_dir)
            for index in range(20):
                store.append_health(
                    {
                        "record": "poll_gap",
                        "at_wall": f"2026-08-28T12:00:{index:02d}+00:00",
                        "at_mono_ms": index * 1_000,
                        "payload": {},
                    }
                )
            _, suspensions, _, complete = store.read_health(
                note_limit=2, suspension_limit=1
            )
        self.assertEqual(suspensions, [])
        self.assertIs(complete, False)

    def test_torn_health_tail_is_not_returned(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ShellStateStore(temp_dir)
            self.append(store, 1)
            with store.health_path.open("ab") as handle:
                handle.write(b'{"record":"system_log"')
            notes, _, skipped, _ = store.read_health(
                note_limit=5, suspension_limit=1
            )
        self.assertEqual([note["at_mono_ms"] for note in notes], [1_000])
        self.assertEqual(skipped, 0)

    def test_health_source_is_reduced_to_basename_and_line(self):
        source = "('/config/custom_components/package_fast/runtime.py', 1130)"
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ShellStateStore(temp_dir)
            self.append(store, 1, source=source)
            notes, _, _, _ = store.read_health(note_limit=5, suspension_limit=1)
        self.assertEqual(notes[0]["payload"]["source"], "runtime.py:1130")
        self.assertNotIn("/config/", json.dumps(notes))


if __name__ == "__main__":
    unittest.main()
