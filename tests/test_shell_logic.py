# test_shell_logic.py — HA-free integration-shell policy tests.

from __future__ import annotations

import ast
import json
import sys
import tempfile
import unittest
from collections import deque
from io import BytesIO
from pathlib import Path
from unittest import mock

from PIL import Image

from homeassistant.package_fast.core import DetectorConfig
from homeassistant.package_fast.custom_components.package_fast.const import (
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_MAX_STORAGE_MB,
    DEFAULT_PERSIST_FRAMES,
    FETCH_P95_LIMIT_MS,
    MAX_ERROR_RATE,
    MIN_DISTINCT_FPS,
    POLL_GAP_LIMIT_MS,
    SENSOR_UPDATE_SECONDS,
    SLO_ERROR_BUDGET_INTERVAL_MS,
)
from homeassistant.package_fast.custom_components.package_fast.shell_logic import (
    FeedSuspectMonitor,
    RetentionEntry,
    SLOLimits,
    SlidingSLOMonitor,
    SuppressionMaskPolicy,
    bbox_iou,
    bounded_cache_put,
    heartbeat_can_advance,
    match_sol_episode,
    parse_sol_decision,
    percentile,
    plan_retention,
    relevant_system_log,
)
from homeassistant.package_fast.custom_components.package_fast.storage import (
    FramePersistenceError,
    FrameWriteSkipped,
    SparseFrame,
    SparseFrameStore,
)


RUNTIME_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components/package_fast/runtime.py"
)


def runtime_method_source(name):
    source = RUNTIME_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one runtime method {name}, found {len(matches)}")
    return ast.get_source_segment(source, matches[0])


class PercentileTests(unittest.TestCase):
    def test_nearest_rank_is_deterministic(self):
        self.assertEqual(percentile([], 0.95), 0.0)
        self.assertEqual(percentile(range(1, 21), 0.95), 19.0)


class ShellBoundaryTests(unittest.TestCase):
    def test_b1_frame_cache_reregistration_does_not_evict_a_live_frame(self):
        cache = {}
        order = deque()
        for key in ("a", "b", "c", "c", "d", "e", "f"):
            bounded_cache_put(cache, order, key, key, limit=4)
        self.assertEqual(list(order), ["c", "d", "e", "f"])
        self.assertEqual(list(cache), ["c", "d", "e", "f"])

    def test_b2_suspended_state_cannot_advance_heartbeat(self):
        self.assertFalse(heartbeat_can_advance("SUSPENDED"))
        self.assertTrue(heartbeat_can_advance("IDLE"))

    def test_b1_runtime_suspends_only_for_distinct_persistence_errors(self):
        persist = runtime_method_source("_persist_durable")
        process = runtime_method_source("_process_frame")
        self.assertNotIn("failed |=", persist)
        self.assertIn("except FramePersistenceError", process)
        self.assertNotIn("except FrameWriteSkipped", process)

    def test_b3_worker_survives_callbacks_and_stop_join_is_bounded(self):
        worker = runtime_method_source("_worker_loop")
        stop = runtime_method_source("async_stop")
        self.assertGreaterEqual(worker.count("except Exception"), 3)
        self.assertIn("record_executor_exception", worker)
        self.assertIn("asyncio.wait_for", stop)
        self.assertIn("QUEUE_JOIN_TIMEOUT_SECONDS", stop)

    def test_b4_listener_is_registered_before_state_resample(self):
        start = runtime_method_source("async_start")
        self.assertLess(
            start.index("async_track_state_change_event"),
            start.index("_resample_state_inputs"),
        )

    def test_n7_n8_n10_n12_runtime_wiring_is_canonical_and_shutdown_safe(self):
        prepare = runtime_method_source("_prepare_journal_records")
        sol = runtime_method_source("record_sol_decision")
        startup = runtime_method_source("async_start")
        recovery = runtime_method_source("take_startup_recovery")
        self.assertIn('"gaps_over_1500ms"', prepare)
        self.assertIn('record_type="sol_result"', sol)
        self.assertIn("self.journal.commit", sol)
        self.assertIn("EVENT_HOMEASSISTANT_STOP", startup)
        self.assertIn("_startup_recovery", recovery)


class SLOTests(unittest.TestCase):
    def setUp(self):
        self.limits = SLOLimits(
            minimum_samples=20,
            minimum_armed_span_ms=5_000,
            window_ms=60_000,
        )

    def _monitor(self, *, hashes=None, fetches=None, gaps=None, failures=()):
        hashes = hashes or [f"h{index}" for index in range(25)]
        fetches = fetches or [100.0] * len(hashes)
        gaps = gaps or [500.0] * len(hashes)
        monitor = SlidingSLOMonitor(self.limits)
        snapshot = None
        for index, content_hash in enumerate(hashes):
            success = index not in failures
            snapshot = monitor.record(
                at_mono_ms=index * 500,
                fetch_ms=fetches[index],
                gap_ms=gaps[index],
                success=success,
                content_hash=content_hash if success else None,
                armed=True,
            )
        assert snapshot is not None
        return snapshot

    def test_clean_two_hz_window_is_healthy(self):
        snapshot = self._monitor()
        self.assertTrue(snapshot.qualified)
        self.assertTrue(snapshot.healthy)
        self.assertAlmostEqual(snapshot.distinct_fps, 2.0)

    def test_fetch_p95_violation_is_named(self):
        fetches = [100.0] * 23 + [1_000.0, 1_000.0]
        snapshot = self._monitor(fetches=fetches)
        self.assertIn("fetch_p95", snapshot.violations)

    def test_gap_and_error_rates_fail_closed(self):
        gaps = [500.0] * 25
        gaps[4] = 1_501.0
        snapshot = self._monitor(gaps=gaps, failures={8})
        self.assertIn("poll_gap_rate", snapshot.violations)
        self.assertIn("fetch_error_rate", snapshot.violations)

    def test_static_armed_feed_violates_freshness(self):
        snapshot = self._monitor(hashes=["same"] * 25)
        self.assertEqual(snapshot.duplicate_count, 24)
        self.assertEqual(snapshot.distinct_fps, 0.0)
        self.assertIn("distinct_fps", snapshot.violations)

    def test_incomplete_window_cannot_auto_resume(self):
        monitor = SlidingSLOMonitor(self.limits)
        snapshot = monitor.record(
            at_mono_ms=0,
            fetch_ms=100,
            gap_ms=500,
            success=True,
            content_hash="one",
            armed=False,
        )
        self.assertFalse(snapshot.qualified)
        self.assertFalse(snapshot.healthy)

    def test_designed_two_second_idle_spacing_is_not_a_poll_gap(self):
        monitor = SlidingSLOMonitor(self.limits)
        for index in range(25):
            snapshot = monitor.record(
                at_mono_ms=index * 2_000,
                fetch_ms=100,
                gap_ms=2_000,
                success=True,
                content_hash=f"idle-{index}",
                armed=False,
            )
        self.assertEqual(snapshot.poll_gap_count, 0)
        self.assertNotIn("poll_gap_rate", snapshot.violations)

    def test_n1_error_budget_is_time_windowed_and_idle_rate_safe(self):
        def observe(failures):
            monitor = SlidingSLOMonitor()
            for index in range(60):
                success = index not in failures
                snapshot = monitor.record(
                    at_mono_ms=index * 2_000,
                    fetch_ms=100,
                    gap_ms=2_000,
                    success=success,
                    content_hash=f"idle-{index}" if success else None,
                    armed=False,
                )
            return snapshot

        one_error = observe({30})
        self.assertAlmostEqual(one_error.error_rate, 1 / 240)
        self.assertNotIn("fetch_error_rate", one_error.violations)
        self.assertIn("fetch_error_rate", observe({20, 40}).violations)


class FeedSuspectTests(unittest.TestCase):
    def test_static_streak_needs_a_person_edge(self):
        monitor = FeedSuspectMonitor(identical_frames=4, person_window_ms=5_000)
        for index in range(4):
            observation = monitor.observe("same", index * 500)
        self.assertFalse(observation.suspect)

        monitor.note_person_edge(2_000)
        for index in range(4):
            observation = monitor.observe("same", 2_500 + index * 500)
        self.assertTrue(observation.suspect)

    def test_changed_hash_clears_and_a_b_a_is_not_a_static_streak(self):
        monitor = FeedSuspectMonitor(identical_frames=3, person_window_ms=5_000)
        monitor.note_person_edge(0)
        for index, content_hash in enumerate(("A", "B", "A", "B", "A")):
            observation = monitor.observe(content_hash, index * 500)
            self.assertFalse(observation.suspect)
            self.assertEqual(observation.streak, 1)


class RetentionTests(unittest.TestCase):
    def test_age_then_byte_cap_prunes_oldest_non_keep(self):
        entries = (
            RetentionEntry("fixed", 0, 20, keep=True),
            RetentionEntry("old-keep", 0, 60, keep=True),
            RetentionEntry("old", 1_000, 30),
            RetentionEntry("middle", 8_000, 40),
            RetentionEntry("new", 9_000, 50),
        )
        plan = plan_retention(
            entries,
            now_ms=10_000,
            max_bytes=150,
            max_age_ms=5_000,
            incoming_bytes=20,
        )
        self.assertEqual(plan.delete_paths, ("old", "middle"))
        self.assertTrue(plan.allow_write)
        self.assertEqual(plan.bytes_after_write, 200 - 70 + 20)

    def test_keep_pressure_skips_incoming_write(self):
        plan = plan_retention(
            (RetentionEntry("labeled", 0, 101, keep=True),),
            now_ms=10_000,
            max_bytes=100,
            max_age_ms=1,
            incoming_bytes=1,
        )
        self.assertEqual(plan.delete_paths, ())
        self.assertFalse(plan.allow_write)


class SparseFrameStoreTests(unittest.TestCase):
    @staticmethod
    def _jpeg() -> bytes:
        output = BytesIO()
        Image.new("RGB", (20, 20), "white").save(output, format="JPEG")
        return output.getvalue()

    def test_baseline_and_decision_crop_follow_err07_layout(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = SparseFrameStore(
                temporary, max_bytes=1_000_000, max_age_days=21
            )
            episode = "01K3QCMQ00Q5M9E4XNPG9FWK2J"
            relative = f"episodes/2026/08/27/{episode}"
            store.register_episode(episode, relative)
            frame = SparseFrame(
                frame_id="f_00000001",
                at_wall="2026-08-27T16:00:00.000+00:00",
                at_mono_ms=1_000,
                sha256="abc",
                jpeg_bytes=self._jpeg(),
                bbox_full=(0, 0, 10, 10),
            )
            self.assertGreater(store.persist(episode, "baseline", frame), 0)
            self.assertGreater(store.persist(episode, "decision", frame), 0)

            episode_dir = Path(temporary) / relative
            self.assertTrue((episode_dir / "baseline.jpg").is_file())
            crops = list(episode_dir.glob("f_*_1000.jpg"))
            self.assertEqual(len(crops), 1)
            with Image.open(crops[0]) as crop:
                self.assertEqual(crop.size, (10, 10))

    def test_keep_pressure_raises_a_countable_skip(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            kept = root / "episodes/2026/08/26/kept"
            kept.mkdir(parents=True)
            (kept / "keep").touch()
            (kept / "payload.jpg").write_bytes(b"x" * 2_000)
            store = SparseFrameStore(root, max_bytes=1_000, max_age_days=21)
            episode = "01K3QCMQ00Q5M9E4XNPG9FWK2J"
            store.register_episode(episode, f"episodes/2026/08/27/{episode}")
            frame = SparseFrame(
                frame_id="f1",
                at_wall="2026-08-27T16:00:00.000+00:00",
                at_mono_ms=1_000,
                sha256="abc",
                jpeg_bytes=self._jpeg(),
            )
            with self.assertRaises(FrameWriteSkipped):
                store.persist(episode, "baseline", frame)
            self.assertTrue(kept.is_dir())

    def test_b1_oserror_is_a_distinct_fatal_persistence_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = SparseFrameStore(
                temporary, max_bytes=1_000_000, max_age_days=21
            )
            episode = "01K3QCMQ00Q5M9E4XNPG9FWK2J"
            store.register_episode(episode, f"episodes/2026/08/27/{episode}")
            frame = SparseFrame(
                frame_id="f1",
                at_wall="2026-08-27T16:00:00.000+00:00",
                at_mono_ms=1_000,
                sha256="abc",
                jpeg_bytes=self._jpeg(),
            )
            with mock.patch(
                "homeassistant.package_fast.custom_components.package_fast.storage._atomic_write",
                side_effect=OSError("disk unavailable"),
            ), self.assertRaises(FramePersistenceError):
                store.persist(episode, "baseline", frame)

    def test_b1_prune_delete_failure_uses_the_same_fatal_error_class(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            episode_dir = root / "episodes/2026/08/27/episode"
            episode_dir.mkdir(parents=True)
            (episode_dir / "frame.jpg").write_bytes(b"x")
            store = SparseFrameStore(root, max_bytes=1, max_age_days=21)
            with mock.patch(
                "homeassistant.package_fast.custom_components.package_fast.storage.shutil.rmtree",
                side_effect=OSError("delete failed"),
            ), self.assertRaises(FramePersistenceError):
                store.prune(now_ms=0, incoming_bytes=1)


class MaskPolicyTests(unittest.TestCase):
    def _policy(self):
        return SuppressionMaskPolicy(
            hits_required=3,
            window_ms=86_400_000,
            ttl_ms=86_400_000,
            iou_threshold=0.5,
        )

    def test_three_shadow_hits_create_one_decaying_mask(self):
        policy = self._policy()
        bbox = (0.1, 0.2, 0.2, 0.2)
        self.assertIsNone(policy.observe(bbox, at_ms=0, announce_eligible=False))
        self.assertIsNone(policy.observe(bbox, at_ms=1_000, announce_eligible=False))
        created = policy.observe(bbox, at_ms=2_000, announce_eligible=False)
        self.assertIsNotNone(created)
        assert created is not None
        self.assertEqual(created.bbox, bbox)
        self.assertEqual(len(policy.active_masks(2_000)), 1)
        self.assertEqual(policy.active_masks(created.expires_ms), ())

    def test_announce_eligible_region_never_seeds_a_mask(self):
        policy = self._policy()
        bbox = (0.1, 0.2, 0.2, 0.2)
        policy.observe(bbox, at_ms=0, announce_eligible=False)
        policy.observe(bbox, at_ms=1_000, announce_eligible=False)
        policy.observe(bbox, at_ms=2_000, announce_eligible=True)
        for at_ms in (3_000, 4_000, 5_000):
            self.assertIsNone(
                policy.observe(bbox, at_ms=at_ms, announce_eligible=False)
            )
        self.assertEqual(policy.active_masks(5_000), ())

    def test_b5_weak_margin_deposit_with_person_context_never_seeds_mask(self):
        policy = self._policy()
        bbox = (0.1, 0.2, 0.2, 0.2)
        for at_ms in (0, 1_000, 2_000):
            self.assertIsNone(
                policy.observe(
                    bbox,
                    at_ms=at_ms,
                    announce_eligible=False,
                    person_context_present=True,
                )
            )
        self.assertEqual(policy.active_masks(2_000), ())

    def test_policy_state_round_trips_across_restart(self):
        policy = self._policy()
        bbox = (0.1, 0.2, 0.2, 0.2)
        for at_ms in (0, 1_000, 2_000):
            policy.observe(bbox, at_ms=at_ms, announce_eligible=False)
        restored = self._policy()
        restored.restore_state(policy.to_state(), now_ms=3_000)
        self.assertEqual(restored.active_masks(3_000), policy.active_masks(3_000))

    def test_bbox_iou_uses_normalized_xywh(self):
        self.assertEqual(bbox_iou((0, 0, 0.5, 0.5), (0, 0, 0.5, 0.5)), 1.0)
        self.assertEqual(bbox_iou((0, 0, 0.1, 0.1), (0.5, 0.5, 0.1, 0.1)), 0.0)


class SolJoinTests(unittest.TestCase):
    def test_lane_specific_decision_parser_rejects_garage_cross_talk(self):
        early = parse_sol_decision(
            "FRONT EARLY | delivery | 94% | visible parcel", "early"
        )
        self.assertEqual((early.label, early.confidence), ("delivery", 0.94))
        self.assertIsNone(
            parse_sol_decision("GARAGE | delivery | 99% | crate", "final")
        )

    def test_timestamp_join_prefers_open_then_most_recent_episode(self):
        episodes = {
            "closed": {
                "opened_at_wall": "2026-08-27T16:00:00+00:00",
                "closed_at_wall": "2026-08-27T16:01:00+00:00",
            },
            "open": {
                "opened_at_wall": "2026-08-27T16:02:00+00:00",
            },
        }
        self.assertEqual(
            match_sol_episode(
                episodes,
                "2026-08-27T16:03:00+00:00",
                maximum_age_ms=600_000,
            ),
            "open",
        )

class SoakFilterTests(unittest.TestCase):
    def test_system_log_filter_selects_domain_and_blocking_warnings(self):
        self.assertTrue(relevant_system_log({"message": "package_fast failed"}))
        self.assertTrue(
            relevant_system_log({"message": "Detected blocking call to open"})
        )
        self.assertFalse(relevant_system_log({"message": "ordinary warning"}))

    def test_import_does_not_load_home_assistant_runtime(self):
        self.assertNotIn("homeassistant.core", sys.modules)
        self.assertNotIn("homeassistant.config_entries", sys.modules)


class PackagingContractTests(unittest.TestCase):
    def test_shell_defaults_match_core_and_err08_design(self):
        config = DetectorConfig()
        self.assertEqual(config.idle_rate_hz, 0.5)
        self.assertEqual(config.armed_rate_hz, 2.0)
        self.assertFalse(DEFAULT_PERSIST_FRAMES)
        self.assertEqual(DEFAULT_MAX_STORAGE_MB, 1_536)
        self.assertEqual(DEFAULT_MAX_AGE_DAYS, 21)

    def test_n2_phase_zero_slo_constants_are_pinned(self):
        config = DetectorConfig()
        self.assertEqual(FETCH_P95_LIMIT_MS, 900.0)
        self.assertEqual(MIN_DISTINCT_FPS, 1.5)
        self.assertEqual(MAX_ERROR_RATE, 0.005)
        self.assertEqual(POLL_GAP_LIMIT_MS, 1_500.0)
        self.assertEqual(config.poll_gap_seconds, 1.5)
        self.assertEqual(SLO_ERROR_BUDGET_INTERVAL_MS, 500)
        self.assertGreaterEqual(SENSOR_UPDATE_SECONDS, 30.0)

    def test_n4_manifest_orders_camera_and_enforces_single_entry(self):
        package_root = Path(__file__).resolve().parents[1]
        manifest = json.loads(
            (
                package_root
                / "custom_components/package_fast/manifest.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["dependencies"], ["camera"])
        self.assertIs(manifest["single_config_entry"], True)

    def test_n9_runtime_import_is_dispatched_to_import_executor(self):
        package_root = Path(__file__).resolve().parents[1]
        setup_source = (
            package_root / "custom_components/package_fast/__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("hass.async_add_import_executor_job(_import_runtime)", setup_source)

    def test_hacs_vendored_core_is_byte_identical(self):
        package_root = Path(__file__).resolve().parents[1]
        canonical = package_root / "core"
        vendored = package_root / "custom_components/package_fast/core"
        for filename in (
            "__init__.py",
            "config.py",
            "detector.py",
            "envelopes.py",
            "journal.py",
            "pipeline.py",
        ):
            with self.subTest(filename=filename):
                self.assertEqual(
                    (vendored / filename).read_bytes(),
                    (canonical / filename).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
