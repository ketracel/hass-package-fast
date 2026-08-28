# test_journal.py — Typed-log durability, reduction, recovery, and privacy tests.
#
# Rationale: ERRATA.md ERR-07 is a one-way-door contract.  These tests pin the
# schema-v2 envelope, per-episode sequence, first-wins/idempotent fold rules,
# commit-before-publication failure behavior, derived meta, and restart close.

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from homeassistant.package_fast.core.envelopes import DetectionEnvelope
from homeassistant.package_fast.core.journal import (
    REDUCER_VERSION,
    Journal,
    reduce_records,
)


WALL = "2026-08-27T16:00:00.000+00:00"
EPISODE = "01K3QCMQ00Q5M9E4XNPG9FWK2J"


def opened(episode_id: str = EPISODE) -> DetectionEnvelope:
    return DetectionEnvelope(
        record_type="episode_opened",
        episode_id=episode_id,
        at_wall=WALL,
        at_mono_ms=1_000,
        lifecycle_payload={
            "opened_by": "g4_person",
            "test": False,
            "g4_event_ids": ["event"],
            "person_context": {"g4_on_at": WALL},
            "baseline": {"frame_ids": ["f0"], "sha256": "base"},
            "frames_dir": f"episodes/2026/08/27/{episode_id}",
            "poll_config": {"armed_rate_hz": 2.0},
            "detector": {"algorithm_version": "0.3.0", "config_digest": "sha256:x"},
        },
    )


def detection(episode_id: str = EPISODE, detection_id: str = "d_01") -> DetectionEnvelope:
    return DetectionEnvelope(
        record_type="detection",
        episode_id=episode_id,
        at_wall=WALL,
        at_mono_ms=2_000,
        detection_id=detection_id,
        kind="deposit",
        bbox_norm=(0.2, 0.3, 0.1, 0.1),
        bbox_full=(192, 216, 288, 288),
        area_frac=0.01,
        polarity="added",
        first_seen_frame="f1",
        confirmed_frame="f3",
        confirm_count=3,
        frame_sha256s={"f1": "one", "f3": "three"},
        scores={"edge_gain": 0.4, "stability_sad": 0.0, "illum_guard": "clear"},
        latency_ms_from_first_visible=1_000,
        announce_eligible=True,
        person_context_present=True,
    )


def fast_result(episode_id: str = EPISODE) -> DetectionEnvelope:
    return DetectionEnvelope(
        record_type="fast_result",
        episode_id=episode_id,
        at_wall=WALL,
        at_mono_ms=2_000,
        lifecycle_payload={
            "label": "delivery",
            "decided_at_wall": WALL,
            "decided_at_mono_ms": 2_000,
        },
    )


def closed(episode_id: str = EPISODE, reason: str = "quiet_75s") -> DetectionEnvelope:
    return DetectionEnvelope(
        record_type="episode_closed",
        episode_id=episode_id,
        at_wall=WALL,
        at_mono_ms=77_000,
        lifecycle_payload={"close_reason": reason, "poll": {}, "metrics": {}},
    )


class JournalWriterTests(unittest.TestCase):
    def test_commit_assigns_per_episode_sequence_and_writes_derived_meta(self):
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary)
            staged = [opened(), detection(), fast_result(), closed()]
            self.assertEqual(journal.commit(staged), staged)
            records = journal.records()
            self.assertEqual([record["seq"] for record in records], [1, 2, 3, 4])
            self.assertTrue(all(record["schema_version"] == 2 for record in records))
            meta = Path(temporary) / f"episodes/2026/08/27/{EPISODE}/meta.json"
            self.assertTrue(meta.is_file())
            self.assertGreater(journal.store_bytes, 0)
            self.assertEqual(journal.metrics_snapshot()["reducer_version"], REDUCER_VERSION)

    def test_sequence_is_independent_per_episode(self):
        other = "01K3QCMQ00Q5M9E4XNPG9FWK2K"
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary)
            journal.commit([opened(), opened(other), detection(), detection(other)])
            self.assertEqual(
                [(record["episode_id"], record["seq"]) for record in journal.records()],
                [(EPISODE, 1), (other, 1), (EPISODE, 2), (other, 2)],
            )

    def test_fault_before_fsync_returns_nothing_and_rolls_back_tail(self):
        def fault(stage, context):
            if stage == "before_fsync" and any(
                record["record"] == "detection" for record in context["records"]
            ):
                raise OSError("injected disk failure")

        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary, fault_hook=fault)
            self.assertEqual(journal.commit([opened()]), [opened()])
            self.assertEqual(journal.commit([detection(), fast_result()]), [])
            self.assertEqual([record["record"] for record in journal.records()], ["episode_opened"])
            self.assertEqual(journal.journal_write_failures, 1)

    def test_ground_truth_block_is_rejected(self):
        bad = DetectionEnvelope(
            record_type="episode_opened",
            episode_id=EPISODE,
            at_wall=WALL,
            at_mono_ms=1_000,
            lifecycle_payload={"ground_truth": {"label": "delivery"}},
        )
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary)
            self.assertEqual(journal.commit([bad]), [])
            self.assertEqual(journal.records(), [])

    def test_frame_persistence_is_cleanly_disableable(self):
        calls = []

        def callback(episode_id, role, frame):
            calls.append((episode_id, role, frame))
            return 123

        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(
                temporary,
                frame_persistence_enabled=False,
                frame_persist_callback=callback,
            )
            self.assertFalse(journal.persist_frame(EPISODE, "confirm", object()))
            self.assertEqual(calls, [])
            self.assertEqual(journal.shadow_write_skips, 0)
            journal.note_shadow_write_skip()
            self.assertEqual(journal.shadow_write_skips, 1)


class ReducerTests(unittest.TestCase):
    def test_first_wins_and_detection_is_idempotent(self):
        base_records = []
        for seq, envelope in enumerate(
            [opened(), opened(), detection(), detection(), fast_result(), fast_result(), closed()],
            1,
        ):
            base_records.append(
                {
                    "schema_version": 2,
                    "record": envelope.record_type,
                    "seq": seq,
                    "episode_id": envelope.episode_id,
                    "at_wall": envelope.at_wall,
                    "at_mono_ms": envelope.at_mono_ms,
                    "payload": envelope.payload,
                }
            )
        reduced = reduce_records(base_records)
        episode = reduced["episodes"][EPISODE]
        self.assertEqual(len(episode["detections"]), 1)
        self.assertEqual(reduced["metrics"]["unique_detection_keys"], 1)
        self.assertGreaterEqual(reduced["journal_anomalies"], 2)
        self.assertEqual(episode["agreement"], "fast_only")
        self.assertNotIn("agreement", base_records[-1]["payload"])

    def test_agreement_is_computed_from_fast_and_sol(self):
        envelopes = [opened(), fast_result(), closed()]
        records = [
            {
                "schema_version": 2,
                "record": envelope.record_type,
                "seq": index,
                "episode_id": EPISODE,
                "at_wall": envelope.at_wall,
                "at_mono_ms": envelope.at_mono_ms,
                "payload": envelope.payload,
            }
            for index, envelope in enumerate(envelopes, 1)
        ]
        records.insert(
            2,
            {
                "schema_version": 2,
                "record": "sol_result",
                "seq": 3,
                "episode_id": EPISODE,
                "at_wall": WALL,
                "at_mono_ms": 2_500,
                "payload": {
                    "lane": "audit",
                    "label": "delivery",
                    "confidence": 0.99,
                    "decided_at_wall": WALL,
                },
            },
        )
        records[-1]["seq"] = 4
        self.assertEqual(reduce_records(records)["episodes"][EPISODE]["agreement"], "agree")


class RecoveryTests(unittest.TestCase):
    def test_open_episode_is_closed_as_interrupted_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary, recover=False)
            journal.commit([opened()])
            recovered = Journal(
                temporary,
                wall_clock=lambda: "2026-08-27T16:05:00.000+00:00",
                mono_clock_ms=lambda: 301_000,
            )
            records = recovered.records()
            self.assertEqual([record["seq"] for record in records], [1, 2])
            episode = recovered.reduce()["episodes"][EPISODE]
            self.assertEqual(episode["close_reason"], "interrupted_restart")
            self.assertEqual(episode["closed_at_mono_ms"], 301_000)

    def test_incomplete_uncommitted_tail_is_removed_before_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary, recover=False)
            journal.commit([opened()])
            with journal.path.open("ab") as handle:
                handle.write(b'{"schema_version":2,"record":"detection"')
            recovered = Journal(
                temporary,
                wall_clock=lambda: "2026-08-27T16:05:00.000+00:00",
                mono_clock_ms=lambda: 301_000,
            )
            self.assertEqual(recovered.recovery_tail_truncations, 1)
            self.assertEqual(
                [record["record"] for record in recovered.records()],
                ["episode_opened", "episode_closed"],
            )


if __name__ == "__main__":
    unittest.main()
