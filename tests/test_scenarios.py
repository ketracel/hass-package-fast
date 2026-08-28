# test_scenarios.py — Binding S1–S14 end-to-end synthetic sequence suite.
#
# Rationale: CONVERGED.md §5i requires deterministic temporal replay and the
# work order names fourteen acceptance scenes.  Each test uses receive-domain
# monotonic timestamps, fixed-seed 480×360 imagery, and complete frame order;
# only Journal.commit() output is treated as publishable where durability is at
# issue.

from __future__ import annotations

import tempfile
import unittest

from homeassistant.package_fast.core.detector import Detector, DetectorState
from homeassistant.package_fast.core.journal import Journal
from homeassistant.package_fast.tests import synth


def prime(detector: Detector, base) -> list:
    staged = []
    for index, timestamp in enumerate((6_000, 7_000, 8_000)):
        staged.extend(
            detector.step(
                synth.frame(synth.variant(base, index), f"quiet_{index}", timestamp), []
            )
        )
    return staged


def physical(staged, kind=None):
    records = [
        item
        for item in staged
        if item.record_type == "detection"
        and item.kind in {"deposit", "removal", "moved_object"}
    ]
    return [item for item in records if kind is None or item.kind == kind]


class SyntheticScenarioTests(unittest.TestCase):
    def setUp(self):
        self.base = synth.scene(seed=7703)

    def test_s01_clean_deposit(self):
        detector = Detector()
        staged = prime(detector, self.base)
        sequence = [
            (synth.courier(self.base, center_x=220), [synth.signal("g4_person_on", 10_000)]),
            (synth.courier(self.base, center_x=270), []),
            (synth.courier(self.base, center_x=320), []),
        ]
        parcel = synth.with_objects(self.base, [synth.BOX_B])
        sequence.extend(
            [
                (parcel, [synth.signal("g4_person_off", 11_500)]),
                (parcel, []),
                (parcel, []),
            ]
        )
        for index, (image, signals) in enumerate(sequence):
            staged.extend(
                detector.step(
                    synth.frame(
                        synth.variant(image, 100 + index),
                        f"s1_{index}",
                        10_000 + index * 500,
                    ),
                    signals,
                )
            )
        deposits = physical(staged, "deposit")
        self.assertEqual(len(deposits), 1)
        self.assertTrue(deposits[0].announce_eligible)
        self.assertEqual(deposits[0].confirm_count, 3)
        self.assertEqual(deposits[0].latency_ms_from_first_visible, 1_000)
        self.assertLessEqual(
            deposits[0].latency_ms_from_first_visible,
            detector.config.planning_bound_armed_ms,
        )

    def test_s02_lateral_passer_motion_only(self):
        detector = Detector()
        staged = prime(detector, self.base)
        images = [
            synth.courier(self.base, center_x=x, carrying=False)
            for x in (70, 155, 250, 350, 435)
        ] + [self.base, self.base]
        for index, image in enumerate(images):
            signals = []
            if index == 0:
                signals = [synth.signal("g4_person_on", 10_000)]
            elif index == 5:
                signals = [synth.signal("g4_person_off", 12_500)]
            staged.extend(
                detector.step(
                    synth.frame(
                        synth.variant(image, 120 + index),
                        f"s2_{index}",
                        10_000 + index * 500,
                    ),
                    signals,
                )
            )
        self.assertEqual(physical(staged), [])

    def test_s03_shadow_sweep(self):
        detector = Detector()
        staged = prime(detector, self.base)
        for index, center in enumerate((80, 150, 220, 290, 370)):
            image = synth.shadow_sweep(self.base, center_x=center)
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(image, 140 + index), f"s3_{index}", 10_000 + index * 500),
                    [],
                )
            )
        self.assertEqual(physical(staged), [])
        self.assertNotEqual(detector.state, DetectorState.DETECTED)

    def test_s04_global_illumination_ir_flip(self):
        detector = Detector()
        staged = prime(detector, self.base)
        staged.extend(
            detector.step(
                synth.frame(synth.variant(synth.ir_flip(self.base), 160), "s4_flip", 10_000),
                [synth.signal("g4_person_on", 10_000)],
            )
        )
        self.assertEqual(detector.state, DetectorState.REBASE)
        self.assertEqual(physical(staged), [])
        self.assertEqual([item.kind for item in staged if item.kind == "rebase"], ["rebase"])

    def test_s05_preexisting_object_plus_second_deposit(self):
        baseline = synth.scene(seed=7703, objects=[synth.BOX_A])
        detector = Detector()
        staged = prime(detector, baseline)
        after = synth.with_objects(baseline, [synth.BOX_B])
        for index in range(3):
            signals = [synth.signal("g4_person_on", 10_000)] if index == 0 else []
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(after, 180 + index), f"s5_{index}", 10_000 + index * 500),
                    signals,
                )
            )
        deposits = physical(staged, "deposit")
        self.assertEqual(len(deposits), 1)
        self.assertGreater(deposits[0].bbox_norm[0], 0.5)

    def test_s06_retrieval_is_journal_only(self):
        baseline = synth.scene(seed=7703, objects=[synth.BOX_A])
        detector = Detector()
        staged = prime(detector, baseline)
        for index in range(3):
            signals = [synth.signal("g4_person_on", 10_000)] if index == 0 else []
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(self.base, 200 + index), f"s6_{index}", 10_000 + index * 500),
                    signals,
                )
            )
        removals = physical(staged, "removal")
        self.assertEqual(len(removals), 1)
        self.assertFalse(removals[0].announce_eligible)
        self.assertEqual(physical(staged, "deposit"), [])

    def test_s07_moved_object_pair_is_journal_only(self):
        baseline = synth.scene(seed=7703, objects=[synth.BOX_A])
        after = synth.with_objects(self.base, [synth.BOX_B])
        detector = Detector()
        staged = prime(detector, baseline)
        for index in range(3):
            signals = [synth.signal("g4_person_on", 10_000)] if index == 0 else []
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(after, 220 + index), f"s7_{index}", 10_000 + index * 500),
                    signals,
                )
            )
        moved = physical(staged, "moved_object")
        self.assertEqual(len(moved), 1)
        self.assertFalse(moved[0].announce_eligible)
        self.assertEqual(physical(staged, "deposit"), [])

    def test_s08_multi_trip_two_deposits_one_episode(self):
        detector = Detector()
        staged = prime(detector, self.base)
        first = synth.with_objects(self.base, [synth.BOX_A])
        for index in range(3):
            signals = []
            if index == 0:
                signals = [synth.signal("g4_person_on", 10_000)]
            elif index == 2:
                signals = [synth.signal("g4_person_off", 11_000)]
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(first, 240 + index), f"s8_a{index}", 10_000 + index * 500),
                    signals,
                )
            )

        second = synth.with_objects(self.base, [synth.BOX_A, synth.BOX_B])
        for index in range(3):
            signals = []
            if index == 0:
                signals = [synth.signal("g4_person_on", 76_000)]
            elif index == 2:
                signals = [synth.signal("g4_person_off", 77_000)]
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(second, 250 + index), f"s8_b{index}", 76_000 + index * 500),
                    signals,
                )
            )
        deposits = physical(staged, "deposit")
        self.assertEqual(len(deposits), 2)
        self.assertEqual(len({item.episode_id for item in deposits}), 1)
        self.assertEqual(len([item for item in staged if item.record_type == "episode_opened"]), 1)
        self.assertEqual(len([item for item in staged if item.record_type == "episode_closed"]), 0)

    def test_s09_duplicate_hashes_add_no_confirmation_credit(self):
        detector = Detector()
        staged = prime(detector, self.base)
        parcel = synth.variant(synth.with_objects(self.base, [synth.BOX_B]), 270)
        first = synth.frame(parcel, "s9_first", 10_000)
        staged.extend(detector.step(first, [synth.signal("g4_person_on", 10_000)]))
        for index, timestamp in enumerate((10_500, 11_000), 1):
            staged.extend(detector.step(synth.frame(parcel, f"s9_dup{index}", timestamp), []))
        self.assertEqual(physical(staged, "deposit"), [])
        self.assertEqual(detector.duplicate_frames, 2)
        for index, timestamp in enumerate((11_500, 12_000), 1):
            image = synth.variant(synth.with_objects(self.base, [synth.BOX_B]), 270 + index)
            staged.extend(detector.step(synth.frame(image, f"s9_new{index}", timestamp), []))
        deposits = physical(staged, "deposit")
        self.assertEqual(len(deposits), 1)
        self.assertEqual(deposits[0].confirm_count, 3)

    def test_s10_out_of_order_monotonic_time_quarantines(self):
        detector = Detector()
        staged = prime(detector, self.base)
        staged.extend(
            detector.step(
                synth.frame(synth.variant(self.base, 290), "s10_open", 10_000),
                [synth.signal("g4_person_on", 10_000)],
            )
        )
        staged.extend(
            detector.step(
                synth.frame(synth.variant(self.base, 291), "s10_regress", 9_500), []
            )
        )
        closed = [item for item in staged if item.record_type == "episode_closed"]
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0].payload["close_reason"], "timing_quarantine")
        self.assertEqual(detector.state, DetectorState.SUSPENDED)

    def test_s11_camera_shift_during_rebase_is_journaled_and_clears_masks(self):
        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary)
            detector = Detector(journal=journal)
            journal.commit(prime(detector, self.base))
            detector.add_suppression_mask((0.1, 0.1, 0.2, 0.2), 9_000)
            shifted = synth.camera_shift(synth.ir_flip(self.base), 6, 0)
            staged = detector.step(
                synth.frame(synth.variant(shifted, 310), "s11_shift", 10_000),
                [synth.signal("g4_person_on", 10_000)],
            )
            durable = journal.commit(staged)
            self.assertEqual(detector.state, DetectorState.REBASE)
            self.assertEqual(detector.active_suppression_masks, 0)
            self.assertEqual(detector.suppression_masks_invalidated, 1)
            self.assertEqual([item.kind for item in durable if item.kind == "camera_shift"], ["camera_shift"])
            self.assertIn("camera_shift", [record["payload"].get("kind") for record in journal.records()])

    def test_s12_restart_mid_episode_closes_interrupted_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            detector = Detector(journal_dir=temporary)
            detector.journal.commit(prime(detector, self.base))
            staged = detector.step(
                synth.frame(synth.variant(self.base, 330), "s12_open", 10_000),
                [synth.signal("g4_person_on", 10_000)],
            )
            episode_id = next(item.episode_id for item in staged if item.record_type == "episode_opened")
            detector.journal.commit(staged)

            restarted = Detector(
                journal=Journal(
                    temporary,
                    wall_clock=lambda: "2026-08-27T16:05:00.000+00:00",
                    mono_clock_ms=lambda: 301_000,
                )
            )
            episode = restarted.journal.reduce()["episodes"][episode_id]
            self.assertEqual(episode["close_reason"], "interrupted_restart")
            self.assertEqual(episode["journal_anomalies"], 0)
            self.assertEqual(len([record for record in restarted.journal.records() if record["record"] == "episode_closed"]), 1)

    def test_s13_crash_injected_write_publishes_nothing(self):
        def fault(stage, context):
            if stage == "before_fsync" and any(
                record["record"] == "detection" for record in context["records"]
            ):
                raise OSError("synthetic fsync-path crash")

        with tempfile.TemporaryDirectory() as temporary:
            journal = Journal(temporary, fault_hook=fault)
            detector = Detector(journal=journal)
            journal.commit(prime(detector, self.base))
            parcel = synth.with_objects(self.base, [synth.BOX_B])
            staged = []
            for index in range(3):
                signals = [synth.signal("g4_person_on", 10_000)] if index == 0 else []
                output = detector.step(
                    synth.frame(synth.variant(parcel, 350 + index), f"s13_{index}", 10_000 + index * 500),
                    signals,
                )
                if index < 2:
                    journal.commit(output)
                else:
                    staged = output
            self.assertEqual(len(physical(staged, "deposit")), 1)
            published = journal.commit(staged)
            self.assertEqual(published, [])
            reduced = journal.reduce()
            episode = next(iter(reduced["episodes"].values()))
            self.assertEqual(episode["detections"], [])
            self.assertIsNone(episode.get("fast_result"))

    def test_s14_person_absent_disturbance_is_shadow_tier(self):
        detector = Detector()
        staged = prime(detector, self.base)
        parcel = synth.with_objects(self.base, [synth.BOX_B])
        for index in range(3):
            staged.extend(
                detector.step(
                    synth.frame(synth.variant(parcel, 370 + index), f"s14_{index}", 10_000 + index * 500),
                    [],
                )
            )
        opened = [item for item in staged if item.record_type == "episode_opened"]
        deposits = physical(staged, "deposit")
        self.assertEqual(len(opened), 1)
        self.assertEqual(opened[0].payload["opened_by"], "roi_disturbance")
        self.assertEqual(len(deposits), 1)
        self.assertFalse(deposits[0].person_context_present)
        self.assertFalse(deposits[0].announce_eligible)
        self.assertLessEqual(
            deposits[0].latency_ms_from_first_visible,
            detector.config.planning_bound_idle_ms,
        )


if __name__ == "__main__":
    unittest.main()
