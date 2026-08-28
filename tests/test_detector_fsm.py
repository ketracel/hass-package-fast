# test_detector_fsm.py — State, clock, baseline, and fail-closed unit tests.
#
# Rationale: the CONVERGED.md Architecture FSM is a safety boundary, not UI
# decoration.  These tests pin entry/exit timing, the two-second baseline age,
# master control, and the three-strike SUSPENDED behavior independently of the
# richer end-to-end scenarios.

from __future__ import annotations

import unittest

from homeassistant.package_fast.core.detector import Detector, DetectorState
from homeassistant.package_fast.core.envelopes import FrameEnvelope
from homeassistant.package_fast.tests import synth


def prime(detector: Detector, base=None) -> None:
    base = base or synth.scene()
    for index, timestamp in enumerate((6_000, 7_000, 8_000)):
        detector.step(
            synth.frame(synth.variant(base, index), f"quiet_{index}", timestamp), []
        )


class DetectorFsmTests(unittest.TestCase):
    def setUp(self):
        self.base = synth.scene()

    def test_g4_opens_with_two_second_old_quiet_baseline(self):
        detector = Detector()
        prime(detector, self.base)
        output = detector.step(
            synth.frame(synth.variant(self.base, 10), "person", 10_000),
            [synth.signal("g4_person_on", 10_000, event_id="g4-event")],
        )
        opened = next(item for item in output if item.record_type == "episode_opened")
        self.assertEqual(detector.state, DetectorState.EPISODE_OPEN)
        self.assertEqual(opened.payload["opened_by"], "g4_person")
        self.assertEqual(opened.payload["g4_event_ids"], ["g4-event"])
        self.assertGreaterEqual(opened.payload["baseline"]["age_ms_at_open"], 2_000)

    def test_g6_arms_but_does_not_open_and_expires_at_120_seconds(self):
        detector = Detector()
        prime(detector, self.base)
        opened = detector.step(
            synth.frame(synth.variant(self.base, 20), "g6", 10_000),
            [synth.signal("g6_person_on", 10_000)],
        )
        self.assertEqual(opened, [])
        self.assertEqual(detector.state, DetectorState.ARMED)
        detector.step(
            synth.frame(synth.variant(self.base, 21), "expired", 130_000), []
        )
        self.assertEqual(detector.state, DetectorState.IDLE)

    def test_g4_quiet_closes_after_75_seconds(self):
        detector = Detector()
        prime(detector, self.base)
        detector.step(
            synth.frame(synth.variant(self.base, 30), "open", 10_000),
            [synth.signal("g4_person_on", 10_000)],
        )
        detector.step(
            synth.frame(synth.variant(self.base, 31), "off", 10_500),
            [synth.signal("g4_person_off", 10_500)],
        )
        output = detector.step(
            synth.frame(synth.variant(self.base, 32), "close", 85_500), []
        )
        closed = next(item for item in output if item.record_type == "episode_closed")
        self.assertEqual(closed.payload["close_reason"], "quiet_75s")
        self.assertEqual(detector.state, DetectorState.CLOSED)

    def test_master_off_disables_and_master_on_restarts_idle(self):
        detector = Detector()
        prime(detector, self.base)
        output = detector.step(
            synth.frame(synth.variant(self.base, 40), "disabled", 10_000),
            [synth.signal("master_off", 10_000)],
        )
        self.assertEqual(output, [])
        self.assertEqual(detector.state, DetectorState.DISABLED)
        detector.step(
            synth.frame(synth.variant(self.base, 41), "enabled", 10_500),
            [synth.signal("master_on", 10_500)],
        )
        self.assertEqual(detector.state, DetectorState.IDLE)

    def test_three_decode_exceptions_in_ten_minutes_suspend(self):
        detector = Detector()
        prime(detector, self.base)
        detector.step(
            synth.frame(synth.variant(self.base, 50), "open", 10_000),
            [synth.signal("g4_person_on", 10_000)],
        )
        staged = []
        for index in range(3):
            broken = FrameEnvelope(
                frame_id=f"broken_{index}",
                gray_array=[],
                at_wall=synth.signal("g4_person_off", 11_000 + index).at_wall,
                at_mono_ms=11_000 + index,
                sha256=f"broken-{index}",
            )
            staged.extend(detector.step(broken, []))
        self.assertEqual(detector.state, DetectorState.SUSPENDED)
        self.assertEqual(detector.suspension_reason, "exception_budget")
        closed = [item for item in staged if item.record_type == "episode_closed"]
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0].payload["close_reason"], "suspended")

    def test_manual_test_can_detect_but_never_announce(self):
        detector = Detector()
        prime(detector, self.base)
        parcel = synth.with_objects(self.base, [synth.BOX_B])
        staged = []
        for index in range(3):
            signals = [synth.signal("manual_test", 10_000)] if index == 0 else []
            staged.extend(
                detector.step(
                    synth.frame(
                        synth.variant(parcel, 60 + index),
                        f"test_{index}",
                        10_000 + 500 * index,
                    ),
                    signals,
                )
            )
        deposit = next(item for item in staged if item.kind == "deposit")
        self.assertFalse(deposit.announce_eligible)
        self.assertTrue(next(item for item in staged if item.record_type == "episode_opened").payload["test"])

    def test_rebase_requires_ten_seconds_of_global_stability(self):
        detector = Detector()
        prime(detector, self.base)
        flipped = synth.ir_flip(self.base)
        detector.step(
            synth.frame(synth.variant(flipped, 70), "flip", 10_000),
            [synth.signal("g4_person_on", 10_000)],
        )
        self.assertEqual(detector.state, DetectorState.REBASE)
        detector.step(
            synth.frame(synth.variant(flipped, 71), "stable_start", 10_500), []
        )
        self.assertEqual(detector.state, DetectorState.REBASE)
        final = synth.frame(synth.variant(flipped, 72), "stable_done", 20_500)
        detector.step(final, [])
        self.assertEqual(detector.state, DetectorState.EPISODE_OPEN)
        self.assertEqual(detector.baseline_sha256, final.sha256)


if __name__ == "__main__":
    unittest.main()
