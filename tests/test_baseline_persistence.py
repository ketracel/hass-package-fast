"""Exercise real core/journal/storage and runtime persistence methods without HA."""

from __future__ import annotations

from collections import deque
from dataclasses import replace
import hashlib
from io import BytesIO
from pathlib import Path
import tempfile
import textwrap
import unittest

from homeassistant.package_fast.core.detector import Detector
from homeassistant.package_fast.core.envelopes import FrameEnvelope
from homeassistant.package_fast.core.journal import Journal
from homeassistant.package_fast.custom_components.package_fast.shell_logic import bounded_cache_put
from homeassistant.package_fast.custom_components.package_fast.storage import SparseFrame, SparseFrameStore
from homeassistant.package_fast.tests import synth
from homeassistant.package_fast.tests.test_shell_logic import runtime_method_source
from typing import Mapping


def persistence_harness():
    # Execute the shipping methods, not a test-side copy of the lookup policy.
    source = "from __future__ import annotations\nclass Harness:\n" + "\n".join(
        textwrap.indent(runtime_method_source(name), "    ")
        for name in ("_cache_frame", "_frame", "_persist_one", "_persist_durable")
    )
    namespace = dict(globals())
    exec(source, namespace)
    shell = namespace["Harness"]()
    shell.detector = Detector()
    shell._frame_cache = {}
    shell._frame_order = deque()
    shell._frame_cache_limit = 48
    return shell


def jpeg_frame(image, frame_id, at_ms):
    output = BytesIO()
    image.save(output, format="JPEG", quality=95)
    raw = output.getvalue()
    return FrameEnvelope(
        frame_id=frame_id,
        at_wall=synth.signal("manual_test", at_ms).at_wall,
        at_mono_ms=at_ms,
        sha256=hashlib.sha256(raw).hexdigest(),
        jpeg_bytes=raw,
    )


class BaselinePersistenceTests(unittest.TestCase):
    def aged_baseline(self):
        shell = persistence_harness()
        baseline = jpeg_frame(synth.scene(), "original-before", 0)
        for index in range(60):
            # Duplicate fetches rotate the shell cache but not the quiet ring.
            frame = replace(baseline, frame_id=f"poll-{index}", at_mono_ms=index * 2000)
            if index == 0:
                frame = baseline
            shell._cache_frame(frame)
            shell.detector.step(frame, [])
        self.assertNotIn(baseline.frame_id, shell._frame_cache)
        self.assertEqual(len(shell._frame_cache), 48)
        return shell, baseline

    def test_evicted_baseline_persists_exact_original_bytes(self):
        for enabled in (True, False):
            with self.subTest(persistence_enabled=enabled), tempfile.TemporaryDirectory() as root:
                shell, baseline = self.aged_baseline()
                current = jpeg_frame(synth.with_objects(synth.scene(), [synth.BOX_A]), "after", 120_000)
                sparse = shell._cache_frame(current)
                records = shell.detector.step(current, [synth.signal("manual_test", 120_000)])
                opened = next(item for item in records if item.record_type == "episode_opened")
                self.assertEqual(opened.payload["baseline"]["frame_ids"], [baseline.frame_id])
                self.assertEqual(opened.payload["baseline"]["age_ms_at_open"], 120_000)
                shell.frame_store = SparseFrameStore(root, max_bytes=1_000_000, max_age_days=3)
                shell.journal = Journal(root, frame_persistence_enabled=enabled,
                                        frame_persist_callback=shell.frame_store.persist)
                durable = shell.journal.commit(records)
                shell._persist_durable(durable, sparse)
                path = Path(root) / opened.payload["frames_dir"] / "baseline.jpg"
                if enabled:
                    self.assertEqual(path.read_bytes(), baseline.jpeg_bytes)
                    self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), opened.payload["baseline"]["sha256"])
                    self.assertEqual(shell.journal.shadow_write_skips, 0)
                else:
                    self.assertEqual(list(Path(root).rglob("*.jpg")), [])
                self.assertLessEqual(len(shell._frame_cache), 48)
                self.assertIsNone(shell._frame("unknown-frame"))

    def test_open_then_close_in_one_step_retains_baseline_until_next_step(self):
        shell, baseline = self.aged_baseline()
        current = replace(baseline, frame_id="current", at_mono_ms=120_000)
        records = shell.detector.step(current, [
            synth.signal("manual_test", 120_000), synth.signal("master_off", 120_000)
        ])
        self.assertEqual([item.record_type for item in records], ["episode_opened", "episode_closed"])
        self.assertIsNone(shell.detector.episode_id)
        self.assertEqual(shell._frame(baseline.frame_id).jpeg_bytes, baseline.jpeg_bytes)
        shell.detector.step(replace(current, at_mono_ms=122_000), [])
        self.assertIsNone(shell._frame(baseline.frame_id))

    def test_first_frame_has_no_invented_before_image(self):
        shell = persistence_harness()
        frame = jpeg_frame(synth.scene(), "first", 0)
        records = shell.detector.step(frame, [synth.signal("manual_test", 0)])
        self.assertIsNone(records[0].payload["baseline"])
        self.assertIsNone(shell._frame(None))

    def test_rebase_in_opening_step_keeps_original_before_image(self):
        shell, baseline = self.aged_baseline()
        shell.detector.config = replace(shell.detector.config,
                                        disturbance_confirm_frames=3,
                                        rebase_stability_seconds=0)
        bright = synth.illumination_ramp(synth.scene(), 1.7)
        for index in range(3):
            frame = jpeg_frame(bright, f"bright-{index}", 120_000 + index * 1000)
            # Distinct camera bytes, stable decoded exposure for rebase.
            raw = frame.jpeg_bytes + b" " * index
            frame = replace(frame, jpeg_bytes=raw, sha256=hashlib.sha256(raw).hexdigest())
            records = shell.detector.step(frame, [])
        opened = next(item for item in records if item.record_type == "episode_opened")
        self.assertEqual(shell.detector.baseline_sha256, frame.sha256)
        self.assertEqual(opened.payload["baseline"]["sha256"], baseline.sha256)
        self.assertEqual(shell._frame(baseline.frame_id).jpeg_bytes, baseline.jpeg_bytes)


if __name__ == "__main__":
    unittest.main()
