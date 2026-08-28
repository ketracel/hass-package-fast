# test_pipeline.py — Contract tests for §5c field-math primitives.
#
# Rationale: thresholds are verified at their public primitive boundaries so
# failures localize before the FSM/scenario suites.  The same assertions run
# with NumPy imported and with its import blocked.

from __future__ import annotations

from dataclasses import replace
import hashlib
from io import BytesIO
import os
import unittest

from PIL import Image

from homeassistant.package_fast.core.config import DetectorConfig
from homeassistant.package_fast.core.envelopes import FrameEnvelope
from homeassistant.package_fast.core.pipeline import (
    StationarityTracker,
    backend_name,
    decode_gray,
    estimate_camera_shift,
    illumination_change_fraction,
    motion_map,
    photometric_normalize,
    segment_changes,
)
from homeassistant.package_fast.tests import synth


class ConfigTests(unittest.TestCase):
    def test_digest_is_canonical_and_threshold_sensitive(self):
        config = DetectorConfig()
        self.assertEqual(config.config_digest, DetectorConfig().config_digest)
        self.assertTrue(config.config_digest.startswith("sha256:"))
        self.assertEqual(len(config.config_digest), 71)
        self.assertNotEqual(
            config.config_digest,
            replace(config, confirm_k=config.confirm_k + 1).config_digest,
        )

    def test_backend_is_explicit_and_matches_acceptance_environment(self):
        active = backend_name()
        self.assertIn(active, {"numpy", "pillow"})
        expected = os.environ.get("PACKAGE_FAST_EXPECT_BACKEND")
        if expected is not None:
            self.assertEqual(active, expected)


class FieldMathTests(unittest.TestCase):
    def setUp(self):
        self.config = DetectorConfig()
        self.base = synth.scene(seed=7703)

    def test_median_ratio_normalization(self):
        brighter = synth.illumination_ramp(self.base, 1.5)
        normalized, ratio = photometric_normalize(brighter, self.base, self.config)
        self.assertAlmostEqual(ratio, 2.0 / 3.0, delta=0.03)
        difference = sum(
            abs(a - b) for a, b in zip(normalized.tobytes(), self.base.tobytes())
        ) / (normalized.width * normalized.height)
        self.assertLess(difference, 1.5)

    def test_ir_flip_trips_global_guard(self):
        flipped = synth.ir_flip(self.base)
        normalized, _ = photometric_normalize(flipped, self.base, self.config)
        self.assertGreater(
            illumination_change_fraction(normalized, self.base, self.config),
            self.config.illumination_fraction,
        )

    def test_motion_map_dilates_and_crosses_occlusion_threshold(self):
        first = synth.courier(self.base, center_x=120)
        second = synth.courier(self.base, center_x=250)
        mask, fraction = motion_map(second, first, self.config)
        self.assertGreater(fraction, self.config.motion_fraction)
        self.assertGreater(sum(value != 0 for value in mask.tobytes()), 0)

    def test_added_and_removed_polarity(self):
        parcel = synth.with_objects(self.base, [synth.BOX_B])
        added = segment_changes(parcel, self.base, self.config)
        removed = segment_changes(self.base, parcel, self.config)
        self.assertEqual([component.polarity for component in added.components], ["added"])
        self.assertEqual([component.polarity for component in removed.components], ["removed"])
        self.assertGreater(added.components[0].edge_gain, self.config.shadow_edge_gain_min)

    def test_soft_shadow_is_rejected(self):
        shadow = synth.shadow_sweep(self.base, center_x=220)
        result = segment_changes(shadow, self.base, self.config)
        self.assertEqual(result.components, ())
        self.assertGreaterEqual(result.rejected_shadows, 1)

    def test_decode_downscales_960_input_and_keeps_source_mapping(self):
        large = self.base.resize((960, 720), Image.Resampling.NEAREST)
        envelope = synth.frame(large, "large", 1_000)
        decoded = decode_gray(envelope, self.config)
        self.assertEqual(decoded.image.size, (480, 360))
        self.assertEqual(decoded.source_size, (960, 720))
        self.assertEqual(decoded.scale_x, 2.0)
        self.assertEqual(decoded.scale_y, 2.0)

    def test_jpeg_envelope_decodes_without_shell_help(self):
        encoded = BytesIO()
        self.base.save(encoded, format="JPEG", quality=95)
        payload = encoded.getvalue()
        envelope = FrameEnvelope(
            frame_id="jpeg",
            jpeg_bytes=payload,
            at_wall="2026-08-27T16:00:00.000+00:00",
            at_mono_ms=1_000,
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        decoded = decode_gray(envelope, self.config)
        self.assertEqual(decoded.image.mode, "L")
        self.assertEqual(decoded.image.size, self.config.working_size)

    def test_camera_shift_edge_profile(self):
        shifted = synth.camera_shift(self.base, 7, -5)
        dx, dy, correlation = estimate_camera_shift(self.base, shifted, 12)
        self.assertEqual((dx, dy), (7, -5))
        self.assertGreater(correlation, 0.7)


class TrackingTests(unittest.TestCase):
    def test_k_two_means_three_distinct_sightings(self):
        config = DetectorConfig()
        base = synth.scene()
        parcel = synth.with_objects(base, [synth.BOX_B])
        tracker = StationarityTracker(config)
        previous = base
        confirmed = []
        for index in range(3):
            image = synth.variant(parcel, index + 20)
            segmented = segment_changes(image, base, config)
            motion, _ = motion_map(image, previous, config)
            envelope = synth.frame(image, f"p{index}", 10_000 + index * 500)
            confirmed = tracker.update(
                segmented.components, image, previous, motion, envelope
            )
            if index < 2:
                self.assertEqual(confirmed, [])
            previous = image
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(confirmed[0].confirm_count, 3)


if __name__ == "__main__":
    unittest.main()
