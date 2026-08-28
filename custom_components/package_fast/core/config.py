# config.py — Frozen detector knobs and their reproducibility digest.
#
# Rationale: every decision boundary must be replayable from one canonical
# value object.  Defaults come from CONVERGED.md §5c and the FSM; poll timing
# mirrors ERRATA.md ERR-06.  Additional "strong margin" knobs make §5c-6's
# otherwise qualitative announce tier explicit and testable.

"""Configuration for the pure package-fast detector core."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    """All detector thresholds, including shell-facing cadence expectations."""

    algorithm_version: str = "0.3.0"

    input_width: int = 960
    input_height: int = 720
    working_width: int = 480
    working_height: int = 360
    roi_norm: tuple[float, float, float, float] = (0.0, 0.0, 1.0, 1.0)

    normalization_ratio_min: float = 0.5
    normalization_ratio_max: float = 2.0
    illumination_delta: int = 18
    illumination_fraction: float = 0.35

    motion_delta: int = 12
    motion_dilation_px: int = 5
    motion_fraction: float = 0.12

    change_delta: int = 18
    morph_open_px: int = 3
    morph_close_px: int = 5
    sobel_edge_threshold: int = 24
    shadow_edge_gain_min: float = 0.15

    track_iou_min: float = 0.5
    confirm_iou_min: float = 0.8
    stability_sad_max: float = 8.0
    confirm_k: int = 2

    area_frac_min: float = 0.005
    area_frac_max: float = 0.45
    moved_area_ratio_min: float = 0.5
    moved_area_ratio_max: float = 2.0
    suppression_decay_seconds: float = 24.0 * 60.0 * 60.0

    person_context_seconds: float = 180.0
    strong_edge_gain_min: float = 0.22
    strong_stability_sad_max: float = 6.0
    strong_area_margin_frac: float = 0.001

    idle_rate_hz: float = 0.5
    armed_rate_hz: float = 2.0
    ring_frames: int = 40
    baseline_min_age_seconds: float = 2.0
    disturbance_confirm_frames: int = 2
    armed_timeout_seconds: float = 120.0
    episode_quiet_seconds: float = 75.0

    rebase_stability_seconds: float = 10.0
    rebase_stable_delta: int = 12
    rebase_stable_fraction: float = 0.02
    camera_shift_threshold_px: int = 4
    camera_shift_search_px: int = 12

    exception_limit: int = 3
    exception_window_seconds: float = 10.0 * 60.0

    fetch_budget_seconds: float = 1.0
    fetch_retries: int = 1
    poll_gap_seconds: float = 1.5
    planning_bound_armed_ms: int = 4_000
    planning_bound_idle_ms: int = 5_000

    def __post_init__(self) -> None:
        if min(self.input_width, self.input_height, self.working_width, self.working_height) <= 0:
            raise ValueError("frame dimensions must be positive")
        if self.confirm_k < 1:
            raise ValueError("confirm_k must count at least one post-sighting frame")
        if self.disturbance_confirm_frames < 2:
            raise ValueError("disturbance_confirm_frames must preserve the two-frame episode gate")
        if self.ring_frames < 2:
            raise ValueError("ring_frames must be at least two")
        for name in ("morph_open_px", "morph_close_px"):
            value = getattr(self, name)
            if value < 1 or value % 2 == 0:
                raise ValueError(f"{name} must be a positive odd number")
        for name in (
            "illumination_fraction",
            "motion_fraction",
            "shadow_edge_gain_min",
            "track_iou_min",
            "confirm_iou_min",
            "area_frac_min",
            "area_frac_max",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.area_frac_min >= self.area_frac_max:
            raise ValueError("area_frac_min must be below area_frac_max")
        if self.track_iou_min > self.confirm_iou_min:
            raise ValueError("matching IoU cannot exceed confirmation IoU")
        x0, y0, x1, y1 = self.roi_norm
        if not (0.0 <= x0 < x1 <= 1.0 and 0.0 <= y0 < y1 <= 1.0):
            raise ValueError("roi_norm must be an in-frame (x0, y0, x1, y1) box")

    @property
    def input_size(self) -> tuple[int, int]:
        return (self.input_width, self.input_height)

    @property
    def working_size(self) -> tuple[int, int]:
        return (self.working_width, self.working_height)

    def as_dict(self) -> dict[str, Any]:
        """Return the complete JSON-compatible threshold set."""

        return asdict(self)

    def canonical_json(self) -> str:
        """Return the byte-stable JSON representation used for replay identity."""

        return json.dumps(
            self.as_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )

    @property
    def config_digest(self) -> str:
        digest = hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"
