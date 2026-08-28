"""Constants for the package-fast Home Assistant shell."""

from __future__ import annotations

DOMAIN = "package_fast"
PLATFORMS = ("binary_sensor", "sensor", "switch")

DEFAULT_CAMERA_ENTITY = "camera.g4_doorbell_pro_package_camera"
G4_PERSON_ENTITY = "binary_sensor.g4_doorbell_pro_person_detected"
G6_PERSON_ENTITY = "binary_sensor.g6_bullet_front_person_detected"
MASTER_ENTITY = "input_boolean.package_detection_enabled"
PROMOTED_ENTITY = "input_boolean.package_fast_promoted"
SOL_DECISION_ENTITY = "input_text.package_detection_last_decision"
SOL_LANE_COUNTERS = {
    "counter.package_detection_daily_front_early_calls": "early",
    "counter.package_detection_daily_front_final_calls": "final",
}

EVENT_SHADOW = "package_fast_shadow_v1"
EVENT_CONFIRMED = "package_fast_confirmed_v1"
EVENT_SYSTEM_LOG = "system_log_event"

CONF_CAMERA_ENTITY = "camera_entity"
CONF_IDLE_RATE_HZ = "idle_rate_hz"
CONF_ARMED_RATE_HZ = "armed_rate_hz"
CONF_PERSIST_FRAMES = "persist_frames"
CONF_MAX_STORAGE_MB = "max_storage_mb"
CONF_MAX_AGE_DAYS = "max_age_days"
CONF_MASK_HITS = "mask_hits"
CONF_MASK_WINDOW_HOURS = "mask_window_hours"
CONF_MASK_TTL_HOURS = "mask_ttl_hours"
CONF_MASK_IOU = "mask_iou_threshold"

# HA's nightly backup includes /media, so this spool stays short-lived while
# the durable archive lives in the package-detection corpus on razorback.
DEFAULT_PERSIST_FRAMES = False
DEFAULT_MAX_STORAGE_MB = 256
DEFAULT_MAX_AGE_DAYS = 3
DEFAULT_MASK_HITS = 3
DEFAULT_MASK_WINDOW_HOURS = 24.0
DEFAULT_MASK_TTL_HOURS = 24.0
DEFAULT_MASK_IOU = 0.5

# Phase-0 qualification envelope (CONVERGED + ERR-06).
FETCH_P95_LIMIT_MS = 900.0
POLL_GAP_LIMIT_MS = 1_500.0
# Phase-0 measured ~1.0 distinct FPS on 2026-08-27/28; threshold = measured - margin.
MIN_DISTINCT_FPS = 0.8
MAX_ERROR_RATE = 0.005
MAX_GAP_RATE = 0.005
SLO_WINDOW_SECONDS = 120.0
SLO_MIN_SAMPLES = 20
SLO_MIN_ARMED_SPAN_SECONDS = 10.0
SLO_RESUME_CLEAN_SECONDS = 60.0
SLO_ERROR_BUDGET_INTERVAL_MS = 500

# A static feed for three seconds at the source's 2 Hz rate is enough to make
# a concurrent person edge suspect.  The threshold is deliberately separate
# from detector confirmation credit.
FEED_STATIC_FRAMES = 6
FEED_PERSON_WINDOW_SECONDS = 30.0

FRAME_MOMENTARY_SECONDS = 5.0
METRICS_FLUSH_SECONDS = 30.0
SENSOR_UPDATE_SECONDS = 30.0
QUEUE_JOIN_TIMEOUT_SECONDS = 5.0
RETENTION_SWEEP_SECONDS = 60.0 * 60.0
STARTUP_STABILIZATION_SECONDS = 60.0
SOL_JOIN_MAX_SECONDS = 10.0 * 60.0
MEDIA_ROOT = "/media/package_fast"

INTEGRATION_VERSION = "0.2.1"
