# __init__.py — Public, dependency-light package-fast core surface.
#
# Rationale: CONVERGED.md §Architecture freezes a synchronous step() seam so
# HA and razorback replay use identical code.  Durable publication remains the
# ERRATA.md ERR-07 journal's responsibility.

"""Pure detector, pipeline primitives, envelopes, and typed journal."""

from .config import DetectorConfig
from .detector import Detector, DetectorState
from .envelopes import DetectionEnvelope, FrameEnvelope, SignalEnvelope
from .journal import Journal, JournalReducer, reduce_journal, reduce_records

__all__ = [
    "DetectionEnvelope",
    "Detector",
    "DetectorConfig",
    "DetectorState",
    "FrameEnvelope",
    "Journal",
    "JournalReducer",
    "SignalEnvelope",
    "reduce_journal",
    "reduce_records",
]
