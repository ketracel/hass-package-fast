# envelopes.py — Stable replay and publication boundary for package-fast.
#
# Rationale: wall time is join/display metadata while every ordering and
# latency decision uses monotonic milliseconds.  This is the reversible core
# seam specified by CONVERGED.md §Architecture/§5b and the typed-record seam
# required by ERRATA.md ERR-07.

"""Version-stable input and staged-output dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping


SignalKind = Literal[
    "g4_person_on",
    "g4_person_off",
    "g6_person_on",
    "g6_person_off",
    "master_off",
    "master_on",
    "manual_test",
]
RecordType = Literal[
    "episode_opened", "detection", "fast_result", "sol_result", "episode_closed"
]


@dataclass(frozen=True, slots=True)
class FrameEnvelope:
    """One received camera frame with both clocks and a content identity."""

    frame_id: str
    at_wall: str
    at_mono_ms: int
    sha256: str
    jpeg_bytes: bytes | None = None
    gray_array: Any | None = None

    def __post_init__(self) -> None:
        if bool(self.jpeg_bytes is not None) == bool(self.gray_array is not None):
            raise ValueError("exactly one of jpeg_bytes or gray_array is required")
        if not self.frame_id:
            raise ValueError("frame_id is required")
        if not isinstance(self.at_mono_ms, int):
            raise TypeError("at_mono_ms must be an integer")
        if not self.sha256:
            raise ValueError("sha256 is required for distinct-content accounting")


@dataclass(frozen=True, slots=True)
class SignalEnvelope:
    """A detector control/person edge received alongside a frame."""

    kind: SignalKind
    at_wall: str
    at_mono_ms: int
    meta: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in {
            "g4_person_on",
            "g4_person_off",
            "g6_person_on",
            "g6_person_off",
            "master_off",
            "master_on",
            "manual_test",
        }:
            raise ValueError(f"unsupported signal kind: {self.kind}")
        if not isinstance(self.at_mono_ms, int):
            raise TypeError("at_mono_ms must be an integer")


@dataclass(frozen=True, slots=True)
class DetectionEnvelope:
    """A staged ERR-07 record, with detection fields available directly.

    ``Detector.step`` returns these in memory.  They are not publication-safe
    until ``Journal.commit`` returns them after flush and fsync.
    """

    record_type: RecordType
    episode_id: str
    at_wall: str
    at_mono_ms: int

    detection_id: str | None = None
    kind: str | None = None
    bbox_norm: tuple[float, float, float, float] | None = None
    bbox_full: tuple[int, int, int, int] | None = None
    area_frac: float | None = None
    polarity: str | None = None
    first_seen_frame: str | None = None
    confirmed_frame: str | None = None
    confirm_count: int | None = None
    frame_sha256s: Mapping[str, str] = field(default_factory=dict)
    scores: Mapping[str, Any] = field(default_factory=dict)
    veto_bits: int = 0
    latency_ms_from_first_visible: int | None = None
    announce_eligible: bool = False
    person_context_present: bool = False
    announced: bool = False
    shadow: bool = True
    lifecycle_payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("episode_id is required")
        if self.record_type == "detection" and not self.detection_id:
            raise ValueError("detection records require detection_id")

    @property
    def payload(self) -> dict[str, Any]:
        """Materialize the ERR-07 payload for this staged record."""

        if self.record_type != "detection":
            return dict(self.lifecycle_payload)
        return {
            "detection_id": self.detection_id,
            "kind": self.kind,
            "bbox_norm": list(self.bbox_norm) if self.bbox_norm is not None else None,
            "bbox_full": list(self.bbox_full) if self.bbox_full is not None else None,
            "area_frac": self.area_frac,
            "polarity": self.polarity,
            "first_seen_frame": self.first_seen_frame,
            "confirmed_frame": self.confirmed_frame,
            "confirm_count": self.confirm_count,
            "frame_sha256s": dict(self.frame_sha256s),
            "scores": dict(self.scores),
            "veto_bits": self.veto_bits,
            "latency_ms_from_first_visible": self.latency_ms_from_first_visible,
            "announce_eligible": self.announce_eligible,
            "person_context_present": self.person_context_present,
            "announced": self.announced,
            "shadow": self.shadow,
        }

