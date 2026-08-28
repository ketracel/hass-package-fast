"""Package-fast heartbeat, health, state, and daily accounting sensors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import DOMAIN
from .entity import PackageFastEntity


@dataclass(frozen=True, kw_only=True)
class PackageFastSensorDescription(SensorEntityDescription):
    value_fn: Callable[[dict[str, Any]], Any]
    attributes_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None


def _value(key: str):
    return lambda snapshot: snapshot.get(key)


SENSORS = (
    PackageFastSensorDescription(
        key="heartbeat",
        name="Package Fast heartbeat",
        device_class=SensorDeviceClass.TIMESTAMP,
        icon="mdi:heart-pulse",
        value_fn=lambda snapshot: (
            datetime.fromisoformat(snapshot["heartbeat"])
            if snapshot.get("heartbeat")
            else None
        ),
        attributes_fn=lambda snapshot: {
            "status": snapshot.get("heartbeat_status"),
            "suspension_reason": snapshot.get("suspension_reason"),
            "slo_qualified": snapshot.get("slo_qualified"),
            "slo_violations": snapshot.get("slo_violations"),
            "journal_write_failures": snapshot.get("journal_write_failures"),
        },
    ),
    PackageFastSensorDescription(
        key="fetch_p95_ms",
        name="Package Fast fetch p95 ms",
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:camera-clock",
        value_fn=_value("fetch_p95_ms"),
    ),
    PackageFastSensorDescription(
        key="state",
        name="Package Fast state",
        icon="mdi:state-machine",
        value_fn=_value("state"),
        attributes_fn=lambda snapshot: {
            "suspension_reason": snapshot.get("suspension_reason"),
            "freshness_fps": snapshot.get("freshness_fps"),
            "active_suppression_masks": snapshot.get(
                "active_suppression_masks"
            ),
        },
    ),
    PackageFastSensorDescription(
        key="cpu_ms_per_frame_p95",
        name="Package Fast cpu ms per frame p95",
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:cpu-64-bit",
        value_fn=_value("cpu_ms_per_frame_p95"),
    ),
    PackageFastSensorDescription(
        key="daily_poll_gaps",
        name="Package Fast daily poll gaps",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:timeline-alert",
        value_fn=_value("daily_poll_gaps"),
    ),
    PackageFastSensorDescription(
        key="daily_duplicates",
        name="Package Fast daily duplicates",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:content-duplicate",
        value_fn=_value("daily_duplicates"),
    ),
    PackageFastSensorDescription(
        key="daily_detections",
        name="Package Fast daily detections",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:package-variant-closed-check",
        value_fn=_value("daily_detections"),
    ),
    PackageFastSensorDescription(
        key="daily_suspensions",
        name="Package Fast daily suspensions",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:pause-circle",
        value_fn=_value("daily_suspensions"),
    ),
    PackageFastSensorDescription(
        key="daily_interrupted_restarts",
        name="Package Fast daily interrupted restarts",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:restart-alert",
        value_fn=_value("daily_interrupted_restarts"),
    ),
    PackageFastSensorDescription(
        key="daily_restarts",
        name="Package Fast daily HA starts",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:restart",
        value_fn=_value("daily_restarts"),
    ),
    PackageFastSensorDescription(
        key="daily_system_log_warnings",
        name="Package Fast daily system log warnings",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:message-alert",
        value_fn=_value("daily_system_log_warnings"),
    ),
    PackageFastSensorDescription(
        key="daily_shadow_write_skips",
        name="Package Fast daily shadow write skips",
        state_class=SensorStateClass.TOTAL_INCREASING,
        icon="mdi:harddisk-remove",
        value_fn=_value("daily_shadow_write_skips"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    runtime = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        PackageFastSensor(runtime, description) for description in SENSORS
    )


class PackageFastSensor(PackageFastEntity, SensorEntity):
    entity_description: PackageFastSensorDescription

    def __init__(self, runtime, description: PackageFastSensorDescription) -> None:
        super().__init__(runtime)
        self.entity_description = description
        self._attr_unique_id = f"{runtime.entry.entry_id}_{description.key}"

    @property
    def native_value(self):
        return self.entity_description.value_fn(dict(self.runtime.snapshot))

    @property
    def extra_state_attributes(self):
        if self.entity_description.attributes_fn is None:
            return None
        return self.entity_description.attributes_fn(dict(self.runtime.snapshot))
