"""Momentary package-fast deposit result."""

from __future__ import annotations

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import DOMAIN
from .entity import PackageFastEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    runtime = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([PackageFastDeposit(runtime)])


class PackageFastDeposit(PackageFastEntity, BinarySensorEntity):
    """Latest durable deposit result, on momentarily for automation/UI use."""

    _attr_name = "Package Fast deposit"

    def __init__(self, runtime) -> None:
        super().__init__(runtime)
        self._attr_unique_id = f"{runtime.entry.entry_id}_deposit"

    @property
    def is_on(self) -> bool:
        return bool(self.runtime.snapshot.get("deposit", False))

    @property
    def extra_state_attributes(self):
        latest = self.runtime.snapshot.get("latest_deposit")
        return {"latest_result": latest} if latest is not None else {}

