"""Shared entity projection for the package-fast runtime."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity

from .const import DOMAIN, INTEGRATION_VERSION
from .runtime import PackageFastRuntime


class PackageFastEntity(Entity):
    """Push-updated projection; no entity performs its own polling."""

    _attr_should_poll = False

    def __init__(self, runtime: PackageFastRuntime) -> None:
        self.runtime = runtime
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, runtime.entry.entry_id)},
            name="Package Fast",
            manufacturer="77OS",
            model="Deterministic package detector",
            sw_version=INTEGRATION_VERSION,
        )

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            self.runtime.async_add_listener(self.async_write_ha_state)
        )

