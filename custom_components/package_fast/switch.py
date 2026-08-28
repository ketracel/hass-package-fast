"""Fast-detector poller kill switch."""

from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_ON
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN
from .entity import PackageFastEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    runtime = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([PackageFastDetectorSwitch(runtime)])


class PackageFastDetectorSwitch(PackageFastEntity, SwitchEntity, RestoreEntity):
    """Turn the package-fast poller on/off without changing the master."""

    _attr_name = "Package Fast detector"
    _attr_icon = "mdi:package-variant"

    def __init__(self, runtime) -> None:
        super().__init__(runtime)
        self._attr_unique_id = f"{runtime.entry.entry_id}_detector"

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        previous = await self.async_get_last_state()
        await self.runtime.async_set_enabled(
            previous is None or previous.state == STATE_ON
        )

    @property
    def is_on(self) -> bool:
        return self.runtime.enabled

    async def async_turn_on(self, **kwargs) -> None:
        await self.runtime.async_set_enabled(True)

    async def async_turn_off(self, **kwargs) -> None:
        await self.runtime.async_set_enabled(False)

    @property
    def extra_state_attributes(self):
        return {
            "master_enabled": self.runtime.master_on,
            "effective_enabled": self.runtime.enabled and self.runtime.master_on,
        }

