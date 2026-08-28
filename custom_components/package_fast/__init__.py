"""Home Assistant integration for the package-fast detector.

Home Assistant imports are deliberately local to the setup hooks.  That keeps
the package importable in the offline core/shell test environment, where Home
Assistant itself is not installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .const import DOMAIN, PLATFORMS

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant


def _import_runtime():
    """Import Pillow-pulling runtime modules only inside HA's import executor."""

    from .runtime import PackageFastRuntime

    return PackageFastRuntime


def _import_websocket_registration():
    """Import the HA-coupled WebSocket module outside the event loop."""

    from .websocket import async_register_commands

    return async_register_commands


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up one package-fast config entry."""

    PackageFastRuntime = await hass.async_add_import_executor_job(_import_runtime)
    async_register_commands = await hass.async_add_import_executor_job(
        _import_websocket_registration
    )
    runtime = await PackageFastRuntime.async_create(hass, entry)
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = runtime
    entry.async_on_unload(entry.add_update_listener(_async_reload_entry))

    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        await runtime.async_start()
        async_register_commands(hass)
    except Exception:
        await runtime.async_stop(close_episode=False)
        hass.data[DOMAIN].pop(entry.entry_id, None)
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload package-fast without leaving fetch or executor work behind."""

    runtime: Any = hass.data[DOMAIN][entry.entry_id]
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unloaded:
        return False

    await runtime.async_stop(close_episode=not hass.is_stopping)
    hass.data[DOMAIN].pop(entry.entry_id, None)
    if not hass.data[DOMAIN]:
        hass.data.pop(DOMAIN)
    return True


async def _async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply option changes through a clean unload/reload boundary."""

    await hass.config_entries.async_reload(entry.entry_id)


__all__ = ["async_setup_entry", "async_unload_entry"]
