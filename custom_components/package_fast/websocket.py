"""Authenticated, read-only WebSocket surfaces for package-fast."""

from __future__ import annotations

from functools import partial
from typing import Any

import voluptuous as vol

from homeassistant.components import websocket_api
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .paging import (
    HEALTH_DEFAULT_LIMIT,
    HEALTH_MAX_LIMIT,
    JOURNAL_DEFAULT_LIMIT,
    JOURNAL_MAX_LIMIT,
)


def _reject_bool(value: Any) -> Any:
    """Reject booleans before voluptuous applies integer validators."""

    if isinstance(value, bool):
        raise vol.Invalid("boolean values are not integers")
    return value


def _loaded_runtime(hass: HomeAssistant) -> Any | None:
    """Return the single loaded config-entry runtime, if any."""

    runtimes = hass.data.get(DOMAIN)
    if not isinstance(runtimes, dict) or not runtimes:
        return None
    return next(iter(runtimes.values()))


@websocket_api.require_admin
@websocket_api.async_response
@websocket_api.websocket_command(
    {
        vol.Required("type"): "package_fast/journal",
        vol.Optional("since_seq"): vol.All(_reject_bool, int, vol.Range(min=0)),
        vol.Optional("episode_id"): vol.All(str, vol.Length(min=1)),
        vol.Optional("limit", default=JOURNAL_DEFAULT_LIMIT): vol.All(
            _reject_bool, int, vol.Range(min=1, max=JOURNAL_MAX_LIMIT)
        ),
    }
)
async def websocket_journal(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return one page from the configured canonical journal."""

    runtime = _loaded_runtime(hass)
    if runtime is None:
        connection.send_error(msg["id"], "not_loaded", "Package Fast is not loaded")
        return
    result = await hass.async_add_executor_job(
        partial(
            runtime.backend.read_journal,
            since_seq=msg.get("since_seq"),
            episode_id=msg.get("episode_id"),
            limit=msg["limit"],
        )
    )
    connection.send_result(msg["id"], result)


@websocket_api.require_admin
@websocket_api.async_response
@websocket_api.websocket_command(
    {
        vol.Required("type"): "package_fast/health",
        vol.Optional("limit", default=HEALTH_DEFAULT_LIMIT): vol.All(
            _reject_bool, int, vol.Range(min=1, max=HEALTH_MAX_LIMIT)
        ),
    }
)
async def websocket_health(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Return current detector health and bounded diagnostic history."""

    runtime = _loaded_runtime(hass)
    if runtime is None:
        connection.send_error(msg["id"], "not_loaded", "Package Fast is not loaded")
        return
    result = await hass.async_add_executor_job(
        partial(runtime.backend.read_health, limit=msg["limit"])
    )
    connection.send_result(msg["id"], result)


def async_register_commands(hass: HomeAssistant) -> None:
    """Register both package-fast WebSocket commands."""

    websocket_api.async_register_command(hass, websocket_journal)
    websocket_api.async_register_command(hass, websocket_health)


__all__ = ["async_register_commands", "websocket_health", "websocket_journal"]
