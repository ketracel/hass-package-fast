"""Config and options flows for package-fast."""

from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CONF_ARMED_RATE_HZ,
    CONF_CAMERA_ENTITY,
    CONF_IDLE_RATE_HZ,
    CONF_MASK_HITS,
    CONF_MASK_IOU,
    CONF_MASK_TTL_HOURS,
    CONF_MASK_WINDOW_HOURS,
    CONF_MAX_AGE_DAYS,
    CONF_MAX_STORAGE_MB,
    CONF_PERSIST_FRAMES,
    DEFAULT_CAMERA_ENTITY,
    DEFAULT_MASK_HITS,
    DEFAULT_MASK_IOU,
    DEFAULT_MASK_TTL_HOURS,
    DEFAULT_MASK_WINDOW_HOURS,
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_MAX_STORAGE_MB,
    DEFAULT_PERSIST_FRAMES,
    DOMAIN,
)
from .core import DetectorConfig


class PackageFastConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Create the integration's credential-free singleton entry."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if user_input is None:
            return self.async_show_form(
                step_id="user", data_schema=vol.Schema({}), errors={}
            )
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(title="Package Fast", data={})

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return PackageFastOptionsFlow()


class PackageFastOptionsFlow(config_entries.OptionsFlow):
    """Expose shell knobs while leaving detector thresholds frozen."""

    async def async_step_init(self, user_input=None):
        defaults = DetectorConfig()
        errors = {}
        if user_input is not None:
            if float(user_input[CONF_ARMED_RATE_HZ]) < float(
                user_input[CONF_IDLE_RATE_HZ]
            ):
                errors["base"] = "armed_below_idle"
            else:
                return self.async_create_entry(title="", data=user_input)

        # Preserve the submitted values when redisplaying a validation error.
        options = (
            user_input
            if user_input is not None
            else self.config_entry.options
        )
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_CAMERA_ENTITY,
                    default=options.get(CONF_CAMERA_ENTITY, DEFAULT_CAMERA_ENTITY),
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="camera")
                ),
                vol.Required(
                    CONF_IDLE_RATE_HZ,
                    default=options.get(CONF_IDLE_RATE_HZ, defaults.idle_rate_hz),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=0.1,
                        max=2.0,
                        step=0.1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Required(
                    CONF_ARMED_RATE_HZ,
                    default=options.get(CONF_ARMED_RATE_HZ, defaults.armed_rate_hz),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1.5,
                        max=2.0,
                        step=0.1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Required(
                    CONF_PERSIST_FRAMES,
                    default=options.get(
                        CONF_PERSIST_FRAMES, DEFAULT_PERSIST_FRAMES
                    ),
                ): selector.BooleanSelector(),
                vol.Required(
                    CONF_MAX_STORAGE_MB,
                    default=options.get(
                        CONF_MAX_STORAGE_MB, DEFAULT_MAX_STORAGE_MB
                    ),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=64,
                        max=8_192,
                        step=64,
                        mode=selector.NumberSelectorMode.BOX,
                        unit_of_measurement="MB",
                    )
                ),
                vol.Required(
                    CONF_MAX_AGE_DAYS,
                    default=options.get(CONF_MAX_AGE_DAYS, DEFAULT_MAX_AGE_DAYS),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1,
                        max=90,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                        unit_of_measurement="days",
                    )
                ),
                vol.Required(
                    CONF_MASK_HITS,
                    default=options.get(CONF_MASK_HITS, DEFAULT_MASK_HITS),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=3,
                        max=20,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Required(
                    CONF_MASK_WINDOW_HOURS,
                    default=options.get(
                        CONF_MASK_WINDOW_HOURS, DEFAULT_MASK_WINDOW_HOURS
                    ),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1,
                        max=168,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                        unit_of_measurement="h",
                    )
                ),
                vol.Required(
                    CONF_MASK_TTL_HOURS,
                    default=options.get(
                        CONF_MASK_TTL_HOURS, DEFAULT_MASK_TTL_HOURS
                    ),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1,
                        max=168,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                        unit_of_measurement="h",
                    )
                ),
                vol.Required(
                    CONF_MASK_IOU,
                    default=options.get(CONF_MASK_IOU, DEFAULT_MASK_IOU),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=0.1,
                        max=1.0,
                        step=0.05,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
            }
        )
        return self.async_show_form(
            step_id="init", data_schema=schema, errors=errors
        )
