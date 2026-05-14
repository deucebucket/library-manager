"""LayerRegistry -- knows about all processing layers and their ordering.

The registry is the single source of truth for which layers exist, what
order they run in, and which config keys control them. Nothing in this
module executes layers -- it only describes and queries them.
"""

import logging
from typing import Dict, List, Optional

from library_manager.pipeline.layer_info import LayerInfo

logger = logging.getLogger(__name__)


class LayerRegistry:
    """Registry of all processing layers in the pipeline.

    Provides lookup by layer_id, ordered listing, and enable/disable
    checks against a config dict.
    """

    def __init__(self):
        self._layers: Dict[str, LayerInfo] = {}

    def register(self, info: LayerInfo) -> None:
        """Register a layer.

        Args:
            info: LayerInfo describing the layer.

        Raises:
            ValueError: If a layer with the same layer_id is already registered.
        """
        if info.layer_id in self._layers:
            raise ValueError(f"Layer '{info.layer_id}' is already registered")
        self._layers[info.layer_id] = info
        logger.debug(f"Registered layer: {info.layer_id} (order={info.default_order})")

    def get_layer(self, layer_id: str) -> Optional[LayerInfo]:
        """Return LayerInfo for a given layer_id, or None if not found."""
        return self._layers.get(layer_id)

    def get_ordered_layers(self, config: Optional[dict] = None) -> List[LayerInfo]:
        """Return all layers in pipeline order.

        If *config* contains a ``pipeline_order`` list of layer_ids, that
        ordering is used (unknown ids are skipped with a warning). Otherwise
        layers are sorted by their ``default_order``.

        Args:
            config: Optional config dict. If None or missing pipeline_order,
                    default ordering is used.

        Returns:
            List of LayerInfo in execution order.
        """
        if config and 'pipeline_order' in config:
            ordered = []
            for layer_id in config['pipeline_order']:
                info = self._layers.get(layer_id)
                if info:
                    ordered.append(info)
                else:
                    logger.warning(f"pipeline_order references unknown layer: {layer_id}")
            return ordered

        return sorted(self._layers.values(), key=lambda li: li.default_order)

    def get_enabled_layers(self, config: dict) -> List[LayerInfo]:
        """Return only enabled layers, in pipeline order.

        A layer is enabled when its ``config_enable_key`` is truthy in
        *config* (or defaults to True if the key is absent).

        Args:
            config: Config dict to check enable keys against.

        Returns:
            List of enabled LayerInfo in execution order.
        """
        return [
            info for info in self.get_ordered_layers(config)
            if self.is_enabled(info.layer_id, config)
        ]

    def is_enabled(self, layer_id: str, config: dict) -> bool:
        """Check whether a layer is enabled in *config*.

        Looks up the layer's ``config_enable_key`` in *config*. If the key
        is missing from config, the layer is considered enabled (safe
        default -- existing behavior before registry existed).

        Args:
            layer_id: The layer to check.
            config: Config dict.

        Returns:
            True if enabled, False otherwise.

        Raises:
            KeyError: If layer_id is not registered.
        """
        info = self._layers.get(layer_id)
        if info is None:
            raise KeyError(f"Unknown layer_id: {layer_id}")
        return bool(config.get(info.config_enable_key, True))

    def get_all_layer_ids(self) -> List[str]:
        """Return all registered layer IDs in default order."""
        return [info.layer_id for info in sorted(
            self._layers.values(), key=lambda li: li.default_order
        )]

    def validate_order(self, order: List[str]) -> tuple:
        """Validate a proposed pipeline_order list.

        Checks that every id in *order* is registered and that there are no
        duplicates. Layers missing from *order* are noted as warnings (they
        won't run).

        Args:
            order: List of layer_ids representing the desired execution order.

        Returns:
            Tuple of (is_valid, errors) where errors is a list of strings.
        """
        errors: List[str] = []
        seen = set()

        for layer_id in order:
            if layer_id in seen:
                errors.append(f"Duplicate layer_id in order: {layer_id}")
            seen.add(layer_id)

            if layer_id not in self._layers:
                errors.append(f"Unknown layer_id: {layer_id}")

        # Warn about registered layers not present in the order
        missing = set(self._layers.keys()) - seen
        for m in sorted(missing):
            errors.append(f"Registered layer '{m}' is missing from order (it will not run)")

        return (len(errors) == 0, errors)

    def __len__(self) -> int:
        return len(self._layers)

    def __contains__(self, layer_id: str) -> bool:
        return layer_id in self._layers


def build_default_registry() -> LayerRegistry:
    """Build and return a registry pre-populated with all current layers.

    The ordering matches the execution sequence in worker.process_all_queue:
      1. audio_id      -- Audio transcription + BookDB identification
      2. audio_credits  -- AI audio clip analysis (Gemini)
      3. sl_requeue     -- Skaldleita re-verification after nightly merge
      4. api_lookup     -- API database lookups (Audnexus, OpenLibrary, etc.)
      5. ai_verify      -- AI verification of folder-based guesses

    Config enable keys are taken from config.py DEFAULT_CONFIG.
    """
    registry = LayerRegistry()

    registry.register(LayerInfo(
        layer_id="audio_id",
        layer_name="Audio ID",
        description="Transcribe audiobook intro via Skaldleita/Whisper and identify from narrator announcement.",
        config_enable_key="enable_audio_identification",
        default_order=1,
        supports_circuit_breaker=True,
        circuit_breaker_apis=("bookdb",),
    ))

    registry.register(LayerInfo(
        layer_id="audio_credits",
        layer_name="AI Audio Analysis",
        description="Send longer audio clip to Gemini AI for deeper analysis when transcription was unclear.",
        config_enable_key="enable_audio_analysis",
        default_order=2,
        supports_circuit_breaker=True,
        circuit_breaker_apis=("gemini",),
    ))

    registry.register(LayerInfo(
        layer_id="sl_requeue",
        layer_name="SL Requeue Check",
        description="Re-verify books against Skaldleita after nightly database merge.",
        config_enable_key="enable_api_lookups",
        default_order=3,
        supports_circuit_breaker=True,
        circuit_breaker_apis=("bookdb",),
    ))

    registry.register(LayerInfo(
        layer_id="api_lookup",
        layer_name="API Lookup",
        description="Look up book metadata from Skaldleita, Audnexus, OpenLibrary, and Google Books.",
        config_enable_key="enable_api_lookups",
        default_order=4,
        supports_circuit_breaker=False,
    ))

    registry.register(LayerInfo(
        layer_id="ai_verify",
        layer_name="AI Verify",
        description="Use AI to verify folder-name-based identification as a last resort.",
        config_enable_key="enable_ai_verification",
        default_order=5,
        supports_circuit_breaker=False,
    ))

    return registry


# Module-level default instance -- importable from anywhere
default_registry = build_default_registry()


__all__ = ['LayerRegistry', 'LayerInfo', 'build_default_registry', 'default_registry']
