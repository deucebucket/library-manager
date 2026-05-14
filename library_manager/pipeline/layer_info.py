"""LayerInfo dataclass for describing processing layers.

Each processing layer in the pipeline has metadata describing its identity,
configuration, and capabilities. LayerInfo captures this without any
runtime behavior -- it's purely descriptive.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LayerInfo:
    """Metadata describing a single processing layer.

    This is a pure data object -- it knows WHAT a layer is, not HOW it runs.
    The registry uses these to track ordering, enable/disable state, and
    circuit breaker dependencies.

    Attributes:
        layer_id: Unique identifier (e.g. "audio_id", "api_lookup").
        layer_name: Human-readable display name.
        description: What this layer does, shown in UI/logs.
        config_enable_key: Config key that enables/disables this layer.
        default_order: Default position in the pipeline (1-based).
        supports_circuit_breaker: Whether this layer uses circuit breakers.
        circuit_breaker_apis: Which API circuit breakers this layer depends on.
    """

    layer_id: str
    layer_name: str
    description: str
    config_enable_key: str
    default_order: int
    supports_circuit_breaker: bool = False
    circuit_breaker_apis: tuple = field(default_factory=tuple)

    def __post_init__(self):
        if not self.layer_id:
            raise ValueError("layer_id cannot be empty")
        if self.default_order < 1:
            raise ValueError("default_order must be >= 1")


__all__ = ['LayerInfo']
