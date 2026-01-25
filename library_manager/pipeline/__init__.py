"""Pipeline framework for Library Manager processing layers.

This module provides the base classes and orchestration for the
multi-layer book identification pipeline.
"""

from library_manager.pipeline.base_layer import ProcessingLayer, LayerResult

__all__ = [
    'ProcessingLayer',
    'LayerResult',
]
