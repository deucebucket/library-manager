"""Pipeline framework for Library Manager processing layers.

This module provides the base classes and orchestration for the
multi-layer book identification pipeline.
"""

from library_manager.pipeline.base_layer import ProcessingLayer, LayerResult, LayerAction
from library_manager.pipeline.layer_content import process_layer_4_content

__all__ = [
    'ProcessingLayer',
    'LayerResult',
    'LayerAction',
    'process_layer_4_content',
]
