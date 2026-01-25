"""Pipeline framework for Library Manager processing layers.

This module provides the base classes and orchestration for the
multi-layer book identification pipeline.
"""

from library_manager.pipeline.base_layer import ProcessingLayer, LayerResult, LayerAction
from library_manager.pipeline.layer_content import process_layer_4_content
from library_manager.pipeline.layer_audio_credits import process_layer_3_audio
from library_manager.pipeline.layer_api import process_layer_1_api
from library_manager.pipeline.layer_audio_id import process_layer_1_audio

__all__ = [
    'ProcessingLayer',
    'LayerResult',
    'LayerAction',
    'process_layer_4_content',
    'process_layer_3_audio',
    'process_layer_1_api',
    'process_layer_1_audio',
]
