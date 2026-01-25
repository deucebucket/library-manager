"""Utility functions for Library Manager."""

from library_manager.utils.naming import (
    calculate_title_similarity,
    extract_series_from_title,
    clean_search_title,
    standardize_initials,
    clean_author_name,
    extract_author_title,
)
from library_manager.utils.validation import (
    is_unsearchable_query,
    is_garbage_match,
    is_placeholder_author,
    is_drastic_author_change,
)
from library_manager.utils.audio import (
    get_first_audio_file,
    extract_audio_sample,
    extract_audio_sample_from_middle,
)
from library_manager.utils.path_safety import (
    sanitize_path_component,
    build_new_path,
)

__all__ = [
    # naming
    'calculate_title_similarity',
    'extract_series_from_title',
    'clean_search_title',
    'standardize_initials',
    'clean_author_name',
    'extract_author_title',
    # validation
    'is_unsearchable_query',
    'is_garbage_match',
    'is_placeholder_author',
    'is_drastic_author_change',
    # audio
    'get_first_audio_file',
    'extract_audio_sample',
    'extract_audio_sample_from_middle',
    # path_safety
    'sanitize_path_component',
    'build_new_path',
]
