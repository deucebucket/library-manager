"""Models package for library-manager."""

from .book_profile import (
    SOURCE_WEIGHTS,
    FIELD_WEIGHTS,
    FieldValue,
    BookProfile,
    detect_multibook_vs_chapters,
    save_book_profile,
    load_book_profile,
    build_profile_from_sources,
    set_db_getter,
    set_narrator_saver
)

__all__ = [
    'SOURCE_WEIGHTS',
    'FIELD_WEIGHTS',
    'FieldValue',
    'BookProfile',
    'detect_multibook_vs_chapters',
    'save_book_profile',
    'load_book_profile',
    'build_profile_from_sources',
    'set_db_getter',
    'set_narrator_saver'
]
