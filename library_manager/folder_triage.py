"""
Folder triage - categorize folder names by cleanliness.

Determines processing strategy per-folder:
- CLEAN: Use folder name as hints for author/title parsing
- MESSY: Skip path parsing, rely on audio/metadata only
- GARBAGE: Skip path parsing, expect harder match, lower confidence

Issue #110 Part 2
"""
import os
import re
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Scene release tags, torrent markers, quality indicators
MESSY_PATTERNS: List[str] = [
    r'\{[a-z]+\}',              # {mb}, {cbt}
    r'\[[A-Z0-9]+\]',           # [FLAC], [MP3]
    r'\([^)]*(?:narrator|read by|unabridged|abridged|rip|scene|kbps)\b[^)]*\)',  # (narrator: Thorne), (Unabridged)
    r'^\d{4}\s*-',              # 2023 -
    r'\d{2}\.\d{2}\.\d{2}',     # 01.10.42
    r'\d+k\b',                  # 62k, 128k
    r'\d+kbps',                 # 64kbps
    r'\bHQ\b|\bLQ\b',           # Quality markers
    r'-[A-Z]{2,4}$',            # -TEAM suffix (scene release)
    r'\.com\b',                 # Website in name
    r'\bwww\.',                 # Website prefix
    r'\b(rip|ripped|scene)\b',  # Rip indicators
    r'\b(x264|aac|mp3|flac|ogg|m4b)\b',  # Codec in name
]

# Completely useless folder names
GARBAGE_PATTERNS: List[str] = [
    r'^[a-f0-9]{12,}$',         # Hash-only names (12+ hex chars)
    r'^[\d\s\-\.]+$',           # Numbers only
    r'^(New Folder|tmp|downloads?|torrents?|audiobooks?|untitled)$',
    r'^(CD|Disc|Track)\s*\d+$', # Disc/track folders
    r'^Unknown\s*(Artist|Author|Album)?$',  # Generic unknowns
]

# Compiled patterns for performance (compiled once at import time)
_MESSY_COMPILED = [re.compile(p, re.IGNORECASE) for p in MESSY_PATTERNS]
_GARBAGE_COMPILED = [re.compile(p, re.IGNORECASE) for p in GARBAGE_PATTERNS]


def triage_folder(folder_name: str) -> str:
    """
    Categorize a folder name by cleanliness.

    Returns:
        'clean'   - Folder name looks like a real author/title
        'messy'   - Has scene tags or markers but might have useful info
        'garbage' - Completely useless (hash, numbers, generic placeholder)
    """
    if not folder_name or not folder_name.strip():
        return 'garbage'

    folder_name = folder_name.strip()

    # Check garbage first (most restrictive)
    for pattern in _GARBAGE_COMPILED:
        if pattern.match(folder_name):
            return 'garbage'

    # Check messy patterns
    for pattern in _MESSY_COMPILED:
        if pattern.search(folder_name):
            return 'messy'

    return 'clean'


def triage_book_path(book_path: str) -> Tuple[str, str]:
    """
    Triage the book folder from a full book path.

    For a path like /audiobooks/Author Name/Book Title,
    triages the immediate parent folder (Book Title).

    Returns:
        tuple: (triage_result, folder_name)
    """
    folder_name = os.path.basename(book_path) if book_path else ''
    return triage_folder(folder_name), folder_name


def should_use_path_hints(triage_result: str) -> bool:
    """Whether path-derived hints should be trusted for this triage category."""
    return triage_result == 'clean'


def confidence_modifier(triage_result: str) -> int:
    """Confidence adjustment based on folder triage category."""
    if triage_result == 'garbage':
        return -10
    return 0
