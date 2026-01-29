"""Validation utilities for detecting garbage matches and placeholder values."""
import re
import logging

logger = logging.getLogger(__name__)

# Import from sibling module
from library_manager.utils.naming import calculate_title_similarity


def is_unsearchable_query(title):
    """
    Check if a title is clearly not a book title and shouldn't be searched.
    Returns True for things like:
    - chapter1, chapter2, Chapter 19
    - 01.mp3, track_05
    - audiobook, full audiobook
    - disc1, cd2, part3
    """
    if not title:
        return True

    title_lower = title.lower().strip()

    # Just numbers (01, 001, 1)
    if re.match(r'^\d+$', title_lower):
        return True

    # Chapter + number patterns (chapter1, chapter 5, ch01)
    if re.match(r'^(?:chapter|ch|chap)\s*\d+$', title_lower):
        return True

    # Track/disc/part patterns (track01, disc2, part 3, cd1)
    if re.match(r'^(?:track|disc|cd|part|pt)\s*\d+$', title_lower):
        return True

    # Just "audiobook" or "full audiobook"
    if re.match(r'^(?:full\s+)?audiobook$', title_lower):
        return True

    # Very short titles (1-2 chars) are usually garbage
    if len(title_lower) <= 2:
        return True

    return False


def is_garbage_match(original_title, suggested_title, threshold=0.3):
    """
    Check if an API suggestion is garbage (very low title similarity).
    Returns True if the match should be rejected.

    Examples that should be rejected:
    - "Chapter 19" -> "College Accounting, Chapters 1-9" (only matches "chapter")
    - "Death Genesis" -> "The Darkborn AfterLife Genesis" (only matches "genesis")
    - "Mr. Murder" -> "Frankenstein" (no overlap)
    - "Expeditionary Force Book 14 - Match Game" -> "Match Game" (lost series context!)

    Threshold of 0.3 means at least 30% word overlap required.
    """
    similarity = calculate_title_similarity(original_title, suggested_title)

    # Count significant words (3+ chars)
    orig_words = [w for w in original_title.lower().split() if len(w) > 2]
    sugg_words = [w for w in suggested_title.lower().split() if len(w) > 2]
    orig_count = len(orig_words)
    sugg_count = len(sugg_words)

    # If original is very short (1-2 words), be more lenient
    if orig_count <= 2 and similarity >= 0.2:
        return False

    # Issue #76: If original has MANY words (like series info) but suggested is much shorter,
    # the match likely lost important context (series name, book number, etc.)
    # "Expeditionary Force Book 14 - Match Game" (7 words) -> "Match Game" (2 words) = suspicious
    # BUT: If suggested is contained in original, it might be correct (just the standalone title)
    sugg_in_orig = suggested_title.lower() in original_title.lower()
    if orig_count >= 5 and sugg_count <= 2 and not sugg_in_orig:
        # Suggested title is tiny compared to original - require higher similarity
        if similarity < 0.5:
            logger.info(f"Garbage match rejected (context loss): '{original_title}' -> '{suggested_title}' "
                       f"(similarity: {similarity:.2f}, {orig_count} words -> {sugg_count} words)")
            return True

    # Issue #76: Also check if series indicators are lost
    # If original contains "Book X", "Series", "#X" but suggested doesn't, be suspicious
    # BUT: If suggested title is contained in original (like "Storm Front" in "Dresden Files Book 1 Storm Front"),
    # that's likely correct - the API just returned the standalone title
    series_indicators = ['book', 'series', 'volume', 'vol', 'part', 'chapter']
    orig_has_series = any(ind in original_title.lower() for ind in series_indicators)
    sugg_has_series = any(ind in suggested_title.lower() for ind in series_indicators)

    if orig_has_series and not sugg_has_series and similarity < 0.5 and not sugg_in_orig:
        logger.info(f"Garbage match rejected (series context lost): '{original_title}' -> '{suggested_title}' "
                   f"(similarity: {similarity:.2f})")
        return True

    if similarity < threshold:
        logger.info(f"Garbage match rejected: '{original_title}' vs '{suggested_title}' (similarity: {similarity:.2f})")
        return True

    return False


def is_placeholder_author(name):
    """Check if an author name is a placeholder/system name that should be replaced."""
    if not name:
        return True
    name_lower = name.lower().strip()
    placeholder_authors = {'unknown', 'unknown author', 'various', 'various authors', 'va', 'n/a', 'none',
                           'audiobook', 'audiobooks', 'ebook', 'ebooks', 'book', 'books',
                           'author', 'authors', 'narrator', 'untitled', 'no author',
                           'metadata', 'tmp', 'temp', 'streams', 'cache', 'data', 'log', 'logs',
                           'audio', 'media', 'files', 'downloads', 'torrents',
                           # Issue #46: Common watch/import folder names
                           'watch', 'incoming', 'new', 'import', 'imports', 'inbox', 'input', 'drop'}
    return name_lower in placeholder_authors


def is_drastic_author_change(old_author, new_author):
    """
    Check if an author change is "drastic" (completely different person)
    vs just formatting (case change, initials expanded, etc.)

    Returns True if the change is drastic and should require approval.
    """
    if not old_author or not new_author:
        return False

    # Normalize for comparison
    old_norm = old_author.lower().strip()
    new_norm = new_author.lower().strip()

    # Placeholder authors - going FROM these to a real author is NOT drastic
    placeholder_authors = {'unknown', 'various', 'various authors', 'va', 'n/a', 'none',
                           'audiobook', 'audiobooks', 'ebook', 'ebooks', 'book', 'books',
                           'author', 'authors', 'narrator', 'untitled', 'no author',
                           'metadata', 'tmp', 'temp', 'streams', 'cache'}  # System folders too
    if old_norm in placeholder_authors:
        return False  # Finding the real author is always good


    # If they're the same after normalization, not drastic
    if old_norm == new_norm:
        return False

    # Extract key words (remove common prefixes/suffixes)
    def get_name_parts(name):
        # Remove punctuation and split
        clean = re.sub(r'[^\w\s]', ' ', name.lower())
        parts = [p for p in clean.split() if len(p) > 1]
        return set(parts)

    old_parts = get_name_parts(old_author)
    new_parts = get_name_parts(new_author)

    # If no overlap at all, definitely drastic
    if not old_parts.intersection(new_parts):
        # Check for initials match (e.g., "J.R.R. Tolkien" vs "Tolkien")
        # Get last names (usually the longest word or last word)
        old_last = max(old_parts, key=len) if old_parts else ""
        new_last = max(new_parts, key=len) if new_parts else ""

        if old_last and new_last and (old_last in new_last or new_last in old_last):
            return False  # Probably same person

        return True  # Completely different

    # Some overlap - check how much
    overlap = len(old_parts.intersection(new_parts))
    total = max(len(old_parts), len(new_parts))

    # If less than 30% overlap, consider it drastic
    if total > 0 and overlap / total < 0.3:
        return True

    return False


__all__ = [
    'is_unsearchable_query',
    'is_garbage_match',
    'is_placeholder_author',
    'is_drastic_author_change',
]
