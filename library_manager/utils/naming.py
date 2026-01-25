"""Title cleaning and series extraction utilities."""
import re
import logging

logger = logging.getLogger(__name__)


def calculate_title_similarity(title1, title2):
    """
    Calculate word overlap similarity between two titles.
    Returns a score from 0.0 to 1.0
    """
    if not title1 or not title2:
        return 0.0

    # Normalize: lowercase, remove punctuation, split into words
    def normalize(t):
        t = t.lower()
        t = re.sub(r'[^\w\s]', ' ', t)
        words = set(t.split())
        # Remove common stop words that don't help matching
        stop_words = {'the', 'a', 'an', 'of', 'and', 'or', 'in', 'to', 'for', 'by', 'part', 'book', 'volume'}
        return words - stop_words

    words1 = normalize(title1)
    words2 = normalize(title2)

    if not words1 or not words2:
        return 0.0

    # Calculate Jaccard similarity (intersection over union)
    intersection = words1 & words2
    union = words1 | words2

    return len(intersection) / len(union) if union else 0.0


def extract_series_from_title(title):
    """
    Extract series name and number from title patterns like:
    - "The Firefly Series, Book 8: Coup de Grâce" -> (Firefly, 8, Coup de Grâce)
    - "The Firefly Series, Book 8꞉ Firefly꞉ Coup de Grâce" -> (Firefly, 8, Firefly: Coup de Grâce)
    - "Mistborn Book 1: The Final Empire" -> (Mistborn, 1, The Final Empire)
    - "The Expanse #3 - Abaddon's Gate" -> (The Expanse, 3, Abaddon's Gate)
    """
    # Normalize colon-like characters (Windows uses ꞉ instead of : in filenames)
    normalized = title.replace('꞉', ':').replace('：', ':')  # U+A789 and full-width colon

    # Pattern: "Series Name, Book N: Title" or "Series Name Book N: Title"
    # Also handles "The X Series, Book N: Title"
    match = re.search(r'^(?:The\s+)?(.+?)\s*(?:Series)?,?\s*Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        # Clean up series name (remove trailing "Series" if it got in)
        series = re.sub(r'\s*Series\s*$', '', series, flags=re.IGNORECASE)
        return series, int(match.group(2)), match.group(3).strip()

    # Pattern: "Series #N - Title" or "Series #N: Title"
    match = re.search(r'^(.+?)\s*#(\d+)\s*[:\s-]+(.+)$', normalized)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N - Title"
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N" at END (no subtitle) - e.g., "Dark One Book 1"
    # Series name = title before "Book N", actual title = same as series
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series  # Title = series name

    # Pattern: "Series #N" at END (no subtitle) - e.g., "Mistborn #1"
    match = re.search(r'^(.+?)\s*#(\d+)\s*$', normalized)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series

    # Pattern: "Title (Book N)" - book number in parentheses at end
    # e.g., "Ivypool's Heart (Book 17)" -> extract number, title stays same
    match = re.search(r'^(.+?)\s*\(Book\s+(\d+)\)\s*$', normalized, re.IGNORECASE)
    if match:
        title_clean = match.group(1).strip()
        return None, int(match.group(2)), title_clean  # Series unknown, just got number

    return None, None, title


def clean_search_title(messy_name):
    """Clean up a messy filename to extract searchable title."""
    # Remove common junk patterns
    clean = messy_name

    # Convert underscores to spaces first (common in filenames)
    clean = clean.replace('_', ' ')

    # Remove bracketed content like [bitsearch.to], [64k], [r1.1]
    clean = re.sub(r'\[.*?\]', '', clean)
    # Remove parenthetical junk like (Unabridged), (2019) - but keep series info like (Book 1)
    clean = re.sub(r'\((?:Unabridged|Abridged|MP3|M4B|EPUB|PDF|64k|128k|192k|256k|320k|VBR|r\d+\.\d+|multi|mono|stereo).*?\)', '', clean, flags=re.IGNORECASE)
    # Issue #50: Remove Calibre-style IDs - just bare numbers in parens at end like "(123)"
    # These are Calibre's internal book IDs, not series info. Must NOT match "(Book 1)" etc.
    clean = re.sub(r'\s*\(\d+\)$', '', clean)
    # Remove curly brace junk like {465mb}, {narrator}, {128k}
    clean = re.sub(r'\{[^}]*\}', '', clean)
    # Issue #48: Remove standalone encoding info (bitrates, file sizes, channel info)
    clean = re.sub(r'\b\d+k(?:bps)?\b', '', clean, flags=re.IGNORECASE)  # 128k, 64kbps
    clean = re.sub(r'\b\d+(?:\.\d+)?(?:mb|gb|kb)\b', '', clean, flags=re.IGNORECASE)  # 463mb, 1.2gb
    clean = re.sub(r'\b(?:mono|stereo|multi)\b', '', clean, flags=re.IGNORECASE)  # audio channels
    clean = re.sub(r'\b(?:vbr|cbr|aac|lame|opus)\b', '', clean, flags=re.IGNORECASE)  # codec info
    # Remove file extensions
    clean = re.sub(r'\.(mp3|m4b|m4a|epub|pdf|mobi|webm|opus)$', '', clean, flags=re.IGNORECASE)
    # Remove "by Author" at the end temporarily for searching
    clean = re.sub(r'\s+by\s+[\w\s]+$', '', clean, flags=re.IGNORECASE)
    # Remove audiobook-related junk (YouTube rip artifacts)
    clean = re.sub(r'\b(full\s+)?audiobook\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(complete|unabridged|abridged)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(audio\s*book|audio)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\b(free|download|hd|hq)\b', '', clean, flags=re.IGNORECASE)
    # NOTE: We intentionally DON'T strip date/timestamp patterns here
    # Books like "11/22/63" by Stephen King have dates AS titles
    # The layered verification system (API + AI + Audio) will determine the real title
    # This function just cleans obvious junk for searching - title verification is separate

    # Strip leading track/chapter numbers like "06 - Title", "01. Title", "Track 05 - Title"
    # Also handles "02 Night" (number + space, no separator) which is common in downloads
    # These are common in audiobook folders but mess up search
    clean = re.sub(r'^(?:track\s*)?\d+\s*[-–—:.]?\s+', '', clean, flags=re.IGNORECASE)

    # Remove extra whitespace
    clean = re.sub(r'\s+', ' ', clean)
    # Remove leading/trailing junk
    clean = clean.strip(' -_.')
    return clean


__all__ = [
    'calculate_title_similarity',
    'extract_series_from_title',
    'clean_search_title',
]
