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
    clean = re.sub(r'\b(?:vbr|cbr|aac|lame|opus|mp3|m4b|m4a|flac|wav|ogg)\b', '', clean, flags=re.IGNORECASE)  # codec/format info
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


def strip_encoding_junk(text):
    """Issue #125: Strip encoding/format info from finalized title or author.

    Applied in build_new_path() as a last-gate filter so no matter where
    data comes from (BookDB, AI, API), encoding garbage is caught before
    it becomes a rename proposal.
    """
    if not text or not isinstance(text, str):
        return text

    clean = text

    # Bracketed junk: [bitsearch.to], [rarbg], [EN], [64420]
    clean = re.sub(r'\[(?:bitsearch\.to|rarbg|EN|\d+)\]', '', clean, flags=re.IGNORECASE)

    # Format markers in parentheses
    clean = re.sub(r'\((?:unabridged|abridged|audiobook|audio|graphicaudio|'
                   r'uk\s*version|us\s*version|uk|us|multi|mono|stereo|'
                   r'r\d+\.\d+|[A-Z])\)',
                   '', clean, flags=re.IGNORECASE)

    # Curly brace content: {465mb}, {narrator}, {mb}, {1.27gb}
    clean = re.sub(r'\{[^}]*\}', '', clean)

    # Codec+bitrate combos: MP3 320kbps, M4B 64k, AAC 256k
    clean = re.sub(r'\b(?:mp3|m4b|aac)\s*\d+\s*k(?:bps)?\b', '', clean, flags=re.IGNORECASE)
    # Bitrates: 62k, 64k, 128k, 192k, 320k, 320kbps
    clean = re.sub(r'\b\d{2,3}\s*k(?:bps)?\b', '', clean, flags=re.IGNORECASE)
    # File extensions in names (before standalone codec strip)
    clean = re.sub(r'\.(?:mp3|m4b|m4a|epub|pdf|mobi|webm|opus|flac|wav|ogg)$',
                   '', clean, flags=re.IGNORECASE)

    # Standalone codecs (with lookahead to protect "MP3 Player" etc.)
    clean = re.sub(r'\bmp3\b(?!\s+\w)', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bm4b\b(?!\s+\w)', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bflac\b(?!\s+\w)', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\baac\b(?!\s+\w)', '', clean, flags=re.IGNORECASE)
    # Quality modes
    clean = re.sub(r'\bvbr\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bcbr\b', '', clean, flags=re.IGNORECASE)

    # File sizes: 463mb, 1.2gb (without braces - braces already handled above)
    clean = re.sub(r'\b\d+(?:\.\d+)?(?:mb|gb|kb)\b', '', clean, flags=re.IGNORECASE)

    # Duration timestamps: 01.10.42, 23.35.16, 69.35.47
    clean = re.sub(r'\b\d{1,2}\.\d{2}\.\d{2}\b', '', clean)

    # Channel info
    clean = re.sub(r'\b(?:mono|stereo|multi)\b', '', clean, flags=re.IGNORECASE)

    # Version numbers: v01, v02
    clean = re.sub(r'\bv\d+\b', '', clean, flags=re.IGNORECASE)

    # Clean up whitespace and trailing junk
    clean = re.sub(r'\s+', ' ', clean).strip()
    clean = re.sub(r'^[-_\s]+|[-_\s]+$', '', clean)
    clean = re.sub(r'\s*-\s*$', '', clean)

    return clean or text  # Fall back to original if we stripped everything


def standardize_initials(name):
    """Issue #54: Normalize author initials to consistent "A. B." format.

    Examples:
        "James S A Corey" → "James S. A. Corey"
        "James S.A. Corey" → "James S. A. Corey"
        "J.R.R. Tolkien" → "J. R. R. Tolkien"
        "JRR Tolkien" → "J. R. R. Tolkien"
        "C.S. Lewis" → "C. S. Lewis"
        "CS Lewis" → "C. S. Lewis"

    Preserves:
        - Full names: "Stephen King" (unchanged)
        - Mc/Mac/O' prefixes: "Freida McFadden" (unchanged)
    """
    if not name:
        return name

    words = name.split()
    result = []

    for word in words:
        # Skip Mc/Mac/O' prefixes - these are part of surnames, not initials
        if re.match(r'^(Mc|Mac|O\')', word, re.IGNORECASE):
            result.append(word)
            continue

        # Check if this word is entirely uppercase letters (initials without periods)
        # e.g., "JRR" → "J. R. R."
        if re.match(r'^[A-Z]{2,}$', word):
            # Split into individual letters with periods
            expanded = '. '.join(list(word)) + '.'
            result.append(expanded)
            continue

        # Check if this is initials with periods stuck together
        # e.g., "J.R.R." → "J. R. R."
        if re.match(r'^([A-Z]\.)+$', word):
            # Add spaces after each period
            expanded = ' '.join(word.split('.')[:-1]) + '.'
            expanded = '. '.join(c for c in expanded.replace(' ', '') if c != '.') + '.'
            result.append(expanded)
            continue

        # Check if this is a single letter (initial without period)
        # e.g., "S" in "James S A Corey" → "S."
        if re.match(r'^[A-Z]$', word):
            result.append(word + '.')
            continue

        # Check if this is a single initial with period
        # e.g., "S." - already correct
        if re.match(r'^[A-Z]\.$', word):
            result.append(word)
            continue

        # Regular word - keep as is
        result.append(word)

    return ' '.join(result)


def clean_author_name(author, config=None):
    """Issue #50: Strip junk suffixes from author names.

    Handles Calibre-style folder names like 'Peter F. Hamilton Bibliography'
    which should become just 'Peter F. Hamilton'.

    If config is provided and standardize_author_initials is enabled,
    also normalizes initials to "A. B." format (Issue #54).
    """
    if not author:
        return author

    clean = author
    # Common junk suffixes found in library folder names
    junk_patterns = [
        r'\s+bibliography\s*$',
        r'\s+collection\s*$',
        r'\s+anthology\s*$',
        r'\s+complete\s+works\s*$',
        r'\s+selected\s+works\s*$',
        r'\s+best\s+of\s*$',
        r'\s+works\s+of\s*$',
        r'\s+omnibus\s*$',
    ]
    for pattern in junk_patterns:
        clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)

    # Also strip Calibre-style IDs from author names: "Author Name (123)"
    clean = re.sub(r'\s*\(\d+\)\s*$', '', clean)

    clean = clean.strip()

    # Issue #54: Standardize initials if enabled
    if config and config.get('standardize_author_initials', False):
        clean = standardize_initials(clean)

    return clean


def extract_author_title(messy_name, clean_author=True):
    """Try to extract author and title from a folder name like 'Author - Title' or 'Author/Title'.

    Args:
        messy_name: The folder name to parse
        clean_author: If True, run clean_author_name on extracted author (default True)
    """
    # Common separators: " - ", " / ", " _ "
    separators = [' - ', ' / ', ' _ ', ' – ']  # includes en-dash

    for sep in separators:
        if sep in messy_name:
            parts = messy_name.split(sep, 1)
            if len(parts) == 2:
                author = parts[0].strip()
                title = parts[1].strip()
                # Basic validation - author shouldn't be too long or look like a title/series
                # Merijeek: "The Gentleman Bastard Sequence #3" was treated as author
                # Check for series indicators: #N, Book N, Series, Saga, Chronicles, etc.
                series_patterns = r'\d{4}|book\s*\d|vol\s*\d|part\s*\d|\[|#\d+|series|saga|chronicles|trilogy|quartet'
                if len(author) < 50 and not re.search(series_patterns, author, re.IGNORECASE):
                    # Issue #50: Clean author name (strip Bibliography, Collection, etc.)
                    if clean_author:
                        author = clean_author_name(author)
                    return author, title

    # No separator found - just return the whole thing as title
    return None, messy_name


__all__ = [
    'calculate_title_similarity',
    'extract_series_from_title',
    'clean_search_title',
    'standardize_initials',
    'clean_author_name',
    'extract_author_title',
]
