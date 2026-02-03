"""Path sanitization and building utilities."""
import re
import logging
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Language code to full name mapping for multi-language naming
LANGUAGE_NAMES = {
    'en': 'English', 'de': 'German', 'fr': 'French', 'es': 'Spanish',
    'it': 'Italian', 'pt': 'Portuguese', 'nl': 'Dutch', 'sv': 'Swedish',
    'no': 'Norwegian', 'da': 'Danish', 'fi': 'Finnish', 'pl': 'Polish',
    'ru': 'Russian', 'ja': 'Japanese', 'zh': 'Chinese', 'ko': 'Korean',
    'ar': 'Arabic', 'he': 'Hebrew', 'hi': 'Hindi', 'tr': 'Turkish',
    'cs': 'Czech', 'hu': 'Hungarian', 'el': 'Greek', 'th': 'Thai',
    'vi': 'Vietnamese', 'uk': 'Ukrainian', 'ro': 'Romanian', 'id': 'Indonesian'
}

# Patterns to strip from titles when strip_unabridged is enabled (Issue #92)
UNABRIDGED_PATTERNS = [
    r'\s*\(Unabridged\)',
    r'\s*\[Unabridged\]',
    r'\s*-\s*Unabridged\b',
    r'\s*,\s*Unabridged\b',
    r'\s+Unabridged$',  # Trailing "Unabridged"
    r'\s*\(Abridged\)',  # Also strip abridged markers
    r'\s*\[Abridged\]',
    r'\s*-\s*Abridged\b',
    r'\s*,\s*Abridged\b',
    r'\s+Abridged$',
]


def strip_unabridged_markers(title: str) -> str:
    """Remove (Unabridged), [Unabridged], etc. from title.

    Issue #92: Users may prefer clean titles without edition markers.
    """
    cleaned = title
    for pattern in UNABRIDGED_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


# Common name prefixes (particles) that are part of surnames
# These should stay with the last name: "de Balzac" -> last="de Balzac"
NAME_PREFIXES = {
    'de', 'da', 'di', 'del', 'della', 'van', 'von', 'der', 'den', 'ter',
    'le', 'la', 'du', 'des', 'el', 'al', 'ibn', 'bin', 'ben', 'mc', "o'"
}

# Name suffixes that should be kept with the name but not treated as last name
NAME_SUFFIXES = {
    'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'vi',
    'phd', 'ph.d', 'ph.d.', 'md', 'm.d', 'm.d.', 'esq', 'esq.'
}


def parse_author_name(author: str) -> Tuple[str, str]:
    """Parse author name into (first_name, last_name) components.

    Issue #96: Support for "LastName, FirstName" folder format.

    Handles various edge cases:
    - Single names: "Madonna" -> ("", "Madonna")
    - Standard names: "Brandon Sanderson" -> ("Brandon", "Sanderson")
    - Multiple names: "J. R. R. Tolkien" -> ("J. R. R.", "Tolkien")
    - Prefixes: "Ursula K. Le Guin" -> ("Ursula K.", "Le Guin")
    - Suffixes: "Robert Downey Jr." -> ("Robert", "Downey Jr.")
    - Already formatted: "Sanderson, Brandon" -> ("Brandon", "Sanderson")

    Returns:
        Tuple of (first_name, last_name). If single name, first_name is empty.
    """
    if not author or not isinstance(author, str):
        return ('', '')

    author = author.strip()
    if not author:
        return ('', '')

    # Check if already in "Last, First" format
    if ',' in author:
        parts = [p.strip() for p in author.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            # Handle suffix that might be after comma: "Downey, Robert Jr."
            # vs actual Last, First format: "Sanderson, Brandon"
            second_part = parts[1]
            second_lower = second_part.lower().rstrip('.')

            # Check if second part is just a suffix
            if second_lower in NAME_SUFFIXES:
                # This is "LastName, Jr." format - not a first name
                # Remove the comma and continue with regular parsing
                # "Downey, Jr." -> "Downey Jr."
                author = parts[0] + ' ' + parts[1]
            else:
                # Standard "Last, First" format
                return (parts[1], parts[0])

    # Split into words
    words = author.split()
    if len(words) == 1:
        # Single name like "Madonna" or "Prince"
        return ('', words[0])

    # Extract suffix if present at end
    suffix = ''
    while words and words[-1].lower().rstrip('.') in NAME_SUFFIXES:
        suffix = words.pop() + (' ' + suffix if suffix else '')
    suffix = suffix.strip()

    if not words:
        # Only had suffix somehow
        return ('', suffix)

    if len(words) == 1:
        # Single name + suffix
        last = words[0] + (' ' + suffix if suffix else '')
        return ('', last)

    # Check for name prefix patterns
    # "Ursula K. Le Guin" -> Le Guin is last name (Le is prefix)
    # "Ludwig van Beethoven" -> van Beethoven is last name (van is prefix)
    # Strategy: scan backwards from end, collecting last name parts
    # A prefix only counts if it's followed by another word

    # First pass: identify where the last name starts
    # Look for prefix patterns from position 1 onwards (not first word)
    last_name_start = len(words) - 1  # Default: last word is the last name

    for i in range(1, len(words)):
        word_lower = words[i].lower().rstrip("'")
        if word_lower in NAME_PREFIXES:
            # Found a prefix - check if there's at least one word after it
            if i < len(words) - 1:
                last_name_start = i
                break

    # If no prefix found, just use the last word
    last_parts = words[last_name_start:]
    words = words[:last_name_start]

    last_name = ' '.join(last_parts)
    if suffix:
        last_name = last_name + ' ' + suffix

    first_name = ' '.join(words)

    return (first_name, last_name)


def format_author_lf(author: str) -> str:
    """Format author as "LastName, FirstName".

    Issue #96: Support for user-requested folder format.

    Examples:
        "Brandon Sanderson" -> "Sanderson, Brandon"
        "J. R. R. Tolkien" -> "Tolkien, J. R. R."
        "Madonna" -> "Madonna"
        "Ursula K. Le Guin" -> "Le Guin, Ursula K."
    """
    first, last = parse_author_name(author)
    if not first:
        return last
    return f"{last}, {first}"


def format_author_fl(author: str) -> str:
    """Format author as "FirstName LastName" (standard format).

    Useful when input might be in "Last, First" format and needs normalizing.
    """
    first, last = parse_author_name(author)
    if not first:
        return last
    return f"{first} {last}"


def format_language_tag(lang_code: str, lang_name: str = None, fmt: str = "bracket_full") -> str:
    """Format language tag based on user preference.

    Args:
        lang_code: ISO 639-1 language code (e.g., 'pl', 'ru')
        lang_name: Full language name (optional, will lookup from LANGUAGE_NAMES)
        fmt: Tag format - "code", "full", "bracket_code", "bracket_full"

    Returns:
        Formatted tag string
    """
    name = lang_name or LANGUAGE_NAMES.get(lang_code, lang_code.upper())

    if fmt == "code":
        return f"_{lang_code}"
    elif fmt == "full":
        return f" {name}"
    elif fmt == "bracket_code":
        return f" [{lang_code}]"
    elif fmt == "bracket_full":
        return f" ({name})"
    return ""


def apply_language_tag(title: str, tag: str, position: str) -> str:
    """Apply language tag to title in specified position.

    Args:
        title: Book title
        tag: Formatted language tag (e.g., " (Russian)")
        position: "before_title", "after_title" (subfolder handled separately)

    Returns:
        Title with tag applied
    """
    if position == "before_title":
        return f"{tag.strip()} {title}"
    elif position == "after_title":
        return f"{title}{tag}"
    # subfolder position handled separately in path construction
    return title


def _apply_template_modifiers(template: str, field: str, value: str) -> str:
    """Apply modifiers like .pad(N) to template fields.

    Supports FileBot-style modifiers:
    - {series_num.pad(2)} -> zero-pad to 2 digits (1 -> 01, 10 -> 10)
    - {series_num.pad(3)} -> zero-pad to 3 digits (1 -> 001, 10 -> 010)

    Issue #80: Feature request by derp90
    """
    # Pattern: {field.pad(N)} where N is 1-9
    pad_pattern = re.compile(r'\{' + re.escape(field) + r'\.pad\((\d+)\)\}')

    match = pad_pattern.search(template)
    while match:
        pad_width = int(match.group(1))

        # Try to pad the value
        # Issue #94: If value is empty, return empty (not "00")
        if not value:
            padded = ''
        else:
            try:
                num = float(str(value).replace(',', '.'))
                if num == int(num):
                    padded = str(int(num)).zfill(pad_width)
                else:
                    # Decimal: pad the integer part only
                    int_part = int(num)
                    decimal_part = str(num).split('.')[-1]
                    padded = f"{str(int_part).zfill(pad_width)}.{decimal_part}"
            except (ValueError, TypeError):
                # Can't parse as number, use original
                padded = str(value)

        # Replace this specific match
        template = template[:match.start()] + padded + template[match.end():]

        # Find next match (position shifted, search again)
        match = pad_pattern.search(template)

    return template


def sanitize_path_component(name):
    """Sanitize a path component to prevent directory traversal and invalid chars.

    CRITICAL SAFETY FUNCTION - prevents catastrophic file moves.
    """
    if not name or not isinstance(name, str):
        return None

    # Strip whitespace
    name = name.strip()

    # Block empty strings
    if not name:
        return None

    # Block directory traversal attempts
    if '..' in name or name.startswith('/') or name.startswith('\\'):
        logger.warning(f"BLOCKED dangerous path component: {name}")
        return None

    # Remove/replace dangerous characters
    # Windows: < > : " / \ | ? *
    # Also remove control characters
    dangerous_chars = '<>:"/\\|?*\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f'
    for char in dangerous_chars:
        name = name.replace(char, '')

    # Final strip and check
    name = name.strip('. ')  # Windows doesn't like trailing dots/spaces
    if not name or len(name) < 2:
        return None

    return name


def build_new_path(lib_path, author, title, series=None, series_num=None, narrator=None, year=None,
                   edition=None, variant=None, language=None, language_code=None, config=None):
    """Build a new path based on the naming format configuration.

    Audiobookshelf-compatible format (when series_grouping enabled):
    - Narrator in curly braces: {Ray Porter}
    - Series number prefix: "1 - Title"
    - Year in parentheses: (2003)
    - Edition in brackets: [30th Anniversary Edition]
    - Variant in brackets: [Graphic Audio]
    - Language tag: (Russian), [pl], etc. (when enabled)

    Args:
        lib_path: Library root path
        author: Author name
        title: Book title
        series: Series name (optional)
        series_num: Series number (optional)
        narrator: Narrator name (optional)
        year: Publication year (optional)
        edition: Edition info (optional)
        variant: Variant info like "Graphic Audio" (optional)
        language: Full language name like "Russian" (optional)
        language_code: ISO 639-1 code like "ru" (optional)
        config: Configuration dict

    SAFETY: Returns None if path would be invalid/dangerous.
    """
    naming_format = config.get('naming_format', 'author/title') if config else 'author/title'
    series_grouping = config.get('series_grouping', False) if config else False

    # CRITICAL SAFETY: Sanitize all path components
    safe_author = sanitize_path_component(author)
    safe_title = sanitize_path_component(title)
    safe_series = sanitize_path_component(series) if series else None

    # CRITICAL: Reject if author or title are invalid
    if not safe_author or not safe_title:
        logger.error(f"BLOCKED: Invalid author '{author}' or title '{title}' - would create dangerous path")
        return None

    # Issue #92: Strip "Unabridged"/"Abridged" markers if enabled
    if config and config.get('strip_unabridged', False):
        safe_title = strip_unabridged_markers(safe_title)

    # Build title folder name
    title_folder = safe_title

    # Add series number prefix if series grouping enabled and we have series info
    # Merijeek: ABS compatibility - pad single-digit numbers for better sorting/detection
    if series_grouping and safe_series and series_num:
        # Normalize series_num to string and zero-pad if single digit
        try:
            num = float(str(series_num).replace(',', '.'))  # Handle "1,5" -> 1.5
            if num == int(num):
                # Whole number - pad to 2 digits (e.g., 1 -> "01", 10 -> "10")
                formatted_num = f"{int(num):02d}"
            else:
                # Decimal number (e.g., 1.5) - keep as-is
                formatted_num = str(series_num)
        except (ValueError, TypeError):
            formatted_num = str(series_num)
        title_folder = f"{formatted_num} - {safe_title}"

    # Add edition/variant in brackets (e.g., [30th Anniversary Edition], [Graphic Audio])
    # These distinguish different versions of the same book
    if variant:
        safe_variant = sanitize_path_component(variant)
        if safe_variant:
            title_folder = f"{title_folder} [{safe_variant}]"
    elif edition:
        safe_edition = sanitize_path_component(edition)
        if safe_edition:
            title_folder = f"{title_folder} [{safe_edition}]"

    # Add year if present (and no edition/variant already added for version distinction)
    if year and not edition and not variant:
        title_folder = f"{title_folder} ({year})"

    # Add narrator - curly braces for ABS format, parentheses otherwise
    if narrator:
        safe_narrator = sanitize_path_component(narrator)
        if safe_narrator:
            if series_grouping:
                # ABS format uses curly braces for narrator
                title_folder = f"{title_folder} {{{safe_narrator}}}"
            else:
                # Legacy format uses parentheses
                title_folder = f"{title_folder} ({safe_narrator})"

    # Multi-language naming: add language tag to title folder if enabled
    # (subfolder position handled separately during path construction)
    lang_subfolder = None  # Will be set if position is "subfolder"
    if config and language_code:
        preferred_lang = config.get('preferred_language', 'en')
        multilang_mode = config.get('multilang_naming_mode', 'native')
        tag_enabled = config.get('language_tag_enabled', False)

        # Determine if we should add a tag
        should_tag = (tag_enabled or multilang_mode == 'tagged') and language_code != preferred_lang

        if should_tag:
            tag_format = config.get('language_tag_format', 'bracket_full')
            tag_position = config.get('language_tag_position', 'after_title')

            if tag_position == 'subfolder':
                # Will create Author/Language/Title structure
                lang_name = language or LANGUAGE_NAMES.get(language_code, language_code.upper())
                lang_subfolder = sanitize_path_component(lang_name)
            else:
                # Add tag to title folder
                lang_tag = format_language_tag(language_code, language, tag_format)
                title_folder = apply_language_tag(title_folder, lang_tag, tag_position)

    if naming_format == 'custom':
        # Custom template: parse and replace tags
        custom_template = config.get('custom_naming_template', '{author}/{title}') if config else '{author}/{title}'

        # Prepare all available data for replacement
        safe_narrator = sanitize_path_component(narrator) if narrator else ''
        safe_year = str(year) if year else ''
        safe_edition = sanitize_path_component(edition) if edition else ''
        safe_variant = sanitize_path_component(variant) if variant else ''
        # Issue #57 (Merijeek): Zero-pad series numbers for ABS compatibility
        if series_num:
            try:
                num = float(str(series_num).replace(',', '.'))
                if num == int(num):
                    safe_series_num = f"{int(num):02d}"  # 1 -> "01", 10 -> "10"
                else:
                    safe_series_num = str(series_num)  # Keep decimals as-is
            except (ValueError, TypeError):
                safe_series_num = str(series_num)
        else:
            safe_series_num = ''

        # Issue #94: Don't use series_num if series name is missing
        # Having a number without a folder name creates broken paths like "Author/01 - Title"
        # instead of "Author/Series/01 - Title". Better to omit both than have orphan numbers.
        series_num_for_template = series_num  # Keep raw value for .pad() modifier
        if not safe_series and (safe_series_num or series_num):
            logger.debug(f"Clearing series_num '{series_num}' because series name is missing")
            safe_series_num = ''
            series_num_for_template = ''  # Also clear for .pad() modifier

        # Build the path from template
        path_str = custom_template

        # Issue #80: Apply .pad(N) modifiers before standard replacements
        # This allows {series_num.pad(2)} -> "01", {series_num.pad(3)} -> "001"
        path_str = _apply_template_modifiers(path_str, 'series_num', series_num_for_template or '')

        # Issue #96: Author name format variations
        author_first, author_last = parse_author_name(author)
        safe_author_first = sanitize_path_component(author_first) if author_first else ''
        safe_author_last = sanitize_path_component(author_last) if author_last else safe_author
        # {author_lf} = "LastName, FirstName", {author_fl} = "FirstName LastName"
        author_lf = format_author_lf(author)
        author_fl = format_author_fl(author)
        safe_author_lf = sanitize_path_component(author_lf) if author_lf else safe_author
        safe_author_fl = sanitize_path_component(author_fl) if author_fl else safe_author

        path_str = path_str.replace('{author}', safe_author)
        path_str = path_str.replace('{author_first}', safe_author_first)
        path_str = path_str.replace('{author_last}', safe_author_last)
        path_str = path_str.replace('{author_lf}', safe_author_lf)
        path_str = path_str.replace('{author_fl}', safe_author_fl)
        path_str = path_str.replace('{title}', safe_title)
        path_str = path_str.replace('{series}', safe_series or '')
        path_str = path_str.replace('{series_num}', safe_series_num)
        path_str = path_str.replace('{narrator}', safe_narrator)
        path_str = path_str.replace('{year}', safe_year)
        path_str = path_str.replace('{edition}', safe_edition)
        path_str = path_str.replace('{variant}', safe_variant)
        # Multi-language template tags
        safe_language = LANGUAGE_NAMES.get(language_code, language_code.upper()) if language_code else ''
        path_str = path_str.replace('{language}', safe_language)
        path_str = path_str.replace('{lang_code}', language_code or '')

        # Clean up empty brackets/parens from missing optional data
        path_str = re.sub(r'\(\s*\)', '', path_str)  # Empty ()
        path_str = re.sub(r'\[\s*\]', '', path_str)  # Empty []
        path_str = re.sub(r'\{\s*\}', '', path_str)  # Empty {} (literal, not tags)
        path_str = re.sub(r'\s+-\s+(?=-|/|$)', '', path_str)  # Dangling " - " before separator
        path_str = re.sub(r'/\s*-\s+', '/', path_str)  # Leading "- " or " - " after slash (Issue #16, #22)
        path_str = re.sub(r'^-\s+', '', path_str)  # Leading "- " at start
        path_str = re.sub(r'^\s*-\s+', '', path_str)  # Leading " - " at start (with space)
        path_str = re.sub(r'\s+-$', '', path_str)  # Trailing " -" at end
        path_str = re.sub(r'/+', '/', path_str)  # Multiple slashes
        path_str = re.sub(r'\s{2,}', ' ', path_str)  # Multiple spaces
        path_str = path_str.strip(' /')

        # Split by / to create path components
        parts = [p.strip() for p in path_str.split('/') if p.strip()]
        if not parts:
            logger.error(f"BLOCKED: Custom template resulted in empty path")
            return None

        result_path = lib_path
        for part in parts:
            result_path = result_path / part
    elif naming_format == 'author - title':
        # Flat structure: Author - Title (single folder)
        folder_name = f"{safe_author} - {title_folder}"
        result_path = lib_path / folder_name
    elif naming_format == 'author_lf/title':
        # Issue #96: Library-style format: "LastName, FirstName/Title"
        author_lf = format_author_lf(author)
        safe_author_lf = sanitize_path_component(author_lf) if author_lf else safe_author
        if series_grouping and safe_series:
            result_path = lib_path / safe_author_lf / safe_series / title_folder
        else:
            result_path = lib_path / safe_author_lf / title_folder
    elif lang_subfolder:
        # Language subfolder: Author/Language/Title (or Author/Series/Language/Title)
        if series_grouping and safe_series:
            result_path = lib_path / safe_author / safe_series / lang_subfolder / title_folder
        else:
            result_path = lib_path / safe_author / lang_subfolder / title_folder
    elif series_grouping and safe_series:
        # Series grouping enabled AND book has series: Author/Series/Title
        result_path = lib_path / safe_author / safe_series / title_folder
    else:
        # Default: Author/Title (two-level)
        result_path = lib_path / safe_author / title_folder

    # CRITICAL SAFETY: Verify path is within library and has minimum depth
    try:
        # Resolve to absolute path
        result_path = result_path.resolve()
        lib_path_resolved = Path(lib_path).resolve()

        # Ensure result is within library path
        result_path.relative_to(lib_path_resolved)

        # Ensure minimum depth (at least 1 folder below library root)
        relative = result_path.relative_to(lib_path_resolved)
        if len(relative.parts) < 1:
            logger.error(f"BLOCKED: Path too shallow - would dump files at library root: {result_path}")
            return None

    except ValueError:
        logger.error(f"BLOCKED: Path escapes library! lib={lib_path}, result={result_path}")
        return None

    return result_path


__all__ = [
    'sanitize_path_component',
    'build_new_path',
    'format_language_tag',
    'apply_language_tag',
    'strip_unabridged_markers',
    'parse_author_name',
    'format_author_lf',
    'format_author_fl',
    'LANGUAGE_NAMES',
    'NAME_PREFIXES',
    'NAME_SUFFIXES',
]
