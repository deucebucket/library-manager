"""Path sanitization and building utilities."""
import re
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


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
                   edition=None, variant=None, config=None):
    """Build a new path based on the naming format configuration.

    Audiobookshelf-compatible format (when series_grouping enabled):
    - Narrator in curly braces: {Ray Porter}
    - Series number prefix: "1 - Title"
    - Year in parentheses: (2003)
    - Edition in brackets: [30th Anniversary Edition]
    - Variant in brackets: [Graphic Audio]

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

        # Build the path from template
        path_str = custom_template
        path_str = path_str.replace('{author}', safe_author)
        path_str = path_str.replace('{title}', safe_title)
        path_str = path_str.replace('{series}', safe_series or '')
        path_str = path_str.replace('{series_num}', safe_series_num)
        path_str = path_str.replace('{narrator}', safe_narrator)
        path_str = path_str.replace('{year}', safe_year)
        path_str = path_str.replace('{edition}', safe_edition)
        path_str = path_str.replace('{variant}', safe_variant)

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
]
