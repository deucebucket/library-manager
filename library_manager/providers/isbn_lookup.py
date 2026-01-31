"""ISBN extraction from ebook files and BookDB lookup.

Issue #67: Extract ISBN from EPUB/PDF/MOBI files and look up metadata via BookDB.
"""

import re
import logging
import requests
from typing import Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# BookDB API for ISBN lookup
BOOKDB_API_URL = "https://bookdb.deucebucket.com"


def normalize_isbn(isbn: str) -> tuple:
    """
    Normalize an ISBN by removing hyphens/spaces.
    Returns (isbn10, isbn13) tuple.
    """
    clean = re.sub(r'[\s\-]', '', isbn.upper())
    clean = re.sub(r'^ISBN[:\s]*', '', clean, flags=re.IGNORECASE)

    isbn10, isbn13 = None, None

    if len(clean) == 13 and clean.startswith('978'):
        isbn13 = clean
        # Derive ISBN-10
        base = clean[3:12]
        total = sum((10 - i) * int(d) for i, d in enumerate(base))
        check = (11 - (total % 11)) % 11
        isbn10 = base + ('X' if check == 10 else str(check))
    elif len(clean) == 10:
        isbn10 = clean
        # Derive ISBN-13
        base = '978' + clean[:9]
        total = sum((1 if i % 2 == 0 else 3) * int(d) for i, d in enumerate(base))
        check = (10 - (total % 10)) % 10
        isbn13 = base + str(check)

    return isbn10, isbn13


def extract_isbn_from_epub(filepath: str) -> Optional[str]:
    """
    Extract ISBN from EPUB metadata (OPF file).
    Returns ISBN string or None.
    """
    try:
        import ebooklib
        from ebooklib import epub
    except ImportError:
        logger.debug("ebooklib not installed - skipping EPUB ISBN extraction")
        return None

    try:
        book = epub.read_epub(filepath, options={'ignore_ncx': True})

        # Check dc:identifier elements
        identifiers = book.get_metadata('DC', 'identifier')
        for identifier in identifiers:
            value = identifier[0] if isinstance(identifier, tuple) else identifier
            attrs = identifier[1] if isinstance(identifier, tuple) and len(identifier) > 1 else {}

            # Check for ISBN scheme
            scheme = attrs.get('scheme', '').lower() if isinstance(attrs, dict) else ''
            opf_scheme = attrs.get('{http://www.idpf.org/2007/opf}scheme', '').lower() if isinstance(attrs, dict) else ''

            if 'isbn' in scheme or 'isbn' in opf_scheme:
                isbn10, isbn13 = normalize_isbn(str(value))
                if isbn13:
                    return isbn13
                if isbn10:
                    return isbn10

            # Check if value looks like an ISBN
            clean = re.sub(r'[\s\-]', '', str(value))
            if re.match(r'^(97[89])?\d{9}[\dXx]$', clean):
                isbn10, isbn13 = normalize_isbn(clean)
                if isbn13:
                    return isbn13
                if isbn10:
                    return isbn10

        logger.debug(f"No ISBN found in EPUB metadata: {filepath}")
        return None

    except Exception as e:
        logger.debug(f"Failed to extract ISBN from EPUB {filepath}: {e}")
        return None


def extract_isbn_from_pdf(filepath: str) -> Optional[str]:
    """
    Extract ISBN from PDF metadata or first few pages.
    Returns ISBN string or None.
    """
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader
        except ImportError:
            logger.debug("pypdf/PyPDF2 not installed - skipping PDF ISBN extraction")
            return None

    try:
        reader = PdfReader(filepath)

        # Check PDF metadata
        if reader.metadata:
            for key, value in reader.metadata.items():
                if value:
                    isbn = _extract_isbn_pattern(str(value))
                    if isbn:
                        return isbn

        # Check first 5 pages for ISBN pattern
        for page_num in range(min(5, len(reader.pages))):
            try:
                text = reader.pages[page_num].extract_text() or ""
                isbn = _extract_isbn_pattern(text)
                if isbn:
                    return isbn
            except Exception as e:
                logger.debug(f"Failed to extract text from PDF page {page_num}: {e}")
                continue

        logger.debug(f"No ISBN found in PDF: {filepath}")
        return None

    except Exception as e:
        logger.debug(f"Failed to extract ISBN from PDF {filepath}: {e}")
        return None


def extract_isbn_from_mobi(filepath: str) -> Optional[str]:
    """
    Extract ISBN from MOBI/AZW metadata.
    MOBI files store ASIN in EXTH header, ISBN may be in source field.
    Returns ISBN string or None.
    """
    try:
        with open(filepath, 'rb') as f:
            # Read MOBI header
            header = f.read(1024 * 10)  # First 10KB should contain metadata

            # Look for ISBN pattern in raw bytes
            text = header.decode('latin-1', errors='ignore')
            isbn = _extract_isbn_pattern(text)
            if isbn:
                return isbn

        logger.debug(f"No ISBN found in MOBI: {filepath}")
        return None

    except Exception as e:
        logger.debug(f"Failed to extract ISBN from MOBI {filepath}: {e}")
        return None


def _extract_isbn_pattern(text: str) -> Optional[str]:
    """
    Extract ISBN-10 or ISBN-13 from text using regex.
    Returns normalized ISBN or None.
    """
    # ISBN-13 pattern (with optional hyphens)
    match = re.search(r'\b(97[89][-\s]?\d[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d)\b', text)
    if match:
        isbn10, isbn13 = normalize_isbn(match.group(1))
        if isbn13:
            return isbn13

    # ISBN-10 pattern (with optional hyphens)
    match = re.search(r'\b(\d[-\s]?\d{4}[-\s]?\d{4}[-\s]?[\dXx])\b', text)
    if match:
        isbn10, isbn13 = normalize_isbn(match.group(1))
        if isbn10:
            return isbn10

    # Simple numeric patterns
    match = re.search(r'\b(97[89]\d{10})\b', text.replace('-', '').replace(' ', ''))
    if match:
        return match.group(1)

    match = re.search(r'\b(\d{9}[\dXx])\b', text.replace('-', '').replace(' ', ''))
    if match:
        isbn10, isbn13 = normalize_isbn(match.group(1))
        return isbn13 or isbn10

    return None


def extract_isbn_from_file(filepath: str) -> Optional[str]:
    """
    Extract ISBN from any supported ebook format.
    Detects file type by extension.
    Returns ISBN string or None.
    """
    path = Path(filepath)
    ext = path.suffix.lower()

    if ext == '.epub':
        return extract_isbn_from_epub(filepath)
    elif ext == '.pdf':
        return extract_isbn_from_pdf(filepath)
    elif ext in ('.mobi', '.azw', '.azw3', '.prc'):
        return extract_isbn_from_mobi(filepath)
    else:
        logger.debug(f"Unsupported ebook format for ISBN extraction: {ext}")
        return None


def lookup_isbn(isbn: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
    """
    Look up book metadata by ISBN via BookDB API.
    Returns book metadata dict or None.
    """
    try:
        url = f"{BOOKDB_API_URL}/api/isbn/{isbn}"
        response = requests.get(url, timeout=timeout)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"ISBN lookup success: {isbn} -> {data.get('title', 'Unknown')}")
            return data
        elif response.status_code == 404:
            logger.debug(f"ISBN not found in BookDB: {isbn}")
            return None
        else:
            logger.warning(f"ISBN lookup failed with status {response.status_code}: {isbn}")
            return None

    except requests.RequestException as e:
        logger.warning(f"ISBN lookup request failed: {e}")
        return None


def identify_ebook_by_isbn(filepath: str) -> Optional[Dict[str, Any]]:
    """
    Full pipeline: extract ISBN from ebook file and look up metadata.
    Returns book metadata dict or None.
    """
    isbn = extract_isbn_from_file(filepath)
    if not isbn:
        return None

    return lookup_isbn(isbn)
