"""SearXNG provider for fallback book metadata searches.

When primary APIs (BookDB, Audnexus, etc.) fail to find a book,
SearXNG can search the web and extract metadata from results.

Search results often contain structured data from:
- Amazon/Audible (including ASINs)
- Goodreads (author, series, ratings)
- FantasticFiction (series order)
- Wikipedia (author info)

This is a FALLBACK provider - use primary APIs first.
"""

import logging
import re
import urllib.parse

import requests

from library_manager.providers import rate_limit_wait

logger = logging.getLogger(__name__)

# Default SearXNG instance - can be overridden in config
DEFAULT_SEARXNG_URL = "http://localhost:8888"


def search_searxng(title, author=None, searxng_url=None):
    """Search SearXNG for book metadata as a fallback.

    Args:
        title: Book title to search for
        author: Optional author name (improves results significantly)
        searxng_url: Optional SearXNG instance URL (default: localhost:8888)

    Returns:
        dict with title, author, series, series_num, source, confidence
        or None if not found
    """
    rate_limit_wait('searxng')

    url = searxng_url or DEFAULT_SEARXNG_URL

    # Build search query - include "audiobook" to get better results
    query_parts = [title]
    if author:
        query_parts.append(author)
    query_parts.append("audiobook")
    query = " ".join(query_parts)

    try:
        search_url = f"{url}/search?q={urllib.parse.quote(query)}&format=json"
        logger.debug(f"SearXNG: Searching for '{query}'")

        resp = requests.get(search_url, timeout=20, headers={'Accept': 'application/json'})

        if resp.status_code != 200:
            logger.debug(f"SearXNG: Search returned status {resp.status_code}")
            return None

        data = resp.json()
        results = data.get('results', [])

        if not results:
            logger.debug(f"SearXNG: No results for '{query}'")
            return None

        # Parse results to extract book metadata
        extracted = _extract_book_metadata(results, title, author)

        if extracted:
            logger.info(f"SearXNG found: {extracted.get('author', 'Unknown')} - {extracted.get('title', 'Unknown')}")
            return extracted

        return None

    except requests.exceptions.Timeout:
        logger.warning(f"SearXNG search timed out for '{title}'")
        return None
    except requests.exceptions.ConnectionError:
        logger.debug(f"SearXNG: Connection failed (is SearXNG running?)")
        return None
    except Exception as e:
        logger.debug(f"SearXNG search failed: {e}")
        return None


def _extract_book_metadata(results, original_title, original_author):
    """Extract book metadata from SearXNG search results.

    Parses titles and URLs from various sources to find author, title, series info.
    """
    candidates = []

    for result in results[:15]:  # Check top 15 results
        url = result.get('url', '')
        title = result.get('title', '')
        content = result.get('content', '')

        extracted = None

        # Try to extract from different sources
        if 'amazon.com' in url or 'audible.com' in url:
            extracted = _parse_amazon_audible(title, url, content)
        elif 'goodreads.com' in url:
            extracted = _parse_goodreads(title, url, content)
        elif 'fantasticfiction.com' in url:
            extracted = _parse_fantasticfiction(title, url, content)
        elif 'bookseriesinorder.com' in url:
            extracted = _parse_bookseriesinorder(title, url, content)

        if extracted and extracted.get('author') and extracted.get('title'):
            # Score the result based on how well it matches our query
            score = _score_result(extracted, original_title, original_author)
            extracted['score'] = score
            candidates.append(extracted)

    if not candidates:
        return None

    # Return best scoring candidate
    candidates.sort(key=lambda x: x.get('score', 0), reverse=True)
    best = candidates[0]

    # Only return if score is reasonable (> 0.3)
    if best.get('score', 0) < 0.3:
        logger.debug(f"SearXNG: Best candidate score too low ({best.get('score', 0):.2f})")
        return None

    best['source'] = 'searxng'
    best['confidence'] = min(0.7, best.get('score', 0.5))  # Cap confidence at 0.7 for web results
    return best


def _parse_amazon_audible(title, url, content):
    """Parse Amazon/Audible search result."""
    result = {'source_site': 'amazon'}

    # Extract ASIN from URL if present
    asin_match = re.search(r'/(?:dp|pd)/([A-Z0-9]{10})', url)
    if asin_match:
        result['asin'] = asin_match.group(1)

    # Strip common prefixes
    clean_title = re.sub(r'^Amazon\.com:\s*', '', title)
    clean_title = re.sub(r'\s*-\s*Audible$', '', clean_title)

    # Pattern 1: "Title: Series, Book N (Audible...): Author, Narrator..."
    # Example: "Match Game: Expeditionary Force, Book 14 (Audible Audio Edition): Craig Alanson, R.C. Bray..."
    match = re.search(r'^(.+?):\s*(.+?),\s*Book\s*(\d+)\s*(?:\([^)]+\))?:\s*([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)', clean_title)
    if match:
        result['title'] = match.group(1).strip()
        result['series'] = match.group(2).strip()
        result['series_num'] = match.group(3)
        result['author'] = match.group(4).strip()
        return result

    # Pattern 2: "Title Audiobook by Author"
    # Example: "Match Game Audiobook by Craig Alanson"
    match = re.search(r'^(.+?)\s+Audiobook\s+by\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)', clean_title)
    if match:
        result['title'] = match.group(1).strip()
        result['author'] = match.group(2).strip()
        return result

    # Pattern 3: "Title (Series): ISBN: Author, Name: Books"
    match = re.search(r'^(.+?)\s*\(([^)]+)\).*?:\s*(\d{10,13}):\s*([^:]+?):', clean_title)
    if match:
        result['title'] = match.group(1).strip()
        series_part = match.group(2).strip()
        if series_part.lower() not in ['audible', 'kindle', 'paperback', 'hardcover', 'audio edition']:
            result['series'] = series_part
        author = match.group(4).strip()
        if ',' in author:
            parts = author.split(',')
            author = f"{parts[1].strip()} {parts[0].strip()}"
        result['author'] = author
        return result

    # Pattern 4: Check content for "by Author" pattern
    if content:
        # "Match Game Audiobook By Craig Alanson"
        match = re.search(r'(?:Audiobook\s+)?[Bb]y\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)+)', content)
        if match:
            result['author'] = match.group(1).strip()

        # Check for series info in content: "Expeditionary Force, Book 14"
        match = re.search(r'([A-Z][a-zA-Z\s]+?),\s*Book\s*(\d+)', content)
        if match:
            result['series'] = match.group(1).strip()
            result['series_num'] = match.group(2)

    return result if result.get('author') or result.get('asin') else None


def _parse_goodreads(title, url, content):
    """Parse Goodreads search result."""
    result = {'source_site': 'goodreads'}

    # Pattern: "Book Title (Series Name, #N) by Author Name"
    # Or: "Series Name Series by Author Name"

    # Book page pattern
    match = re.search(r'^(.+?)\s*(?:\(([^,]+),\s*#(\d+)\))?\s*(?:by\s+)?(.+?)(?:\s*-\s*Goodreads)?$', title)
    if match:
        result['title'] = match.group(1).strip()
        if match.group(2):
            result['series'] = match.group(2).strip()
        if match.group(3):
            result['series_num'] = match.group(3)
        if match.group(4):
            result['author'] = match.group(4).strip()
        return result

    # Series page pattern: "Expeditionary Force Series by Craig Alanson"
    match = re.search(r'^(.+?)\s+Series\s+by\s+(.+?)(?:\s*-\s*Goodreads)?$', title)
    if match:
        result['series'] = match.group(1).strip()
        result['author'] = match.group(2).strip()
        return result

    # Author page pattern
    match = re.search(r'^(.+?)\s*\(Author of\s+(.+?)\)', title)
    if match:
        result['author'] = match.group(1).strip()
        result['title'] = match.group(2).strip()
        return result

    return result if result.get('author') else None


def _parse_fantasticfiction(title, url, content):
    """Parse FantasticFiction search result."""
    result = {'source_site': 'fantasticfiction'}

    # Pattern: "Match Game (Expeditionary Force, book 14) by Craig Alanson"
    match = re.search(r'^(.+?)\s*\(([^,]+),\s*book\s*(\d+)\)\s*by\s+(.+?)$', title, re.I)
    if match:
        result['title'] = match.group(1).strip()
        result['series'] = match.group(2).strip()
        result['series_num'] = match.group(3)
        result['author'] = match.group(4).strip()
        return result

    # Simpler pattern: "Title by Author"
    match = re.search(r'^(.+?)\s+by\s+(.+?)$', title)
    if match:
        result['title'] = match.group(1).strip()
        result['author'] = match.group(2).strip()
        return result

    return result if result.get('author') else None


def _parse_bookseriesinorder(title, url, content):
    """Parse BookSeriesInOrder search result."""
    result = {'source_site': 'bookseriesinorder'}

    # Pattern: "Author Name - Book Series In Order"
    match = re.search(r'^(.+?)\s*-\s*Book Series In Order', title)
    if match:
        result['author'] = match.group(1).strip()
        return result

    return result if result.get('author') else None


def _score_result(extracted, original_title, original_author):
    """Score how well an extracted result matches the original query."""
    score = 0.0

    ext_title = (extracted.get('title') or '').lower()
    ext_author = (extracted.get('author') or '').lower()
    orig_title = (original_title or '').lower()
    orig_author = (original_author or '').lower()

    # Title word overlap
    if ext_title and orig_title:
        ext_words = set(re.findall(r'\w+', ext_title))
        orig_words = set(re.findall(r'\w+', orig_title))
        if orig_words:
            overlap = len(ext_words & orig_words) / len(orig_words)
            score += overlap * 0.5

    # Author match
    if ext_author and orig_author:
        ext_author_words = set(re.findall(r'\w+', ext_author))
        orig_author_words = set(re.findall(r'\w+', orig_author))
        if orig_author_words:
            overlap = len(ext_author_words & orig_author_words) / len(orig_author_words)
            score += overlap * 0.3

    # Bonus for having series info
    if extracted.get('series'):
        score += 0.1

    # Bonus for having ASIN
    if extracted.get('asin'):
        score += 0.1

    return min(1.0, score)


def test_searxng_connection(searxng_url=None):
    """Test if SearXNG is available and responding."""
    url = searxng_url or DEFAULT_SEARXNG_URL
    try:
        resp = requests.get(f"{url}/search?q=test&format=json", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if 'results' in data:
                return {'success': True, 'results_count': len(data.get('results', []))}
        return {'success': False, 'error': f'Status {resp.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'success': False, 'error': 'Connection refused - is SearXNG running?'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


__all__ = [
    'search_searxng',
    'test_searxng_connection',
    'DEFAULT_SEARXNG_URL',
]
