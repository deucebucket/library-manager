"""Google Books API provider for book metadata lookups.

Google Books provides rich book metadata including series information.
An API key is optional but recommended for higher rate limits.
"""

import logging
import re
import requests

from library_manager.providers import rate_limit_wait

logger = logging.getLogger(__name__)

__all__ = ['search_google_books']


def search_google_books(title, author=None, api_key=None, lang=None):
    """Search Google Books for book metadata.

    Args:
        title: Book title to search for
        author: Optional author name
        api_key: Optional Google API key for higher rate limits
        lang: Optional ISO 639-1 language code to restrict results (e.g., 'de' for German)
    """
    rate_limit_wait('googlebooks')
    try:
        import urllib.parse
        query = title
        if author:
            query += f" inauthor:{author}"

        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5"
        if api_key:
            url += f"&key={api_key}"
        if lang and lang != 'en':
            # langRestrict filters results to books in this language
            url += f"&langRestrict={lang}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        items = data.get('items', [])

        if not items:
            return None

        # Get best match
        best = items[0].get('volumeInfo', {})
        authors = best.get('authors', [])

        # Try to extract series from subtitle (e.g., "A Mistborn Novel", "Book 2 of The Expanse")
        series_name = None
        series_num = None
        subtitle = best.get('subtitle', '')
        if subtitle:
            # "A Mistborn Novel" -> Mistborn
            match = re.search(r'^A\s+(.+?)\s+Novel$', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
            # "Book 2 of The Expanse" -> The Expanse, 2
            match = re.search(r'Book\s+(\d+)\s+of\s+(.+)', subtitle, re.IGNORECASE)
            if match:
                series_num = int(match.group(1))
                series_name = match.group(2)
            # "The Expanse Book 2" or "Mistborn #1"
            match = re.search(r'(.+?)\s+(?:Book|#)\s*(\d+)', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
                series_num = int(match.group(2))

        result = {
            'title': best.get('title', ''),
            'author': authors[0] if authors else '',
            'year': best.get('publishedDate', '')[:4] if best.get('publishedDate') else None,
            'series': series_name,
            'series_num': series_num,
            'source': 'googlebooks'
        }

        if result['title'] and result['author']:
            logger.info(f"Google Books found: {result['author']} - {result['title']}" +
                       (f" (Series: {series_name})" if series_name else ""))
            return result
        return None
    except Exception as e:
        logger.debug(f"Google Books search failed: {e}")
        return None
