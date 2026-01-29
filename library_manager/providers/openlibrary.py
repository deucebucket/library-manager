"""OpenLibrary API provider for book metadata lookups.

OpenLibrary is a free, open-source book database with no API key required.
"""

import logging
import requests

from library_manager.providers import rate_limit_wait

logger = logging.getLogger(__name__)

__all__ = ['search_openlibrary']


def search_openlibrary(title, author=None, lang=None):
    """Search OpenLibrary for book metadata. Free, no API key needed.

    Args:
        title: Book title to search for
        author: Optional author name
        lang: Optional ISO 639-1 language code to filter results
    """
    rate_limit_wait('openlibrary')
    try:
        import urllib.parse
        query = urllib.parse.quote(title)
        url = f"https://openlibrary.org/search.json?title={query}&limit=5"
        if author:
            url += f"&author={urllib.parse.quote(author)}"
        if lang and lang != 'en':
            # OpenLibrary supports language filtering
            url += f"&language={lang}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        docs = data.get('docs', [])

        if not docs:
            return None

        # Get the best match (first result usually best)
        best = docs[0]
        result = {
            'title': best.get('title', ''),
            'author': best.get('author_name', [''])[0] if best.get('author_name') else '',
            'year': best.get('first_publish_year'),
            'source': 'openlibrary'
        }

        # Only return if we got useful data
        if result['title'] and result['author']:
            logger.info(f"OpenLibrary found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"OpenLibrary search failed: {e}")
        return None
