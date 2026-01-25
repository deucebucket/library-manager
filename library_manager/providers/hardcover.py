"""Hardcover.app API provider for book metadata lookups.

Hardcover uses a GraphQL API and provides good series information.
"""

import logging
import requests

from library_manager.providers import rate_limit_wait

logger = logging.getLogger(__name__)

__all__ = ['search_hardcover']


def search_hardcover(title, author=None):
    """Search Hardcover.app API for book metadata."""
    rate_limit_wait('hardcover')
    try:
        import urllib.parse
        # Hardcover GraphQL API
        query = title
        if author:
            query = f"{title} {author}"

        # Hardcover uses GraphQL - request series info too
        graphql_query = {
            "query": """
                query SearchBooks($query: String!) {
                    search(query: $query, limit: 5) {
                        books {
                            title
                            contributions { author { name } }
                            releaseYear
                            series { name position }
                        }
                    }
                }
            """,
            "variables": {"query": query}
        }

        resp = requests.post(
            "https://api.hardcover.app/v1/graphql",
            json=graphql_query,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )

        if resp.status_code != 200:
            return None

        data = resp.json()
        books = data.get('data', {}).get('search', {}).get('books', [])

        if not books:
            return None

        best = books[0]
        contributions = best.get('contributions', [])
        author_name = contributions[0].get('author', {}).get('name', '') if contributions else ''

        # Extract series info from Hardcover response
        series_info = best.get('series', {}) or {}
        series_name = series_info.get('name') if isinstance(series_info, dict) else None
        series_num = series_info.get('position') if isinstance(series_info, dict) else None

        result = {
            'title': best.get('title', ''),
            'author': author_name,
            'year': best.get('releaseYear'),
            'series': series_name,
            'series_num': series_num,
            'source': 'hardcover'
        }

        if result['title'] and result['author']:
            logger.info(f"Hardcover found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"Hardcover search failed: {e}")
        return None
