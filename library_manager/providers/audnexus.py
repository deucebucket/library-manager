"""Audnexus API provider for audiobook metadata.

Audnexus is a community-maintained API that provides Audible metadata.
It's useful for narrator info and audiobook-specific details but can
be slow or unavailable at times. Circuit breaker skips it temporarily
after repeated timeouts.
"""
import logging
import time
import urllib.parse

import requests

from library_manager.providers import (
    rate_limit_wait,
    is_circuit_open,
    record_api_failure,
    record_api_success,
)

logger = logging.getLogger(__name__)


def search_audnexus(title, author=None, region=None):
    """Search Audnexus API for audiobook metadata. Pulls from Audible.

    Audnexus is a community-maintained API that provides Audible metadata.
    It's useful for narrator info and audiobook-specific details but can
    be slow or unavailable at times. Circuit breaker skips it temporarily
    after repeated timeouts.

    Args:
        title: Book title to search for
        author: Optional author name
        region: Optional Audible region code (us, de, fr, it, es, jp, etc.)

    Returns:
        dict with title, author, year, narrator, series, series_num, source
        or None if not found or API unavailable
    """
    # Circuit breaker: skip if API has been failing
    if is_circuit_open('audnexus'):
        return None

    rate_limit_wait('audnexus')
    try:
        # Audnexus search endpoint
        query = title
        if author:
            query = f"{title} {author}"

        url = f"https://api.audnex.us/books?title={urllib.parse.quote(query)}"
        # Add region parameter for localized results
        if region and region != 'us':
            url += f"&region={region}"

        logger.debug(f"Audnexus: Searching for '{query}'")
        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})

        # Success - reset circuit breaker
        record_api_success('audnexus')

        if resp.status_code != 200:
            logger.debug(f"Audnexus: API returned status {resp.status_code}")
            return None

        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            logger.debug(f"Audnexus: No results for '{query}'")
            return None

        # Get best match
        best = data[0]

        # Extract series info - Audnexus returns series as object or seriesName/seriesPosition fields
        series_name = None
        series_num = None
        if best.get('series'):
            # Series can be an object with name field
            if isinstance(best['series'], dict):
                series_name = best['series'].get('name')
                series_num = best['series'].get('position')
            elif isinstance(best['series'], str):
                series_name = best['series']
        # Also check flat fields (some Audnexus responses use these)
        if not series_name:
            series_name = best.get('seriesName') or best.get('series_name')
        if not series_num:
            series_num = best.get('seriesPosition') or best.get('series_position')

        result = {
            'title': best.get('title', ''),
            'author': best.get('authors', [{}])[0].get('name', '') if best.get('authors') else '',
            'year': best.get('releaseDate', '')[:4] if best.get('releaseDate') else None,
            'narrator': best.get('narrators', [{}])[0].get('name', '') if best.get('narrators') else None,
            'series': series_name,
            'series_num': series_num,
            'source': 'audnexus'
        }

        if result['title'] and result['author']:
            logger.info(f"Audnexus found: {result['author']} - {result['title']}")
            return result
        logger.debug(f"Audnexus: Result missing title or author for '{query}'")
        return None
    except requests.exceptions.Timeout:
        # Timeout - increment circuit breaker
        record_api_failure('audnexus')
        logger.warning(f"Audnexus search timed out for '{title}'")
        return None
    except Exception as e:
        logger.warning(f"Audnexus search failed for '{title}': {e}")
        return None


__all__ = [
    'search_audnexus',
]
