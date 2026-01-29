"""Audnexus API provider for audiobook metadata.

Audnexus is a community-maintained API that provides Audible metadata.

NOTE (Jan 2026): Audnexus API has changed significantly:
- Title search endpoint (/books?title=...) has been REMOVED
- API is now ASIN-only: /books/{ASIN}?region=us
- Author search still works: /authors?name=...
- This provider now tries author search as a workaround but has limited discovery ability

The API is now primarily useful for ENRICHMENT (when you already have an ASIN)
rather than DISCOVERY (finding books by title).
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

    NOTE: As of Jan 2026, Audnexus has removed title search. This function
    now tries an author-based search approach which has limited effectiveness.

    The API now requires ASIN for direct book lookups:
    - /books/{ASIN}?region=us - Full book details (requires known ASIN)
    - /authors?name=... - Find author ASINs (still works)

    Args:
        title: Book title to search for
        author: Optional author name (required for any chance of finding results)
        region: Optional Audible region code (us, de, fr, it, es, jp, etc.)

    Returns:
        dict with title, author, year, narrator, series, series_num, source
        or None if not found or API unavailable
    """
    # Circuit breaker: skip if API has been failing
    if is_circuit_open('audnexus'):
        return None

    rate_limit_wait('audnexus')

    # Default region to 'us' (required parameter for Audnexus API)
    if not region:
        region = 'us'

    try:
        # Strategy: Try author search if author provided, otherwise skip
        # (Title search no longer exists in Audnexus API)

        if not author:
            logger.debug(f"Audnexus: Skipping - no author provided (title search removed from API)")
            return None

        # Step 1: Search for author by name
        author_url = f"https://api.audnex.us/authors?name={urllib.parse.quote(author)}"
        logger.debug(f"Audnexus: Searching authors for '{author}'")

        resp = requests.get(author_url, timeout=10, headers={'Accept': 'application/json'})

        if resp.status_code == 404:
            # API endpoint may have changed - log and skip
            logger.debug(f"Audnexus: Author search endpoint returned 404 - API may have changed")
            return None

        if resp.status_code != 200:
            logger.debug(f"Audnexus: Author search returned status {resp.status_code}")
            return None

        authors_data = resp.json()
        if not authors_data or not isinstance(authors_data, list):
            logger.debug(f"Audnexus: No authors found for '{author}'")
            return None

        # Find best matching author (exact match preferred)
        author_lower = author.lower()
        best_author = None
        for a in authors_data:
            if a.get('name', '').lower() == author_lower:
                best_author = a
                break
        if not best_author and authors_data:
            best_author = authors_data[0]  # Fall back to first result

        if not best_author or not best_author.get('asin'):
            logger.debug(f"Audnexus: No author ASIN found for '{author}'")
            return None

        author_asin = best_author['asin']
        author_name = best_author.get('name', author)

        # Note: Unfortunately, Audnexus doesn't provide a way to get an author's books
        # The /authors/{asin} endpoint only returns author info, not their book list
        # Without ASINs, we can't look up specific books

        # For now, return author info so at least we validate the author exists
        # This is limited but better than nothing
        logger.debug(f"Audnexus: Found author '{author_name}' (ASIN: {author_asin}) but cannot search books without ASIN")

        # Success - reset circuit breaker (API is responding)
        record_api_success('audnexus')

        # We can't return a full book result without ASIN-based lookup
        # Return None to let other APIs handle discovery
        return None

    except requests.exceptions.Timeout:
        # Timeout - increment circuit breaker
        record_api_failure('audnexus')
        logger.warning(f"Audnexus search timed out for '{title}'")
        return None
    except Exception as e:
        logger.debug(f"Audnexus search failed: {e}")
        return None


def lookup_audnexus_by_asin(asin, region='us'):
    """Look up a specific audiobook by ASIN.

    This is the recommended way to use Audnexus as of Jan 2026.
    Use when you already have an ASIN from another source.

    Args:
        asin: Audible ASIN (e.g., 'B01N48VJFJ')
        region: Audible region code (default: 'us')

    Returns:
        dict with full audiobook metadata or None if not found
    """
    if is_circuit_open('audnexus'):
        return None

    rate_limit_wait('audnexus')

    try:
        url = f"https://api.audnex.us/books/{asin}?region={region}"
        logger.debug(f"Audnexus: Looking up ASIN {asin}")

        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})

        record_api_success('audnexus')

        if resp.status_code != 200:
            logger.debug(f"Audnexus: ASIN lookup returned status {resp.status_code}")
            return None

        data = resp.json()
        if not data or not data.get('title'):
            return None

        # Extract series info from seriesPrimary field
        series_name = None
        series_num = None
        if data.get('seriesPrimary'):
            series_name = data['seriesPrimary'].get('name')
            series_num = data['seriesPrimary'].get('position')

        result = {
            'asin': asin,
            'title': data.get('title', ''),
            'author': data.get('authors', [{}])[0].get('name', '') if data.get('authors') else '',
            'year': data.get('releaseDate', '')[:4] if data.get('releaseDate') else None,
            'narrator': data.get('narrators', [{}])[0].get('name', '') if data.get('narrators') else None,
            'series': series_name,
            'series_num': series_num,
            'description': data.get('description', ''),
            'runtime_minutes': data.get('runtimeLengthMin'),
            'source': 'audnexus'
        }

        logger.info(f"Audnexus found: {result['author']} - {result['title']}")
        return result

    except requests.exceptions.Timeout:
        record_api_failure('audnexus')
        logger.warning(f"Audnexus ASIN lookup timed out for '{asin}'")
        return None
    except Exception as e:
        logger.debug(f"Audnexus ASIN lookup failed: {e}")
        return None


__all__ = [
    'search_audnexus',
    'lookup_audnexus_by_asin',
]
