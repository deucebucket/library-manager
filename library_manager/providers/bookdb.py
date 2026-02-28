"""Skaldleita API provider for Library Manager.

This module provides access to Skaldleita (formerly BookDB), our metadata service with:
- Fuzzy matching via Qdrant vectors (great for messy filenames)
- 50M+ book database
- GPU-powered Whisper audio identification
- Series info including book position
- Local/P2P cache support

Note: Internal names use 'bookdb' for backwards compatibility with existing configs.
"""

import os
import time
import logging
import subprocess
import tempfile
import requests
from pathlib import Path

from library_manager.providers.rate_limiter import (
    rate_limit_wait,
    is_circuit_open,
    record_api_failure,
    record_api_success,
    handle_rate_limit_response,
    API_CIRCUIT_BREAKER,
)
from library_manager.utils.voice_embedding import (
    is_voice_embedding_available,
    extract_voice_embedding_from_clip,
)

logger = logging.getLogger(__name__)

# Skaldleita API endpoint (our metadata service, legacy name: BookDB)
BOOKDB_API_URL = "https://bookdb.deucebucket.com"  # URL unchanged for backwards compatibility
# Public API key for Library Manager users (no config needed)
BOOKDB_PUBLIC_KEY = "lm-public-2024_85TbJ2lbrXGm38tBgliPAcAexLA_AeWxyqvHPbwRIrA"

# User-Agent for tracking requests (helps identify Library Manager traffic)
def get_lm_version():
    """Get Library Manager version from app.py"""
    try:
        import sys
        # Try to get version from app module if loaded
        if 'app' in sys.modules:
            return getattr(sys.modules['app'], 'APP_VERSION', 'unknown')
    except:
        pass
    return 'unknown'


def get_user_agent():
    """Get User-Agent string with version from app.py"""
    return f"LibraryManager/{get_lm_version()}"


# Request signing - uses shared module for Skaldleita sync
# See library_manager/signing.py for constants and derivation logic
# Skaldleita fetches that file to stay in sync automatically
from library_manager.signing import generate_signature


def get_signed_headers():
    """
    Generate signed headers for Skaldleita API requests.

    Returns dict with User-Agent, X-LM-Signature, and X-LM-Timestamp.
    Secret is derived from version - changes with each release.
    Skaldleita fetches signing.py to stay in sync.

    See library_manager/signing.py for derivation logic.
    """
    timestamp = str(int(time.time()))
    lm_version = get_lm_version()
    signature = generate_signature(lm_version, timestamp)

    return {
        'User-Agent': f'LibraryManager/{lm_version}',
        'X-LM-Signature': signature,
        'X-LM-Timestamp': timestamp,
    }


def search_bookdb(title, author=None, api_key=None, retry_count=0, bookdb_url=None, config=None,
                  data_dir=None, cache_getter=None):
    """
    Search our Skaldleita metadata service.
    Uses fuzzy matching via Qdrant vectors - great for messy filenames.
    Returns series info including book position if found.

    Now with local/P2P cache support - checks cache first, falls back to API.

    Args:
        title: Book title to search for
        author: Optional author name for better matching
        api_key: Skaldleita API key (uses public key if not provided)
        retry_count: Internal retry counter for rate limiting
        bookdb_url: Custom Skaldleita URL (uses default if not provided)
        config: App config dict for cache settings
        data_dir: Data directory path for cache storage
        cache_getter: Function to get cache instance (for dependency injection)

    Returns:
        dict with title, author, year, series, series_num, etc. or None
    """
    if not api_key:
        return None

    # Check cache first (local + P2P if enabled)
    if cache_getter and config and data_dir:
        try:
            cache = cache_getter(
                data_dir=data_dir,
                enable_p2p=config.get('enable_p2p_cache', False)
            )
            if cache:
                cached = cache.get(title, author)
                if cached:
                    logger.info(f"[CACHE] Hit for: {author} - {title} (source: {cached.get('_cache_source', 'local')})")
                    return cached
        except Exception as e:
            logger.debug(f"Cache lookup error (continuing to API): {e}")

    # Check circuit breaker - skip if we've been rate limited too much
    cb = API_CIRCUIT_BREAKER.get('bookdb', {})
    if cb.get('circuit_open_until', 0) > time.time():
        remaining = int(cb['circuit_open_until'] - time.time())
        logger.debug(f"Skaldleita: Circuit open, skipping ({remaining}s remaining)")
        return None

    rate_limit_wait('bookdb')  # 3.6s delay = max 1000/hr, never skips

    # Use configured URL or fall back to default cloud URL
    base_url = bookdb_url or BOOKDB_API_URL

    try:
        # Build the filename to match - include author if we have it
        filename = f"{author} - {title}" if author else title

        headers = get_signed_headers()
        headers["X-API-Key"] = api_key

        resp = requests.post(
            f"{base_url}/match",
            json={"filename": filename},
            headers=headers,
            timeout=10
        )

        # Handle rate limiting with exponential backoff
        if resp.status_code == 429:
            rl = handle_rate_limit_response(resp, 'bookdb', retry_count)
            if rl['should_retry']:
                time.sleep(rl['wait_seconds'])
                return search_bookdb(title, author, api_key, retry_count + 1, bookdb_url,
                                     config, data_dir, cache_getter)
            return None

        if resp.status_code != 200:
            logger.debug(f"Skaldleita returned status {resp.status_code}")
            return None

        # Success - reset circuit breaker failures
        if 'bookdb' in API_CIRCUIT_BREAKER:
            API_CIRCUIT_BREAKER['bookdb']['failures'] = 0

        data = resp.json()

        # Check confidence threshold
        if data.get('confidence', 0) < 0.5:
            logger.debug(f"Skaldleita match below confidence threshold: {data.get('confidence')}")
            return None

        series = data.get('series')
        books = data.get('books', [])

        # Need either series or books to return a result
        if not series and not books:
            return None

        # Find the best matching book
        best_book = None
        if books:
            # Try to match title to a specific book
            title_lower = title.lower()
            for book in books:
                book_title = book.get('title', '').lower()
                if title_lower in book_title or book_title in title_lower:
                    best_book = book
                    break
            # If no specific match, use first book
            if not best_book:
                best_book = books[0]

        # Build result - handle standalone books (no series) and series books
        result = {
            'title': best_book.get('title') if best_book else (series.get('name') if series else None),
            'author': best_book.get('author_name') if best_book else (series.get('author_name', '') if series else ''),
            'year': best_book.get('year_published') if best_book else None,
            'series': series.get('name') if series else None,
            'series_num': best_book.get('series_position') if best_book else None,
            'variant': series.get('variant') if series else None,
            'edition': best_book.get('edition') if best_book else None,
            'source': 'bookdb',
            'confidence': data.get('confidence', 0)
        }

        # Defense-in-depth: also checked in BookProfile.finalize(), but catching
        # here prevents bad data propagation through cache and downstream layers
        # Skaldleita bug #90 - series name imported as author entity
        if result.get('author') and result.get('series'):
            if result['author'].lower().strip() == result['series'].lower().strip():
                logger.warning(f"[BOOKDB] Corrupt data: author '{result['author']}' equals series name, discarding")
                result['author'] = None

        if result['title'] and result['author']:
            logger.info(f"Skaldleita found: {result['author']} - {result['title']}" +
                       (f" ({result['series']} #{result['series_num']})" if result['series'] else "") +
                       f" [confidence: {result['confidence']:.2f}]")

            # Cache successful result (local + P2P if enabled)
            if cache_getter and config and data_dir:
                try:
                    cache = cache_getter(
                        data_dir=data_dir,
                        enable_p2p=config.get('enable_p2p_cache', False)
                    )
                    if cache:
                        cache.put(title, author, result,
                                  source='bookdb',
                                  confidence=result.get('confidence', 0))
                        logger.debug(f"[CACHE] Stored: {author} - {title}")
                except Exception as e:
                    logger.debug(f"Cache write error (non-fatal): {e}")

            return result
        return None

    except Exception as e:
        logger.debug(f"Skaldleita search failed: {e}")
        return None


def identify_audio_with_bookdb(audio_file, extract_seconds=90, bookdb_url=None):
    """
    Use Skaldleita's GPU-powered Whisper API to identify a book from audio.

    This uses a fair round-robin queue system:
    1. Submit audio, get ticket + queue position
    2. Poll for result (shows queue progress to user)
    3. Return result when complete

    This is PREFERRED over local transcription + Gemini because:
    1. Skaldleita has a GTX 1080 running Whisper (faster)
    2. No Gemini rate limits
    3. Skaldleita cross-references against its 50M+ book database
    4. Fair multi-user access via round-robin queue

    Args:
        audio_file: Path to the audio file
        extract_seconds: How many seconds to extract (default 90)
        bookdb_url: Custom Skaldleita URL (uses BOOKDB_URL env var or default if not provided)

    Returns:
        dict with author, title, narrator, series, etc. or None
    """
    url = bookdb_url or os.environ.get('BOOKDB_URL', BOOKDB_API_URL)
    logger.info(f"[SKALDLEITA] Starting identification for: {audio_file}")
    logger.debug(f"[SKALDLEITA] Using API URL: {url}")

    try:
        audio_path = Path(audio_file)
        if not audio_path.exists():
            logger.warning(f"[SKALDLEITA] File not found: {audio_file}")
            return None

        # Extract first N seconds to a temp file
        with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Use ffmpeg to extract the intro - use fast seek for large files
            logger.debug(f"[SKALDLEITA] Extracting {extract_seconds}s from {audio_path.name}")
            cmd = [
                'ffmpeg', '-y',
                '-ss', '0',  # Fast input seek
                '-i', str(audio_path),
                '-t', str(extract_seconds),
                '-vn',  # No video (faster)
                '-acodec', 'libmp3lame', '-q:a', '5',
                '-loglevel', 'error',
                tmp_path
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=60)

            if result.returncode != 0:
                logger.warning(f"[SKALDLEITA] ffmpeg extraction failed: {result.stderr.decode()[:200]}")
                return None

            # Check file size to ensure it's valid
            tmp_size = os.path.getsize(tmp_path)
            logger.debug(f"[SKALDLEITA] Extracted file size: {tmp_size} bytes")
            if tmp_size < 1000:
                logger.warning(f"[SKALDLEITA] Extracted file too small ({tmp_size} bytes), likely invalid audio")
                return None

            # Extract voice embedding while we have the clip (contributes to narrator ID)
            voice_embedding = None
            if is_voice_embedding_available():
                logger.debug("[SKALDLEITA] Extracting voice embedding from clip...")
                voice_embedding = extract_voice_embedding_from_clip(tmp_path)
                if voice_embedding:
                    logger.info(f"[SKALDLEITA] Voice embedding extracted (256-dim)")
                else:
                    logger.debug("[SKALDLEITA] Voice embedding extraction failed (non-fatal)")

            # Submit to Skaldleita queue
            logger.info(f"[SKALDLEITA] Submitting to queue: {url}/api/identify_audio")
            with open(tmp_path, 'rb') as f:
                # Build request with optional voice embedding
                files = {'audio': (audio_path.name, f, 'audio/mpeg')}
                data = {}
                if voice_embedding:
                    # Send pre-computed embedding - Skaldleita won't need to extract it
                    import json
                    data['voice_embedding'] = json.dumps(voice_embedding)
                    logger.debug("[SKALDLEITA] Including voice embedding in request")

                response = requests.post(
                    f"{url}/api/identify_audio",
                    files=files,
                    data=data,
                    headers=get_signed_headers(),
                    timeout=30  # Just submitting, should be fast
                )

            if response.status_code != 200:
                logger.warning(f"[SKALDLEITA] API returned {response.status_code}: {response.text[:200]}")
                return None

            submit_data = response.json()

            # Check if it's the new queue system (has ticket_id) or old sync system
            if 'ticket_id' in submit_data:
                # New queue system - poll for result
                ticket_id = submit_data['ticket_id']
                queue_position = submit_data.get('queue_position', '?')
                estimated_seconds = submit_data.get('estimated_seconds', '?')

                logger.info(f"[SKALDLEITA] Queued! Position: {queue_position}, ETA: ~{estimated_seconds}s (ticket: {ticket_id})")

                # Poll for result
                poll_url = f"{url}/api/identify_audio/{ticket_id}"
                max_wait = 300  # 5 minutes max
                poll_interval = 2  # Poll every 2 seconds
                waited = 0
                last_position = queue_position

                while waited < max_wait:
                    time.sleep(poll_interval)
                    waited += poll_interval

                    try:
                        poll_response = requests.get(poll_url, headers=get_signed_headers(), timeout=10)
                        if poll_response.status_code != 200:
                            continue

                        status_data = poll_response.json()
                        status = status_data.get('status')

                        # Update user on queue position changes
                        new_position = status_data.get('queue_position')
                        if new_position and new_position != last_position:
                            logger.info(f"[SKALDLEITA] Queue position: {new_position}")
                            last_position = new_position

                        if status == 'processing':
                            logger.info(f"[SKALDLEITA] Processing audio...")

                        elif status == 'complete':
                            # Got result!
                            data = status_data.get('result', {})
                            logger.info(f"[SKALDLEITA] Complete! Processing result...")
                            break

                        elif status == 'error':
                            logger.warning(f"[SKALDLEITA] Job failed: {status_data.get('error')}")
                            return None

                    except Exception as poll_err:
                        logger.debug(f"[SKALDLEITA] Poll error (will retry): {poll_err}")
                        continue

                else:
                    logger.warning(f"[SKALDLEITA] Timed out waiting for result after {max_wait}s")
                    return None

            else:
                # Old sync system - response has result directly
                data = submit_data

            # Process result (same for both systems)
            transcript = data.get('transcript') or ''
            matched_books = data.get('matched_books') or []
            logger.info(f"[SKALDLEITA] Result received - transcript: {len(transcript)} chars, matches: {len(matched_books)}")

            if data.get('error'):
                logger.warning(f"[SKALDLEITA] API error: {data['error']}")
                return None

            best_match = matched_books[0] if matched_books else None

            # Phase 2: Capture source and requeue_suggested from SL response
            sl_source = data.get('source', 'audio')  # 'database', 'audio', or 'live_scrape'
            requeue_suggested = data.get('requeue_suggested', False)

            result = {
                'author': data.get('author') or (best_match.get('author_name') if best_match else None),
                'title': data.get('title') or (best_match.get('title') if best_match else None),
                'narrator': data.get('narrator'),
                'series': best_match.get('series_name') if best_match else None,
                'series_num': best_match.get('series_position') if best_match else None,
                'source': 'bookdb_audio',
                'sl_source': sl_source,  # Where SL got the data: 'database' or 'audio'
                'requeue_suggested': requeue_suggested,  # True if LM should retry later
                'confidence': 'high' if best_match or sl_source == 'database' else 'medium',
                'transcript': transcript[:500],
            }

            if result['author'] and result['title']:
                logger.info(f"[SKALDLEITA] Identified: {result['author']} - {result['title']}" +
                           (f" ({result['series']} #{result['series_num']})" if result.get('series') else ""))
                return result

            if transcript:
                logger.info(f"[SKALDLEITA] No match but got transcript ({len(transcript)} chars) - returning for AI fallback")
                return {'transcript': transcript, 'source': 'bookdb_audio'}

            logger.warning(f"[SKALDLEITA] No identification and no transcript returned")
            return None

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except requests.exceptions.Timeout:
        logger.warning("[SKALDLEITA] Request timed out")
        return None
    except Exception as e:
        logger.warning(f"[SKALDLEITA] Error: {e}")
        return None


def contribute_to_bookdb(title, author=None, narrator=None, series=None,
                         series_position=None, source='unknown', confidence='medium',
                         bookdb_url=None):
    """
    Contribute book metadata to the Skaldleita community database.

    This allows users who identify books via Gemini, OpenRouter, local Whisper,
    or other methods to contribute back to the community database. Even users
    who don't use Skaldleita for identification can help enrich it.

    Args:
        title: Book title (required)
        author: Author name
        narrator: Narrator name (for audiobooks)
        series: Series name
        series_position: Position in series (float, e.g., 1.0, 2.5)
        source: How this book was identified (gemini, openrouter, whisper, folder_parse, manual, etc.)
        confidence: How confident we are (low, medium, high)
        bookdb_url: Custom Skaldleita URL (uses default if not provided)

    Returns:
        dict with status, is_new, consensus_count or None on failure
    """
    if not title or len(title.strip()) < 2:
        logger.debug("[SKALDLEITA CONTRIBUTE] Title too short, skipping")
        return None

    # Check circuit breaker - don't spam if rate limited
    cb = API_CIRCUIT_BREAKER.get('bookdb', {})
    if cb.get('circuit_open_until', 0) > time.time():
        logger.debug("[SKALDLEITA CONTRIBUTE] Circuit open, skipping")
        return None

    url = bookdb_url or BOOKDB_API_URL

    try:
        payload = {
            "title": title.strip(),
            "author": author.strip() if author else None,
            "narrator": narrator.strip() if narrator else None,
            "series": series.strip() if series else None,
            "series_position": series_position,
            "source": source,
            "confidence": confidence,
        }

        response = requests.post(
            f"{url}/api/contribute",
            json=payload,
            headers=get_signed_headers(),
            timeout=10
        )

        if response.status_code == 429:
            logger.debug("[SKALDLEITA CONTRIBUTE] Rate limited, skipping")
            return None

        if response.status_code != 200:
            logger.debug(f"[SKALDLEITA CONTRIBUTE] API returned {response.status_code}")
            return None

        data = response.json()

        if data.get('status') == 'accepted':
            is_new = data.get('is_new', False)
            consensus = data.get('consensus_count', 1)
            logger.info(f"[SKALDLEITA CONTRIBUTE] {'New' if is_new else 'Existing'} contribution accepted: "
                       f"{author} - {title} (source: {source}, consensus: {consensus})")
            return data

        return None

    except requests.exceptions.Timeout:
        logger.debug("[SKALDLEITA CONTRIBUTE] Request timed out")
        return None
    except Exception as e:
        logger.debug(f"[SKALDLEITA CONTRIBUTE] Error: {e}")
        return None


def lookup_community_consensus(title, author=None, bookdb_url=None):
    """
    Look up community consensus for a book.

    This checks if other users have contributed metadata for this book.
    Useful as a fallback when Skaldleita's main database doesn't have a match.

    Args:
        title: Book title (required)
        author: Optional author name for better matching
        bookdb_url: Custom Skaldleita URL (uses default if not provided)

    Returns:
        dict with book metadata if found, None otherwise
    """
    if not title or len(title.strip()) < 2:
        return None

    url = bookdb_url or BOOKDB_API_URL

    try:
        params = {"title": title.strip()}
        if author:
            params["author"] = author.strip()

        response = requests.get(
            f"{url}/api/community/lookup",
            params=params,
            headers=get_signed_headers(),
            timeout=10
        )

        if response.status_code != 200:
            return None

        data = response.json()

        if data.get('found'):
            logger.info(f"[SKALDLEITA COMMUNITY] Found consensus for {title}: "
                       f"{data.get('author')} (contributors: {data.get('contributor_count', 1)})")
            return data

        return None

    except Exception as e:
        logger.debug(f"[SKALDLEITA COMMUNITY] Lookup error: {e}")
        return None


__all__ = [
    'BOOKDB_API_URL',
    'BOOKDB_PUBLIC_KEY',
    'get_signed_headers',
    'search_bookdb',
    'identify_audio_with_bookdb',
    'contribute_to_bookdb',
    'lookup_community_consensus',
]
