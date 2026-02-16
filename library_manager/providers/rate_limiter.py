"""Rate limiting and circuit breaker for API calls.

This module provides shared rate limiting across all API providers to prevent
hitting rate limits and getting blocked.
"""
import time
import threading
import logging

logger = logging.getLogger(__name__)

# Rate limiting to stay under API limits
# Format: api_name -> {last_call, min_delay}
# Skaldleita (bookdb): 3.6 sec delay = 1000 requests/hour max, spread evenly (never hits limit)
API_RATE_LIMITS = {
    'bookdb': {'last_call': 0, 'min_delay': 3.6},        # Skaldleita: 3600s / 1000 = 3.6s between calls = exactly 1000/hr
    'audnexus': {'last_call': 0, 'min_delay': 2.0},      # ~100/hr community API - be nice
    'openlibrary': {'last_call': 0, 'min_delay': 1.5},   # They request max 1/sec, add buffer
    'googlebooks': {'last_call': 0, 'min_delay': 1.0},   # 1000/day, no per-sec limit but be safe
    'hardcover': {'last_call': 0, 'min_delay': 1.5},     # Beta API - be cautious
    'openrouter': {'last_call': 0, 'min_delay': 5.0},    # Free tier: 20 req/min + daily limits - be conservative
    'gemini': {'last_call': 0, 'min_delay': 7.0},        # Free tier: 10 RPM (Jan 2026), 250 RPD for Flash
    'searxng': {'last_call': 0, 'min_delay': 0.5},       # Local service, minimal delay
}
API_RATE_LOCK = threading.Lock()

# Circuit breaker for APIs that are timing out/failing
# After X consecutive failures, skip the API for a cooldown period
# Issue #74: Reduced cooldowns to prevent queue from stalling too long
API_CIRCUIT_BREAKER = {
    'audnexus': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 300},   # 5 min cooldown after 3 failures
    'bookdb': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 5, 'cooldown': 120},     # Skaldleita: 2 min cooldown after 5 rate limits
    'openrouter': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 600}, # 10 min cooldown after 3 failures (was 1hr)
    'gemini': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 300},     # 5 min cooldown after 3 quota errors (was 30min)
}


def rate_limit_wait(api_name):
    """
    Wait if needed to respect rate limits for the given API.

    For Skaldleita: 3.6s delay ensures exactly 1000 requests/hour max.
    All requests go through - no skipping, just proper pacing.
    """
    with API_RATE_LOCK:
        if api_name not in API_RATE_LIMITS:
            return True  # Unknown API, allow

        limit_info = API_RATE_LIMITS[api_name]
        now = time.time()
        elapsed = now - limit_info['last_call']
        wait_time = limit_info['min_delay'] - elapsed

        if wait_time > 0:
            logger.debug(f"Rate limiting {api_name}: waiting {wait_time:.1f}s")
            time.sleep(wait_time)

        API_RATE_LIMITS[api_name]['last_call'] = time.time()
        return True  # Always succeeds - we just pace, never skip


def is_circuit_open(api_name):
    """Check if the circuit breaker is open for the given API."""
    cb = API_CIRCUIT_BREAKER.get(api_name, {})
    if cb.get('circuit_open_until', 0) > time.time():
        remaining = int(cb['circuit_open_until'] - time.time())
        logger.debug(f"[CIRCUIT BREAKER] {api_name} is open, {remaining}s remaining")
        return True
    return False


def record_api_failure(api_name):
    """Record an API failure and potentially trip the circuit breaker."""
    if api_name not in API_CIRCUIT_BREAKER:
        return
    cb = API_CIRCUIT_BREAKER[api_name]
    cb['failures'] = cb.get('failures', 0) + 1
    if cb['failures'] >= cb.get('max_failures', 3):
        cb['circuit_open_until'] = time.time() + cb.get('cooldown', 300)
        logger.warning(f"[CIRCUIT BREAKER] {api_name} tripped - cooling down for {cb.get('cooldown', 300)}s")


def record_api_success(api_name):
    """Record an API success and reset the circuit breaker."""
    if api_name in API_CIRCUIT_BREAKER:
        API_CIRCUIT_BREAKER[api_name]['failures'] = 0


def handle_rate_limit_response(response, api_name, retry_count=0, max_retries=2):
    """
    Handle a 429 response with exponential backoff and circuit breaker.

    Args:
        response: The requests.Response object (must be status 429)
        api_name: API name for circuit breaker tracking (e.g. 'bookdb')
        retry_count: Current retry attempt (0-based)
        max_retries: Maximum number of retries before giving up

    Returns:
        dict with:
            'should_retry': bool - whether caller should retry the request
            'wait_seconds': int - how long to wait before retrying (0 if not retrying)
            'circuit_open': bool - whether circuit breaker tripped
            'retry_after': str - raw Retry-After header value
    """
    retry_after_raw = response.headers.get('Retry-After', '')

    result = {
        'should_retry': False,
        'wait_seconds': 0,
        'circuit_open': False,
        'retry_after': retry_after_raw,
    }

    # Update circuit breaker
    record_api_failure(api_name)

    cb = API_CIRCUIT_BREAKER.get(api_name, {})
    if cb.get('circuit_open_until', 0) > time.time():
        result['circuit_open'] = True
        logger.warning(f"[RATE LIMIT] {api_name}: Circuit breaker tripped, backing off")
        return result

    if retry_count >= max_retries:
        logger.warning(f"[RATE LIMIT] {api_name}: Max retries ({max_retries}) reached")
        return result

    # Calculate wait time: use Retry-After header, with exponential backoff fallback
    try:
        wait_time = int(retry_after_raw) if retry_after_raw else 0
    except ValueError:
        wait_time = 0

    if wait_time <= 0:
        # Exponential backoff: 30s, 60s, 120s...
        wait_time = 30 * (2 ** retry_count)

    # Cap at 5 minutes
    wait_time = min(wait_time, 300)

    result['should_retry'] = True
    result['wait_seconds'] = wait_time

    logger.info(f"[RATE LIMIT] {api_name}: Rate limited, waiting {wait_time}s "
                f"(attempt {retry_count + 1}/{max_retries}, Retry-After: {retry_after_raw or 'none'})")

    return result


__all__ = [
    'API_RATE_LIMITS',
    'API_RATE_LOCK',
    'API_CIRCUIT_BREAKER',
    'rate_limit_wait',
    'is_circuit_open',
    'record_api_failure',
    'record_api_success',
    'handle_rate_limit_response',
]
