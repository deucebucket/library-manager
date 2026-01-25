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
API_RATE_LIMITS = {
    'bookdb': {'last_call': 0, 'min_delay': 1.0},        # Our API - 750/hr, increased to prevent self-banning
    'audnexus': {'last_call': 0, 'min_delay': 2.0},      # ~100/hr community API - be nice
    'openlibrary': {'last_call': 0, 'min_delay': 1.5},   # They request max 1/sec, add buffer
    'googlebooks': {'last_call': 0, 'min_delay': 1.0},   # 1000/day, no per-sec limit but be safe
    'hardcover': {'last_call': 0, 'min_delay': 1.5},     # Beta API - be cautious
    'openrouter': {'last_call': 0, 'min_delay': 5.0},    # Free tier: 20 req/min + daily limits - be conservative
    'gemini': {'last_call': 0, 'min_delay': 7.0},        # Free tier: 10 RPM (Jan 2026), 250 RPD for Flash
}
API_RATE_LOCK = threading.Lock()

# Circuit breaker for APIs that are timing out/failing
# After X consecutive failures, skip the API for a cooldown period
# Issue #74: Reduced cooldowns to prevent queue from stalling too long
API_CIRCUIT_BREAKER = {
    'audnexus': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 300},   # 5 min cooldown after 3 failures
    'bookdb': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 5, 'cooldown': 120},     # 2 min cooldown after 5 rate limits
    'openrouter': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 600}, # 10 min cooldown after 3 failures (was 1hr)
    'gemini': {'failures': 0, 'circuit_open_until': 0, 'max_failures': 3, 'cooldown': 300},     # 5 min cooldown after 3 quota errors (was 30min)
}


def rate_limit_wait(api_name):
    """Wait if needed to respect rate limits for the given API."""
    with API_RATE_LOCK:
        if api_name not in API_RATE_LIMITS:
            return

        limit_info = API_RATE_LIMITS[api_name]
        now = time.time()
        elapsed = now - limit_info['last_call']
        wait_time = limit_info['min_delay'] - elapsed

        if wait_time > 0:
            logger.debug(f"Rate limiting {api_name}: waiting {wait_time:.1f}s")
            time.sleep(wait_time)

        API_RATE_LIMITS[api_name]['last_call'] = time.time()


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


__all__ = [
    'API_RATE_LIMITS',
    'API_RATE_LOCK',
    'API_CIRCUIT_BREAKER',
    'rate_limit_wait',
    'is_circuit_open',
    'record_api_failure',
    'record_api_success',
]
