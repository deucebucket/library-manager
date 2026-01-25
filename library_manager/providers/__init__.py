"""API providers for Library Manager.

This package contains the API providers used for book metadata lookups:
- BookDB (primary, our API)
- Audnexus (audiobook specialist)
- OpenLibrary (open source)
- Google Books
- Hardcover

And AI providers for book identification:
- Gemini
- OpenRouter
- Ollama
"""

from library_manager.providers.rate_limiter import (
    rate_limit_wait,
    is_circuit_open,
    record_api_failure,
    record_api_success,
    API_RATE_LIMITS,
    API_CIRCUIT_BREAKER,
)

__all__ = [
    # Rate limiting
    'rate_limit_wait',
    'is_circuit_open',
    'record_api_failure',
    'record_api_success',
    'API_RATE_LIMITS',
    'API_CIRCUIT_BREAKER',
]
