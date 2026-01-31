"""API providers for Library Manager.

This package contains the API providers used for book metadata lookups:
- Skaldleita (primary, our API - internally still called 'bookdb')
- Audnexus (audiobook specialist)
- OpenLibrary (open source)
- Google Books
- Hardcover
- SearXNG (fallback web search)

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
from library_manager.providers.audnexus import search_audnexus
from library_manager.providers.openlibrary import search_openlibrary
from library_manager.providers.googlebooks import search_google_books
from library_manager.providers.hardcover import search_hardcover
from library_manager.providers.bookdb import (
    BOOKDB_API_URL,
    BOOKDB_PUBLIC_KEY,
    search_bookdb,
    identify_audio_with_bookdb,
)
from library_manager.providers.ollama import (
    DEFAULT_OLLAMA_URL,
    DEFAULT_OLLAMA_MODEL,
    call_ollama,
    call_ollama_simple,
    get_ollama_models,
    test_ollama_connection,
)
from library_manager.providers.openrouter import (
    OPENROUTER_API_URL,
    DEFAULT_MODEL as OPENROUTER_DEFAULT_MODEL,
    call_openrouter,
    call_openrouter_simple,
    identify_book_from_transcript,
    test_openrouter_connection,
)
from library_manager.providers.gemini import (
    GEMINI_API_URL,
    DEFAULT_TEXT_MODEL as GEMINI_DEFAULT_TEXT_MODEL,
    DEFAULT_AUDIO_MODEL as GEMINI_DEFAULT_AUDIO_MODEL,
    _call_gemini_simple,
    call_gemini,
    analyze_audio_with_gemini,
    detect_audio_language,
    try_gemini_content_identification,
)
from library_manager.providers.searxng import (
    DEFAULT_SEARXNG_URL,
    search_searxng,
    test_searxng_connection,
)
from library_manager.providers.fingerprint import (
    is_fpcalc_available,
    generate_fingerprint,
    lookup_fingerprint,
    contribute_fingerprint,
    identify_by_fingerprint,
)
from library_manager.providers.isbn_lookup import (
    extract_isbn_from_file,
    lookup_isbn,
    identify_ebook_by_isbn,
)

__all__ = [
    # Rate limiting
    'rate_limit_wait',
    'is_circuit_open',
    'record_api_failure',
    'record_api_success',
    'API_RATE_LIMITS',
    'API_CIRCUIT_BREAKER',
    # API providers
    'search_audnexus',
    'search_openlibrary',
    'search_google_books',
    'search_hardcover',
    # Skaldleita (legacy name: BookDB)
    'BOOKDB_API_URL',
    'BOOKDB_PUBLIC_KEY',
    'search_bookdb',
    'identify_audio_with_bookdb',
    # Ollama
    'DEFAULT_OLLAMA_URL',
    'DEFAULT_OLLAMA_MODEL',
    'call_ollama',
    'call_ollama_simple',
    'get_ollama_models',
    'test_ollama_connection',
    # OpenRouter
    'OPENROUTER_API_URL',
    'OPENROUTER_DEFAULT_MODEL',
    'call_openrouter',
    'call_openrouter_simple',
    'identify_book_from_transcript',
    'test_openrouter_connection',
    # Gemini
    'GEMINI_API_URL',
    'GEMINI_DEFAULT_TEXT_MODEL',
    'GEMINI_DEFAULT_AUDIO_MODEL',
    '_call_gemini_simple',
    'call_gemini',
    'analyze_audio_with_gemini',
    'detect_audio_language',
    'try_gemini_content_identification',
    # SearXNG (fallback)
    'DEFAULT_SEARXNG_URL',
    'search_searxng',
    'test_searxng_connection',
    # Fingerprinting (Issue #78)
    'is_fpcalc_available',
    'generate_fingerprint',
    'lookup_fingerprint',
    'contribute_fingerprint',
    'identify_by_fingerprint',
    # ISBN extraction (Issue #67)
    'extract_isbn_from_file',
    'lookup_isbn',
    'identify_ebook_by_isbn',
]
