"""
In-app documentation hints for Library Manager.
Provides contextual help text for UI tooltips and hover explanations.
"""

HINTS = {
    # === Identification Layers ===
    'layer_1': 'Database Lookups: Searches Skaldleita, Audnexus, OpenLibrary, Google Books, and Hardcover for metadata matches. Free, fast, no API key needed.',
    'layer_2': 'AI Verification: When databases return uncertain matches, AI (Gemini, OpenRouter, or Ollama) cross-checks the results. Uses your configured AI provider.',
    'layer_3': 'Audio Analysis: Extracts the first 90 seconds of audio to identify the book from narrator intros and title announcements. Can use Skaldleita GPU or your own Gemini API.',
    'layer_4': 'Content Analysis: Last resort. Transcribes story text with Whisper and sends it to AI to identify the book. Slowest but catches edge cases other layers miss.',

    # === AI Providers ===
    'skaldleita': 'Free GPU-powered audio identification service. Transcribes your audiobook intro and matches it against 50M+ books. Does not use your API quota.',
    'gemini': 'Google Gemini AI. Free tier offers 14,400 calls/day with Gemma 3 models. Handles both text verification and native audio analysis.',
    'openrouter': 'API gateway to multiple AI models. Free models available (Llama, Gemma). Used as fallback when Gemini is unavailable or for Layer 4 content analysis.',
    'ollama': 'Self-hosted AI. Run models locally with no API costs or rate limits. Requires separate Ollama installation.',

    # === Confidence & Verification ===
    'confidence_threshold': 'Minimum confidence percentage before a book is considered identified. Higher values mean more certainty but slower processing. Lower values accept weaker matches faster.',
    'confidence_percentage': 'How certain the system is about this identification. Built from multiple sources: audio analysis (85 weight), ID3 tags (80), metadata files (75), database lookups (65), AI (60), path analysis (40). Multiple agreeing sources boost confidence.',
    'deep_scan_mode': 'Runs ALL enabled identification layers for every book, even if an earlier layer already found a confident match. Slower but more thorough.',

    # === Status Meanings ===
    'status_pending': 'A rename has been proposed. Review the suggested author/title and click Apply to rename, or Reject to dismiss.',
    'status_verified': 'This book is already in the correct Author/Title folder. No changes needed.',
    'status_fixed': 'This book was successfully renamed and moved to its new Author/Title location.',
    'status_queued': 'Waiting to be identified. Will be processed automatically when the worker runs, or click Process Queue to start now.',
    'status_error': 'Something went wrong during identification or renaming. Check the error message for details.',
    'status_attention': 'Could not be auto-identified with enough confidence. Needs manual review - click Edit to set the correct author and title.',
    'status_orphan': 'Loose audio files without a proper folder structure. Click Organize to move them into an Author/Title folder.',
    'status_locked': 'Protected from automatic changes. Unlock to allow the system to process this book again.',
    'status_duplicate': 'Multiple copies of the same book detected in your library.',
    'status_reversed': 'Author and title folders appear swapped (e.g., Title/Author instead of Author/Title).',

    # === Settings - Library Tab ===
    'library_paths': 'Folders containing your audiobook library. Each path is scanned for book folders. Supports multiple paths (one per line).',
    'naming_format': 'How renamed folders are structured. Author/Title works with Audiobookshelf, Plex, and Jellyfin. Custom templates let you include series, narrator, year, and more.',
    'series_grouping': 'Groups series books under a shared folder: Author/Series Name/1 - Title. Keeps multi-book series organized together.',
    'standardize_initials': 'Normalizes author initials to a consistent format (e.g., "JRR Tolkien" and "J.R.R. Tolkien" both become "J. R. R. Tolkien"). Prevents duplicate author folders.',
    'strip_unabridged': 'Removes "(Unabridged)", "[Unabridged]", and similar markers from book titles during rename.',
    'multilang_naming': 'Controls how non-English books are named. Native keeps the original language title. Preferred translates to your language. Tagged adds a language indicator.',

    # === Settings - Watch Folder ===
    'watch_folder': 'Monitors a folder for new audiobooks and automatically organizes them into your library. Great for processing downloads or imports.',
    'watch_interval': 'How often (in seconds) to check the watch folder for new files.',
    'watch_min_age': 'Minimum file age before processing. Prevents picking up files still being downloaded or copied.',
    'watch_hard_links': 'Use hard links instead of moving files. Only works when watch folder and library are on the same filesystem. Saves disk space during processing.',

    # === Settings - Processing Tab ===
    'background_processing': 'Automatically processes queue items without manual intervention. Disable to only process when you click Process Queue.',
    'scan_interval': 'Hours between automatic library scans. The system checks for new or changed books at this interval.',
    'batch_size': 'Number of books processed in each batch. Higher values process faster but use more API calls at once.',
    'max_requests_per_hour': 'Rate limit for API calls. Prevents hitting provider rate limits. Range: 10-500.',

    # === Settings - AI Setup Tab ===
    'gemini_api_key': 'Free API key from Google AI Studio (aistudio.google.com). Enables Gemini AI for text verification and audio analysis. 14,400 free calls per day.',
    'openrouter_api_key': 'API key from openrouter.ai. Provides access to free AI models as fallback, and enables Layer 4 content analysis.',
    'bookdb_api_key': 'Optional Skaldleita API key. Increases your rate limit from 500 to 1000 requests per hour. Free to register.',
    'google_books_api_key': 'Optional Google Books API key for higher rate limits on book lookups.',
    'ai_provider': 'Which AI to try first for text verification. Falls back to other configured providers automatically if the primary fails.',
    'provider_chain': 'Order in which providers are tried. If the first one fails or is unavailable, the next one is used automatically.',

    # === Settings - Safety Tab ===
    'auto_fix': 'Automatically applies safe renames without asking. Only applies non-drastic changes (e.g., fixing capitalization). Drastic author changes still require approval.',
    'protect_author_changes': 'When the author changes completely (e.g., "Unknown" to "Stephen King"), the fix is sent to Pending for manual review instead of auto-applying.',
    'trust_the_process': 'YOLO mode. Auto-applies ALL changes when AI and audio analysis agree, including drastic author changes. No safety net. Back up your library first.',
    'skip_confirmations': 'Removes "Are you sure?" popups when clicking Apply, Reject, or Undo. Faster workflow but no second chances.',

    # === Settings - Advanced Tab ===
    'metadata_embedding': 'Writes metadata tags (title, author, narrator, series) directly into audio files when fixes are applied. Supports MP3, M4B, FLAC, and Ogg.',
    'ebook_management': 'Enables scanning and organizing ebook files (.epub, .mobi, .azw3, .pdf). Can merge ebooks into the same Author/Title folders as audiobooks or keep them separate.',
    'isbn_lookup': 'Extracts ISBN from EPUB/PDF metadata for more accurate book matching.',
    'error_reporting': 'Shares anonymous error reports to help improve Library Manager. Never includes file paths, API keys, or personal data.',
    'community_contributions': 'Shares extracted metadata (author, title, narrator) with other Library Manager users. When 2+ users agree on metadata, it becomes verified for everyone.',
    'p2p_cache': 'Shares book lookup results via a decentralized peer-to-peer network. Helps when Skaldleita is temporarily unavailable.',
    'language_detection': 'Uses Gemini to detect the spoken language of audiobooks from audio samples.',
    'strict_language_matching': 'Only matches books in your preferred language. Prevents cross-language mismatches (e.g., a Russian audiobook matching an English database entry).',
    'preserve_original_titles': 'Keeps foreign language titles as-is instead of translating them to your preferred language.',
    'deep_verification': 'Re-verifies your entire library against APIs, even books that look correctly named. Use when you suspect misattributed books in an imported collection.',

    # === Trust Mode ===
    'sl_trust_full': 'Accepts Skaldleita matches at 80%+ confidence and skips AI verification. Recommended - GPU Whisper with 50M book database is usually accurate.',
    'sl_trust_boost': 'Uses Skaldleita results as a strong hint, then verifies with database APIs. Skips AI. Good middle ground.',
    'sl_trust_legacy': 'Uses AI to verify uncertain Skaldleita matches. Most thorough but uses more API quota.',

    # === Source Icons ===
    'source_bookdb': 'Identified via Skaldleita - GPU-powered audio fingerprinting matched against 50M+ book database.',
    'source_audio': 'Identified from audio analysis - narrator intro or title announcement detected.',
    'source_ai': 'Verified by AI - an AI model confirmed the identification.',
    'source_id3': 'Metadata from embedded ID3/audio tags in the file itself.',
    'source_json': 'Metadata from a JSON sidecar file (e.g., metadata.json, info.json).',
    'source_path': 'Inferred from the folder path and filename structure.',
    'source_googlebooks': 'Matched via Google Books API.',
    'source_openlibrary': 'Matched via OpenLibrary API.',
    'source_audnexus': 'Matched via Audnexus (Audible metadata).',
    'source_hardcover': 'Matched via Hardcover API (indie/modern books).',
    'source_user': 'Manually set by user - overrides all other sources.',

    # === Voice ID ===
    'voice_id': 'Identifies narrators by voice fingerprint - like Shazam for audiobooks. Builds a community narrator library that improves over time.',

    # === Misc UI ===
    'free_badge': 'This feature is completely free - no API key or payment required.',
    'uses_tokens_badge': 'This feature uses API calls from your configured provider. Check your provider dashboard for usage.',
    'scan_library': 'Scans your library paths for new or changed audiobook folders. Does not process them - just discovers what needs to be identified.',
    'process_queue': 'Starts processing all queued books through the identification pipeline (Layer 1 through Layer 4, depending on your settings).',
}


def get_hint(key: str, default: str = '') -> str:
    """Get a hint by key, returns empty string if not found."""
    return HINTS.get(key, default)


def get_all_hints() -> dict:
    """Get all hints for template rendering."""
    return HINTS.copy()
