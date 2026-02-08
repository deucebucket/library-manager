#!/usr/bin/env python3
"""
Library Metadata Manager - Web UI
Automatically fixes book metadata using AI.

Features:
- Web dashboard with stats
- Queue of books needing fixes
- History of all fixes made
- Settings management
- Multi-provider AI (Gemini, OpenRouter, Ollama)
"""

APP_VERSION = "0.9.0-beta.119"
GITHUB_REPO = "deucebucket/library-manager"  # Your GitHub repo

# Versioning Guide:
# 0.9.0-beta.1  = Initial beta (basic features)
# 0.9.0-beta.2  = Garbage filtering, series grouping, dismiss errors
# 0.9.0-beta.3  = UI cleanup - merged Advanced/Tools tabs
# 0.9.0-beta.4  = Improved series detection, DB locking fix, system folder filtering
# 0.9.0-beta.11 = Series folder lib_path fix
# 0.9.0-beta.12 = CRITICAL SAFETY: Path sanitization, library boundary checks, depth validation
# 0.9.0-rc.1    = Release candidate (feature complete, final testing)
# 1.0.0         = First stable release (everything works!)

import os
import sys
import json
import time
import shutil
import sqlite3
import threading
import logging
import requests
import re
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_file, session
from flask_babel import Babel, gettext as _, lazy_gettext as _l
from audio_tagging import embed_tags_for_path, build_metadata_for_embedding

# Import from refactored modules
from library_manager.config import (
    BASE_DIR, DATA_DIR, DB_PATH, CONFIG_PATH, SECRETS_PATH,
    DEFAULT_CONFIG, DEFAULT_SECRETS,
    migrate_legacy_config, init_config, needs_setup,
    load_config, save_config, save_secrets, load_secrets
)
from library_manager.database import (
    init_db, get_db, set_db_path, cleanup_garbage_entries,
    cleanup_duplicate_history_entries, insert_history_entry
)
from library_manager.models.book_profile import (
    SOURCE_WEIGHTS, FIELD_WEIGHTS, FieldValue, BookProfile,
    detect_multibook_vs_chapters, save_book_profile, load_book_profile,
    build_profile_from_sources, set_db_getter, set_narrator_saver
)
from library_manager.utils import (
    # naming
    calculate_title_similarity, extract_series_from_title, clean_search_title,
    standardize_initials, clean_author_name, extract_author_title,
    # validation
    is_unsearchable_query, is_garbage_author_match, is_garbage_match, is_placeholder_author, is_drastic_author_change,
    # audio
    AUDIO_EXTENSIONS, EBOOK_EXTENSIONS,
    get_first_audio_file, extract_audio_sample, extract_audio_sample_from_middle,
    find_audio_files, find_ebook_files,
    # path_safety
    sanitize_path_component, build_new_path,
)
from library_manager.providers import (
    rate_limit_wait, is_circuit_open, record_api_failure, record_api_success,
    API_RATE_LIMITS, API_CIRCUIT_BREAKER,
    search_audnexus, search_openlibrary, search_google_books, search_hardcover,
    BOOKDB_API_URL, BOOKDB_PUBLIC_KEY,
    search_bookdb as _search_bookdb_raw, identify_audio_with_bookdb,
    call_ollama as _call_ollama_raw, call_ollama_simple as _call_ollama_simple_raw,
    get_ollama_models, test_ollama_connection,
    call_openrouter, call_openrouter_simple, identify_book_from_transcript,
    test_openrouter_connection,
    # Gemini provider
    call_gemini as _call_gemini_raw,
    _call_gemini_simple as _call_gemini_simple_raw,
    analyze_audio_with_gemini as _analyze_audio_with_gemini_raw,
    detect_audio_language as _detect_audio_language_raw,
    try_gemini_content_identification as _try_gemini_content_identification_raw,
)
from library_manager.pipeline import (
    process_layer_3_audio as _process_layer_3_audio_raw,
    process_layer_1_api as _process_layer_1_api_raw,
    process_layer_1_audio as _process_layer_1_audio_raw,
    process_layer_2_ai as _process_queue_raw,
    process_sl_requeue_verification as _process_sl_requeue_verification_raw,
)
from library_manager.worker import (
    process_all_queue as _process_all_queue_raw,
    start_worker as _start_worker_raw,
    stop_worker as _stop_worker_raw,
    is_worker_running as _is_worker_running_raw,
    get_processing_status,
    update_processing_status,
    set_current_book,
    clear_current_book,
    LAYER_NAMES,
)
from library_manager.instance import (
    get_instance_id,
    get_instance_data,
    save_instance_data,
)

# Try to import P2P cache (optional - gracefully degrades if not available)
try:
    from p2p_cache import get_cache as get_book_cache, GUNDB_AVAILABLE
    P2P_CACHE_AVAILABLE = True
except ImportError:
    P2P_CACHE_AVAILABLE = False
    GUNDB_AVAILABLE = False
    def get_book_cache(*args, **kwargs):
        return None

# Try to import narrator detection from BookDB local database
try:
    from metadata_scraper.database import (
        is_known_narrator as _is_known_narrator,
        add_narrator as _add_narrator,
    )
    BOOKDB_LOCAL_AVAILABLE = True
except ImportError:
    BOOKDB_LOCAL_AVAILABLE = False
    _is_known_narrator = None
    _add_narrator = None

# Import BookDB API client for community contributions
try:
    from library_manager.providers.bookdb import (
        contribute_to_bookdb as _contribute_to_bookdb_api,
        lookup_community_consensus as _lookup_community_api,
    )
    BOOKDB_API_AVAILABLE = True
except ImportError:
    BOOKDB_API_AVAILABLE = False
    _contribute_to_bookdb_api = None
    _lookup_community_api = None

# Combined availability flag
BOOKDB_AVAILABLE = BOOKDB_LOCAL_AVAILABLE or BOOKDB_API_AVAILABLE

def check_if_narrator(name):
    """
    Check if a name is a known audiobook narrator (not an author).
    Returns dict with 'is_narrator' and 'name' if found.
    """
    if not BOOKDB_AVAILABLE or not name:
        return {'is_narrator': False}
    try:
        return _is_known_narrator(name)
    except Exception:
        return {'is_narrator': False}

def auto_save_narrator(name, source='auto_extract'):
    """
    Automatically save a narrator to BookDB if not already known.
    Called whenever we encounter a narrator name during processing.
    Returns True if narrator was added, False if already existed or error.
    """
    if not BOOKDB_AVAILABLE or not _add_narrator or not name:
        return False

    # Clean up the name
    name = name.strip()
    if not name or len(name) < 2:
        return False

    # Skip obvious non-narrator values
    skip_patterns = ['unknown', 'various', 'narrator', 'reader', 'n/a', 'none',
                     'unabridged', 'abridged', 'audiobook', 'audio']
    if name.lower() in skip_patterns:
        return False

    # Check if already known
    if check_if_narrator(name).get('is_narrator'):
        return False  # Already in database

    # Looks like a valid narrator name - add to BookDB
    try:
        _add_narrator(name, source=source)
        logging.info(f"[BOOKDB] Auto-added narrator: {name}")
        return True
    except Exception as e:
        logging.debug(f"Could not auto-add narrator {name}: {e}")
        return False

def contribute_to_community(title, author=None, narrator=None, series=None,
                            series_position=None, source='unknown', confidence='medium'):
    """
    Contribute book metadata to the BookDB community database.
    Requires opt-in via 'contribute_to_community' config setting.

    This allows users who identify books via Gemini, OpenRouter, local Whisper,
    or any other method to contribute back. Even users who don't use BookDB
    for identification can help enrich the database.

    The contribution system builds consensus:
    - 1 contributor = low confidence
    - 2 contributors agreeing = medium confidence
    - 3+ contributors agreeing = high confidence

    Args:
        title: Book title (required)
        author: Author name
        narrator: Narrator name (for audiobooks)
        series: Series name
        series_position: Position in series
        source: How this was identified (gemini, openrouter, whisper, folder_parse, manual, etc.)
        confidence: How confident we are (low, medium, high)

    Returns True if contributed, False otherwise.
    """
    if not BOOKDB_API_AVAILABLE or not _contribute_to_bookdb_api:
        logger.debug("[COMMUNITY] BookDB API not available, skipping contribution")
        return False

    if not title:
        return False

    # Get config to check opt-in
    config = load_config()
    if not config.get('contribute_to_community', False):
        logger.debug("[COMMUNITY] Contribution disabled in config")
        return False

    try:
        result = _contribute_to_bookdb_api(
            title=title,
            author=author,
            narrator=narrator,
            series=series,
            series_position=series_position,
            source=source,
            confidence=confidence
        )
        if result:
            logger.info(f"[COMMUNITY] Contributed: {author}/{title} (source: {source})")
            return True
        return False
    except Exception as e:
        logger.debug(f"Could not contribute to community: {e}")
        return False


# Keep old function name for backwards compatibility
def contribute_audio_extraction(title, author=None, narrator=None, series=None,
                                series_position=None, language=None, confidence='medium'):
    """Legacy wrapper - use contribute_to_community instead."""
    return contribute_to_community(
        title=title, author=author, narrator=narrator,
        series=series, series_position=series_position,
        source='audio_credits', confidence=confidence
    )

def lookup_community_consensus(title, author=None):
    """
    Look up community consensus for a book.
    Returns metadata if found with sufficient confidence.
    Uses the BookDB API to check the community consensus database.
    """
    if not BOOKDB_API_AVAILABLE or not _lookup_community_api:
        return None

    try:
        consensus = _lookup_community_api(title, author)
        if consensus and consensus.get('found'):
            logger.debug(f"[COMMUNITY] Found consensus for {title}: {consensus.get('author')} ({consensus.get('confidence')})")
            return consensus
        return None
    except Exception as e:
        logger.debug(f"Could not lookup community consensus: {e}")
        return None


# ============== BOOK PROFILE SYSTEM ==============
# Moved to library_manager/models/book_profile.py
# Wire narrator saver now that auto_save_narrator is defined
set_narrator_saver(auto_save_narrator)


# ============== SEARCH QUEUE / PROGRESS TRACKING ==============
# Tracks progress of chaos scans and batch operations for UI feedback

class SearchProgress:
    """Thread-safe progress tracker for chaos handler and batch operations."""

    def __init__(self):
        self._lock = threading.Lock()
        self._state = {
            'active': False,
            'operation': None,
            'total': 0,
            'processed': 0,
            'current_item': None,
            'status_message': None,  # Real-time status for user feedback
            'results': [],
            'started_at': None,
            'queue': []  # Items waiting to be processed
        }

    def set_status(self, message):
        """Set current status message for user feedback (e.g., 'BookDB timeout, trying AI...')"""
        with self._lock:
            self._state['status_message'] = message

    def start(self, operation, total, queue_items=None):
        """Start a new operation."""
        with self._lock:
            self._state = {
                'active': True,
                'operation': operation,
                'total': total,
                'processed': 0,
                'current_item': None,
                'status_message': 'Starting...',
                'results': [],
                'started_at': datetime.now().isoformat(),
                'queue': queue_items or []
            }

    def update(self, current_item, result=None):
        """Update progress with current item being processed."""
        with self._lock:
            self._state['processed'] += 1
            self._state['current_item'] = current_item
            if result:
                self._state['results'].append(result)
            # Remove from queue
            if current_item in self._state['queue']:
                self._state['queue'].remove(current_item)

    def finish(self):
        """Mark operation as complete."""
        with self._lock:
            self._state['active'] = False
            self._state['current_item'] = None
            self._state['queue'] = []

    def get_state(self):
        """Get current progress state."""
        with self._lock:
            state = self._state.copy()
            if state['total'] > 0:
                state['percent'] = round((state['processed'] / state['total']) * 100, 1)
            else:
                state['percent'] = 0
            state['queue_position'] = len(state['queue'])
            return state

# Global progress tracker
search_progress = SearchProgress()


# ============== LOCAL BOOKDB CONNECTION ==============
# Direct SQLite connection to local metadata database for fast lookups

BOOKDB_LOCAL_PATH = "/mnt/bookdb-ssd/metadata.db"

def get_bookdb_connection():
    """Get a connection to the local BookDB SQLite database."""
    if os.path.exists(BOOKDB_LOCAL_PATH):
        try:
            return sqlite3.connect(BOOKDB_LOCAL_PATH, timeout=5)
        except Exception as e:
            logging.debug(f"Could not connect to local BookDB: {e}")
    return None


# ============== SMART MATCHING UTILITIES ==============
def extract_folder_metadata(folder_path):
    """
    Extract metadata clues from files in the book folder.
    Looks for: .nfo files, cover images with text, metadata files
    Returns dict with any found metadata hints.
    """
    hints = {}
    folder = Path(folder_path)

    if not folder.exists():
        return hints

    # Look for .nfo files (common in audiobook releases)
    nfo_files = list(folder.glob('*.nfo')) + list(folder.glob('*.NFO'))
    for nfo in nfo_files:
        try:
            content = nfo.read_text(errors='ignore')
            # Look for author/title patterns in NFO
            author_match = re.search(r'(?:author|by|written by)[:\s]+([^\n\r]+)', content, re.IGNORECASE)
            title_match = re.search(r'(?:title|book)[:\s]+([^\n\r]+)', content, re.IGNORECASE)
            if author_match:
                hints['nfo_author'] = author_match.group(1).strip()
            if title_match:
                hints['nfo_title'] = title_match.group(1).strip()
        except Exception:
            pass

    # Look for metadata.json or info.json
    for meta_file in ['metadata.json', 'info.json', 'audiobook.json']:
        meta_path = folder / meta_file
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                if 'author' in meta:
                    hints['meta_author'] = meta['author']
                if 'title' in meta:
                    hints['meta_title'] = meta['title']
                if 'narrator' in meta:
                    hints['meta_narrator'] = meta['narrator']
            except Exception:
                pass

    # Look for desc.txt or description.txt
    for desc_file in ['desc.txt', 'description.txt', 'readme.txt']:
        desc_path = folder / desc_file
        if desc_path.exists():
            try:
                content = desc_path.read_text(errors='ignore')[:2000]  # First 2KB
                hints['description'] = content
            except Exception:
                pass

    # Check audio file metadata using mutagen (if available)
    audio_files = list(folder.glob('*.m4b')) + list(folder.glob('*.mp3')) + list(folder.glob('*.m4a'))
    if audio_files:
        try:
            from mutagen import File
            audio = File(audio_files[0], easy=True)
            if audio:
                if 'albumartist' in audio:
                    hints['audio_author'] = audio['albumartist'][0]
                elif 'artist' in audio:
                    hints['audio_author'] = audio['artist'][0]
                if 'album' in audio:
                    hints['audio_title'] = audio['album'][0]
        except Exception:
            pass

    return hints


def extract_narrator_from_folder(folder_path):
    """
    Try to extract narrator information from audio files in a folder.
    Checks multiple sources: ID3 tags, NFO files, metadata files.
    Returns narrator name string or None if not found.
    """
    folder = Path(folder_path)
    if not folder.exists():
        return None

    # First, try audio file tags
    audio_extensions = ['.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus']
    audio_files = []
    for ext in audio_extensions:
        audio_files.extend(folder.glob(f'*{ext}'))
        audio_files.extend(folder.glob(f'*{ext.upper()}'))

    if audio_files:
        try:
            from mutagen import File
            from mutagen.mp3 import MP3
            from mutagen.mp4 import MP4

            audio = File(audio_files[0])
            if audio:
                # MP4/M4B - check for narrator in various fields
                if hasattr(audio, 'tags') and audio.tags:
                    # M4B/M4A often have narrator in '----:com.apple.iTunes:NARRATOR' or similar
                    for key in audio.tags.keys():
                        key_lower = str(key).lower()
                        if 'narrator' in key_lower or 'read by' in key_lower:
                            val = audio.tags[key]
                            if hasattr(val, 'text'):
                                return str(val.text[0]) if val.text else None
                            elif isinstance(val, list) and val:
                                return str(val[0])
                            else:
                                return str(val)

                # Check 'composer' field - sometimes used for narrator
                if 'composer' in audio:
                    return audio['composer'][0]
                if '\xa9wrt' in audio:  # MP4 composer
                    return audio['\xa9wrt'][0]

        except Exception as e:
            logging.debug(f"Could not extract narrator from audio: {e}")

    # Try NFO files - often have narrator info
    nfo_files = list(folder.glob('*.nfo')) + list(folder.glob('*.NFO'))
    for nfo in nfo_files:
        try:
            content = nfo.read_text(errors='ignore')
            # Look for narrator patterns
            narrator_match = re.search(
                r'(?:narrator|narrated by|read by|performed by|reader)[:\s]+([^\n\r]+)',
                content, re.IGNORECASE
            )
            if narrator_match:
                narrator = narrator_match.group(1).strip()
                # Clean up common suffixes
                narrator = re.sub(r'\s*\(.*\)$', '', narrator)
                if narrator and len(narrator) > 2:
                    return narrator
        except Exception:
            pass

    # Try metadata files
    for meta_file in ['metadata.json', 'info.json', 'audiobook.json', 'book.json']:
        meta_path = folder / meta_file
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                for key in ['narrator', 'narrators', 'read_by', 'reader', 'performed_by']:
                    if key in meta:
                        val = meta[key]
                        if isinstance(val, list):
                            return val[0] if val else None
                        return val
            except Exception:
                pass

    return None


# Configure logging - start with console only, add file handler after DATA_DIR is detected
APP_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Silence Flask's HTTP request logging (only show errors)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)
app.secret_key = 'library-manager-secret-key-2024'

# ============== INTERNATIONALIZATION (i18n) ==============
# Flask-Babel for UI translations - book metadata (author/title) is NOT translated
SUPPORTED_LANGUAGES = {
    'en': 'English',
    'es': 'Español',
    'de': 'Deutsch',
    'fr': 'Français',
    'pl': 'Polski',
    'ru': 'Русский',
    'pt': 'Português',
    'it': 'Italiano',
    'nl': 'Nederlands',
    'cs': 'Čeština',
    'ko': '한국어',
    'ja': '日本語',
    'zh': '中文',
}

def get_locale():
    """Get the user's selected UI language from config, session, or browser."""
    # First check session (temporary override)
    if 'ui_language' in session:
        return session['ui_language']
    # Then check saved config
    config = load_config()
    if config.get('ui_language'):
        return config['ui_language']
    # Fall back to browser preference, then English
    return request.accept_languages.best_match(SUPPORTED_LANGUAGES.keys()) or 'en'

babel = Babel(app, locale_selector=get_locale)

# ============== MODULE WIRING ==============
# Wire up the refactored modules with their dependencies

# Set database path for the database module
set_db_path(DB_PATH)

# Wire book profile module to use our database getter and narrator saver
set_db_getter(get_db)

# Add file handler now that we know DATA_DIR (log to persistent storage)
LOG_FILE = DATA_DIR / 'app.log'
_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(_file_handler)

# Log where we're storing data (helps debug mount issues)
_startup_logger = logging.getLogger(__name__)
_startup_logger.info(f"Data directory: {DATA_DIR} (config persistence location)")

# Note: set_narrator_saver is called below after auto_save_narrator is defined

# ============== LEGACY CONFIGURATION REMOVED ==============
# Configuration code has been moved to library_manager/config.py
# Database code has been moved to library_manager/database.py

# ============== ANONYMOUS ERROR REPORTING ==============

ERROR_REPORTS_PATH = DATA_DIR / "error_reports.json"

def sanitize_error_data(error_msg: str, traceback_str: str = None, book_info: dict = None) -> dict:
    """
    Sanitize error data to remove any personal/sensitive information.

    Removes:
    - File system paths (except relative library structure)
    - API keys and tokens
    - Usernames and home directories
    - IP addresses
    - Any config values that might contain secrets

    Keeps:
    - Error type and message (sanitized)
    - App version
    - Book title/author (only if enabled and relevant to error)
    """
    import re

    # Patterns to remove
    path_pattern = r'(/home/[^/\s]+|/Users/[^/\s]+|/data|/audiobooks|[A-Z]:\\[^\s]+)'
    ip_pattern = r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
    api_key_pattern = r'(sk-[a-zA-Z0-9]+|AIza[a-zA-Z0-9_-]+|[a-zA-Z0-9]{32,})'

    def sanitize_string(s: str) -> str:
        if not s:
            return s
        # Replace paths with [PATH]
        s = re.sub(path_pattern, '[PATH]', s)
        # Replace IPs with [IP]
        s = re.sub(ip_pattern, '[IP]', s)
        # Replace potential API keys with [KEY]
        s = re.sub(api_key_pattern, '[KEY]', s)
        return s

    sanitized = {
        "version": APP_VERSION,
        "timestamp": datetime.now().isoformat(),
        "error": sanitize_string(str(error_msg)[:500]),  # Limit length
    }

    if traceback_str:
        # Only keep last 10 lines of traceback, sanitized
        tb_lines = traceback_str.strip().split('\n')[-10:]
        sanitized["traceback"] = [sanitize_string(line) for line in tb_lines]

    # Only include book info if error reporting settings allow it
    config = load_config()
    if book_info and config.get('error_reporting_include_titles', True):
        # Only include title/author, nothing else
        sanitized["book"] = {
            "title": book_info.get("title", "")[:100] if book_info.get("title") else None,
            "author": book_info.get("author", "")[:100] if book_info.get("author") else None,
        }

    return sanitized


def report_anonymous_error(error_msg: str, traceback_str: str = None, book_info: dict = None, context: str = None):
    """
    Report an error anonymously if the user has opted in.

    This stores error reports locally. In a future version, these could be
    sent to a central server to help identify common issues.

    Args:
        error_msg: The error message
        traceback_str: Optional traceback string
        book_info: Optional dict with 'title' and 'author' keys (only used if error is book-related)
        context: Optional context string (e.g., "layer_1_processing", "scan", "api_call")
    """
    try:
        config = load_config()
        if not config.get('anonymous_error_reporting', False):
            return  # User has not opted in

        # Sanitize the error data
        report = sanitize_error_data(error_msg, traceback_str, book_info)
        if context:
            report["context"] = context[:50]  # Limit context length

        # Load existing reports
        reports = []
        if ERROR_REPORTS_PATH.exists():
            try:
                with open(ERROR_REPORTS_PATH, 'r') as f:
                    reports = json.load(f)
            except:
                reports = []

        # Add new report (keep last 100 reports to avoid file bloat)
        reports.append(report)
        reports = reports[-100:]

        # Save reports
        with open(ERROR_REPORTS_PATH, 'w') as f:
            json.dump(reports, f, indent=2)

        logger.debug(f"Anonymous error report saved (context: {context})")

    except Exception as e:
        # Never let error reporting cause additional errors
        logger.debug(f"Failed to save error report: {e}")


def get_error_reports() -> list:
    """Get all stored error reports for the debug menu."""
    if ERROR_REPORTS_PATH.exists():
        try:
            with open(ERROR_REPORTS_PATH, 'r') as f:
                return json.load(f)
        except:
            return []
    return []


def clear_error_reports():
    """Clear all stored error reports."""
    if ERROR_REPORTS_PATH.exists():
        ERROR_REPORTS_PATH.unlink()


# Directory for permanent report storage (for developer review)
REPORTS_DIR = Path("/home/deucebucket/library-manager-reports")


def send_error_report_email(reports: list, user_message: str = None) -> dict:
    """
    Send error reports via email to the developer.
    Always saves locally first, then attempts email.

    Local storage is the primary method - email is best-effort.
    Returns success if local save works, even if email fails.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from datetime import datetime

    # Create report content
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_id = f"report_{timestamp}"

    # Build report body
    body_parts = []
    body_parts.append(f"Error Report from Library Manager v{APP_VERSION}")
    body_parts.append(f"Submitted: {datetime.now().isoformat()}")
    body_parts.append(f"Report ID: {report_id}")
    body_parts.append("")

    if user_message:
        body_parts.append("=== User Message ===")
        body_parts.append(user_message)
        body_parts.append("")

    body_parts.append(f"=== {len(reports)} Error(s) ===")
    for i, report in enumerate(reports, 1):
        body_parts.append(f"\n--- Error {i} ---")
        body_parts.append(f"Time: {report.get('timestamp', 'Unknown')}")
        body_parts.append(f"Context: {report.get('context', 'Unknown')}")
        body_parts.append(f"Error: {report.get('error', 'No error message')}")
        if report.get('traceback'):
            body_parts.append(f"Traceback:\n{report['traceback']}")
        if report.get('book_info'):
            body_parts.append(f"Book: {report['book_info']}")

    report_body = "\n".join(body_parts)

    # Step 1: Save locally (required)
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        local_file = REPORTS_DIR / f"{report_id}.txt"
        with open(local_file, 'w') as f:
            f.write(report_body)
        logger.info(f"Error report saved locally: {local_file}")
    except Exception as e:
        logger.error(f"Failed to save error report locally: {e}")
        return {
            'success': False,
            'error': f'Failed to save report: {e}'
        }

    # Step 2: Try to send email (best-effort)
    email_sent = False
    email_error = None
    try:
        msg = MIMEMultipart()
        msg['From'] = 'library-manager@localhost'
        msg['To'] = 'lib-man-reports@deucebucket.com'
        msg['Subject'] = f'[Library Manager] Error Report - {len(reports)} error(s)'
        msg.attach(MIMEText(report_body, 'plain'))

        with smtplib.SMTP('localhost', 25) as server:
            server.send_message(msg)

        email_sent = True
        logger.info(f"Error report emailed: {report_id}")
    except Exception as e:
        email_error = str(e)
        logger.warning(f"Email send failed (report still saved locally): {e}")

    # Clear local error reports after successful save
    clear_error_reports()

    # Return success - local save is what matters
    if email_sent:
        return {
            'success': True,
            'message': f'Report saved and emailed (ID: {report_id})',
            'report_id': report_id,
            'email_sent': True
        }
    else:
        return {
            'success': True,
            'message': f'Report saved locally (ID: {report_id}). Email failed: {email_error}',
            'report_id': report_id,
            'email_sent': False,
            'email_error': email_error
        }


# ============== LANGUAGE DETECTION ==============

def detect_title_language(text):
    """
    Detect the language of a title/text using langdetect.
    Returns ISO 639-1 language code or 'en' as fallback.
    """
    if not text or len(text.strip()) < 3:
        return 'en'  # Too short to detect

    try:
        from langdetect import detect, DetectorFactory
        # Make detection deterministic
        DetectorFactory.seed = 0
        detected = detect(text)
        logger.debug(f"Detected language '{detected}' for text: {text}")
        return detected
    except Exception as e:
        logger.debug(f"Language detection failed for '{text}': {e}")
        return 'en'  # Default to English on failure


def should_preserve_original_title(original_title, suggested_title, config):
    """
    Check if we should keep the original title instead of replacing it.

    This prevents replacing foreign language titles with English translations,
    e.g., "Der Bücherdrache" should NOT become "The Book Dragon" for German users.

    Returns True if original title should be preserved.
    """
    if not config.get('preserve_original_titles', True):
        return False

    if not original_title or not suggested_title:
        return False

    # If titles are the same, no preservation needed
    if original_title.strip().lower() == suggested_title.strip().lower():
        return False

    # Detect language of original title
    original_lang = detect_title_language(original_title)
    suggested_lang = detect_title_language(suggested_title)

    # Get user's preferred language
    preferred_lang = config.get('preferred_language', 'en')

    # If original is in user's preferred language but suggestion is in English,
    # preserve the original (user wants German, original is German, don't replace with English)
    if original_lang == preferred_lang and suggested_lang == 'en' and preferred_lang != 'en':
        logger.info(f"Preserving original title '{original_title}' ({original_lang}) - user prefers {preferred_lang}")
        return True

    # If original is in a non-English language and suggestion is English,
    # preserve original if "preserve_original_titles" is enabled
    if original_lang != 'en' and suggested_lang == 'en':
        logger.info(f"Preserving foreign title '{original_title}' ({original_lang}) instead of English '{suggested_title}'")
        return True

    return False


# Audible marketplace mappings for language preference
AUDIBLE_LANGUAGE_ENDPOINTS = {
    'en': 'audible.com',      # US/English
    'de': 'audible.de',       # German
    'fr': 'audible.fr',       # French
    'it': 'audible.it',       # Italian
    'es': 'audible.es',       # Spanish
    'jp': 'audible.co.jp',    # Japanese (ISO code is 'ja' but Audible uses 'jp')
    'ja': 'audible.co.jp',    # Japanese
    'au': 'audible.com.au',   # Australia (English)
    'uk': 'audible.co.uk',    # UK (English)
    'in': 'audible.in',       # India (English)
    'ca': 'audible.ca',       # Canada (English/French)
}


def get_audible_region_for_language(lang_code):
    """
    Get the appropriate Audible region code for a language.
    Audnexus uses these region codes in its API.
    """
    # Map language codes to Audnexus region codes
    region_map = {
        'en': 'us',   # Default English to US
        'de': 'de',   # German
        'fr': 'fr',   # French
        'it': 'it',   # Italian
        'es': 'es',   # Spanish
        'ja': 'jp',   # Japanese
        'pt': 'us',   # Portuguese -> US (no dedicated Audible)
        'nl': 'de',   # Dutch -> Germany (no dedicated Audible)
    }
    return region_map.get(lang_code, 'us')


# ISO 639-1 language code to full name mapping
LANGUAGE_NAMES = {
    'en': 'English', 'de': 'German', 'fr': 'French', 'es': 'Spanish',
    'it': 'Italian', 'pt': 'Portuguese', 'nl': 'Dutch', 'sv': 'Swedish',
    'no': 'Norwegian', 'da': 'Danish', 'fi': 'Finnish', 'pl': 'Polish',
    'ru': 'Russian', 'ja': 'Japanese', 'zh': 'Chinese', 'ko': 'Korean',
    'ar': 'Arabic', 'he': 'Hebrew', 'hi': 'Hindi', 'tr': 'Turkish',
    'cs': 'Czech', 'hu': 'Hungarian', 'el': 'Greek', 'th': 'Thai',
    'vi': 'Vietnamese', 'uk': 'Ukrainian', 'ro': 'Romanian', 'id': 'Indonesian'
}


def get_localized_title_via_ai(title, author, target_language, config):
    """
    Ask AI to find the official localized title of a book.
    This helps when APIs don't have results in the user's preferred language.

    Args:
        title: The book title (usually in English)
        author: The author name
        target_language: ISO 639-1 code (e.g., 'de' for German)
        config: App config with AI credentials

    Returns:
        dict with localized_title and is_translation, or None on failure
    """
    if not target_language or target_language == 'en':
        return None  # No translation needed

    lang_name = LANGUAGE_NAMES.get(target_language, target_language)

    prompt = f"""You are a book metadata expert with knowledge of international book translations.

Given this book:
- Title: {title}
- Author: {author}

What is the official {lang_name} ({target_language}) published title of this book?

RULES:
1. If this book has an official {lang_name} translation, return that title
2. If the book was originally written in {lang_name}, return the original title
3. If no official translation exists, return the original title
4. DO NOT invent translations - only use real published titles

Return JSON only:
{{"localized_title": "the {lang_name} title", "is_translation": true/false, "notes": "brief explanation"}}

If you're not certain, return: {{"localized_title": "{title}", "is_translation": false, "notes": "no official translation found"}}"""

    provider = config.get('ai_provider', 'openrouter')

    try:
        if provider == 'ollama':
            result = _call_ollama_simple(prompt, config)
        elif provider == 'gemini' and config.get('gemini_api_key'):
            result = _call_gemini_simple(prompt, config)
        elif config.get('openrouter_api_key'):
            result = call_openrouter_simple(prompt, config)
        else:
            return None

        if result and result.get('localized_title'):
            logger.info(f"AI localization: '{title}' -> '{result['localized_title']}' ({lang_name})")
            return result
    except Exception as e:
        logger.debug(f"AI localization failed: {e}")

    return None


def _call_gemini_simple(prompt, config):
    """Simple Gemini call for localization queries.

    Wrapper that passes app-level dependencies to the extracted module.
    """
    return _call_gemini_simple_raw(prompt, config, parse_json_response_fn=parse_json_response)


def _call_ollama_simple(prompt, config):
    """Simple Ollama call for localization queries.

    Wrapper that passes app-level dependencies to the extracted module.
    """
    return _call_ollama_simple_raw(prompt, config, parse_json_fn=parse_json_response)


# ============== BOOK METADATA APIs ==============

# Issue #61: Scan lock to prevent concurrent scans causing SQLite errors
SCAN_LOCK = threading.Lock()
scan_in_progress = False

# BookDB wrapper function - provides app-level dependencies to the extracted module
def search_bookdb(title, author=None, api_key=None, retry_count=0, bookdb_url=None, config=None):
    """
    Search BookDB metadata service.
    Wrapper that provides app-level dependencies (DATA_DIR, cache) to the provider module.
    """
    # Provide cache_getter only if P2P cache is available
    cache_getter = get_book_cache if P2P_CACHE_AVAILABLE else None
    return _search_bookdb_raw(
        title=title,
        author=author,
        api_key=api_key,
        retry_count=retry_count,
        bookdb_url=bookdb_url,
        config=config,
        data_dir=DATA_DIR,
        cache_getter=cache_getter
    )


def lookup_book_metadata(messy_name, config, folder_path=None):
    """Try to look up book metadata from multiple APIs, cycling through until found.
    Now with garbage match filtering and folder metadata extraction."""
    # Try to extract author and title separately for better search
    author_hint, title_part = extract_author_title(messy_name)
    clean_title = clean_search_title(title_part)

    if not clean_title or len(clean_title) < 3:
        return None

    # Extract metadata from folder files if path provided
    folder_hints = {}
    if folder_path:
        folder_hints = extract_folder_metadata(folder_path)
        if folder_hints:
            logger.debug(f"Found folder metadata hints: {folder_hints}")
            # Use folder metadata as additional hints
            if 'audio_author' in folder_hints and not author_hint:
                author_hint = folder_hints['audio_author']
            if 'audio_title' in folder_hints:
                # Prefer audio metadata title if clean_title looks like garbage
                if len(clean_title) < 5 or clean_title.lower().startswith('chapter'):
                    clean_title = folder_hints['audio_title']

    if author_hint:
        logger.debug(f"Looking up metadata for: '{clean_title}' by '{author_hint}'")
    else:
        logger.debug(f"Looking up metadata for: {clean_title}")

    def validate_result(result, original_title):
        """Check if API result is a garbage match."""
        if not result:
            return None
        suggested_title = result.get('title', '')
        if is_garbage_match(original_title, suggested_title):
            logger.info(f"REJECTED garbage match: '{original_title}' -> '{suggested_title}'")
            return None
        return result

    # Get language preference settings
    preferred_lang = config.get('preferred_language', 'en')
    audible_region = get_audible_region_for_language(preferred_lang)

    # 0. Try BookDB first (our private metadata service with fuzzy matching)
    # Use user's key if configured, otherwise fall back to public key
    bookdb_key = config.get('bookdb_api_key') or BOOKDB_PUBLIC_KEY
    result = validate_result(search_bookdb(clean_title, author=author_hint, api_key=bookdb_key), clean_title)
    if result:
        return result

    # 1. Try Audnexus (best for audiobooks, pulls from Audible)
    result = validate_result(search_audnexus(clean_title, author=author_hint, region=audible_region), clean_title)
    if result:
        return result

    # 2. Try OpenLibrary (free, huge database)
    result = validate_result(search_openlibrary(clean_title, author=author_hint, lang=preferred_lang), clean_title)
    if result:
        return result

    # 3. Try Google Books
    google_key = config.get('google_books_api_key')
    result = validate_result(search_google_books(clean_title, author=author_hint, api_key=google_key, lang=preferred_lang), clean_title)
    if result:
        return result

    # 4. Try Hardcover.app (modern Goodreads alternative)
    result = validate_result(search_hardcover(clean_title, author=author_hint), clean_title)
    if result:
        return result

    logger.debug(f"No valid API results for: {clean_title}")
    return None


def gather_all_api_candidates(title, author=None, config=None):
    """
    Search ALL APIs and return ALL results (not just the first match).
    This is used for verification when we need multiple perspectives.
    Now with garbage match filtering and language preference support.
    """
    candidates = []
    clean_title = clean_search_title(title)

    if not clean_title or len(clean_title) < 3:
        return candidates

    # Get language preference settings
    preferred_lang = config.get('preferred_language', 'en') if config else 'en'
    audible_region = get_audible_region_for_language(preferred_lang)

    # Search each API and collect all results
    bookdb_url = config.get('bookdb_url') if config else None
    apis = [
        ('BookDB', lambda t, a: search_bookdb(t, a, (config.get('bookdb_api_key') if config else None) or BOOKDB_PUBLIC_KEY, bookdb_url=bookdb_url)),
        ('Audnexus', lambda t, a: search_audnexus(t, a, region=audible_region)),
        ('OpenLibrary', lambda t, a: search_openlibrary(t, a, lang=preferred_lang)),
        ('GoogleBooks', lambda t, a: search_google_books(t, a, config.get('google_books_api_key') if config else None, lang=preferred_lang)),
        ('Hardcover', search_hardcover),
    ]

    for api_name, search_func in apis:
        try:
            # Search with author hint
            result = search_func(clean_title, author)
            if result:
                # Filter garbage title matches
                suggested_title = result.get('title', '')
                suggested_author = result.get('author', '')
                if is_garbage_match(clean_title, suggested_title):
                    logger.info(f"[LAYER 1] REJECTED garbage title from {api_name}: '{clean_title}' -> '{suggested_title}'")
                elif author and is_garbage_author_match(author, suggested_author):
                    logger.info(f"[LAYER 1] REJECTED garbage author from {api_name}: '{author}' -> '{suggested_author}'")
                else:
                    # Ensure source attribution for Book Profile system
                    result['source'] = api_name.lower()
                    result['search_query'] = f"{author} - {clean_title}" if author else clean_title
                    candidates.append(result)
                    logger.info(f"[LAYER 1] {api_name} matched: {result.get('author')} - {result.get('title')}")

            # Also search without author (might find different results)
            if author:
                result_no_author = search_func(clean_title, None)
                if result_no_author:
                    suggested_title = result_no_author.get('title', '')
                    suggested_author = result_no_author.get('author', '')
                    if is_garbage_match(clean_title, suggested_title):
                        logger.debug(f"REJECTED garbage title from {api_name}: '{clean_title}' -> '{suggested_title}'")
                    elif is_garbage_author_match(author, suggested_author):
                        logger.debug(f"REJECTED garbage author from {api_name}: '{author}' -> '{suggested_author}'")
                    elif result_no_author.get('author') != (result.get('author') if result else None):
                        # Ensure source attribution for Book Profile system
                        result_no_author['source'] = api_name.lower()
                        result_no_author['search_query'] = clean_title
                        candidates.append(result_no_author)
        except Exception as e:
            logger.debug(f"Error searching {api_name}: {e}")

    # Deduplicate by author+title
    seen = set()
    unique_candidates = []
    for c in candidates:
        key = f"{c.get('author', '').lower()}|{c.get('title', '').lower()}"
        if key not in seen:
            seen.add(key)
            unique_candidates.append(c)

    # Issue #81: Strict language filtering - remove candidates that don't match preferred language
    if config and config.get('strict_language_matching', True) and preferred_lang != 'en':
        filtered_candidates = []
        for c in unique_candidates:
            title_lang = detect_title_language(c.get('title', ''))
            # Accept if language matches preference OR if detection is uncertain (short titles, mixed content)
            if title_lang == preferred_lang or title_lang == 'en':
                # Accept English as fallback since many databases only have English metadata
                filtered_candidates.append(c)
            else:
                logger.info(f"[LAYER 1] Filtered out {title_lang} result for {preferred_lang} user: {c.get('title')}")

        if filtered_candidates:
            unique_candidates = filtered_candidates
        else:
            # Don't filter everything out - keep original if no matches
            logger.info(f"[LAYER 1] No {preferred_lang} results found, keeping all candidates")

    return unique_candidates


def build_verification_prompt(original_input, original_author, original_title, proposed_author, proposed_title, candidates):
    """
    Build a verification prompt that shows ALL API candidates and asks AI to vote.
    Issue #76: Now includes series context extraction for better matching.
    """
    candidate_list = ""
    for i, c in enumerate(candidates, 1):
        series_info = ""
        if c.get('series'):
            series_info = f" [Series: {c.get('series')}"
            if c.get('series_num'):
                series_info += f" #{c.get('series_num')}"
            series_info += "]"
        candidate_list += f"  CANDIDATE_{i}: {c.get('author', 'Unknown')} - {c.get('title', 'Unknown')}{series_info} (from {c.get('source', 'Unknown')})\n"

    if not candidate_list:
        candidate_list = "  No API results found.\n"

    # Issue #76: Extract series info from original input for better matching
    series_context = ""
    extracted = extract_series_from_title(original_input)
    if extracted[0]:  # Has series name
        series_name, series_num, standalone_title = extracted
        series_context = f"""
DETECTED SERIES CONTEXT:
  - Series Name: {series_name}
  - Book Number: {series_num if series_num else 'Unknown'}
  - Standalone Title: {standalone_title}

CRITICAL SERIES RULE:
The original input contains EXPLICIT series information "{series_name}".
If a candidate does NOT match this series, it is almost certainly WRONG!
- "Expeditionary Force Book 14 - Match Game" -> "Doc Raymond - Match Game" = WRONG (different series/author!)
- "Dresden Files 1 - Storm Front" -> "Harry Dresden - Storm Front" = Could be CORRECT (same series character)
- Candidates that share a common word but are from DIFFERENT series are WRONG.
"""

    return f"""You are a book metadata verification expert. A drastic author change was detected and needs your verification.

ORIGINAL INPUT: {original_input}
  - Current Author: {original_author}
  - Current Title: {original_title}
{series_context}
PROPOSED CHANGE:
  - New Author: {proposed_author}
  - New Title: {proposed_title}

ALL API SEARCH RESULTS:
{candidate_list}

CRITICAL RULE - REJECT GARBAGE MATCHES:
The API sometimes returns COMPLETELY UNRELATED books that share one word. These are ALWAYS WRONG:
- "Chapter 19" -> "College Accounting, Chapters 1-9" = WRONG (different book!)
- "Death Genesis" -> "The Darkborn AfterLife Genesis" = WRONG (matching on "genesis" only)
- "Mr. Murder" -> "Frankenstein" = WRONG (no title overlap at all!)
- "Mortal Coils" -> "The Life and Letters of Thomas Huxley" = WRONG (completely different book)
- "Expeditionary Force Book 14 - Match Game" -> "Doc Raymond - Match Game" = WRONG (DIFFERENT SERIES!)

If the proposed title shares LESS THAN HALF of its significant words with the original title, it is WRONG.
If the original has SERIES INFO and the proposed book is from a DIFFERENT series, it is WRONG.

YOUR TASK:
Analyze whether the proposed change is CORRECT or WRONG. Consider:

1. SERIES MATCHING FIRST (if applicable) - Does the series match?
   - If original has series info like "Expeditionary Force Book 14", candidates MUST be from that series
   - A book with matching title but WRONG SERIES is WRONG
   - "Expeditionary Force Book 14 - Match Game" should NOT match "Doc Raymond - Match Game" (wrong author/series!)

2. TITLE MATCHING - Is this even the same book?
   - At least 50% of significant words must match
   - "Mr. Murder" and "Dean Koontz's Frankenstein" = WRONG (0% match!)
   - "Midnight Texas 3" and "Night Shift" = CORRECT if Night Shift is book 3 of Midnight Texas

3. RECOGNIZE FAMOUS BOOKS: Some titles are so famous they have a KNOWN author:
   - "Le Petit Prince" / "The Little Prince" = Antoine de Saint-Exupéry (ALWAYS)
   - "Преступление и наказание" / "Crime and Punishment" = Fyodor Dostoevsky (ALWAYS)
   - "الحب في زمن الكوليرا" / "Love in the Time of Cholera" = Gabriel García Márquez (ALWAYS)
   - "1984" = George Orwell (ALWAYS)
   - "Don Quixote" / "Don Quijote" = Miguel de Cervantes (ALWAYS)
   - "War and Peace" / "Война и мир" = Leo Tolstoy (ALWAYS)
   - If the input says "Stephen King - الحب في زمن الكوليرا", this is WRONG - return Gabriel García Márquez.
   - Return the CORRECT author for famous works, ignoring the wrong input author.

4. AUTHOR MATCHING for non-famous works: Does the original author name match or partially match any candidate?
   - "Boyett" matches "Steven Boyett" (same person, use full name)
   - "Boyett" does NOT match "John Dickson Carr" (different person!)
   - "A.C. Crispin" matches "A. C. Crispin" or "Ann C. Crispin" (same person)

5. FIND THE BEST MATCH: Pick the candidate that is the CORRECT book with the CORRECT author.
   - If NO candidates match the series, say UNCERTAIN and recommend using the original series info

RESPOND WITH JSON ONLY:
{{
  "decision": "CORRECT" or "WRONG" or "UNCERTAIN",
  "recommended_author": "The correct author name",
  "recommended_title": "The correct title",
  "reasoning": "Brief explanation of why",
  "confidence": "HIGH" or "MEDIUM" or "LOW"
}}

DECISION RULES:
- If original has series info but NO candidates match that series = UNCERTAIN (not enough data!)
- If titles are completely different books = WRONG (don't just keyword match!)
- If original author matches a candidate (like Boyett -> Steven Boyett) = CORRECT
- For FAMOUS books (Le Petit Prince, Crime and Punishment, etc.): Return the REAL author, even if original is different
- If proposed author is completely different person AND title doesn't match = WRONG
- If uncertain = UNCERTAIN

IMPORTANT: For famous/classic works, CORRECT the author to the real author. "James Patterson - Le Petit Prince" should become "Antoine de Saint-Exupéry - Le Petit Prince"."""


def verify_drastic_change(original_input, original_author, original_title, proposed_author, proposed_title, config):
    """
    Verify a drastic change by gathering all API candidates and having AI vote.
    Returns: {'verified': bool, 'author': str, 'title': str, 'reasoning': str}
    """
    logger.info(f"Verifying drastic change: {original_author} -> {proposed_author}")

    # Gather ALL candidates from ALL APIs
    candidates = gather_all_api_candidates(original_title, original_author, config)

    # Also search with proposed info to get more candidates
    if proposed_author and proposed_author != original_author:
        more_candidates = gather_all_api_candidates(proposed_title, proposed_author, config)
        for c in more_candidates:
            if c not in candidates:
                candidates.append(c)

    logger.info(f"Gathered {len(candidates)} candidates for verification")

    # Build verification prompt
    prompt = build_verification_prompt(
        original_input, original_author, original_title,
        proposed_author, proposed_title, candidates
    )

    # Call AI for verification using the provider chain
    try:
        verification = call_text_provider_chain(prompt, config)
        if not verification:
            logger.error("No AI providers available for verification!")
            return None

        decision = verification.get('decision', 'UNCERTAIN')
        confidence = verification.get('confidence', 'LOW')

        logger.info(f"Verification result: {decision} ({confidence}): {verification.get('reasoning', '')[:100]}")

        return {
            'verified': decision in ['CORRECT'] and confidence in ['HIGH', 'MEDIUM'],
            'decision': decision,
            'author': verification.get('recommended_author', original_author),
            'title': verification.get('recommended_title', original_title),
            'reasoning': verification.get('reasoning', ''),
            'confidence': confidence,
            'candidates_found': len(candidates)
        }
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return None


# ============== AI API ==============

def build_prompt(messy_names, api_results=None):
    """Build the parsing prompt for AI, including any API lookup results."""
    items = []
    narrator_warnings = []
    for i, name in enumerate(messy_names):
        item_text = f"ITEM_{i+1}: {name}"

        # Check if the "author" part is actually a known narrator
        # Parse author from "Author - Title" or "Author / Title" format
        author_part = None
        if ' - ' in name:
            author_part = name.split(' - ')[0].strip()
        elif ' / ' in name:
            author_part = name.split(' / ')[0].strip()

        if author_part:
            narrator_check = check_if_narrator(author_part)
            if narrator_check.get('is_narrator'):
                item_text += f"\n  ⚠️ WARNING: '{author_part}' is a NARRATOR, not an author! Find the real author."
                narrator_warnings.append(author_part)

        # Add API lookup result if available
        if api_results and i < len(api_results) and api_results[i]:
            result = api_results[i]
            series_info = ""
            if result.get('series'):
                series_info = f" [{result['series']}"
                if result.get('series_num'):
                    series_info += f" #{result['series_num']}"
                series_info += "]"
            item_text += f"\n  -> API found: {result['author']} - {result['title']}{series_info} (from {result['source']})"
        items.append(item_text)
    names_list = "\n".join(items)

    # Add narrator warning section if any were found
    narrator_section = ""
    if narrator_warnings:
        narrator_section = f"""
NARRATOR WARNING - CRITICAL:
The following names are KNOWN AUDIOBOOK NARRATORS, NOT AUTHORS:
{', '.join(set(narrator_warnings))}

These people READ books, they don't WRITE them. You MUST find the actual author.
Examples:
- Tim Gerard Reynolds narrates "Ready Player One" but Ernest Cline WROTE it
- Scott Brick narrates many thrillers but is NOT the author
- Ray Porter narrates "The Martian" but Andy Weir WROTE it
DO NOT return a narrator as the author!
"""

    return f"""You are a book metadata expert. For each filename, identify the REAL author and title.

{names_list}
{narrator_section}
IMPORTANT RULE - TRUST THE EXISTING AUTHOR (with exceptions):
If the input is already in "Author / Title" or "Author - Title" format with a human name as author:
- KEEP THAT AUTHOR unless you're 100% certain it's wrong
- Many books have the SAME TITLE by DIFFERENT AUTHORS
- Example: "The Hollow Man" exists by BOTH Steven Boyett AND John Dickson Carr - different books!
- Example: "Yellow" by Aron Beauregard is NOT "The King in Yellow" by Chambers!
- If API returns a DIFFERENT AUTHOR for the same title, TRUST THE INPUT AUTHOR

WHEN TO CHANGE THE AUTHOR:
- If the "author" is marked as a NARRATOR above - these are readers, not writers!
- If the "author" in input is clearly NOT an author name (e.g., "Bastards Series", "Unknown", "Various")
- If the author/title are swapped (e.g., "Mistborn / Brandon Sanderson" -> swap them)
- If it's clearly gibberish
- **FOR FAMOUS/CLASSIC WORKS:** Return the CORRECT author even if input is wrong:
  - "Le Petit Prince" / "The Little Prince" = Antoine de Saint-Exupéry (ALWAYS)
  - "Преступление и наказание" / "Crime and Punishment" = Fyodor Dostoevsky (ALWAYS)
  - "الحب في زمن الكوليرا" / "Love in the Time of Cholera" = Gabriel García Márquez (ALWAYS)
  - "1984" = George Orwell, "Don Quixote" = Miguel de Cervantes, "War and Peace" = Leo Tolstoy
  - If input says "Stephen King - الحب في زمن الكوليرا", output "Gabriel García Márquez - Love in the Time of Cholera"

WHEN TO KEEP THE AUTHOR:
- Input: "Boyett/The Hollow Man" -> Keep author "Boyett" (Steven Boyett wrote this book!)
- Input: "Aron Beauregard/Yellow" -> Keep author "Aron Beauregard" (he wrote "Yellow"!)
- If it looks like a human name (First Last, or Last name), it's probably correct

API RESULTS WARNING - CRITICAL:
- API may return COMPLETELY WRONG books that share only one keyword!
- "Chapter 19" -> "College Accounting" = WRONG (API matched on "chapter" - garbage!)
- "Death Genesis" -> "The Darkborn AfterLife" = WRONG (API matched on "genesis" - garbage!)
- "Mr. Murder" -> "Frankenstein" = WRONG (no title match at all!)
- If API title is COMPLETELY DIFFERENT from input title, IGNORE THE API RESULT
- Same title can exist by different authors - if API author differs, keep INPUT author
- Only use API if input has NO author OR the titles closely match

GENERIC TITLE WARNING - DO NOT HALLUCINATE AUTHORS:
- Some titles are GENERIC and could match multiple different books by different authors
- Generic titles include: "Match Game", "The Game", "Home", "Gone", "Prey", "Storm", "The Hunt", "Hunted"
- If input has NO author and a GENERIC title, DO NOT GUESS the author
- Example: "Match Game" alone (no author) -> could be Craig Alanson, could be someone else
- NEVER invent an author name you're not certain about
- If you don't KNOW which specific book this is, set author to null
- Better to return null than to guess wrong!

TITLE PRESERVATION - VERY IMPORTANT:
- If input title is MORE SPECIFIC than API title, KEEP THE INPUT TITLE
- "Double Cross" is more specific than "Cross" - keep "Double Cross"!
- "Triple Cross" is more specific than "Cross" - keep "Triple Cross"!
- Input "Double Cross" + API "Cross" = Output MUST be "Double Cross" (not "Cross")
- NEVER shorten a specific title to a shorter/more generic one
- The API found the SERIES (Alex Cross) but matched the WRONG BOOK in that series
- Always prefer the longer, more specific title from the input

LANGUAGE/CHARACTER RULES:
- ALWAYS use Latin/English characters for author and title names
- If input is "Dmitry Glukhovsky", output "Dmitry Glukhovsky" (NOT "Дмитрий Глуховский" in Cyrillic)
- If API returns non-Latin characters (Cyrillic, Chinese, etc.), convert to the Latin equivalent
- Keep the library consistent - English alphabet only

OTHER RULES:
- NEVER put "Book 1", "Book 2", etc. in the title field - that goes in series_num
- The title should be the ACTUAL book title, not "Series Name Book N"
- Remove junk: [bitsearch.to], [64k], version numbers, format tags, bitrates, file sizes
- Fix obvious typos in author names (e.g., "Annie Jacobson" -> "Annie Jacobsen")
- Clean up title formatting but PRESERVE the actual title - don't replace it

NARRATOR PRESERVATION (CRITICAL FOR AUDIOBOOKS):
- Parentheses at the END containing a SINGLE PROPER NAME (surname) = NARRATOR
- ONLY extract as narrator if it looks like a person's name (capitalized surname)
- Examples that ARE narrators: "(Kafer)", "(Palmer)", "(Vance)", "(Barker)", "(Glover)", "(Fry)", "(Brick)"

NOT NARRATORS - these are junk to REMOVE:
- Genres: "(Horror)", "(Sci-Fi)", "(Fantasy)", "(Romance)", "(Thriller)", "(Mystery)"
- Formats: "(Unabridged)", "(Abridged)", "(MP3)", "(M4B)", "(Audiobook)", "(AB)"
- Years: "(2020)", "(1985)", any 4-digit number
- Quality: "(64k)", "(128k)", "(HQ)", "(Complete)"
- Sources: "(Audible)", "(Librivox)", "(BBC)"
- Descriptors: "(Complete)", "(Full)", "(Retail)", "(SET)"

HOW TO TELL THE DIFFERENCE:
- Narrator = single capitalized word that looks like a surname (Vance, Brick, Fry)
- NOT narrator = common English words, genres, formats, numbers
- When in doubt, set narrator to null (don't guess)

SERIES DETECTION:
- If the book is part of a known series, set "series" to the series name and "series_num" to the book number
- The "title" field should be the ACTUAL BOOK TITLE - never "Series Book N"
- Examples of series books:
  - "Mistborn Book 1" -> series: "Mistborn", series_num: 1, title: "The Final Empire" (NOT "Mistborn Book 1")
  - "Dark One: The Forgotten" -> series: "Dark One", series_num: 2, title: "The Forgotten" (keep actual subtitle!)
  - "The Reckoners Book 2 - Firefight" -> series: "The Reckoners", series_num: 2, title: "Firefight"
  - "Eragon" -> series: "Inheritance Cycle", series_num: 1, title: "Eragon"
  - "The Eye of the World" -> series: "The Wheel of Time", series_num: 1, title: "The Eye of the World"
  - "Leviathan Wakes" -> series: "The Expanse", series_num: 1, title: "Leviathan Wakes"
- Standalone books (NOT in a series) -> series: null, series_num: null
  - "The Martian" by Andy Weir = standalone, no series
  - "Project Hail Mary" by Andy Weir = standalone, no series
  - "Warbreaker" by Brandon Sanderson = standalone, no series
- Only set series if you're CERTAIN it's part of a series. When in doubt, leave null.
- CRITICAL: Never replace the actual title with "Book N" - preserve what makes each book unique!

EXAMPLES:
- "Clive Barker - 1986 - The Hellbound Heart (Kafer) 64k" -> Author: Clive Barker, Title: The Hellbound Heart, Narrator: Kafer, series: null
- "Brandon Sanderson - Mistborn #1 - The Final Empire" -> Author: Brandon Sanderson, Title: The Final Empire, series: Mistborn, series_num: 1
- "Christopher Paolini/Eragon" -> Author: Christopher Paolini, Title: Eragon, series: Inheritance Cycle, series_num: 1
- "The Martian" (no author) -> Author: Andy Weir, Title: The Martian, series: null (standalone book)
- "James S.A. Corey - Leviathan Wakes" -> Author: James S.A. Corey, Title: Leviathan Wakes, series: The Expanse, series_num: 1

Return JSON array. Each object MUST have "item" matching the ITEM_N label:
[
  {{"item": "ITEM_1", "author": "Author Name", "title": "Book Title", "narrator": "Narrator or null", "series": "Series Name or null", "series_num": 1, "year": null}}
]

Return ONLY the JSON array, nothing else."""

def parse_json_response(text):
    """Extract JSON from AI response - handles various AI response formats."""
    text = text.strip()

    # Remove markdown code blocks
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Extract first JSON object using regex (handles extra text before/after)
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    # Try extracting JSON array
    array_match = re.search(r'\[.*?\]', text, re.DOTALL)
    if array_match:
        try:
            return json.loads(array_match.group())
        except json.JSONDecodeError:
            pass

    # Log failure for debugging
    logger.error(f"Failed to parse JSON from AI response: {text[:200]}...")
    return None

def call_ai(messy_names, config):
    """Call AI API to parse book names, with API lookups for context."""
    # First, try to look up each book in metadata APIs
    api_results = []
    for name in messy_names:
        result = lookup_book_metadata(name, config)
        api_results.append(result)
        if result:
            logger.info(f"API lookup success for: {name[:50]}...")

    # Build prompt with API results included
    prompt = build_prompt(messy_names, api_results)

    # Use the provider chain for fallback support
    return call_text_provider_chain(prompt, config)


def explain_http_error(status_code, provider):
    """Convert HTTP status codes to human-readable errors."""
    errors = {
        400: "Bad request - the API didn't understand our request",
        401: "Invalid API key - check your key in Settings",
        403: "Access denied - your API key doesn't have permission",
        404: "Model not found - the selected model may not exist",
        429: "Rate limit exceeded - too many requests, waiting before retry",
        500: f"{provider} server error - their servers are having issues",
        502: f"{provider} is temporarily down - try again later",
        503: f"{provider} is overloaded - try again in a few minutes",
    }
    return errors.get(status_code, f"Unknown error (HTTP {status_code})")


def call_gemini(prompt, config, retry_count=0):
    """Call Google Gemini API directly with automatic retry on rate limit.

    Wrapper that passes app-level dependencies to the extracted module.
    """
    return _call_gemini_raw(
        prompt=prompt,
        config=config,
        retry_count=retry_count,
        parse_json_response_fn=parse_json_response,
        explain_http_error_fn=explain_http_error,
        report_anonymous_error_fn=report_anonymous_error
    )


def call_ollama(prompt, config):
    """Call local Ollama API for fully self-hosted AI.

    Wrapper that passes app-level dependencies to the extracted module.
    """
    return _call_ollama_raw(
        prompt, config,
        parse_json_fn=parse_json_response,
        explain_error_fn=explain_http_error,
        report_error_fn=report_anonymous_error
    )


# get_ollama_models and test_ollama_connection are imported directly from library_manager.providers


# ============== PROVIDER CHAIN SYSTEM ==============
# Configurable fallback chains for audio and text identification

def call_text_provider_chain(prompt, config):
    """
    Call AI providers in configured order until one succeeds.

    Uses config['text_provider_chain'] to determine order.
    Default: ["gemini", "openrouter"]
    Available: "gemini", "openrouter", "ollama"

    Returns parsed response or None if all providers fail.
    """
    chain = config.get('text_provider_chain', ['gemini', 'openrouter'])
    secrets = load_secrets()

    # Merge secrets into config for provider calls
    merged_config = {**config, **secrets}

    for provider in chain:
        provider = provider.lower().strip()
        logger.debug(f"[PROVIDER CHAIN] Trying text provider: {provider}")

        try:
            if provider == 'gemini':
                if not merged_config.get('gemini_api_key'):
                    logger.debug("[PROVIDER CHAIN] Skipping gemini - no API key")
                    continue
                result = call_gemini(prompt, merged_config)
                if result:
                    logger.info(f"[PROVIDER CHAIN] Success with gemini")
                    return result

            elif provider == 'openrouter':
                if not merged_config.get('openrouter_api_key'):
                    logger.debug("[PROVIDER CHAIN] Skipping openrouter - no API key")
                    continue
                result = call_openrouter(prompt, merged_config)
                if result:
                    logger.info(f"[PROVIDER CHAIN] Success with openrouter")
                    return result

            elif provider == 'ollama':
                result = call_ollama(prompt, merged_config)
                if result:
                    logger.info(f"[PROVIDER CHAIN] Success with ollama")
                    return result

            else:
                logger.warning(f"[PROVIDER CHAIN] Unknown text provider: {provider}")

        except Exception as e:
            logger.warning(f"[PROVIDER CHAIN] {provider} failed with error: {e}")
            continue

    logger.warning("[PROVIDER CHAIN] All text providers failed")
    return None


def call_audio_provider_chain(audio_file, config, mode='credits', duration=90):
    """
    Call audio identification providers in configured order until one succeeds.

    Uses config['audio_provider_chain'] to determine order.
    Default: ["bookdb", "gemini"]
    Available: "bookdb", "gemini", "openrouter", "ollama"

    Note: openrouter and ollama require transcription first (slower).

    FINGERPRINT FAST PATH: Before trying any providers, we attempt fingerprint
    lookup which is instant if the book is already in the database.

    Args:
        audio_file: Path to audio file to analyze
        config: App config
        mode: 'credits' for opening credits, 'identify' for any chapter
        duration: Seconds of audio to analyze

    Returns dict with author, title, etc. or None if all providers fail.
    """
    chain = config.get('audio_provider_chain', ['bookdb', 'gemini'])
    secrets = load_secrets()
    merged_config = {**config, **secrets}

    # ========== FINGERPRINT FAST PATH ==========
    # Try fingerprint lookup FIRST - instant if book is already known
    fingerprint_data = None
    if config.get('enable_fingerprinting', True):  # Enabled by default
        try:
            from library_manager.providers.fingerprint import try_fingerprint_identification, contribute_after_identification
            api_key = merged_config.get('bookdb_api_key')

            logger.info("[AUDIO CHAIN] Trying fingerprint lookup (fast path)...")
            fingerprint_data = try_fingerprint_identification(audio_file, api_key=api_key, duration=120)

            if fingerprint_data and not fingerprint_data.get('_no_match'):
                # Fingerprint matched! Use the result directly
                logger.info(f"[AUDIO CHAIN] FINGERPRINT MATCH: {fingerprint_data.get('author')} - {fingerprint_data.get('title')}")
                fingerprint_data['_source'] = 'fingerprint'
                return fingerprint_data

            if fingerprint_data and fingerprint_data.get('_no_match'):
                logger.debug("[AUDIO CHAIN] No fingerprint match - continuing to providers")
                # Keep fingerprint_data for later contribution

        except ImportError:
            logger.debug("[AUDIO CHAIN] Fingerprint module not available")
        except Exception as e:
            logger.warning(f"[AUDIO CHAIN] Fingerprint lookup error: {e}")
    # ========== END FINGERPRINT FAST PATH ==========

    # Check if we have any viable fallback providers
    has_fallback = False
    for p in chain[1:]:  # Skip first provider
        p = p.lower().strip()
        if p == 'gemini' and merged_config.get('gemini_api_key'):
            has_fallback = True
            break
        elif p == 'openrouter' and merged_config.get('openrouter_api_key'):
            has_fallback = True
            break
        elif p == 'ollama':
            has_fallback = True
            break

    for provider in chain:
        provider = provider.lower().strip()
        logger.info(f"[AUDIO CHAIN] Trying audio provider: {provider}")

        try:
            if provider == 'bookdb':
                # BookDB has GPU Whisper - best option
                # Retry with backoff if connection fails (service might be restarting)
                max_retries = 5 if not has_fallback else 2  # More retries if no fallback
                retry_delay = 10  # Start with 10 seconds

                for attempt in range(max_retries):
                    result = identify_audio_with_bookdb(audio_file)
                    if result and result.get('title'):
                        logger.info(f"[AUDIO CHAIN] Success with bookdb: {result.get('author')} - {result.get('title')}")
                        # Contribute fingerprint - BookDB Whisper ID is different from fingerprint
                        _contribute_fingerprint_async(fingerprint_data, result, merged_config)
                        # Verify/correct narrator using voice matching
                        result = _verify_and_correct_narrator(audio_file, result, merged_config)
                        return result
                    elif result and result.get('transcript'):
                        # Got transcript but no match - still useful, return for potential AI fallback
                        logger.info(f"[AUDIO CHAIN] BookDB returned transcript only")
                        return result
                    elif result is None and attempt < max_retries - 1:
                        # Connection might be down, wait and retry
                        wait_time = retry_delay * (attempt + 1)
                        logger.info(f"[AUDIO CHAIN] BookDB unavailable, waiting {wait_time}s before retry ({attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        break

                if not has_fallback:
                    # No fallback available, keep waiting for BookDB
                    logger.warning("[AUDIO CHAIN] BookDB failed and no fallback configured - will retry on next queue cycle")
                    return None  # Let the queue retry later

            elif provider == 'gemini':
                if not merged_config.get('gemini_api_key'):
                    logger.debug("[AUDIO CHAIN] Skipping gemini - no API key")
                    continue
                # Gemini can directly analyze audio
                result = analyze_audio_with_gemini(audio_file, merged_config, duration=duration, mode=mode)
                if result and (result.get('title') or result.get('author')):
                    logger.info(f"[AUDIO CHAIN] Success with gemini: {result.get('author')} - {result.get('title')}")
                    # Contribute fingerprint for future instant matches
                    _contribute_fingerprint_async(fingerprint_data, result, merged_config)
                    # Verify/correct narrator using voice matching
                    result = _verify_and_correct_narrator(audio_file, result, merged_config)
                    return result

            elif provider == 'openrouter':
                if not merged_config.get('openrouter_api_key'):
                    logger.debug("[AUDIO CHAIN] Skipping openrouter - no API key")
                    continue
                # OpenRouter needs transcription first - try local whisper or skip
                transcript = transcribe_audio_local(audio_file, duration)
                if transcript:
                    result = identify_book_from_transcript(transcript, merged_config)
                    if result and result.get('title'):
                        logger.info(f"[AUDIO CHAIN] Success with openrouter: {result.get('author')} - {result.get('title')}")
                        _contribute_fingerprint_async(fingerprint_data, result, merged_config)
                        result = _verify_and_correct_narrator(audio_file, result, merged_config)
                        return result
                else:
                    logger.debug("[AUDIO CHAIN] openrouter - no transcript available")

            elif provider == 'ollama':
                # Ollama needs transcription first
                transcript = transcribe_audio_local(audio_file, duration)
                if transcript:
                    # Build prompt for book identification
                    prompt = f"""Based on this audiobook transcript excerpt, identify the book.

Transcript:
{transcript[:2000]}

Return JSON with: author, title, narrator (if mentioned), series (if mentioned), confidence (high/medium/low)"""
                    result = call_ollama(prompt, merged_config)
                    if result and result.get('title'):
                        logger.info(f"[AUDIO CHAIN] Success with ollama: {result.get('author')} - {result.get('title')}")
                        _contribute_fingerprint_async(fingerprint_data, result, merged_config)
                        result = _verify_and_correct_narrator(audio_file, result, merged_config)
                        return result
                else:
                    logger.debug("[AUDIO CHAIN] ollama - no transcript available")

            else:
                logger.warning(f"[AUDIO CHAIN] Unknown audio provider: {provider}")

        except Exception as e:
            logger.warning(f"[AUDIO CHAIN] {provider} failed with error: {e}")
            continue

    logger.warning("[AUDIO CHAIN] All audio providers failed")
    return None


def _contribute_fingerprint_async(fingerprint_data, result, config):
    """
    Contribute fingerprint to BookDB after successful identification.
    This runs quickly and doesn't block the main flow.
    """
    if not fingerprint_data or not fingerprint_data.get('_fingerprint'):
        return

    if not result or not result.get('title'):
        return

    try:
        from library_manager.providers.fingerprint import contribute_after_identification
        api_key = config.get('bookdb_api_key')

        success = contribute_after_identification(
            fingerprint_data,
            {
                'author': result.get('author', ''),
                'title': result.get('title', ''),
                'narrator': result.get('narrator', ''),
                'series': result.get('series', ''),
                'series_position': result.get('series_position')
            },
            api_key=api_key
        )

        if success:
            logger.info(f"[FINGERPRINT] Contributed fingerprint for: {result.get('title')}")
        else:
            logger.debug("[FINGERPRINT] Fingerprint contribution skipped or failed")

    except Exception as e:
        logger.debug(f"[FINGERPRINT] Contribution error (non-fatal): {e}")


def _verify_and_correct_narrator(audio_file, result, config):
    """
    Verify narrator using voice matching and correct if mismatch detected.
    Also stores the voice signature for future matching.

    This catches metadata errors where the wrong narrator is tagged.

    Args:
        audio_file: Path to the audio file
        result: Dict with author, title, narrator, etc.
        config: App configuration

    Returns:
        Updated result dict with narrator verification
    """
    # Issue #86: Skip local voice processing when Skaldleita handles audio
    # Skaldleita does voice ID server-side, no need for local pyannote
    from library_manager.config import use_skaldleita_for_audio
    if use_skaldleita_for_audio(config):
        return result

    if not config.get('enable_narrator_verification', True):
        return result

    # ALWAYS store voice signature (even if narrator unknown)
    # This builds the voice database for future matching
    try:
        from library_manager.providers.fingerprint import store_voice_after_identification
        api_key = config.get('bookdb_api_key')
        store_voice_after_identification(audio_file, result, api_key=api_key)
    except Exception as e:
        logger.debug(f"[VOICE] Storage error (non-fatal): {e}")

    tagged_narrator = result.get('narrator', '')
    if not tagged_narrator:
        # No narrator to verify - try to identify by voice alone
        try:
            from library_manager.providers.fingerprint import identify_narrator_by_voice
            api_key = config.get('bookdb_api_key')

            identified = identify_narrator_by_voice(audio_file, threshold=0.6, api_key=api_key)
            if identified:
                logger.info(f"[NARRATOR] Identified by voice: {identified}")
                result['narrator'] = identified
                result['_narrator_source'] = 'voice_id'
        except Exception as e:
            logger.debug(f"[NARRATOR] Voice identification unavailable: {e}")
        return result

    try:
        from library_manager.providers.fingerprint import verify_narrator, contribute_narrator, extract_voice_embedding
        api_key = config.get('bookdb_api_key')

        verification = verify_narrator(audio_file, tagged_narrator, threshold=0.5, api_key=api_key)

        if verification.get('recommendation') == 'correct':
            # Voice matches tagged narrator
            result['_narrator_verified'] = True
            logger.debug(f"[NARRATOR] Verified: {tagged_narrator}")

        elif verification.get('recommendation') == 'mismatch':
            # Voice doesn't match - use voice-matched narrator
            matched = verification.get('matched_narrator', '')
            confidence = verification.get('confidence', 0)

            logger.warning(f"[NARRATOR] MISMATCH: Tagged '{tagged_narrator}' but voice is '{matched}' ({confidence:.2f})")

            # Store the mismatch info for user review
            result['_narrator_mismatch'] = {
                'tagged': tagged_narrator,
                'voice_matched': matched,
                'confidence': confidence
            }

            # Use the voice-matched narrator if confident enough
            if confidence >= 0.7:
                result['narrator'] = matched
                result['_narrator_source'] = 'voice_correction'
                logger.info(f"[NARRATOR] Auto-corrected to: {matched}")
            else:
                # Lower confidence - flag for review but keep original
                result['_narrator_needs_review'] = True

        elif verification.get('recommendation') == 'no_profile':
            # No voice profile exists - the tagged narrator gets contributed
            result['_narrator_contributed'] = True
            logger.debug(f"[NARRATOR] Contributed new profile: {tagged_narrator}")

    except ImportError:
        logger.debug("[NARRATOR] Narrator verification module not available")
    except Exception as e:
        logger.debug(f"[NARRATOR] Verification error (non-fatal): {e}")

    return result


def transcribe_audio_local(audio_file, duration=90):
    """
    Attempt local audio transcription using whisper.cpp or whisper Python.
    Returns transcript text or None.
    """
    import subprocess
    import tempfile

    try:
        # First extract a sample
        sample_path = extract_audio_sample(audio_file, duration_seconds=duration)
        if not sample_path:
            return None

        try:
            # Try whisper.cpp (faster if installed)
            whisper_cpp = shutil.which('whisper.cpp') or shutil.which('whisper-cpp')
            if whisper_cpp:
                result = subprocess.run(
                    [whisper_cpp, '-f', sample_path, '-otxt', '-np'],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode == 0:
                    # Read the output text file
                    txt_path = sample_path + '.txt'
                    if os.path.exists(txt_path):
                        with open(txt_path, 'r') as f:
                            transcript = f.read().strip()
                        os.unlink(txt_path)
                        return transcript

            # Try Python whisper (slower, requires pip install openai-whisper)
            try:
                import whisper
                model = whisper.load_model("base")  # Small model for speed
                result = model.transcribe(sample_path, fp16=False)
                return result.get('text', '')
            except ImportError:
                logger.debug("Local whisper not available - install with: pip install openai-whisper")
                return None

        finally:
            if os.path.exists(sample_path):
                os.unlink(sample_path)

    except Exception as e:
        logger.debug(f"Local transcription failed: {e}")
        return None

def analyze_audio_for_credits(folder_path, config):
    """
    Analyze the FIRST audio file in a folder for opening credits.
    Audiobooks typically announce "Title by Author, read by Narrator"
    in the first 30-45 seconds of the first file.

    Uses the configured audio_provider_chain for fallback support.
    This is optimized for organized audiobook folders.
    """
    first_file = get_first_audio_file(folder_path)
    if not first_file:
        logger.debug(f"No audio files found in {folder_path}")
        return None

    logger.info(f"[AUDIO] Analyzing first file for credits: {os.path.basename(first_file)}")

    # Use the provider chain for audio identification with fallback support
    return call_audio_provider_chain(first_file, config, mode='credits', duration=45)


def analyze_orphan_audio_file(audio_file, config):
    """
    Analyze any audio file to identify what book it belongs to.
    Used for misplaced files or deep organization scans.

    Narrators often announce chapter numbers and sometimes book titles
    at the start of each chapter, not just the first one.

    Uses the configured audio_provider_chain for fallback support.
    """
    logger.info(f"[AUDIO] Analyzing orphan file: {os.path.basename(audio_file)}")
    return call_audio_provider_chain(audio_file, config, mode='identify', duration=45)

# Global whisper model cache
_whisper_model = None
_whisper_model_name = None


def get_whisper_model(model_name=None):
    """Get or create cached faster-whisper model.

    Model sizes and accuracy (WER = Word Error Rate, lower is better):
    - tiny:    ~10% WER, 75MB,  fastest
    - base:    ~7% WER,  150MB, fast
    - small:   ~5% WER,  465MB, moderate
    - medium:  ~4% WER,  1.5GB, slower
    - large-v3: ~3% WER, 3GB,  slowest but most accurate

    For audiobooks with studio quality audio, large-v3 is recommended.
    """
    global _whisper_model, _whisper_model_name

    # Default to large-v3 for best accuracy - audiobooks deserve it
    if model_name is None:
        config = load_config()
        model_name = config.get('whisper_model', 'large-v3')

    if _whisper_model is not None and _whisper_model_name == model_name:
        return _whisper_model

    try:
        from faster_whisper import WhisperModel
        logger.info(f"[WHISPER] Loading faster-whisper model: {model_name}")

        # Check if CUDA is available for faster processing
        # Use ctranslate2's built-in detection (faster-whisper's backend)
        device = "cpu"
        compute_type = "int8"  # Works on both CPU and GPU

        try:
            import ctranslate2
            if ctranslate2.get_cuda_device_count() > 0:
                device = "cuda"
                # int8 works on all CUDA devices including GTX 1080 (compute 6.1)
                # float16 only works on newer GPUs (compute 7.0+)
                logger.info(f"[WHISPER] Using CUDA GPU acceleration (10x faster)")
            else:
                logger.info(f"[WHISPER] Using CPU (no CUDA GPU detected)")
        except ImportError:
            logger.info(f"[WHISPER] Using CPU (ctranslate2 not available)")

        _whisper_model = WhisperModel(model_name, device=device, compute_type=compute_type)
        _whisper_model_name = model_name
        logger.info(f"[WHISPER] Model loaded successfully: {model_name}")
        return _whisper_model
    except ImportError:
        logger.debug("[WHISPER] faster-whisper not installed")
        return None
    except Exception as e:
        logger.warning(f"[WHISPER] Failed to load whisper model: {e}")
        return None


def transcribe_with_whisper(audio_file, config):
    """
    Transcribe audio using local faster-whisper model.
    Returns transcribed text or None on failure.
    """
    model_name = config.get('whisper_model', 'base')  # tiny, base, small, medium, large-v3
    model = get_whisper_model(model_name)

    if not model:
        return None

    try:
        logger.debug(f"[LAYER 4] Transcribing with whisper: {audio_file}")
        segments, info = model.transcribe(audio_file, beam_size=5)

        # Collect all text
        text_parts = []
        for segment in segments:
            text_parts.append(segment.text)

        transcript = ' '.join(text_parts).strip()

        if transcript:
            logger.info(f"[LAYER 4] Whisper transcribed {len(transcript)} chars, language: {info.language}")
            return transcript
        else:
            logger.warning("[LAYER 4] Whisper produced empty transcript")
            return None

    except Exception as e:
        logger.warning(f"[LAYER 4] Whisper transcription failed: {e}")
        return None


def identify_ebook_from_filename(filename, folder_path, config):
    """
    Identify an ebook using ISBN extraction + filename parsing + BookDB search.
    No AI needed - just smart regex and database lookup.

    Flow:
    1. Try ISBN extraction from ebook metadata (EPUB/PDF/MOBI)
    2. If ISBN found, look up via BookDB /api/isbn/{isbn}
    3. Fall back to filename parsing + BookDB search
    """
    # === PHASE 1: ISBN EXTRACTION (Issue #67) ===
    if config.get('enable_isbn_lookup', True) and folder_path:
        try:
            from library_manager.providers.isbn_lookup import identify_ebook_by_isbn
            isbn_result = identify_ebook_by_isbn(folder_path)
            if isbn_result:
                logger.info(f"[EBOOK] ISBN lookup success: {isbn_result.get('author_name')} - {isbn_result.get('title')}")
                return {
                    'author': isbn_result.get('author_name'),
                    'title': isbn_result.get('title'),
                    'series': isbn_result.get('series_name'),
                    'series_num': isbn_result.get('series_position'),
                    'year': isbn_result.get('year_published'),
                    'isbn': isbn_result.get('isbn') or isbn_result.get('isbn13'),
                    'confidence': 'high',
                    'source': 'isbn'
                }
        except ImportError:
            logger.debug("[EBOOK] ISBN lookup not available (ebooklib/pypdf not installed)")
        except Exception as e:
            logger.debug(f"[EBOOK] ISBN extraction failed: {e}")

    # === PHASE 2: FILENAME PARSING ===
    # Clean the filename for parsing
    clean_name = os.path.splitext(filename)[0]
    folder_name = os.path.basename(os.path.dirname(folder_path)) if folder_path else ''

    # Skip garbage filenames that can't be parsed
    if re.match(r'^[a-f0-9]{8,}$', clean_name, re.I):  # Hash filename
        logger.debug(f"[EBOOK] Skipping hash filename: {clean_name}")
        return None
    if re.match(r'^ebook[_\s]*\d+$', clean_name, re.I):  # ebook_1234
        logger.debug(f"[EBOOK] Skipping generic ebook filename: {clean_name}")
        return None

    author = None
    title = None

    # Try common patterns
    # Pattern 1: "Author - Title" or "Author_-_Title"
    match = re.match(r'^(.+?)\s*[-_]+\s*(.+)$', clean_name)
    if match:
        part1, part2 = match.groups()
        # Guess which is author vs title (authors usually shorter, titles have more words)
        if len(part1.split()) <= 3 and not re.search(r'\d{4}', part1):
            author, title = part1.strip(), part2.strip()
        else:
            title, author = part1.strip(), part2.strip()

    # Pattern 2: Use folder name as author if it looks like a name
    if not author and folder_name:
        if re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+)+$', folder_name):  # "First Last" format
            author = folder_name
            title = clean_name

    # Pattern 3: Just use the clean filename as title
    if not title:
        title = clean_name

    if not title:
        return None

    # Search BookDB for verification
    try:
        search_query = f"{author} {title}" if author else title
        logger.debug(f"[EBOOK] Searching BookDB for: {search_query}")

        resp = requests.get(
            f"{BOOKDB_API_URL}/search",
            params={'q': search_query[:100]},  # Limit query length
            timeout=10
        )

        if resp.status_code == 200:
            results = resp.json()
            if results and len(results) > 0:
                best = results[0]
                # Check if it's a reasonable match
                result_title = best.get('name', '')
                result_author = best.get('author_name', '')

                if result_title:
                    logger.info(f"[EBOOK] BookDB found: {result_author} - {result_title}")
                    return {
                        'author': result_author or author,
                        'title': result_title,
                        'series': best.get('series_name'),
                        'series_num': best.get('series_position'),
                        'confidence': 'high' if result_author else 'medium',
                        'source': 'bookdb'
                    }
    except Exception as e:
        logger.debug(f"[EBOOK] BookDB search error: {e}")

    # Return our best guess without BookDB confirmation
    if author and title:
        return {
            'author': author,
            'title': title,
            'confidence': 'low',
            'source': 'filename'
        }

    return None


def transcribe_and_identify_content(audio_file, config):
    """
    Layer 4: Content-based identification.

    Primary: Gemini Audio API (transcription + identification in one call)
    Fallback: faster-whisper (local transcription) + OpenRouter (identification)

    This catches books that have no intro credits (e.g., Part 2 of multi-part files).
    """
    import base64

    # Extract sample from middle of the book
    logger.debug(f"[LAYER 4] Extracting middle sample from: {audio_file}")
    sample_path = extract_audio_sample_from_middle(audio_file, duration_seconds=60)
    if not sample_path:
        logger.warning(f"[LAYER 4] Could not extract middle sample from {audio_file}")
        return None

    logger.debug(f"[LAYER 4] Extracted sample to: {sample_path}")
    result = None

    try:
        # === ATTEMPT 1: Gemini Audio API ===
        gemini_key = config.get('gemini_api_key')
        if gemini_key:
            result = _try_gemini_content_identification(sample_path, gemini_key)

        # === ATTEMPT 2: Whisper + OpenRouter fallback ===
        if result is None:
            openrouter_key = config.get('openrouter_api_key')
            if openrouter_key:
                logger.info("[LAYER 4] Trying fallback: faster-whisper + OpenRouter")
                transcript = transcribe_with_whisper(sample_path, config)
                if transcript:
                    result = identify_book_from_transcript(transcript, config)
                else:
                    logger.warning("[LAYER 4] Whisper transcription failed, no fallback available")

    finally:
        # Clean up temp file
        if sample_path and os.path.exists(sample_path):
            try:
                os.unlink(sample_path)
            except:
                pass

    return result


def _try_gemini_content_identification(sample_path, api_key):
    """Try Gemini Audio API for content identification. Returns result or None.

    Wrapper that passes app-level dependencies to the extracted module.
    """
    return _try_gemini_content_identification_raw(
        sample_path=sample_path,
        api_key=api_key,
        parse_json_response_fn=parse_json_response
    )


def analyze_audio_for_content(folder_path, config):
    """
    Layer 4: Analyze audio CONTENT to identify the book.

    Unlike Layer 3 (credits), this extracts text from the actual story
    and uses AI to identify the book from plot, characters, and style.

    Best for:
    - Multi-part files without intros (Part 2, Part 3, etc.)
    - Books with music-only intros
    - Files with corrupted/cut credits
    """
    first_file = get_first_audio_file(folder_path)
    if not first_file:
        return None

    logger.info(f"[LAYER 4] Analyzing story content: {os.path.basename(first_file)}")
    return transcribe_and_identify_content(first_file, config)

def analyze_audio_with_gemini(audio_file, config, duration=90, mode='credits'):
    """
    Send audio sample to Gemini for analysis.

    Wrapper that passes app-level dependencies to the extracted module.

    Modes:
    - 'credits': Optimized for first file with opening credits (title/author/narrator announcement)
    - 'identify': For any chapter file - extracts chapter info and any identifying details

    Args:
        audio_file: Path to the audio file
        config: App config with Gemini API key
        duration: Seconds of audio to analyze (default 90, use 45 for credits)
        mode: 'credits' or 'identify'

    Returns dict with extracted info or None on failure.
    """
    return _analyze_audio_with_gemini_raw(
        audio_file=audio_file,
        config=config,
        duration=duration,
        mode=mode,
        extract_audio_sample_fn=extract_audio_sample,
        parse_json_response_fn=parse_json_response
    )


def detect_audio_language(audio_file, config):
    """
    Detect the spoken language from an audio file using Gemini.
    This is a lightweight version of analyze_audio_with_gemini focused only on language.

    Wrapper that passes app-level dependencies to the extracted module.

    Args:
        audio_file: Path to audio file
        config: App config with Gemini API key

    Returns:
        dict with 'language' (ISO 639-1 code), 'language_name', and 'confidence', or None
    """
    return _detect_audio_language_raw(
        audio_file=audio_file,
        config=config,
        extract_audio_sample_fn=extract_audio_sample,
        parse_json_response_fn=parse_json_response
    )


def get_audio_metadata_hints(book_path, config=None):
    """
    Get metadata hints from audio files in a book folder.
    Combines ID3 tags + audio analysis for verification.
    Returns dict with all found hints.
    """
    hints = {}
    audio_files = find_audio_files(str(book_path))

    if not audio_files:
        return hints

    # Sort to get first file (usually has intro)
    audio_files.sort()
    first_file = audio_files[0]

    # Try ID3/metadata extraction first (fast)
    try:
        from mutagen import File as MutagenFile
        audio = MutagenFile(first_file, easy=True)
        if audio:
            if audio.get('artist'):
                hints['id3_author'] = audio.get('artist')[0]
            if audio.get('album'):
                hints['id3_album'] = audio.get('album')[0]
            if audio.get('title'):
                hints['id3_title'] = audio.get('title')[0]
    except ImportError:
        logger.debug("mutagen not installed - skipping ID3 extraction")
    except Exception as e:
        logger.debug(f"ID3 extraction failed: {e}")

    # Audio analysis with Gemini (if enabled and configured)
    if config and config.get('enable_audio_analysis', False) and config.get('gemini_api_key'):
        audio_info = analyze_audio_with_gemini(first_file, config)
        if audio_info:
            if audio_info.get('title'):
                hints['audio_title'] = audio_info['title']
            if audio_info.get('author'):
                hints['audio_author'] = audio_info['author']
            if audio_info.get('narrator'):
                hints['audio_narrator'] = audio_info['narrator']
            if audio_info.get('series'):
                hints['audio_series'] = audio_info['series']
            hints['audio_confidence'] = audio_info.get('confidence', 'unknown')

    return hints


# ============== DEEP SCANNER ==============

import re
import hashlib

def detect_media_type(path):
    """Issue #53: Detect what media types exist in a book folder.

    Returns: 'audiobook', 'ebook', or 'both'
    """
    path = Path(path) if isinstance(path, str) else path

    if not path.exists():
        return 'audiobook'  # Default for missing paths

    has_audio = False
    has_ebook = False

    # Check if it's a file or directory
    if path.is_file():
        ext = path.suffix.lower()
        if ext in AUDIO_EXTENSIONS:
            has_audio = True
        elif ext in EBOOK_EXTENSIONS:
            has_ebook = True
    else:
        # It's a directory - scan for files
        try:
            for item in path.rglob('*'):
                if item.is_file():
                    ext = item.suffix.lower()
                    if ext in AUDIO_EXTENSIONS:
                        has_audio = True
                    elif ext in EBOOK_EXTENSIONS:
                        has_ebook = True

                    # Early exit if we found both
                    if has_audio and has_ebook:
                        break
        except PermissionError:
            pass

    if has_audio and has_ebook:
        return 'both'
    elif has_ebook:
        return 'ebook'
    else:
        return 'audiobook'


# Patterns for disc/chapter folders (these are NOT book titles)
DISC_CHAPTER_PATTERNS = [
    r'^(disc|disk|cd|part|chapter|ch)\s*\d+',  # "Disc 1", "Part 2", "Chapter 3"
    r'^\d+\s*[-_]\s*(disc|disk|cd|part|chapter)',  # "1 - Disc", "01_Chapter"
    r'^(side)\s*[ab12]',  # "Side A", "Side 1"
    r'.+\s*-\s*(disc|disk|cd)\s*\d+$',  # "Book Name - Disc 01"
]

# Junk patterns to clean from titles (Issue #64: expanded for torrent naming conventions)
JUNK_PATTERNS = [
    # Torrent site markers
    r'\[bitsearch\.to\]',
    r'\[rarbg\]',
    r'\[EN\]',
    r'\[\d+\]',  # [64420]
    # Audio format markers
    r'\(unabridged\)',
    r'\(abridged\)',
    r'\(audiobook\)',
    r'\(audio\)',
    r'\(graphicaudio\)',
    r'\(uk version\)',
    r'\(us version\)',
    r'\(uk\)',
    r'\(us\)',
    r'\(multi\)',  # multi-file indicator
    r'\(r\d+\.\d+\)',  # (r1.0), (r1.1) - revision markers
    r'\([A-Z]\)',  # (V), (A), etc. - single letter version markers
    # Bitrates and encoding info - expanded (Issue #79: encoding info slipping through)
    r'\b\d{2,3}k\b',  # 62k, 64k, 128k, 192k, 320k
    r'\b\d{2,3}\s*kbps\b',  # 320 kbps, 192kbps
    r'\bmp3\s*\d+\s*kbps\b',  # MP3 320kbps
    r'\bmp3\s*\d+k\b',  # MP3 320k
    r'\bm4b\s*\d+\s*k(?:bps)?\b',  # M4B 64k, M4B 64kbps
    r'\baac\s*\d+\s*k(?:bps)?\b',  # AAC 256k
    r'\bmp3\b(?!\s+\w)',  # Standalone MP3 (but not "MP3 Player" etc)
    r'\bm4b\b(?!\s+\w)',  # Standalone M4B
    r'\bflac\b(?!\s+\w)',  # Standalone FLAC
    r'\baac\b(?!\s+\w)',  # Standalone AAC
    r'\bvbr\b',  # VBR (variable bitrate)
    r'\bcbr\b',  # CBR (constant bitrate)
    # Duration timestamps: 01.10.42, 23.35.16, 69.35.47
    r'\b\d{1,2}\.\d{2}\.\d{2}\b',
    # File sizes
    r'\{mb\}',  # {mb} without number
    r'\{\d+mb\}',  # {388mb}
    r'\{\d+\.\d+gb\}',  # {1.29gb}
    # Version numbers
    r'\bv\d+\b',  # v01, v02
    # Editor/narrator names in brackets: [Dozois,Strahan], [Cramer,Hartwell]
    r'\[[A-Z][a-z]+(?:,\s*[A-Z][a-z]+)+\]',
    # Narrator names in parentheses at end: (Thorne), (Bennett), (Linton)
    r'\s+\([A-Z][a-z]+\)\s*$',
    # File extensions in folder names
    r'\.epub$|\.pdf$|\.mobi$',
]

# Patterns that indicate author name in title
AUTHOR_IN_TITLE_PATTERNS = [
    r'\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$',  # "Title by Author Name"
    r'^([A-Z][a-z]+,\s+[A-Z][a-z]+)\s*-\s*',  # "LastName, FirstName - Title"
    r'\s+-\s+([A-Z][a-z]+\s+[A-Z][a-z]+)\s*$',  # "Title - Author Name"
]


# ============== ORPHAN FILE HANDLING ==============

def read_audio_metadata(file_path):
    """Read ID3/metadata tags from an audio file to identify the book."""
    try:
        from mutagen import File
        from mutagen.easyid3 import EasyID3
        from mutagen.mp3 import MP3
        from mutagen.mp4 import MP4

        audio = File(file_path, easy=True)
        if audio is None:
            return None

        metadata = {}

        # Try to get album (usually the book title for audiobooks)
        if 'album' in audio:
            metadata['album'] = audio['album'][0] if isinstance(audio['album'], list) else audio['album']

        # Try to get artist (sometimes narrator, sometimes author)
        if 'artist' in audio:
            metadata['artist'] = audio['artist'][0] if isinstance(audio['artist'], list) else audio['artist']

        # Try to get album artist (often the author)
        if 'albumartist' in audio:
            metadata['albumartist'] = audio['albumartist'][0] if isinstance(audio['albumartist'], list) else audio['albumartist']

        # Try to get title (track title)
        if 'title' in audio:
            metadata['title'] = audio['title'][0] if isinstance(audio['title'], list) else audio['title']

        return metadata if metadata else None
    except Exception as e:
        logger.debug(f"Could not read metadata from {file_path}: {e}")
        return None


def find_orphan_audio_files(lib_path, config=None):
    """Find audio files sitting directly in author folders (not in book subfolders)."""
    orphans = []

    # Issue #57: Get watch folder to exclude from orphan scanning
    # Watch folder has its own processing flow and shouldn't be treated as an author folder
    watch_folder = None
    if config:
        watch_folder = config.get('watch_folder', '').strip()
    else:
        # Fallback: load config if not provided
        try:
            cfg = load_config()
            watch_folder = cfg.get('watch_folder', '').strip()
        except Exception:
            pass

    for author_dir in Path(lib_path).iterdir():
        if not author_dir.is_dir():
            continue

        # Issue #57: Skip watch folder - it has its own processing flow
        if watch_folder:
            try:
                if author_dir.resolve() == Path(watch_folder).resolve():
                    logger.debug(f"Skipping watch folder in orphan scan: {author_dir}")
                    continue
            except Exception:
                pass

        author = author_dir.name

        # Find audio files directly in author folder
        direct_audio = [f for f in author_dir.iterdir()
                       if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]

        if direct_audio:
            # Group files by potential book (using metadata or filename patterns)
            books = {}

            for audio_file in direct_audio:
                # Try to read metadata
                metadata = read_audio_metadata(str(audio_file))

                if metadata and metadata.get('album'):
                    book_title = metadata['album']
                else:
                    # Fallback: try to extract from filename
                    # Pattern: "Book Title - Chapter 01.mp3" or "01 - Chapter Name.mp3"
                    fname = audio_file.stem
                    # Remove chapter/track numbers
                    book_title = re.sub(r'^\d+[\s\-\.]+', '', fname)
                    book_title = re.sub(r'[\s\-]+\d+$', '', book_title)
                    book_title = re.sub(r'\s*-\s*(chapter|part|track|disc)\s*\d*.*$', '', book_title, flags=re.IGNORECASE)

                    if not book_title or book_title == fname:
                        book_title = "Unknown Album"

                if book_title not in books:
                    books[book_title] = []
                books[book_title].append(audio_file)

            for book_title, files in books.items():
                orphans.append({
                    'author': author,
                    'author_path': str(author_dir),
                    'detected_title': book_title,
                    'files': [str(f) for f in files],
                    'file_count': len(files)
                })

    return orphans


def organize_orphan_files(author_path, book_title, files, config=None):
    """Create a book folder and move orphan files into it, including companion files.

    Issue #57: If files are within a watch folder context and watch_output_folder is configured,
    the organized files will be placed in watch_output_folder/Author/Title instead of in-place.
    """
    import shutil

    author_dir = Path(author_path)
    author_name = author_dir.name  # Extract author name for potential output folder path

    # Issue #57: Check if we should use watch_output_folder instead of organizing in-place
    # This applies when the orphan files are within the watch folder area
    destination_base = author_dir  # Default: organize in place (current author folder)

    if config:
        watch_folder = config.get('watch_folder', '').strip()
        watch_output_folder = config.get('watch_output_folder', '').strip()

        if watch_folder and watch_output_folder:
            try:
                watch_path = Path(watch_folder).resolve()
                author_path_resolved = author_dir.resolve()

                # Check if author_path is within the watch folder tree
                try:
                    author_path_resolved.relative_to(watch_path)
                    # Files are in watch folder context - use output folder
                    destination_base = Path(watch_output_folder) / author_name
                    logger.info(f"Orphan organization: Using watch output folder: {destination_base}")
                except ValueError:
                    # Not in watch folder - organize in place (default)
                    pass
            except Exception as e:
                logger.debug(f"Orphan organization: Path check failed, organizing in place: {e}")

    # Clean up the book title for folder name
    clean_title = book_title

    # Remove format/quality junk from title
    clean_title = re.sub(r'\s*\((?:Unabridged|Abridged|MP3|M4B|64k|128k|HQ|Complete|Full|Retail)\)', '', clean_title, flags=re.IGNORECASE)
    clean_title = re.sub(r'\s*\[.*?\]', '', clean_title)  # Remove bracketed content
    clean_title = re.sub(r'[<>:"/\\|?*]', '', clean_title)  # Remove illegal chars
    clean_title = clean_title.strip()

    if not clean_title:
        return False, "Could not determine book title"

    # Issue #57: Use destination_base (could be author_dir or watch_output_folder/author)
    book_dir = destination_base / clean_title

    # Check if folder already exists
    if book_dir.exists():
        # Check if it's empty or has files
        existing = list(book_dir.iterdir())
        if existing:
            return False, f"Folder already exists with {len(existing)} items: {book_dir}"
    else:
        book_dir.mkdir(parents=True)

    # Companion file extensions to also move (covers, metadata, etc.)
    COMPANION_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp',  # covers
                           '.nfo', '.txt', '.json', '.xml', '.cue',   # metadata
                           '.pdf', '.epub', '.mobi',                   # companion ebooks
                           '.srt', '.vtt', '.lrc'}                     # subtitles/lyrics

    # Find companion files in the same directory as the audio files
    companion_files = set()
    source_dirs = set()
    for file_path in files:
        src = Path(file_path)
        if src.exists():
            source_dirs.add(src.parent)

    for source_dir in source_dirs:
        for f in source_dir.iterdir():
            if f.is_file() and f.suffix.lower() in COMPANION_EXTENSIONS:
                companion_files.add(str(f))

    # Combine audio files and companion files
    all_files = list(files) + list(companion_files)

    # Move files
    moved = 0
    errors = []
    for file_path in all_files:
        try:
            src = Path(file_path)
            if src.exists():
                dest = book_dir / src.name
                shutil.move(str(src), str(dest))
                moved += 1
        except Exception as e:
            errors.append(f"{file_path}: {e}")

    # Clean up empty source directories
    for source_dir in source_dirs:
        try:
            if source_dir.exists() and source_dir != author_dir:
                remaining = list(source_dir.iterdir())
                if not remaining:
                    source_dir.rmdir()
                    logger.info(f"Cleaned up empty folder: {source_dir}")
        except OSError:
            pass

    if errors:
        return False, f"Moved {moved} files, {len(errors)} errors: {errors[0]}"

    logger.info(f"Organized {moved} files (audio + companions) into: {book_dir}")
    return True, f"Created {book_dir.name} with {moved} files"


# ============== CHAOS HANDLER - For completely unorganized libraries ==============

def read_audio_metadata_deep(file_path):
    """
    Read all available metadata from an audio file.
    Returns dict with: album, artist, title, track, duration, etc.
    """
    try:
        from mutagen import File
        from mutagen.mp4 import MP4
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3

        audio = File(file_path)
        if audio is None:
            return None

        metadata = {
            'album': None,
            'artist': None,
            'title': None,
            'track': None,
            'duration': None,
            'bitrate': None
        }

        # Get duration
        if hasattr(audio, 'info') and hasattr(audio.info, 'length'):
            metadata['duration'] = audio.info.length
        if hasattr(audio, 'info') and hasattr(audio.info, 'bitrate'):
            metadata['bitrate'] = audio.info.bitrate

        # Handle different formats
        if isinstance(audio, MP4):
            # M4A/M4B files
            metadata['album'] = audio.tags.get('\xa9alb', [None])[0] if audio.tags else None
            metadata['artist'] = audio.tags.get('\xa9ART', [None])[0] if audio.tags else None
            metadata['title'] = audio.tags.get('\xa9nam', [None])[0] if audio.tags else None
        elif isinstance(audio, MP3) and audio.tags:
            # MP3 files with ID3 tags - use raw tag names
            tags = audio.tags
            # ID3v2 tag names: TALB=album, TPE1=artist, TIT2=title, TRCK=track
            if tags.get('TALB'):
                metadata['album'] = str(tags['TALB'].text[0])
            if tags.get('TPE1'):
                metadata['artist'] = str(tags['TPE1'].text[0])
            if tags.get('TIT2'):
                metadata['title'] = str(tags['TIT2'].text[0])
            if tags.get('TRCK'):
                metadata['track'] = str(tags['TRCK'].text[0])
        elif hasattr(audio, 'tags') and audio.tags:
            # Other formats - try EasyID3 style
            tags = audio.tags
            if hasattr(tags, 'get'):
                metadata['album'] = str(tags.get('album', [None])[0]) if tags.get('album') else None
                metadata['artist'] = str(tags.get('artist', [None])[0]) if tags.get('artist') else None
                metadata['title'] = str(tags.get('title', [None])[0]) if tags.get('title') else None
                metadata['track'] = str(tags.get('tracknumber', [None])[0]) if tags.get('tracknumber') else None

        return metadata
    except Exception as e:
        logger.debug(f"Could not read metadata from {file_path}: {e}")
        return None


def group_loose_files(files):
    """
    Intelligently group loose files that likely belong to the same book.
    Groups by: metadata album > filename pattern > file characteristics

    Returns: list of groups, each group is dict with:
        - files: list of file paths
        - group_type: how they were grouped (metadata, pattern, proximity)
        - detected_info: any detected book info
    """
    groups = []
    ungrouped = list(files)

    # === PHASE 1: Group by ID3 album tag ===
    album_groups = {}
    no_album = []

    for f in ungrouped:
        meta = read_audio_metadata_deep(str(f))
        if meta and meta.get('album') and meta['album'].lower() not in ['unknown', 'audiobook', 'untitled']:
            album = meta['album']
            if album not in album_groups:
                album_groups[album] = {
                    'files': [],
                    'artist': meta.get('artist'),
                    'total_duration': 0
                }
            album_groups[album]['files'].append(f)
            if meta.get('duration'):
                album_groups[album]['total_duration'] += meta['duration']
        else:
            no_album.append(f)

    # Add album groups
    for album, data in album_groups.items():
        groups.append({
            'files': data['files'],
            'group_type': 'metadata',
            'detected_info': {
                'title': album,
                'author': data.get('artist'),
                'duration_hours': round(data['total_duration'] / 3600, 1) if data['total_duration'] else None
            }
        })

    ungrouped = no_album

    # === PHASE 2: Group by filename pattern ===
    # Look for: chapter01, part1, disc1, track01, 01, etc.
    pattern_groups = {}
    still_ungrouped = []

    for f in ungrouped:
        fname = f.stem.lower()

        # Extract base name (remove numbers and common suffixes)
        base = re.sub(r'[\s_-]*(chapter|part|track|disc|cd|side)[\s_-]*\d+.*$', '', fname, flags=re.IGNORECASE)
        base = re.sub(r'[\s_-]*\d+[\s_-]*$', '', base)  # Remove trailing numbers
        base = re.sub(r'^\d+[\s_-]*', '', base)  # Remove leading numbers
        base = base.strip(' _-')

        if base and len(base) > 2:
            if base not in pattern_groups:
                pattern_groups[base] = []
            pattern_groups[base].append(f)
        else:
            still_ungrouped.append(f)

    # Add pattern groups (only if more than 1 file - indicates a set)
    for base, file_list in pattern_groups.items():
        if len(file_list) > 1:
            # Calculate total duration
            total_dur = 0
            for f in file_list:
                meta = read_audio_metadata_deep(str(f))
                if meta and meta.get('duration'):
                    total_dur += meta['duration']

            groups.append({
                'files': file_list,
                'group_type': 'pattern',
                'detected_info': {
                    'title': base.replace('_', ' ').replace('-', ' ').title() if base else None,
                    'author': None,
                    'duration_hours': round(total_dur / 3600, 1) if total_dur else None
                }
            })
        else:
            still_ungrouped.extend(file_list)

    ungrouped = still_ungrouped

    # === PHASE 3: Group numbered sequences ===
    # Files like 01.mp3, 02.mp3, 03.mp3 that share same characteristics
    numbered_files = []
    truly_ungrouped = []

    for f in ungrouped:
        fname = f.stem
        # Check if filename is just a number or very short
        if re.match(r'^\d{1,3}$', fname) or len(fname) <= 3:
            numbered_files.append(f)
        else:
            truly_ungrouped.append(f)

    if numbered_files:
        # Sort by name to keep sequence
        numbered_files.sort(key=lambda x: x.stem)
        total_dur = sum(
            (read_audio_metadata_deep(str(f)) or {}).get('duration', 0)
            for f in numbered_files
        )
        groups.append({
            'files': numbered_files,
            'group_type': 'sequence',
            'detected_info': {
                'title': None,  # Unknown - needs identification
                'author': None,
                'duration_hours': round(total_dur / 3600, 1) if total_dur else None,
                'needs_identification': True
            }
        })

    # === PHASE 4: Individual files that couldn't be grouped ===
    for f in truly_ungrouped:
        meta = read_audio_metadata_deep(str(f))
        groups.append({
            'files': [f],
            'group_type': 'single',
            'detected_info': {
                'title': meta.get('title') or f.stem if meta else f.stem,
                'author': meta.get('artist') if meta else None,
                'duration_hours': round(meta['duration'] / 3600, 1) if meta and meta.get('duration') else None
            }
        })

    return groups


def search_bookdb_api(title, author=None, retry_count=0):
    """
    Search the BookBucket API for a book (public endpoint, no auth needed).
    Uses Qdrant vector search - fast even with 50M books.
    Returns dict with author, title, series if found.
    Filters garbage matches using title similarity.
    If author is provided, uses it to validate/preserve existing author.
    """
    # Clean the search title (remove "audiobook", file extensions, etc.)
    search_title = clean_search_title(title)
    if not search_title or len(search_title) < 3:
        return None

    # Skip unsearchable queries (chapter1, track05, etc.)
    if is_unsearchable_query(search_title):
        logger.debug(f"BookDB API: Skipping unsearchable query '{search_title}'")
        return None

    rate_limit_wait('bookdb')  # 3.6s delay = max 1000/hr, never skips

    try:
        # Use longer timeout for cold start (embedding model can take 45-60s to load)
        # Retry once on timeout
        for attempt in range(2):
            try:
                response = requests.get(
                    f"{BOOKDB_API_URL}/search",
                    params={"q": search_title, "limit": 5},
                    timeout=60 if attempt == 0 else 30
                )
                break
            except requests.exceptions.Timeout:
                if attempt == 0:
                    logger.debug(f"BookDB API timeout on first attempt, retrying...")
                    continue
                raise

        # Handle rate limiting - respect Retry-After header from server
        if response.status_code == 429 and retry_count < 3:
            retry_after = response.headers.get('Retry-After', '60')
            try:
                wait_time = min(int(retry_after), 300)  # Cap at 5 minutes
            except ValueError:
                wait_time = 60 * (retry_count + 1)  # Fallback: 60s, 120s, 180s
            logger.info(f"BookDB API rate limited, waiting {wait_time}s (Retry-After: {retry_after})...")
            time.sleep(wait_time)
            return search_bookdb_api(title, retry_count + 1)

        if response.status_code == 200:
            results = response.json()

            # Find best book match (prefer books over series)
            for item in results:
                if item.get('type') == 'book' and item.get('author_name'):
                    suggested_title = item.get('name', '')

                    # Filter garbage matches - reject low similarity
                    if is_garbage_match(search_title, suggested_title):
                        logger.debug(f"BookDB API: Rejected garbage match '{search_title}' -> '{suggested_title}'")
                        continue

                    result_author = item.get('author_name', '')

                    # TRUST EXISTING AUTHORS: If we have a valid (non-placeholder) author,
                    # keep it even if API returns a different author. Same title can exist
                    # by different authors - "The Destroyer of Worlds" by Matt Ruff is different
                    # from "The Destroyer of Worlds" by someone else.
                    if author and result_author and not is_placeholder_author(author):
                        author_is_drastic = is_drastic_author_change(author, result_author)
                        if author_is_drastic:
                            # Found title match, but keep ORIGINAL author (trust folder structure)
                            logger.debug(f"BookDB API: Title match found but keeping original author '{author}' (API had '{result_author}')")
                            # Don't change author - use original
                        else:
                            # Authors are similar (same person, different format) - use API version
                            author = result_author
                    else:
                        # No original author or it's a placeholder - use API author
                        author = result_author
                    # Fix author format (some have "Last, First")
                    if ',' in author and author.count(',') == 1:
                        parts = author.split(',')
                        author = f"{parts[1].strip()} {parts[0].strip()}"

                    return {
                        'title': suggested_title,
                        'author': author,
                        'series': item.get('series_name'),
                        'year': item.get('year_published'),
                        'source': 'bookdb_api'
                    }

            # Fallback to series if no book match
            for item in results:
                if item.get('type') == 'series' and item.get('author_name'):
                    suggested_title = item.get('name', '')

                    # Filter garbage matches
                    if is_garbage_match(search_title, suggested_title):
                        continue

                    result_author = item.get('author_name', '')

                    # TRUST EXISTING AUTHORS for series too
                    if author and result_author and not is_placeholder_author(author):
                        author_is_drastic = is_drastic_author_change(author, result_author)
                        if author_is_drastic:
                            logger.debug(f"BookDB API: Series match but keeping original author '{author}' (API had '{result_author}')")
                            # Don't change author
                        else:
                            author = result_author
                    else:
                        author = result_author
                    if ',' in author and author.count(',') == 1:
                        parts = author.split(',')
                        author = f"{parts[1].strip()} {parts[0].strip()}"

                    return {
                        'title': suggested_title,
                        'author': author,
                        'series': item.get('name'),
                        'source': 'bookdb_api'
                    }

    except Exception as e:
        logger.debug(f"BookDB API search error: {e}")

    return None


def search_book_searxng(query, duration_hours=None):
    """
    Search for a book using SearXNG.
    Optionally filter by expected audiobook length.
    """
    try:
        # Add audiobook context to query
        search_query = f"{query} audiobook"
        if duration_hours and duration_hours > 1:
            search_query += f" {int(duration_hours)} hours"

        # Use local SearXNG instance
        response = requests.get(
            "http://localhost:8888/search",
            params={
                'q': search_query,
                'format': 'json',
                'categories': 'general'
            },
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])[:5]

            # Extract book info from results
            books = []
            for r in results:
                title = r.get('title', '')
                content = r.get('content', '')

                # Look for author patterns
                author_match = re.search(r'by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', title + ' ' + content)

                books.append({
                    'title': title,
                    'snippet': content[:200],
                    'url': r.get('url'),
                    'author': author_match.group(1) if author_match else None
                })

            return books
    except Exception as e:
        logger.debug(f"SearXNG search failed: {e}")

    return []


def calculate_input_quality(folder_name, filenames, info):
    """
    Score the quality of input data for AI identification.
    Returns a score 0-100 and list of usable clues found.

    Low quality inputs (random numbers, 'unknown', no words) should not be
    trusted to AI as it will hallucinate famous books.
    """
    score = 0
    clues = []

    # Check folder name for useful info
    folder_clean = re.sub(r'[_\-\d\.\[\]\(\)]', ' ', folder_name or '').strip()
    words = [w for w in folder_clean.split() if len(w) > 2 and w.lower() not in ('unknown', 'audiobook', 'audio', 'book', 'mp3', 'the', 'and', 'part')]

    if words:
        score += min(40, len(words) * 10)  # Up to 40 points for meaningful words
        clues.append(f"folder_words: {words[:5]}")

    # Check for author-title pattern (e.g., "Author - Title")
    if ' - ' in (folder_name or ''):
        score += 20
        clues.append("has_author_title_separator")

    # Check metadata tags
    if info.get('title') and info.get('title') not in ('none', 'Unknown', ''):
        score += 25
        clues.append(f"has_title_tag: {info.get('title')[:30]}")
    if info.get('author') and info.get('author') not in ('none', 'Unknown', ''):
        score += 25
        clues.append(f"has_author_tag: {info.get('author')[:30]}")

    # Check filenames for book/chapter info
    meaningful_files = [f for f in filenames[:5] if re.search(r'[a-zA-Z]{4,}', f)]
    if meaningful_files:
        score += 10
        clues.append(f"meaningful_filenames: {len(meaningful_files)}")

    # Penalize garbage inputs
    if re.match(r'^(unknown|audiobook|audio|book)?[\s_\-]*\d+$', folder_name or '', re.IGNORECASE):
        score = max(0, score - 50)  # Heavy penalty for "unknown_123" type names
        clues.append("PENALTY: numeric_garbage_name")

    return min(100, score), clues


def validate_ai_result(ai_result, folder_name, info):
    """
    Validate AI result against input to detect hallucinations.
    Returns (is_valid, adjusted_confidence, reason)

    Key insight: AI correcting the AUTHOR is valid (folder may have wrong author).
    Hallucination = AI invents a completely DIFFERENT BOOK (wrong title).
    """
    if not ai_result or not ai_result.get('title'):
        return False, 'none', 'no_result'

    ai_title = (ai_result.get('title') or '').lower()
    ai_author = (ai_result.get('author') or '').lower()

    from difflib import SequenceMatcher

    # Extract potential title from folder (after author separator if present)
    folder_title = folder_name
    if ' - ' in folder_name:
        parts = folder_name.split(' - ', 1)
        folder_title = parts[1] if len(parts) > 1 else parts[0]
    folder_title = re.sub(r'[\[\(].*?[\]\)]', '', folder_title).strip().lower()

    # Title similarity - primary check
    title_similarity = SequenceMatcher(None, ai_title, folder_title).ratio()

    # Also check title words (handles translations like "Le Petit Prince" vs "The Little Prince")
    ai_title_words = set(re.findall(r'\b[a-zA-Z]{3,}\b', ai_title))
    folder_title_words = set(re.findall(r'\b[a-zA-Z]{3,}\b', folder_title))
    title_word_overlap = len(ai_title_words & folder_title_words)

    # Check if key title words match (excluding common words)
    common_words = {'the', 'and', 'book', 'part', 'volume', 'novel'}
    meaningful_ai_words = ai_title_words - common_words
    meaningful_folder_words = folder_title_words - common_words
    meaningful_overlap = len(meaningful_ai_words & meaningful_folder_words)

    reported_confidence = ai_result.get('confidence', 'medium')

    # Validation rules - focus on TITLE, not author (author correction is valid!)

    # If title has good similarity OR meaningful words overlap, it's probably the same book
    title_matches = (
        title_similarity >= 0.4 or  # Direct similarity
        title_word_overlap >= 2 or  # Multiple words match
        meaningful_overlap >= 1      # At least one meaningful word matches
    )

    if title_matches:
        # Title matches - AI may be correcting the author, which is valid
        # BUT: Check for author hallucination on generic titles

        # Detect if this is a "generic title with no author" case
        # If folder has no author separator and title is short/generic, be suspicious
        has_author_in_input = ' - ' in folder_name or ' / ' in folder_name
        is_short_title = len(folder_title.split()) <= 3

        # Generic title patterns that could match many books
        generic_patterns = ['game', 'hunt', 'prey', 'gone', 'home', 'storm', 'dark', 'night', 'day', 'fire', 'ice', 'blood', 'death', 'life', 'love', 'war', 'peace']
        is_generic_title = is_short_title and any(word in folder_title for word in generic_patterns)

        # If no author in input and generic title, require higher confidence
        if not has_author_in_input and is_generic_title and ai_author:
            logger.warning(f"Generic title '{folder_title}' with no input author -> AI suggested '{ai_author}'. Flagging as low confidence.")
            # Don't reject, but downgrade confidence significantly
            return True, 'low', f'generic_title_author_uncertain (input had no author)'

        if title_similarity >= 0.7:
            return True, reported_confidence, 'validated'
        else:
            # Lower title similarity but words match - likely translation or variant
            logger.info(f"AI title variant accepted: '{folder_title}' -> '{ai_title}' (similarity={title_similarity:.2f}, word_overlap={title_word_overlap})")
            return True, 'medium', 'title_variant_accepted'

    # Title doesn't match - check for complete hallucination
    # Also check against original folder name (might have title at start)
    folder_name_lower = folder_name.lower()
    any_ai_word_in_folder = any(word in folder_name_lower for word in meaningful_ai_words if len(word) > 3)

    if title_similarity < 0.2 and not any_ai_word_in_folder:
        # AI returned something completely different - likely hallucination
        logger.warning(f"AI HALLUCINATION DETECTED: Input='{folder_name[:50]}' -> AI='{ai_author} - {ai_title}' (similarity={title_similarity:.2f})")
        return False, 'none', f'hallucination_detected (similarity={title_similarity:.2f})'

    # Borderline case - accept with low confidence
    logger.info(f"AI result borderline: '{folder_title}' -> '{ai_title}' (similarity={title_similarity:.2f})")
    return True, 'low', 'borderline_match'


def identify_book_with_ai(file_group, config):
    """
    Use AI to identify a book from file information.
    Sends filenames, duration, and any metadata to AI for identification.

    Includes hallucination prevention:
    1. Input quality check - skip AI for garbage inputs
    2. Strict prompt - emphasize returning null over guessing
    3. Output validation - detect when AI invents unrelated books
    """
    if not config:
        return None

    files = file_group.get('files', [])
    info = file_group.get('detected_info', {})
    folder_name = file_group.get('folder_name', '')

    # Build context for AI
    filenames = [Path(f).name if isinstance(f, str) else f.name for f in files[:20]]

    # === HALLUCINATION PREVENTION: Input quality check ===
    input_quality, clues = calculate_input_quality(folder_name, filenames, info)

    if input_quality < 25:
        # Input is garbage - don't even try AI, it will hallucinate
        logger.info(f"AI skipped due to low input quality ({input_quality}): {folder_name[:50]} - clues: {clues}")
        return {'author': None, 'title': None, 'confidence': 'none', 'reason': f'insufficient_input_quality ({input_quality}/100)'}

    # === PROMPT - Identify and CORRECT wrong metadata ===
    prompt = f"""You are identifying an audiobook and CORRECTING wrong metadata.

IMPORTANT: The folder/artist metadata may be WRONG. Your job is to identify the REAL book and author.

RULES:
1. If you recognize the TITLE as a SPECIFIC, WELL-KNOWN book, provide the CORRECT author
2. Example: "James Patterson - Le Petit Prince" -> The title is "Le Petit Prince" which is by Antoine de Saint-Exupéry, NOT James Patterson. Return the correct author.
3. Only return null if the input is truly gibberish with no recognizable book title
4. DO NOT invent books - if input is just numbers or "unknown_123", return null

CRITICAL - GENERIC TITLE WARNING:
Some titles are AMBIGUOUS and could match multiple books by different authors:
- "Match Game", "The Game", "Home", "Gone", "Prey", "Storm" - these are GENERIC titles
- If you see a generic title with NO other context (no series info, no author hint), return LOW confidence
- NEVER invent an author for a generic title - if you don't KNOW the specific book, return null
- Example: "Match Game" alone -> could be Craig Alanson (Expeditionary Force #12) or another book entirely
- If you're not 100% CERTAIN which book this is, use confidence: "low" or return null

REASONING REQUIRED:
You MUST explain WHY you think this is the correct book. What evidence supports your identification?
- Did you recognize a unique title?
- Did series info help identify it?
- Did the author name confirm it?
- Or are you GUESSING based on a generic title? (If guessing, return null!)

Input information:
- Folder name: {folder_name}
- Files ({len(files)} total): {', '.join(filenames[:10])}{'...' if len(filenames) > 10 else ''}
- Duration: {info.get('duration_hours', 'unknown')} hours
- Album tag: {info.get('title', 'none')}
- Artist tag: {info.get('author', 'none')}

If you can CONFIDENTLY identify the book, return:
{{"author": "CORRECT Author Name", "title": "CORRECT Book Title", "series": "Series Name or null", "confidence": "high/medium/low", "reasoning": "Why you identified this specific book"}}

If the title is ambiguous/generic and you cannot be certain, return:
{{"author": null, "title": null, "confidence": "none", "reason": "ambiguous_title - could be multiple books"}}

Only invent nothing. Return null rather than guess."""

    try:
        # Use the provider chain for fallback support
        ai_result = call_text_provider_chain(prompt, config)

        if ai_result and isinstance(ai_result, dict):
            # === HALLUCINATION PREVENTION: Validate output ===
            is_valid, adjusted_confidence, reason = validate_ai_result(ai_result, folder_name, info)

            if not is_valid:
                logger.warning(f"AI result rejected: {reason} - Input: {folder_name[:50]}")
                return {'author': None, 'title': None, 'confidence': 'none', 'reason': reason}

            # Apply adjusted confidence
            ai_result['confidence'] = adjusted_confidence
            if reason != 'validated':
                ai_result['validation_note'] = reason

            # Issue #81: Strict language matching - reject cross-language AI results
            preferred_lang = config.get('preferred_language', 'en') if config else 'en'
            if config and config.get('strict_language_matching', True) and preferred_lang != 'en':
                result_lang = detect_title_language(ai_result.get('title', ''))
                # Reject if result is clearly in a different non-English language
                if result_lang not in (preferred_lang, 'en'):
                    logger.warning(f"AI returned {result_lang} result, user prefers {preferred_lang}: {ai_result.get('title')}")
                    return {'author': None, 'title': None, 'confidence': 'none', 'reason': f'language_mismatch ({result_lang} vs {preferred_lang})'}

            # Contribute to community database if we got a valid identification
            # This helps other users even if they don't use the same AI provider
            if ai_result.get('title') and ai_result.get('author'):
                try:
                    contribute_to_community(
                        title=ai_result.get('title'),
                        author=ai_result.get('author'),
                        series=ai_result.get('series'),
                        series_position=ai_result.get('series_num'),
                        source=ai_result.get('provider', 'ai'),  # gemini, openrouter, ollama, etc.
                        confidence=adjusted_confidence
                    )
                except Exception as contrib_err:
                    logger.debug(f"[COMMUNITY] Contribution failed (non-fatal): {contrib_err}")

            return ai_result

    except Exception as e:
        logger.debug(f"AI identification failed: {e}")

    return None


def transcribe_audio_clip(file_path, duration_seconds=30):
    """
    Transcribe a short clip from an audio file for identification.
    Uses Whisper API or local Whisper if available.

    Returns: transcribed text or None
    """
    try:
        # Try using OpenAI Whisper API first (if we have a key)
        secrets = load_secrets()
        openai_key = secrets.get('openai_api_key') if secrets else None

        if openai_key:
            # Use OpenAI Whisper API
            import subprocess
            import tempfile

            # Extract a clip using ffmpeg
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = tmp.name

            # Extract 30 seconds starting at 60 seconds in (skip intro)
            subprocess.run([
                'ffmpeg', '-y', '-i', str(file_path),
                '-ss', '60', '-t', str(duration_seconds),
                '-acodec', 'libmp3lame', '-ar', '16000',
                tmp_path
            ], capture_output=True, timeout=30)

            # Send to Whisper API
            with open(tmp_path, 'rb') as audio_file:
                response = requests.post(
                    'https://api.openai.com/v1/audio/transcriptions',
                    headers={'Authorization': f'Bearer {openai_key}'},
                    files={'file': audio_file},
                    data={'model': 'whisper-1'},
                    timeout=60
                )

            os.unlink(tmp_path)

            if response.status_code == 200:
                return response.json().get('text')

    except Exception as e:
        logger.debug(f"Audio transcription failed: {e}")

    return None


def search_by_transcription(transcription, config):
    """
    Search for a book using transcribed audio text.
    Searches our BookDB and uses AI to match.
    """
    if not transcription or len(transcription) < 50:
        return None

    # Take a meaningful chunk
    chunk = transcription[:500]

    # Search SearXNG for the quote
    try:
        response = requests.get(
            "http://localhost:8888/search",
            params={
                'q': f'"{chunk[:100]}" audiobook',
                'format': 'json'
            },
            timeout=10
        )

        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                # Use AI to analyze results
                return identify_from_search_results(results, chunk, config)
    except Exception as e:
        logger.debug(f"Transcription search failed: {e}")

    return None


def identify_from_search_results(results, context, config):
    """Use AI to identify book from search results and context."""
    if not config:
        return None

    secrets = load_secrets()
    gemini_key = secrets.get('gemini_api_key') if secrets else None

    if not gemini_key:
        return None

    results_text = "\n".join([
        f"- {r.get('title', 'No title')}: {r.get('content', '')[:100]}"
        for r in results[:5]
    ])

    prompt = f"""Based on this transcribed audio excerpt and search results, identify the audiobook:

Transcribed text: "{context[:300]}"

Search results:
{results_text}

Return JSON: {{"author": "Name", "title": "Title", "confidence": "high/medium/low"}}
If unsure, return {{"confidence": "none"}}"""

    try:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{config.get('gemini_model', 'gemini-2.0-flash')}:generateContent",
            headers={'Content-Type': 'application/json'},
            params={'key': gemini_key},
            json={'contents': [{'parts': [{'text': prompt}]}]},
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            text = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            json_match = re.search(r'\{[^}]+\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
    except Exception as e:
        logger.debug(f"AI identification from results failed: {e}")

    return None


def handle_chaos_library(lib_path, config=None):
    """
    Handle a completely chaotic library - files dumped directly in root with no structure.

    Process:
    1. Find all loose audio files
    2. Group related files (by metadata, patterns, etc.)
    3. Identify each group using multiple methods
    4. Create proper Author/Title structure

    Returns: list of results with actions taken
    """
    lib_root = Path(lib_path)
    results = []

    # Find all loose audio files in root
    loose_files = [
        f for f in lib_root.iterdir()
        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS
    ]

    if not loose_files:
        return [{'status': 'ok', 'message': 'No loose files found in library root'}]

    logger.info(f"CHAOS HANDLER: Found {len(loose_files)} loose files in library root")

    # Group the files
    groups = group_loose_files(loose_files)
    logger.info(f"CHAOS HANDLER: Grouped into {len(groups)} potential books")

    # Initialize progress tracker with queue of items
    queue_items = [g.get('detected_info', {}).get('title', f'Group {i+1}') for i, g in enumerate(groups)]
    search_progress.start('chaos_scan', len(groups), queue_items)

    for i, group in enumerate(groups):
        files = group['files']
        info = group['detected_info']
        group_type = group['group_type']

        logger.info(f"CHAOS HANDLER: Processing group {i+1}/{len(groups)} ({len(files)} files, type={group_type})")

        result = {
            'files': [str(f) for f in files],
            'file_count': len(files),
            'group_type': group_type,
            'detected_info': info
        }

        author = info.get('author')
        title = info.get('title')
        confidence = 'high' if group_type == 'metadata' else 'low'

        # === IDENTIFICATION PIPELINE ===

        # Level 1: Already have metadata
        if author and title and group_type == 'metadata':
            result['identification'] = 'metadata'
            result['author'] = author
            result['title'] = title
            result['confidence'] = 'high'

        # Level 2: Search by detected title/filename
        elif title:
            # Try BookBucket API first (50M books, public endpoint, fast)
            search_progress.set_status(f"Searching BookDB for '{title[:30]}...'")
            api_result = search_bookdb_api(title)
            if api_result and api_result.get('author'):
                author = api_result.get('author')
                title = api_result.get('title') or title
                confidence = 'high'
                result['identification'] = 'bookdb_api'
                search_progress.set_status(f"Found in BookDB: {author}")
                if api_result.get('series'):
                    result['series'] = api_result.get('series')

            # Fall back to AI if API didn't find it OR to verify folder metadata
            # Bug fix: Previously only called AI when no author existed, but folder metadata
            # can be WRONG (e.g., "James Patterson - Le Petit Prince"). AI should verify.
            folder_author = info.get('author')  # Original author from folder name
            needs_ai_verification = (
                not author or  # No author found yet
                (not api_result and folder_author)  # Have folder author but BookDB didn't confirm
            )

            if needs_ai_verification:
                search_progress.set_status(f"BookDB no match, trying AI for '{title[:30]}...'")
                ai_result = identify_book_with_ai(group, config)
                if ai_result and ai_result.get('author'):
                    ai_author = ai_result.get('author')
                    ai_title = ai_result.get('title')
                    ai_confidence = ai_result.get('confidence', 'medium')

                    # If we had a folder author and AI disagrees, log it and use AI
                    if folder_author and ai_author.lower() != folder_author.lower():
                        logger.info(f"AI CORRECTED author: '{folder_author}' -> '{ai_author}' for '{title}'")

                    author = ai_author
                    title = ai_title or title
                    confidence = ai_confidence
                    result['identification'] = 'ai'
                    search_progress.set_status(f"AI identified: {author}")
                    if ai_result.get('series'):
                        result['series'] = ai_result.get('series')
                else:
                    search_progress.set_status(f"Could not identify '{title[:30]}...'")

            # Track if we had to fall back
            if result.get('identification') == 'ai' and api_result is None:
                result['fallback_reason'] = 'BookDB unavailable or no match'

            result['author'] = author or 'Unknown Author'
            result['title'] = title
            result['confidence'] = confidence

        # Level 3: Numbered/unknown files - need transcription
        elif info.get('needs_identification') and len(files) > 0:
            logger.info(f"CHAOS HANDLER: Attempting audio transcription for unknown group")
            search_progress.set_status("No title detected, trying audio transcription...")

            # Try transcription on first file
            transcription = transcribe_audio_clip(str(files[0]))
            if transcription:
                search_progress.set_status("Transcription complete, searching...")
                trans_result = search_by_transcription(transcription, config)
                if trans_result and trans_result.get('confidence') != 'none':
                    author = trans_result.get('author')
                    title = trans_result.get('title')
                    confidence = trans_result.get('confidence', 'low')
                    result['identification'] = 'transcription'
                    result['transcription_sample'] = transcription[:200]
                    search_progress.set_status(f"Transcription identified: {author}")
                else:
                    search_progress.set_status("Transcription search found no match")
            else:
                search_progress.set_status("Audio transcription failed")

            result['author'] = author or 'Unknown Author'
            result['title'] = title or f"Unknown Book ({len(files)} files, {info.get('duration_hours', '?')}h)"
            result['confidence'] = confidence

        else:
            result['author'] = 'Unknown Author'
            result['title'] = title or f"Unknown ({len(files)} files)"
            result['confidence'] = 'none'
            result['identification'] = 'failed'
            search_progress.set_status("Could not identify - no title or metadata")

        # Update progress
        item_name = result.get('title') or f'Group {i+1}'
        search_progress.update(item_name, result)

        results.append(result)

    # Mark progress complete
    search_progress.finish()

    return results


def is_disc_chapter_folder(name):
    """Check if folder name looks like a disc/chapter subfolder."""
    name_lower = name.lower()
    for pattern in DISC_CHAPTER_PATTERNS:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return True
    return False


def clean_title(title):
    """Remove junk from title, return (cleaned_title, issues_found)."""
    issues = []
    cleaned = title

    # Issue #64: Strip year prefix like "2007 - Title" (common in torrent naming)
    year_prefix_match = re.match(r'^(19|20)\d{2}\s*[-–]\s*', cleaned)
    if year_prefix_match:
        issues.append(f"year_prefix: {year_prefix_match.group(0).strip()}")
        cleaned = cleaned[year_prefix_match.end():]

    # Issue #64: Strip series prefix like "DM-08 - Title" or "01 - Title"
    series_prefix_match = re.match(r'^[A-Z]{1,3}[-.]?\d{1,2}\s*[-–]\s*', cleaned)
    if series_prefix_match:
        issues.append(f"series_prefix: {series_prefix_match.group(0).strip()}")
        cleaned = cleaned[series_prefix_match.end():]

    for pattern in JUNK_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            issues.append(f"junk: {pattern}")
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    # Clean up extra whitespace and dashes
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[-_\s]+|[-_\s]+$', '', cleaned)
    cleaned = re.sub(r'\s*-\s*$', '', cleaned)

    return cleaned, issues


def analyze_full_path(audio_file_path, library_root):
    """
    Analyze the COMPLETE path from library root to audio file.
    Works BACKWARDS from the file to understand the structure.

    Returns dict with:
        - book_folder: Path to the folder containing this book's audio
        - detected_author: Best guess at author name
        - detected_title: Best guess at book title
        - detected_series: Series name if detected
        - folder_roles: Dict mapping each folder to its detected role
        - confidence: How confident we are in the detection
        - issues: List of potential problems
    """
    audio_path = Path(audio_file_path)
    lib_root = Path(library_root)

    # Get relative path from library root
    try:
        rel_path = audio_path.relative_to(lib_root)
    except ValueError:
        return None  # File not under library root

    parts = list(rel_path.parts)
    if len(parts) < 2:  # Just filename, no folder structure
        return {
            'book_folder': str(lib_root),
            'detected_author': 'Unknown',
            'detected_title': audio_path.stem,
            'detected_series': None,
            'folder_roles': {},
            'confidence': 'low',
            'issues': ['loose_file_no_structure']
        }

    # Remove filename, work with folders only
    filename = parts[-1]
    folders = parts[:-1]

    # Classify each folder from BOTTOM to TOP
    folder_roles = {}
    issues = []

    def looks_like_person_name(name):
        """Check if name looks like a person's name (First Last pattern)."""
        patterns = [
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',           # First Last
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+$',  # First Middle Last
            r'^[A-Z]\.\s*[A-Z][a-z]+$',               # F. Last
            r'^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+$', # First M. Last
            r'^[A-Z][a-z]+,\s+[A-Z][a-z]+$',          # Last, First
            r'^[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',     # J.R.R. Tolkien
        ]
        return any(re.match(p, name) for p in patterns)

    def looks_like_disc_chapter(name):
        """Check if folder is a disc/chapter/part folder (not meaningful for title)."""
        patterns = [
            r'^(disc|disk|cd|dvd)\s*\d+',
            r'^(part|chapter|ch)\s*\d+',
            r'^\d+\s*[-–]\s*(disc|disk|cd|part)',
            r'^(side)\s*[ab12]',
            r'^\d{1,2}$',  # Just a number like "1", "01"
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def looks_like_book_number(name):
        """Check if folder indicates a numbered book in series."""
        patterns = [
            r'^(book|vol|volume|part)\s*\d+',
            r'^\d+\s*[-–:.]\s*\w',  # "01 - Title", "1. Title"
            r'^#?\d+\s*[-–:]',      # "#1 - Title"
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def looks_like_title_with_year(name):
        """Check if name looks like a title with a year (series/book name)."""
        return bool(re.search(r'\b(19[0-9]{2}|20[0-9]{2})\b', name))

    def looks_like_series_name(name):
        """Check if name looks like a series name."""
        # Series often have: numbers, "series", "saga", "chronicles", or are the same as child folder
        patterns = [
            r'\bseries\b', r'\bsaga\b', r'\bchronicles\b', r'\btrilogy\b',
            r'\bcycle\b', r'\buniverse\b', r'\bbooks?\b',
        ]
        return any(re.search(p, name, re.IGNORECASE) for p in patterns)

    def is_known_series(name):
        """
        Check if name matches a series in our database (with fuzzy matching).
        Returns: (found: bool, lookup_succeeded: bool)
        - (True, True) = found in database
        - (False, True) = not found, but lookup worked
        - (False, False) = lookup failed (connection error, etc)
        """
        try:
            conn = get_bookdb_connection()
            if conn:
                cursor = conn.cursor()
                # Clean name for search
                clean_name = re.sub(r'[^\w\s]', '', name).strip()
                # Try exact match first
                cursor.execute("SELECT COUNT(*) FROM series WHERE LOWER(name) = LOWER(?)", (clean_name,))
                count = cursor.fetchone()[0]
                if count > 0:
                    conn.close()
                    return (True, True)
                # Try fuzzy match - handle "Dark Tower" matching "The Dark Tower"
                # Also handle "Wheel of Time" matching "The Wheel of Time"
                cursor.execute(
                    "SELECT COUNT(*) FROM series WHERE LOWER(name) LIKE ? OR LOWER(name) LIKE ?",
                    (f'%{clean_name.lower()}%', f'%the {clean_name.lower()}%')
                )
                count = cursor.fetchone()[0]
                conn.close()
                return (count > 0, True)
        except Exception as e:
            logging.debug(f"Series lookup failed for '{name}': {e}")
        return (False, False)  # Lookup failed

    def is_known_author(name):
        """
        Check if name matches an author in our database.
        Returns: (found: bool, lookup_succeeded: bool)
        - (True, True) = found in database
        - (False, True) = not found, but lookup worked
        - (False, False) = lookup failed (connection error, etc)
        """
        try:
            conn = get_bookdb_connection()
            if conn:
                cursor = conn.cursor()
                clean_name = re.sub(r'[^\w\s\.]', '', name).strip()
                cursor.execute("SELECT COUNT(*) FROM authors WHERE LOWER(name) = LOWER(?)", (clean_name,))
                count = cursor.fetchone()[0]
                conn.close()
                return (count > 0, True)
        except Exception as e:
            logging.debug(f"Author lookup failed for '{name}': {e}")
        return (False, False)  # Lookup failed

    # Work from bottom (closest to files) to top
    detected_author = None
    detected_title = None
    detected_series = None
    book_folder_idx = None

    for i in range(len(folders) - 1, -1, -1):
        folder = folders[i]

        if looks_like_disc_chapter(folder):
            folder_roles[folder] = 'disc_chapter'
            continue

        # First non-disc folder from bottom is likely the book title
        if book_folder_idx is None:
            book_folder_idx = i
            folder_roles[folder] = 'book_title'
            detected_title = folder

            # Check if this looks like "SeriesName Book N - ActualTitle"
            book_num_match = re.match(r'^(.+?)\s*(?:book|vol)\s*\d+\s*[-–:]\s*(.+)$', folder, re.IGNORECASE)
            if book_num_match:
                detected_series = book_num_match.group(1).strip()
                detected_title = book_num_match.group(2).strip()
            continue

        # Check what this parent folder looks like
        # Priority: database matches > pattern matches > position-based guesses

        # First check database for definitive matches
        # Returns (found, lookup_succeeded) tuples
        author_result = is_known_author(folder)
        series_result = is_known_series(folder)

        # Extract results - if lookup failed, treat as "unknown" not "not found"
        db_is_author = author_result[0] if author_result[1] else None  # None = lookup failed
        db_is_series = series_result[0] if series_result[1] else None

        # Position-aware disambiguation: if we're between author and book, lean towards series
        # Check if parent folder looks like an author (person name pattern)
        parent_is_person = i > 0 and looks_like_person_name(folders[i-1])
        is_middle_position = book_folder_idx is not None and i < book_folder_idx

        # If lookups failed, fall back to pattern-only detection (no database assumptions)
        if db_is_author is None or db_is_series is None:
            # Database unavailable - use patterns only, don't make assumptions
            if 'db_lookup_failed' not in issues:
                issues.append('db_lookup_failed')
            if looks_like_person_name(folder):
                folder_roles[folder] = 'author'
                detected_author = folder
            elif looks_like_book_number(folder):
                folder_roles[folder] = 'book_number'
            elif looks_like_series_name(folder) or looks_like_title_with_year(folder):
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            elif detected_author is None:
                if parent_is_person and is_middle_position:
                    folder_roles[folder] = 'series'
                    detected_series = folder
                else:
                    folder_roles[folder] = 'likely_author'
                    detected_author = folder
            continue  # Skip to next folder

        if db_is_author and not db_is_series:
            # Found in authors DB but not series - but check position context
            if parent_is_person and is_middle_position and not looks_like_person_name(folder):
                # Parent looks like author, we're in middle, treat as series
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            else:
                folder_roles[folder] = 'author'
                detected_author = folder
        elif db_is_series and not db_is_author:
            # Definitely a series from our database
            folder_roles[folder] = 'series'
            if detected_series is None:
                detected_series = folder
        elif db_is_author and db_is_series:
            # Ambiguous - found in both databases
            # Priority: position context > name pattern
            if parent_is_person and is_middle_position:
                # Strong contextual signal: parent looks like author, we're between author and book
                # This is likely a series even if the name looks like a person
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
            elif looks_like_person_name(folder) and not is_middle_position:
                # Looks like a name AND not in a series position - treat as author
                folder_roles[folder] = 'author'
                detected_author = folder
            else:
                # Default to series when ambiguous
                folder_roles[folder] = 'series'
                if detected_series is None:
                    detected_series = folder
        elif looks_like_person_name(folder):
            folder_roles[folder] = 'author'
            detected_author = folder
        elif looks_like_book_number(folder):
            # This folder is a book number, so parent of THAT is probably series or author
            folder_roles[folder] = 'book_number'
            # The detected_title should be updated if we have a better one
            if detected_title and looks_like_book_number(detected_title):
                # Our "title" was actually a book number folder
                detected_title = folder
        elif looks_like_series_name(folder) or looks_like_title_with_year(folder):
            folder_roles[folder] = 'series'
            if detected_series is None:
                detected_series = folder
        elif detected_author is None:
            # Contextual guess: if we already have a book title and this folder
            # is between where author should be and the book, it's likely a series
            # Structure: Author / Series / BookTitle
            if book_folder_idx is not None and i < book_folder_idx and not looks_like_person_name(folder):
                # This is a middle folder - check if parent might be author
                if i > 0 and looks_like_person_name(folders[i-1]):
                    folder_roles[folder] = 'series'
                    detected_series = folder
                else:
                    # Assume author at top level
                    folder_roles[folder] = 'likely_author'
                    detected_author = folder
            else:
                folder_roles[folder] = 'likely_author'
                detected_author = folder

    # Build book folder path
    if book_folder_idx is not None:
        book_folder = lib_root / Path(*folders[:book_folder_idx + 1])
    else:
        book_folder = lib_root / Path(*folders)

    # Validate and add issues
    if detected_author and not looks_like_person_name(detected_author):
        issues.append(f'author_not_name_pattern:{detected_author}')

    if detected_author and looks_like_title_with_year(detected_author):
        issues.append(f'author_looks_like_title:{detected_author}')

    if detected_title and looks_like_person_name(detected_title):
        issues.append(f'title_looks_like_author:{detected_title}')

    # Check for likely reversed structure
    if (detected_author and detected_title and
        looks_like_title_with_year(detected_author) and
        looks_like_person_name(detected_title)):
        issues.append('STRUCTURE_LIKELY_REVERSED')
        # Swap them
        detected_author, detected_title = detected_title, detected_author

    # Confidence level
    if detected_author and looks_like_person_name(detected_author):
        confidence = 'high'
    elif detected_author:
        confidence = 'medium'
    else:
        confidence = 'low'

    # Issue #64: Clean junk from detected title (torrent naming, bitrates, timestamps, etc.)
    if detected_title:
        cleaned_title, title_issues = clean_title(detected_title)
        if title_issues:
            issues.extend(title_issues)
        detected_title = cleaned_title

    return {
        'book_folder': str(book_folder),
        'detected_author': detected_author or 'Unknown',
        'detected_title': detected_title or audio_path.stem,
        'detected_series': detected_series,
        'folder_roles': folder_roles,
        'confidence': confidence,
        'issues': issues,
        'depth': len(folders)
    }


def analyze_path_with_ai(full_path, library_root, config, sample_files=None):
    """
    Use Gemini AI to analyze an ambiguous folder path.
    Called when script-based analysis has low confidence.

    Args:
        full_path: Full path to the book folder
        library_root: Root of the library
        config: App config with API keys
        sample_files: Optional list of audio filenames in the folder
    """
    try:
        rel_path = Path(full_path).relative_to(library_root)
        path_str = str(rel_path)
    except ValueError:
        path_str = full_path

    # Build context about the files
    files_context = ""
    if sample_files:
        files_context = f"\nAudio files in this folder: {', '.join(sample_files[:10])}"
        if len(sample_files) > 10:
            files_context += f" (and {len(sample_files) - 10} more)"

    prompt = f"""Analyze this audiobook folder path and identify the structure.

PATH: {path_str}{files_context}

For audiobook libraries, folders typically represent:
- Author name (person's name like "Brandon Sanderson", "J.R.R. Tolkien")
- Series name (like "The Wheel of Time", "Metro 2033", "Mistborn")
- Book title (the actual book name)
- Disc/Part folders (like "Disc 1", "CD1", "Part 1" - ignore these for metadata)

Analyze this path and determine:
1. Which folder is the AUTHOR (should be a person's name)
2. Which folder is the SERIES (if any - optional)
3. Which folder is the BOOK TITLE
4. Is the structure correct (Author/Series/Book or Author/Book) or reversed?

IMPORTANT:
- A year like "2033" or "1984" in a folder name usually means it's a TITLE, not an author
- Two capitalized words that look like "First Last" are likely an AUTHOR
- If author and title seem swapped, indicate the correct order

Return JSON only:
{{
    "detected_author": "Author Name",
    "detected_series": "Series Name or null",
    "detected_title": "Book Title",
    "structure_correct": true/false,
    "suggested_path": "Correct/Path/Structure",
    "confidence": "high/medium/low",
    "reasoning": "Brief explanation"
}}"""

    # Use the provider chain for fallback support
    return call_text_provider_chain(prompt, config)


def smart_analyze_path(audio_file_or_folder, library_root, config):
    """
    Smart path analysis - tries script first, falls back to AI if needed.

    Returns the analysis result with author, title, series, and any issues.
    """
    path = Path(audio_file_or_folder)

    # If it's a folder, find an audio file inside
    if path.is_dir():
        audio_files = list(path.rglob('*'))
        audio_files = [f for f in audio_files if f.suffix.lower() in AUDIO_EXTENSIONS]
        if audio_files:
            audio_file = str(audio_files[0])
            sample_files = [f.name for f in audio_files[:15]]
        else:
            return {'error': 'No audio files found'}
    else:
        audio_file = str(path)
        sample_files = [path.name]

    # First try script-based analysis
    script_result = analyze_full_path(audio_file, library_root)

    if script_result is None:
        return {'error': 'Path not under library root'}

    # If confidence is high and no major issues, use script result
    if script_result['confidence'] == 'high' and 'STRUCTURE_LIKELY_REVERSED' not in script_result.get('issues', []):
        script_result['method'] = 'script'
        return script_result

    # For low confidence or issues, try AI
    logger.info(f"Script confidence {script_result['confidence']}, trying AI for: {audio_file_or_folder}")

    ai_result = analyze_path_with_ai(
        str(path) if path.is_dir() else str(path.parent),
        library_root,
        config,
        sample_files
    )

    if ai_result:
        ai_result['method'] = 'ai'
        ai_result['script_fallback'] = script_result
        return ai_result

    # Fall back to script result if AI fails
    script_result['method'] = 'script_fallback'
    return script_result


def analyze_author(author):
    """Analyze author name for issues, return list of issues."""
    issues = []

    # System/junk folder names - these should NEVER be processed as books
    system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                      'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                      '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                      'extras', 'bonus', 'misc', 'other', 'various', 'unknown', 'unsorted',
                      'downloads', 'incoming', 'processing', 'completed', 'done', 'failed',
                      'streams', 'chapters', 'parts', 'disc', 'disk', 'cd', 'dvd'}
    if author.lower() in system_folders:
        issues.append("system_folder_not_author")
        return issues  # Don't bother checking anything else

    # Issue #59: Placeholder author names need identification, not verification
    # "Unknown Author", "Various Authors", etc. should be queued for processing
    if is_placeholder_author(author):
        issues.append("placeholder_author")
        return issues  # Needs identification, skip other checks

    # Year in author name
    if re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', author):
        issues.append("year_in_author")

    # Words that are clearly NOT first names (adjectives, articles, title starters)
    not_first_names = {'last', 'first', 'final', 'dark', 'shadow', 'night', 'blood', 'death',
                       'city', 'house', 'world', 'kingdom', 'empire', 'war', 'game', 'fire',
                       'ice', 'storm', 'the', 'a', 'an', 'of', 'and', 'in', 'to', 'for',
                       'new', 'old', 'black', 'white', 'red', 'blue', 'green', 'golden',
                       'lost', 'forgotten', 'hidden', 'secret', 'ancient', 'eternal'}

    # Words that are clearly NOT surnames (plural nouns, abstract concepts)
    not_surnames = {'chances', 'secrets', 'lies', 'dreams', 'tales', 'chronicles', 'stories',
                    'wishes', 'memories', 'shadows', 'nights', 'days', 'years', 'wars',
                    'games', 'fires', 'storms', 'kingdoms', 'empires', 'worlds', 'houses',
                    'cities', 'deaths', 'lives', 'loves', 'hearts', 'souls', 'minds',
                    'stars', 'moons', 'suns', 'gods', 'demons', 'angels', 'dragons',
                    'kings', 'queens', 'lords', 'princes', 'witches', 'wizards'}

    author_words = author.lower().split()

    # Check if it structurally looks like a name
    name_patterns = [
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',           # First Last (exact)
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+[A-Z][a-z]+$',  # First Middle Last
        r'^[A-Z]\.\s*[A-Z][a-z]+$',               # F. Last
        r'^[A-Z][a-z]+\s+[A-Z]\.\s*[A-Z][a-z]+$', # First M. Last
        r'^[A-Z][a-z]+,\s+[A-Z][a-z]+$',          # Last, First
        r'^[A-Z][a-z]+$',                          # Single name (Plato, Madonna)
        r'^[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',     # J.R.R. Tolkien, H.P. Lovecraft
        r'^[A-Z][a-z]+\s+[A-Z]\.([A-Z]\.)+\s*[A-Z][a-z]+$',  # George R.R. Martin
        r'^[A-Z][a-z]+\s+[A-Z]\.[A-Z]\.\s*[A-Z][a-z]+$',     # Brandon R.R. Author
        r'^[A-Z][a-z]+\s+[A-Z]\.\s*(Le|De|Von|Van|La|Du)\s+[A-Z][a-z]+$',  # Ursula K. Le Guin
        r'^[A-Z][a-z]+\s+(Le|De|Von|Van|La|Du)\s+[A-Z][a-z]+$',  # Anne De Vries
        # Issue #52: Additional name patterns
        r'^[A-Z][a-z]+\s+[A-Z]\s+[A-Z]\s+[A-Z][a-z]+$',  # James S A Corey (single initials without periods)
        r'^[A-Z][a-z]+\s+[A-Z]\s+[A-Z][a-z]+$',          # First A Last (one single initial)
        r'^[A-Z][a-z]+\s+(Mc|Mac|O\')[A-Z][a-z]+$',      # Freida McFadden, Anne MacLeod, Mary O'Brien
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+(Mc|Mac|O\')[A-Z][a-z]+$',  # First Middle McLastname
        r'^[A-Z][a-z]+\s+[A-Z]\.\s+(Mc|Mac|O\')[A-Z][a-z]+$',  # First M. McLastname
    ]
    looks_like_name = any(re.match(p, author) for p in name_patterns)

    # Even if it LOOKS like a name structurally, check if the words are actually name-like
    if looks_like_name and len(author_words) >= 2:
        first_word = author_words[0]
        last_word = author_words[-1]

        # "Last Chances" - first word is adjective, last word is plural noun = NOT a name
        if first_word in not_first_names and last_word in not_surnames:
            looks_like_name = False
            issues.append("title_fragment_not_name")
        # "Last Something" - first word alone is a red flag if not a real first name
        elif first_word in not_first_names and last_word in not_surnames:
            looks_like_name = False
            issues.append("title_words_in_author")
        # "Something Chances" - second word is clearly not a surname
        elif last_word in not_surnames:
            looks_like_name = False
            issues.append("not_a_surname")

    # Only flag title words if it DOESN'T look like a valid name
    if not looks_like_name:
        title_words = ['the', 'of', 'and', 'a', 'in', 'to', 'for', 'book', 'series', 'volume',
                       'last', 'first', 'final', 'dark', 'shadow', 'night', 'blood', 'death',
                       'city', 'house', 'world', 'kingdom', 'empire', 'war', 'game', 'fire',
                       'ice', 'storm', 'king', 'queen', 'lord', 'lady', 'prince', 'dragon',
                       'chances', 'secrets', 'lies', 'dreams', 'tales', 'chronicles']
        if any(w in author_words for w in title_words):
            issues.append("title_words_in_author")

        # Two+ words but doesn't match name patterns - probably a title
        if len(author) > 3 and len(author.split()) >= 2:
            issues.append("not_a_name_pattern")

    # LastName, FirstName format
    if re.match(r'^[A-Z][a-z]+,\s+[A-Z][a-z]+', author):
        issues.append("lastname_firstname_format")

    # Format indicators
    if re.search(r'\.(epub|pdf|mp3|m4b)|(\[|\]|\{|\})', author, re.IGNORECASE):
        issues.append("format_junk_in_author")

    # Narrator included (usually with hyphen)
    if re.search(r'\s*-\s*[A-Z][a-z]+\s+[A-Z][a-z]+$', author):
        issues.append("possible_narrator_in_author")

    # Just numbers
    if re.match(r'^\d+$', author):
        issues.append("author_is_just_numbers")

    # Starts with number (might be book title)
    if re.match(r'^\d+\s', author):
        issues.append("author_starts_with_number")

    # Contains "Book N" or "Part N" - probably a title
    if re.search(r'\bbook\s*\d|\bpart\s*\d|\bvolume\s*\d', author, re.IGNORECASE):
        issues.append("author_contains_book_number")

    return issues


def analyze_title(title, author):
    """Analyze title for issues, return list of issues."""
    issues = []

    # Multi-book collection folder - these contain multiple books and need special handling
    # Don't process these as single books - they need to be split first
    # Be conservative - only flag patterns that DEFINITELY mean multiple books
    multi_book_patterns = [
        r'complete\s+series',           # "Complete Series"
        r'complete\s+audio\s+collection', # "Complete Audio Collection"
        r'\d+[-\s]?book\s+(set|box|collection)',  # "7-Book Set", "3 Book Collection"
        r'\d+[-\s]?book\s+and\s+audio',  # "7-Book and Audio Box Set"
        r'all\s+\d+\s+books',            # "All 9 Books"
        r'books?\s+\d+[-\s]?\d+',        # "Books 1-9", "Book 1-3"
    ]
    title_lower = title.lower()
    if any(re.search(p, title_lower) for p in multi_book_patterns):
        issues.append("multi_book_collection")
        return issues  # Don't bother with other checks - this needs manual handling

    # Author name repeated in title
    author_parts = author.lower().split()
    if len(author_parts) >= 2:
        if author.lower() in title.lower():
            issues.append("author_in_title")
        # Check for "by Author" pattern
        if re.search(rf'\bby\s+{re.escape(author)}\b', title, re.IGNORECASE):
            issues.append("by_author_in_title")

    # Year in title (but not book number like "1984")
    year_match = re.search(r'\(?(19[5-9][0-9]|20[0-2][0-9])\)?', title)
    if year_match:
        issues.append("year_in_title")

    # Quality/bitrate info
    if re.search(r'\d+k\b|\d+kbps|\d+mb|\d+gb', title, re.IGNORECASE):
        issues.append("quality_info_in_title")

    # Narrator name pattern (Name) at end
    if re.search(r'\([A-Z][a-z]+\)\s*$', title):
        issues.append("possible_narrator_in_title")

    # Duration pattern HH.MM.SS
    if re.search(r'\d{1,2}\.\d{2}\.\d{2}', title):
        issues.append("duration_in_title")

    # Series prefix like "Series Name Book 1 -"
    if re.search(r'^.+\s+book\s+\d+\s*[-:]\s*.+', title, re.IGNORECASE):
        issues.append("series_prefix_format")

    # Brackets with numbers (catalog IDs)
    if re.search(r'\[\d{4,}\]', title):
        issues.append("catalog_id_in_title")

    # Title looks like author name (just 2 capitalized words)
    title_words = title.split()
    if len(title_words) == 2 and all(w[0].isupper() for w in title_words if w):
        if not any(w.lower() in ['the', 'a', 'of', 'and'] for w in title_words):
            issues.append("title_looks_like_author")

    return issues



def check_audio_file_health(file_path):
    """
    Check if an audio file is valid/corrupt using ffprobe.
    Returns dict with: valid (bool), duration (seconds or None), error (str or None)
    """
    import subprocess

    try:
        # First check: can ffprobe read duration?
        result = subprocess.run(
            ['ffprobe', '-i', file_path, '-show_entries', 'format=duration',
             '-v', 'quiet', '-of', 'csv=p=0'],
            capture_output=True, text=True, timeout=30
        )

        duration_str = result.stdout.strip()

        if not duration_str or duration_str == 'N/A':
            # Try to get more info about why it failed
            probe_result = subprocess.run(
                ['ffprobe', '-i', file_path, '-v', 'error'],
                capture_output=True, text=True, timeout=30
            )
            error_msg = probe_result.stderr.strip() or "Cannot read audio stream"
            return {'valid': False, 'duration': None, 'error': error_msg}

        try:
            duration = float(duration_str)
            # Sanity check - file should have reasonable duration
            if duration < 1:
                return {'valid': False, 'duration': duration, 'error': 'Duration too short (<1 sec)'}
            return {'valid': True, 'duration': duration, 'error': None}
        except ValueError:
            return {'valid': False, 'duration': None, 'error': f'Invalid duration: {duration_str}'}

    except subprocess.TimeoutExpired:
        return {'valid': False, 'duration': None, 'error': 'Timeout reading file'}
    except FileNotFoundError:
        return {'valid': False, 'duration': None, 'error': 'ffprobe not installed'}
    except Exception as e:
        return {'valid': False, 'duration': None, 'error': str(e)}


def get_file_signature(filepath, sample_size=8192):
    """Get a signature for duplicate detection (size + partial hash)."""
    try:
        size = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            sample = f.read(sample_size)
        partial_hash = hashlib.md5(sample).hexdigest()[:16]
        return f"{size}_{partial_hash}"
    except:
        return None


def get_audio_fingerprint(filepath, duration=30, offset=0):
    """
    Get audio fingerprint using chromaprint/fpcalc.

    Args:
        filepath: Path to audio file
        duration: Seconds of audio to fingerprint (default 30)
        offset: Start position in seconds (for sampling middle/end)

    Returns dict with:
        - fingerprint: The chromaprint fingerprint string
        - duration: Total duration of the file in seconds
        - error: Error message if failed
    """
    import subprocess

    try:
        # Build fpcalc command
        cmd = ['fpcalc', '-length', str(duration)]
        if offset > 0:
            # Use ffmpeg to extract segment first (fpcalc doesn't support offset)
            # For now, we'll just fingerprint from the start
            pass
        cmd.append(str(filepath))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode != 0:
            return {'fingerprint': None, 'duration': None, 'error': result.stderr}

        # Parse output
        output = {}
        for line in result.stdout.strip().split('\n'):
            if '=' in line:
                key, value = line.split('=', 1)
                output[key] = value

        return {
            'fingerprint': output.get('FINGERPRINT'),
            'duration': int(output.get('DURATION', 0)),
            'error': None
        }
    except subprocess.TimeoutExpired:
        return {'fingerprint': None, 'duration': None, 'error': 'Timeout'}
    except FileNotFoundError:
        return {'fingerprint': None, 'duration': None, 'error': 'fpcalc not installed'}
    except Exception as e:
        return {'fingerprint': None, 'duration': None, 'error': str(e)}


def compare_fingerprints(fp1, fp2):
    """
    Compare two chromaprint fingerprints and return similarity score.

    Chromaprint fingerprints are base64-encoded integers that can be
    compared using popcount of XOR (Hamming distance).

    Returns similarity score 0.0 to 1.0
    """
    import base64
    import struct

    if not fp1 or not fp2:
        return 0.0

    try:
        # Decode base64 fingerprints to raw bytes
        raw1 = base64.b64decode(fp1 + '==')  # Add padding if needed
        raw2 = base64.b64decode(fp2 + '==')

        # Convert to list of 32-bit integers
        def to_ints(raw):
            # Skip first 4 bytes (header)
            data = raw[4:] if len(raw) > 4 else raw
            ints = []
            for i in range(0, len(data) - 3, 4):
                val = struct.unpack('<I', data[i:i+4])[0]
                ints.append(val)
            return ints

        ints1 = to_ints(raw1)
        ints2 = to_ints(raw2)

        if not ints1 or not ints2:
            return 0.0

        # Compare overlapping portion
        min_len = min(len(ints1), len(ints2))
        if min_len == 0:
            return 0.0

        # Count matching bits (using XOR and popcount)
        total_bits = min_len * 32
        different_bits = 0
        for i in range(min_len):
            xor = ints1[i] ^ ints2[i]
            different_bits += bin(xor).count('1')

        similarity = 1.0 - (different_bits / total_bits)
        return max(0.0, similarity)

    except Exception as e:
        logger.debug(f"Fingerprint comparison error: {e}")
        return 0.0


def analyze_audiobook_completeness(folder_path):
    """
    Analyze an audiobook folder for completeness.

    Returns dict with:
        - total_duration: Total duration in seconds
        - file_count: Number of audio files
        - files: List of {path, duration, fingerprint_start, fingerprint_end}
        - appears_complete: True if ending sounds like a proper ending
        - appears_partial: True if it seems cut off
    """
    folder = Path(folder_path)
    audio_files = sorted([f for f in folder.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS])

    if not audio_files:
        return {'total_duration': 0, 'file_count': 0, 'files': [], 'appears_complete': False}

    files_info = []
    total_duration = 0

    for audio_file in audio_files:
        # Get fingerprint of first 30 seconds
        fp_start = get_audio_fingerprint(str(audio_file), duration=30)

        if fp_start['duration']:
            total_duration += fp_start['duration']
            files_info.append({
                'path': str(audio_file),
                'filename': audio_file.name,
                'duration': fp_start['duration'],
                'fingerprint_start': fp_start['fingerprint']
            })

    return {
        'total_duration': total_duration,
        'total_duration_hours': round(total_duration / 3600, 1),
        'file_count': len(audio_files),
        'files': files_info
    }


def compare_audiobooks_deep(source_path, dest_path):
    """
    Deep comparison of two audiobook folders using audio fingerprinting.

    This goes beyond file hashes to detect:
    - Same recording in different formats/bitrates
    - Partial copies (one is subset of other)
    - Different recordings of same book (different narrators)
    - Corrupt/unreadable files

    Returns dict with:
        - same_recording: True if audio content matches
        - source_is_subset: True if source is partial copy of dest
        - dest_is_subset: True if dest is partial copy of source
        - recording_similarity: 0.0-1.0 how similar the recordings are
        - recommendation: 'keep_source', 'keep_dest', 'keep_both', 'merge'
        - source_corrupt: True if source files are corrupt/unreadable
        - dest_corrupt: True if dest files are corrupt/unreadable
    """
    source_info = analyze_audiobook_completeness(source_path)
    dest_info = analyze_audiobook_completeness(dest_path)

    # Check for corrupt files (files exist but can't be read/have no duration)
    source_corrupt = source_info['file_count'] > 0 and source_info['total_duration'] == 0
    dest_corrupt = dest_info['file_count'] > 0 and dest_info['total_duration'] == 0

    result = {
        'source_duration': source_info['total_duration'],
        'source_duration_hours': source_info.get('total_duration_hours', 0),
        'dest_duration': dest_info['total_duration'],
        'dest_duration_hours': dest_info.get('total_duration_hours', 0),
        'source_files': source_info['file_count'],
        'dest_files': dest_info['file_count'],
        'source_readable_files': len(source_info['files']),
        'dest_readable_files': len(dest_info['files']),
        'same_recording': False,
        'source_is_subset': False,
        'dest_is_subset': False,
        'recording_similarity': 0.0,
        'source_corrupt': source_corrupt,
        'dest_corrupt': dest_corrupt,
        'recommendation': 'keep_both'
    }

    # Handle corrupt files
    if source_corrupt and not dest_corrupt:
        result['recommendation'] = 'keep_dest'
        result['reason'] = 'Source files are corrupt/unreadable'
        return result
    elif dest_corrupt and not source_corrupt:
        result['recommendation'] = 'keep_source'
        result['reason'] = 'Destination files are corrupt/unreadable'
        return result
    elif source_corrupt and dest_corrupt:
        result['recommendation'] = 'keep_both'
        result['reason'] = 'Both versions have corrupt/unreadable files'
        return result

    if not source_info['files'] or not dest_info['files']:
        return result

    # Compare first file fingerprints to detect same recording
    source_first_fp = source_info['files'][0].get('fingerprint_start')
    dest_first_fp = dest_info['files'][0].get('fingerprint_start')

    if source_first_fp and dest_first_fp:
        similarity = compare_fingerprints(source_first_fp, dest_first_fp)
        result['recording_similarity'] = round(similarity, 2)

        # High similarity = same recording
        if similarity >= 0.7:
            result['same_recording'] = True

            # Determine which is more complete
            source_dur = source_info['total_duration']
            dest_dur = dest_info['total_duration']

            if source_dur > dest_dur * 1.1:  # Source is 10%+ longer
                result['dest_is_subset'] = True
                result['recommendation'] = 'keep_source'
                result['reason'] = f'Source is more complete ({source_dur//60}min vs {dest_dur//60}min)'
            elif dest_dur > source_dur * 1.1:  # Dest is 10%+ longer
                result['source_is_subset'] = True
                result['recommendation'] = 'keep_dest'
                result['reason'] = f'Destination is more complete ({dest_dur//60}min vs {source_dur//60}min)'
            else:
                # Similar length - prefer the properly named one (dest)
                result['recommendation'] = 'keep_dest'
                result['reason'] = 'Same recording, similar length - keeping properly named version'
        else:
            # Different recordings
            result['reason'] = f'Different recordings ({similarity:.0%} similarity)'

    return result


def compare_book_folders(source_path, dest_path, deep_analysis=True):
    """
    Compare two book folders to determine if they contain the same audiobook.

    Args:
        source_path: Path to source folder
        dest_path: Path to destination folder
        deep_analysis: If True, use audio fingerprinting for comparison (slower but more accurate)

    Returns dict with:
        - identical: True if folders contain the same audio files
        - source_only: Files only in source
        - dest_only: Files only in destination
        - matching: Files that match between both
        - source_better: True if source has more/better files
        - dest_better: True if destination has more/better files
        - deep_analysis: Results from audio fingerprint comparison (if enabled)
    """
    source = Path(source_path)
    dest = Path(dest_path)

    # Get audio files from both folders
    source_audio = [f for f in source.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
    dest_audio = [f for f in dest.rglob('*') if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]

    # Build signature maps
    source_sigs = {}
    for f in source_audio:
        sig = get_file_signature(str(f))
        if sig:
            source_sigs[sig] = f

    dest_sigs = {}
    for f in dest_audio:
        sig = get_file_signature(str(f))
        if sig:
            dest_sigs[sig] = f

    # Compare
    source_sig_set = set(source_sigs.keys())
    dest_sig_set = set(dest_sigs.keys())

    matching = source_sig_set & dest_sig_set
    source_only = source_sig_set - dest_sig_set
    dest_only = dest_sig_set - source_sig_set

    # Calculate total sizes
    source_total_size = sum(os.path.getsize(str(f)) for f in source_audio)
    dest_total_size = sum(os.path.getsize(str(f)) for f in dest_audio)

    result = {
        'identical': len(matching) > 0 and len(source_only) == 0 and len(dest_only) == 0,
        'matching_count': len(matching),
        'source_only_count': len(source_only),
        'dest_only_count': len(dest_only),
        'source_files': len(source_audio),
        'dest_files': len(dest_audio),
        'source_size': source_total_size,
        'dest_size': dest_total_size,
        'source_better': source_total_size > dest_total_size or len(source_audio) > len(dest_audio),
        'dest_better': dest_total_size > source_total_size or len(dest_audio) > len(source_audio),
        'overlap_ratio': len(matching) / max(len(source_sig_set), len(dest_sig_set), 1)
    }

    # Determine if it's effectively the same book (high overlap or one is subset of other)
    if result['overlap_ratio'] >= 0.8:
        result['same_book'] = True
    elif len(matching) > 0 and (len(source_only) == 0 or len(dest_only) == 0):
        # One is a subset of the other
        result['same_book'] = True
    else:
        result['same_book'] = False

    # Deep analysis using audio fingerprinting (detects same recording in different formats,
    # corrupt files, partial copies)
    if deep_analysis and not result['same_book']:
        try:
            deep_result = compare_audiobooks_deep(source_path, dest_path)
            result['deep_analysis'] = deep_result

            # Update same_book based on deep analysis
            if deep_result.get('same_recording'):
                result['same_book'] = True
                result['same_recording'] = True

            # Update better flags based on corrupt file detection
            if deep_result.get('dest_corrupt') and not deep_result.get('source_corrupt'):
                result['source_better'] = True
                result['dest_better'] = False
                result['dest_corrupt'] = True
            elif deep_result.get('source_corrupt') and not deep_result.get('dest_corrupt'):
                result['source_better'] = False
                result['dest_better'] = True
                result['source_corrupt'] = True

            # Copy recommendation
            result['recommendation'] = deep_result.get('recommendation', 'keep_both')
            result['reason'] = deep_result.get('reason', '')

        except Exception as e:
            logger.debug(f"Deep analysis failed: {e}")
            result['deep_analysis'] = None

    return result


def deep_scan_library(config):
    """
    Deep scan library - the AUTISTIC LIBRARIAN approach.
    Finds ALL issues, duplicates, and structural problems.
    """
    conn = get_db()
    c = conn.cursor()

    checked = 0  # Total book folders examined
    scanned = 0  # New books added to tracking
    queued = 0   # Books added to fix queue
    issues_found = {}  # path -> list of issues

    # Track files for duplicate detection
    file_signatures = {}  # signature -> list of paths
    file_names = {}  # basename -> list of paths

    logger.info("=== DEEP LIBRARY SCAN STARTING ===")

    for lib_path_str in config.get('library_paths', []):
        lib_path = Path(lib_path_str)
        if not lib_path.exists():
            logger.warning(f"Library path not found: {lib_path}")
            continue

        logger.info(f"Scanning: {lib_path}")

        # First pass: Find all audio files to understand actual book locations
        all_audio_files = find_audio_files(lib_path)
        logger.info(f"Found {len(all_audio_files)} audio files")

        # Track file signatures for duplicate detection
        for audio_file in all_audio_files:
            sig = get_file_signature(audio_file)
            if sig:
                if sig not in file_signatures:
                    file_signatures[sig] = []
                file_signatures[sig].append(audio_file)

            basename = os.path.basename(audio_file).lower()
            if basename not in file_names:
                file_names[basename] = []
            file_names[basename].append(audio_file)

        # NEW: Detect loose files in library root (no folder structure)
        loose_files = []
        for item in lib_path.iterdir():
            if item.is_file() and item.suffix.lower() in AUDIO_EXTENSIONS:
                loose_files.append(item)

        if loose_files:
            logger.info(f"Found {len(loose_files)} loose audio files in library root")
            for loose_file in loose_files:
                # Parse filename to extract searchable title
                filename = loose_file.stem  # filename without extension
                cleaned_filename = clean_search_title(filename)
                # Issue #132: Resolve path to prevent duplicates from symlinks/mount differences
                path_str = str(loose_file.resolve())

                # Check if already in books table
                c.execute('SELECT id, user_locked FROM books WHERE path = ?', (path_str,))
                existing = c.fetchone()

                if existing:
                    # Skip user-locked books
                    if existing['user_locked']:
                        continue
                    book_id = existing['id']
                else:
                    # Create books record for the loose file
                    c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                VALUES (?, ?, ?, ?)''',
                             (path_str, 'Unknown', cleaned_filename, 'loose_file'))
                    book_id = c.lastrowid

                # Add to queue with special "loose_file" reason
                c.execute('''INSERT OR REPLACE INTO queue
                            (book_id, reason, added_at, priority)
                            VALUES (?, ?, ?, ?)''',
                         (book_id, f'loose_file_needs_folder:{filename}',
                          datetime.now().isoformat(), 1))  # High priority
                # Set verification_layer=1 to start at Layer 1 (API lookup)
                c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                queued += 1
                issues_found[path_str] = ['loose_file_no_folder']
                logger.info(f"Queued loose file: {filename} -> search for: {cleaned_filename}")

        # NEW: Detect loose EBOOK files in library root (when ebook management enabled)
        if config.get('ebook_management', False):
            loose_ebooks = []
            for item in lib_path.iterdir():
                if item.is_file() and item.suffix.lower() in EBOOK_EXTENSIONS:
                    loose_ebooks.append(item)

            if loose_ebooks:
                logger.info(f"Found {len(loose_ebooks)} loose ebook files in library root")
                for loose_ebook in loose_ebooks:
                    filename = loose_ebook.stem
                    cleaned_filename = clean_search_title(filename)
                    path_str = str(loose_ebook)

                    c.execute('SELECT id, user_locked FROM books WHERE path = ?', (path_str,))
                    existing = c.fetchone()

                    if existing:
                        # Skip user-locked books
                        if existing['user_locked']:
                            continue
                        book_id = existing['id']
                    else:
                        c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                    VALUES (?, ?, ?, ?)''',
                                 (path_str, 'Unknown', cleaned_filename, 'ebook_loose'))
                        book_id = c.lastrowid

                    c.execute('''INSERT OR REPLACE INTO queue
                                (book_id, reason, added_at, priority)
                                VALUES (?, ?, ?, ?)''',
                             (book_id, f'ebook_loose:{filename}',
                              datetime.now().isoformat(), 2))
                    # Set verification_layer=1 to start at Layer 1 (API lookup)
                    c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                    queued += 1
                    issues_found[path_str] = ['ebook_loose_file']
                    logger.info(f"Queued loose ebook: {filename}")

        # Second pass: Analyze folder structure
        for author_dir in lib_path.iterdir():
            if not author_dir.is_dir():
                continue

            author = author_dir.name

            # Issue #46: Skip watch folder if it's inside the library path
            # This prevents the watch folder name from being used as an author
            watch_folder = config.get('watch_folder', '').strip()
            if watch_folder:
                try:
                    if author_dir.resolve() == Path(watch_folder).resolve():
                        logger.debug(f"Skipping watch folder at author level: {author}")
                        continue
                except Exception:
                    pass

            # Skip system folders at author level - these are NEVER authors
            author_system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                                     'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                                     '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                                     'streams', '.streams', '.cache', '.metadata', '@eaDir', '#recycle'}
            if author.lower() in author_system_folders or author.startswith('.') or author.startswith('@'):
                logger.debug(f"Skipping system folder at author level: {author}")
                continue

            author_issues = analyze_author(author)

            # Check if "author" folder is actually a book (has audio files directly)
            direct_audio = [f for f in author_dir.iterdir()
                          if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
            if direct_audio:
                # This "author" folder is actually a book! Process it as such.
                # This handles flat library structures where books are directly in the root
                # (common with torrent downloads, Calibre-style exports, chaos test libraries, etc.)
                issues_found[str(author_dir)] = author_issues + ["author_folder_has_audio_files"]
                logger.info(f"Flat book folder detected (processing as book): {author}")

                # Extract author/title from folder name
                flat_author, flat_title = extract_author_title(author)
                # Issue #132: Resolve path to prevent duplicates
                flat_path = str(author_dir.resolve())

                checked += 1

                # Check if already tracked
                c.execute('SELECT id, status, profile, user_locked FROM books WHERE path = ?', (flat_path,))
                existing_flat = c.fetchone()

                if existing_flat:
                    if existing_flat['user_locked']:
                        continue
                    if existing_flat['status'] in ['verified', 'fixed']:
                        has_profile = existing_flat['profile'] and len(existing_flat['profile']) > 2
                        if has_profile:
                            continue
                    flat_book_id = existing_flat['id']
                else:
                    c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                 VALUES (?, ?, ?, 'pending')''', (flat_path, flat_author, flat_title))
                    conn.commit()
                    flat_book_id = c.lastrowid
                    scanned += 1
                    logger.info(f"Added flat book: {flat_author} - {flat_title}")

                # Queue for processing
                c.execute('SELECT id FROM queue WHERE book_id = ?', (flat_book_id,))
                if not c.fetchone():
                    c.execute('''INSERT INTO queue (book_id, reason, priority)
                                VALUES (?, ?, ?)''',
                             (flat_book_id, 'flat_book_folder', 3))
                    c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (flat_book_id,))
                    conn.commit()
                    queued += 1

                continue  # Don't process subdirs as this is a flat book folder

            # Check if author folder has NO book subfolders (just disc folders)
            subdirs = [d for d in author_dir.iterdir() if d.is_dir()]
            if subdirs:
                all_disc_folders = all(is_disc_chapter_folder(d.name) for d in subdirs)
                if all_disc_folders:
                    issues_found[str(author_dir)] = author_issues + ["author_folder_only_has_disc_folders"]

            for title_dir in author_dir.iterdir():
                if not title_dir.is_dir():
                    continue

                title = title_dir.name
                # Issue #132: Resolve path to prevent duplicates from symlinks/mount differences
                path = str(title_dir.resolve())

                # Issue #53: Strip author prefix from book folder name
                # If folder is "David Baldacci - Dream Town" and parent is "David Baldacci",
                # extract just "Dream Town" as the title
                if author:
                    _, extracted_title = extract_author_title(title)
                    # Only use extracted title if it looks like we stripped the author
                    if extracted_title != title:
                        # Verify the stripped part matches the parent author
                        stripped_author = title[:len(title) - len(extracted_title)].strip(' -–/')
                        if calculate_title_similarity(stripped_author, author) >= 0.85:
                            title = extracted_title
                            logger.debug(f"Stripped author prefix from book folder: '{title_dir.name}' -> '{title}'")

                # Skip if this looks like a disc/chapter folder
                if is_disc_chapter_folder(title):
                    # But flag the parent!
                    issues_found[str(author_dir)] = issues_found.get(str(author_dir), []) + [f"has_disc_folder:{title}"]
                    continue

                # Skip system/metadata folders - these are NEVER books
                # Issue #88: Added @eaDir, #recycle (Synology), .Trash*, .AppleDouble, __MACOSX
                system_folders = {'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
                                  'backup', 'backups', 'old', 'new', 'test', 'tests', 'sample', 'samples',
                                  '.thumbnails', 'thumbnails', 'covers', 'images', 'artwork', 'art',
                                  'extras', 'bonus', 'misc', 'other', 'various', 'unknown', 'unsorted',
                                  'downloads', 'incoming', 'processing', 'completed', 'done', 'failed',
                                  'streams', 'chapters', 'parts', '.streams', '.cache', '.metadata',
                                  '@eadir', '#recycle', '.appledouble', '__macosx', '.trash'}
                if title.lower() in system_folders or title.startswith('.') or title.startswith('@') or title.startswith('#'):
                    logger.debug(f"Skipping system folder: {path}")
                    continue

                # Check if this is a SERIES folder containing book subfolders
                # If so, skip it - we should process the books inside, not the series folder itself
                subdirs = [d for d in title_dir.iterdir() if d.is_dir()]
                if len(subdirs) >= 1:
                    # Count how many look like book folders (numbered, "Book N", etc.)
                    book_folder_patterns = [
                        r'^\d+\s*[-–—:.]?\s*\w',     # "01 Title", "1 - Title", "01. Title"
                        r'^#?\d+\s*[-–—:]',          # "#1 - Title"
                        r'book\s*\d+',               # "Book 1", "Book1"
                        r'vol(ume)?\s*\d+',          # "Volume 1", "Vol 1"
                        r'part\s*\d+',               # "Part 1"
                    ]
                    book_like_count = sum(
                        1 for d in subdirs
                        if any(re.search(p, d.name, re.IGNORECASE) for p in book_folder_patterns)
                    )
                    # Issue #36 fix: Detect series folder even with just 1 book-like subfolder
                    # Also check: if folder has no direct audio but subfolders do, it's a series folder
                    direct_audio = [f for f in title_dir.iterdir()
                                   if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
                    subfolder_has_audio = any(
                        any(f.suffix.lower() in AUDIO_EXTENSIONS for f in d.iterdir() if f.is_file())
                        for d in subdirs
                    )
                    is_series = (book_like_count >= 1) or (not direct_audio and subfolder_has_audio and len(subdirs) >= 1)
                    if is_series:
                        # This is a series folder - process the book subfolders inside it
                        series_name = title  # The folder name is the series name
                        logger.info(f"Processing series folder '{series_name}' with {len(subdirs)} book subfolders: {path}")
                        # Mark in database as series_folder
                        c.execute('SELECT id FROM books WHERE path = ?', (path,))
                        existing = c.fetchone()
                        if existing:
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('series_folder', existing['id']))
                            # Issue #36: Remove from queue if it was previously queued
                            # Series folders should never be in the processing queue
                            c.execute('DELETE FROM queue WHERE book_id = ?', (existing['id'],))
                        else:
                            c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                         VALUES (?, ?, ?, 'series_folder')''', (path, author, title))
                        conn.commit()

                        # Process each book subfolder inside the series
                        for book_dir in subdirs:
                            if not book_dir.is_dir():
                                continue
                            book_title = book_dir.name
                            book_path = str(book_dir)

                            # Issue #88: Skip system folders inside series (Synology @eaDir, etc.)
                            if book_title.lower() in system_folders or book_title.startswith('.') or book_title.startswith('@') or book_title.startswith('#'):
                                logger.debug(f"Skipping system folder in series: {book_path}")
                                continue

                            # Issue #53: Strip author prefix from book folder name
                            # If folder is "David Baldacci - Dream Town" and parent is "David Baldacci",
                            # extract just "Dream Town" as the title
                            if author:
                                _, extracted_title = extract_author_title(book_title)
                                # Only use extracted title if it looks like we stripped the author
                                if extracted_title != book_title:
                                    # Verify the stripped part matches the parent author
                                    stripped_author = book_title[:len(book_title) - len(extracted_title)].strip(' -–/')
                                    if calculate_title_similarity(stripped_author, author) >= 0.85:
                                        book_title = extracted_title
                                        logger.debug(f"Stripped author prefix from book folder: '{book_dir.name}' -> '{book_title}'")

                            # Skip disc/chapter folders
                            if is_disc_chapter_folder(book_title):
                                continue

                            # Check for audio files in this book folder
                            book_audio = [f for f in book_dir.iterdir()
                                         if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
                            if not book_audio:
                                # Also check one level deeper (for disc subfolders)
                                book_audio = [f for f in book_dir.rglob('*')
                                             if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
                            if not book_audio:
                                continue  # No audio files, skip

                            checked += 1

                            # Check if already tracked
                            c.execute('SELECT id, status, profile, user_locked FROM books WHERE path = ?', (book_path,))
                            existing_book = c.fetchone()

                            if existing_book:
                                if existing_book['user_locked']:
                                    continue
                                if existing_book['status'] in ['verified', 'fixed']:
                                    has_profile = existing_book['profile'] and len(existing_book['profile']) > 2
                                    if has_profile:
                                        continue
                                book_id = existing_book['id']
                            else:
                                c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                             VALUES (?, ?, ?, 'pending')''', (book_path, author, book_title))
                                conn.commit()
                                book_id = c.lastrowid
                                scanned += 1

                            # Queue for processing
                            c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                            if not c.fetchone():
                                c.execute('''INSERT INTO queue (book_id, reason, priority)
                                            VALUES (?, ?, ?)''',
                                         (book_id, f'series_book:{series_name}', 3))
                                c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                                conn.commit()
                                queued += 1

                        continue  # Done with this series folder

                # Check if this folder contains multiple AUDIO FILES that look like different books
                # (e.g., "Book 1.m4b", "Book 2.m4b" or "Necroscope Book 1.m4b", "Necroscope Book 2.m4b")
                # Issue #29 fix: Use smart detection to avoid false positives on chapter files
                audio_files = [f for f in title_dir.iterdir()
                               if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
                if len(audio_files) >= 2:
                    # Use smart multibook detection instead of brittle regex
                    multibook_result = detect_multibook_vs_chapters(audio_files, config)

                    if multibook_result['is_multibook']:
                        # Confirmed multi-book collection - skip it
                        logger.info(f"Skipping multi-book collection ({multibook_result['reason']}): {path}")
                        c.execute('SELECT id FROM books WHERE path = ?', (path,))
                        existing = c.fetchone()
                        if existing:
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('multi_book_files', existing['id']))
                            # Issue #36: Remove from queue if it was previously queued
                            c.execute('DELETE FROM queue WHERE book_id = ?', (existing['id'],))
                        else:
                            c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                         VALUES (?, ?, ?, 'multi_book_files')''', (path, author, title))
                        conn.commit()
                        continue

                # This is a valid book folder - count it
                checked += 1

                # Analyze title
                title_issues = analyze_title(title, author)
                cleaned_title, clean_issues = clean_title(title)

                all_issues = author_issues + title_issues + clean_issues

                # NOTE: Removed "reversed structure detection" in beta.69
                # The old code tried to guess if author/title were swapped based on regex patterns.
                # This caused false positives (Issue #52). Instead, we now let all items go through
                # the normal API lookup flow. If the structure IS wrong, APIs won't find matches
                # and the item will end up in "Needs Attention" for the user to fix manually.
                # Trust the APIs, don't guess with patterns.

                # Check for nested structure (disc folders inside book folder)
                nested_dirs = [d for d in title_dir.iterdir() if d.is_dir()]
                disc_dirs = [d for d in nested_dirs if is_disc_chapter_folder(d.name)]
                if disc_dirs:
                    all_issues.append(f"has_{len(disc_dirs)}_disc_folders")

                # Check for ebook files
                ebook_files = [f for f in title_dir.rglob('*') if f.suffix.lower() in EBOOK_EXTENSIONS]
                audio_in_folder = [f for f in title_dir.rglob('*') if f.suffix.lower() in AUDIO_EXTENSIONS]

                if ebook_files:
                    if audio_in_folder:
                        # Mixed folder - ebooks with audiobooks
                        all_issues.append(f"has_{len(ebook_files)}_ebook_files")
                    elif config.get('ebook_management', False):
                        # Ebook-only folder - queue for ebook organization
                        all_issues.append('ebook_only_folder')
                        logger.info(f"Found ebook-only folder: {path} ({len(ebook_files)} ebooks)")

                # Store issues
                if all_issues:
                    issues_found[path] = all_issues

                # Add to database
                c.execute('SELECT id, status, profile, user_locked FROM books WHERE path = ?', (path,))
                existing = c.fetchone()

                if existing:
                    # Skip user-locked books - user has manually set metadata, never change it
                    if existing['user_locked']:
                        continue

                    # Skip books that are properly verified (have profile data)
                    # Re-queue "legacy" verified books that have no profile
                    if existing['status'] in ['verified', 'fixed']:
                        has_profile = existing['profile'] and len(existing['profile']) > 2
                        if has_profile:
                            continue  # Properly verified, skip
                        else:
                            # Legacy verified without profile - re-queue for proper verification
                            logger.info(f"Re-queuing legacy verified book (no profile): {author}/{title}")
                            c.execute('UPDATE books SET status = ?, verification_layer = 1 WHERE id = ?',
                                     ('pending', existing['id']))
                            c.execute('''INSERT OR IGNORE INTO queue (book_id, reason, priority)
                                        VALUES (?, ?, ?)''',
                                     (existing['id'], 'legacy_needs_profile', 3))
                            queued += 1
                            continue
                    book_id = existing['id']
                else:
                    c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                 VALUES (?, ?, ?, 'pending')''', (path, author, title))
                    conn.commit()
                    book_id = c.lastrowid
                    scanned += 1

                # Add to queue if has issues
                if all_issues:
                    # Skip multi-book collections - they need manual splitting, not renaming
                    if 'multi_book_collection' in all_issues:
                        logger.info(f"Skipping multi-book collection (needs manual split): {path}")
                        c.execute('UPDATE books SET status = ? WHERE id = ?',
                                  ('needs_split', book_id))
                        conn.commit()
                        continue

                    reason = "; ".join(all_issues[:3])  # First 3 issues
                    if len(all_issues) > 3:
                        reason += f" (+{len(all_issues)-3} more)"

                    c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
                    if not c.fetchone():
                        c.execute('''INSERT INTO queue (book_id, reason, priority)
                                    VALUES (?, ?, ?)''',
                                 (book_id, reason, min(len(all_issues), 10)))
                        # Set verification_layer=1 to start at Layer 1 (API lookup)
                        c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                        conn.commit()
                        queued += 1

    # Third pass: Flag duplicates
    logger.info("Checking for duplicates...")
    duplicate_count = 0

    for sig, paths in file_signatures.items():
        if len(paths) > 1:
            duplicate_count += 1
            for p in paths:
                book_dir = str(Path(p).parent)
                if book_dir in issues_found:
                    issues_found[book_dir].append(f"duplicate_file:{os.path.basename(p)}")
                else:
                    issues_found[book_dir] = [f"duplicate_file:{os.path.basename(p)}"]

    logger.info(f"Found {duplicate_count} potential duplicate file sets")

    # Update daily stats (INSERT if not exists, then UPDATE to preserve other columns)
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('''UPDATE stats SET
                 scanned = COALESCE(scanned, 0) + ?,
                 queued = COALESCE(queued, 0) + ?
                 WHERE date = ?''', (scanned, queued, today))
    conn.commit()
    conn.close()

    logger.info(f"=== DEEP SCAN COMPLETE ===")
    logger.info(f"Checked: {checked} book folders")
    logger.info(f"Scanned: {scanned} new books added to tracking")
    logger.info(f"Queued: {queued} books need fixing")
    logger.info(f"Already correct: {checked - queued} books")

    return checked, scanned, queued


def scan_library(config, blocking=True):
    """
    Wrapper that calls deep scan with concurrency protection.

    Issue #61: Prevents concurrent scans that cause SQLite UNIQUE constraint
    and database locked errors.

    Args:
        config: Configuration dictionary
        blocking: If True, wait for lock. If False, return immediately if busy.

    Returns:
        (checked, scanned, queued) tuple, or (0, 0, 0) if non-blocking and busy
    """
    global scan_in_progress

    if not blocking:
        # Non-blocking mode: return immediately if scan in progress
        if scan_in_progress:
            logger.info("Scan already in progress, skipping")
            return (0, 0, 0)

    with SCAN_LOCK:
        scan_in_progress = True
        try:
            return deep_scan_library(config)
        finally:
            scan_in_progress = False

def deep_verify_all_books(config):
    """
    Deep Verification Mode - "Hail Mary" full library audit.

    Queues ALL books for API verification regardless of their current status
    or how "clean" they look. This catches cases where folder structure looks
    correct but the author attribution is actually wrong.

    WARNING: This is expensive! It will:
    - Make API calls for EVERY book in your library
    - Take a very long time (hours for large libraries)
    - Use significant API quota

    Use this when:
    - First importing a sketchy collection
    - Suspecting widespread attribution issues
    - Doing a one-time full audit
    """
    conn = get_db()
    c = conn.cursor()

    # Get ALL books that aren't user-locked or in special states
    c.execute('''SELECT id, path, current_author, current_title, status, verification_layer, profile
                 FROM books
                 WHERE (user_locked IS NULL OR user_locked = 0)
                   AND status NOT IN ('series_folder', 'multi_book_files', 'needs_split', 'needs_attention')''')

    all_books = [dict(row) for row in c.fetchall()]

    queued_count = 0
    already_verified = 0
    skipped = 0

    for book in all_books:
        book_id = book['id']

        # Check if already has high-confidence profile from multiple sources
        profile_json = book['profile']
        if profile_json:
            try:
                profile = json.loads(profile_json)
                # If profile has 3+ verification sources and 90%+ confidence, skip
                layers_used = profile.get('verification_layers_used', [])
                confidence = profile.get('overall_confidence', 0)
                if len(layers_used) >= 3 and confidence >= 90:
                    already_verified += 1
                    continue
            except:
                pass

        # Reset verification status and queue for fresh verification
        c.execute('''UPDATE books
                     SET status = 'pending',
                         verification_layer = 1,
                         profile = NULL,
                         confidence = 0
                     WHERE id = ?''', (book_id,))

        # Add to queue if not already there
        c.execute('SELECT id FROM queue WHERE book_id = ?', (book_id,))
        if not c.fetchone():
            c.execute('''INSERT INTO queue (book_id, reason, priority)
                        VALUES (?, 'deep_verification', 5)''', (book_id,))
            queued_count += 1
        else:
            # Already in queue - just update reason
            c.execute('UPDATE queue SET reason = ?, priority = 5 WHERE book_id = ?',
                     ('deep_verification', book_id))
            queued_count += 1

    conn.commit()
    conn.close()

    logger.info(f"=== DEEP VERIFICATION QUEUED ===")
    logger.info(f"Queued for verification: {queued_count}")
    logger.info(f"Already fully verified: {already_verified}")
    logger.info(f"Total books checked: {len(all_books)}")

    return {
        'queued': queued_count,
        'already_verified': already_verified,
        'total': len(all_books)
    }


def check_rate_limit(config):
    """Check if we're within API rate limits. Returns (allowed, calls_this_hour, limit)."""
    conn = get_db()
    c = conn.cursor()

    max_per_hour = config.get('max_requests_per_hour', 30)

    # Get calls in the last hour
    one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
    today = datetime.now().strftime('%Y-%m-%d')

    c.execute('SELECT api_calls FROM stats WHERE date = ?', (today,))
    row = c.fetchone()
    calls_today = row['api_calls'] if row else 0

    conn.close()

    # Simple hourly check - in practice we're using daily count as approximation
    # For more accurate tracking, we'd need a separate API call log table
    allowed = calls_today < max_per_hour
    return allowed, calls_today, max_per_hour


# ============================================================================
# NEW AUDIO-FIRST IDENTIFICATION SYSTEM
# ============================================================================
# Philosophy: Audio content is the SOURCE OF TRUTH, not folder names.
# The narrator literally says "This is [Book] by [Author]" in the intro.
# We transcribe that and use AI to parse it - THEN confirm with APIs.
# ============================================================================

def transcribe_audio_intro(file_path, duration_seconds=45):
    """
    Transcribe the INTRO of an audiobook (first 45 seconds).
    This is where narrators typically announce: title, author, narrator.
    Using 45 seconds keeps file size small for Gemini API (<5MB).

    Returns: transcribed text or None
    """
    import subprocess
    import tempfile

    logger.info(f"[LAYER 1/AUDIO] Transcribing intro: {os.path.basename(str(file_path))}")

    try:
        # Check for symlink - resolve to real file for reading
        file_path = Path(file_path)
        if file_path.is_symlink():
            file_path = file_path.resolve()
            logger.debug(f"[LAYER 1/AUDIO] Resolved symlink to: {file_path}")

        # First, try faster-whisper (local, free)
        whisper_model = get_whisper_model()
        if whisper_model:
            # Extract intro clip - keep high quality audio for accurate transcription
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = tmp.name

            # Use -ss BEFORE -i for fast input seeking
            # Keep 22050Hz stereo for better quality than 16kHz mono
            result = subprocess.run([
                'ffmpeg', '-y',
                '-ss', '0',  # Fast seek to start
                '-i', str(file_path),
                '-t', str(duration_seconds),  # Extract only this duration
                '-acodec', 'libmp3lame', '-ar', '22050', '-ab', '128k',
                tmp_path
            ], capture_output=True, timeout=120)  # 120s for large m4b files with moov at end

            if result.returncode == 0:
                # Build initial prompt from folder hints to help with proper noun spelling
                # Per OpenAI: "Fictitious prompts can steer the model to use particular spellings"
                initial_prompt = "This is an audiobook introduction. The narrator typically announces the book title, author name, and narrator."

                # Add folder hints to the prompt if available
                folder_path = Path(file_path).parent
                folder_name = folder_path.name
                parent_name = folder_path.parent.name if folder_path.parent else ""

                # Extract potential author/title from folder structure for spelling hints
                hints = []
                if parent_name and parent_name not in ['audiobooks', 'Unknown', '']:
                    hints.append(parent_name)
                if folder_name and folder_name not in ['audiobooks', 'Unknown', '']:
                    hints.append(folder_name)

                if hints:
                    initial_prompt += f" Possible names: {', '.join(hints)}."

                # Transcribe with better settings for accuracy
                segments, info = whisper_model.transcribe(
                    tmp_path,
                    beam_size=5,
                    language="en",  # Assume English for audiobooks
                    initial_prompt=initial_prompt,
                    word_timestamps=False,  # Not needed, saves processing
                    vad_filter=True,  # Filter out silence/music for cleaner transcript
                    vad_parameters=dict(
                        min_silence_duration_ms=2000,  # 2 seconds - narrator pauses are normal
                        speech_pad_ms=500,              # Pad speech segments
                        threshold=0.3                   # Lower = less aggressive filtering
                    )
                )
                transcript = " ".join([seg.text for seg in segments]).strip()
                os.unlink(tmp_path)

                if transcript and len(transcript) > 20:
                    logger.info(f"[LAYER 1/AUDIO] Transcribed {len(transcript)} chars via whisper")
                    return transcript

            try:
                os.unlink(tmp_path)
            except:
                pass

        # Fallback: OpenAI Whisper API
        secrets = load_secrets()
        openai_key = secrets.get('openai_api_key') if secrets else None

        if openai_key:
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = tmp.name

            # Use -ss BEFORE -i for fast input seeking
            subprocess.run([
                'ffmpeg', '-y',
                '-ss', '0',
                '-i', str(file_path),
                '-t', str(duration_seconds),
                '-acodec', 'libmp3lame', '-ar', '16000', '-ac', '1',
                tmp_path
            ], capture_output=True, timeout=120)  # 120s for large m4b files

            with open(tmp_path, 'rb') as audio_file:
                response = requests.post(
                    'https://api.openai.com/v1/audio/transcriptions',
                    headers={'Authorization': f'Bearer {openai_key}'},
                    files={'file': audio_file},
                    data={'model': 'whisper-1'},
                    timeout=90
                )

            os.unlink(tmp_path)

            if response.status_code == 200:
                transcript = response.json().get('text', '')
                logger.info(f"[LAYER 1/AUDIO] Transcribed {len(transcript)} chars via OpenAI")
                return transcript

        # Fallback 3: Use Gemini audio understanding (not transcription, but can extract intro)
        config = load_config()
        gemini_key = config.get('gemini_api_key')
        if gemini_key and not is_circuit_open('gemini'):
            logger.info("[LAYER 1/AUDIO] Trying Gemini audio analysis as fallback")
            rate_limit_wait('gemini')  # Respect rate limits
            # Extract intro clip
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = tmp.name

            # Use -ss BEFORE -i for fast input seeking
            result = subprocess.run([
                'ffmpeg', '-y',
                '-ss', '0',
                '-i', str(file_path),
                '-t', str(duration_seconds),
                '-acodec', 'libmp3lame', '-ar', '16000', '-ac', '1',
                tmp_path
            ], capture_output=True, timeout=120)  # 120s for large m4b files

            if result.returncode == 0 and os.path.exists(tmp_path):
                # Read audio file and encode as base64
                import base64
                with open(tmp_path, 'rb') as f:
                    audio_data = base64.standard_b64encode(f.read()).decode('utf-8')
                os.unlink(tmp_path)

                # Send to Gemini with audio
                prompt = """Listen to this audiobook intro. Write out exactly what the narrator says, word for word.
Focus on:
- The book title announcement
- The author name (who WROTE the book)
- The narrator name (who is READING the book)
- Any series information

Just write what you hear - no interpretation or formatting."""

                # Force gemini-2.0-flash for audio - other models don't support audio input
                gemini_model = 'gemini-2.0-flash'  # Audio requires this model
                response = requests.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent",
                    params={'key': gemini_key},
                    json={
                        'contents': [{
                            'parts': [
                                {'text': prompt},
                                {'inline_data': {'mime_type': 'audio/mp3', 'data': audio_data}}
                            ]
                        }]
                    },
                    timeout=60
                )

                if response.status_code == 200:
                    text = response.json().get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                    if text and len(text) > 20:
                        logger.info(f"[LAYER 1/AUDIO] Gemini extracted {len(text)} chars from audio")
                        return text
                elif response.status_code == 429:
                    # Rate limit or quota exceeded - trip circuit breaker
                    try:
                        error_detail = response.json().get('error', {}).get('message', response.text[:500])
                    except:
                        error_detail = response.text[:500]
                    logger.warning(f"[LAYER 1/AUDIO] Gemini audio analysis failed: {response.status_code} - {error_detail}")
                    if 'quota' in error_detail.lower() and ('limit: 0' in error_detail or 'exceeded' in error_detail.lower()):
                        logger.warning("[LAYER 1/AUDIO] Daily quota exhausted - tripping circuit breaker")
                        record_api_failure('gemini')
                        record_api_failure('gemini')  # Trip immediately
                else:
                    # Log full error for debugging
                    try:
                        error_detail = response.json().get('error', {}).get('message', response.text[:200])
                    except:
                        error_detail = response.text[:200]
                    logger.warning(f"[LAYER 1/AUDIO] Gemini audio analysis failed: {response.status_code} - {error_detail}")
            else:
                try:
                    os.unlink(tmp_path)
                except:
                    pass

        logger.warning("[LAYER 1/AUDIO] No transcription method available (no whisper, no OpenAI key, Gemini failed)")

    except Exception as e:
        logger.warning(f"[LAYER 1/AUDIO] Transcription failed: {e}")

    return None


def parse_transcript_with_ai(transcript, folder_hint=None, config=None):
    """
    Use AI to extract structured metadata from a transcription.

    The narrator typically says something like:
    "This is The Drawing of the Three, book two of the Dark Tower series,
     by Stephen King, narrated by Frank Muller"

    Returns: dict with author, title, narrator, series, series_num, confidence
    """
    if not transcript or len(transcript) < 20:
        return None

    config = config or load_config()
    # Merge secrets to get API keys
    secrets = load_secrets()
    config = {**config, **secrets}

    prompt = f"""You are extracting audiobook metadata from a transcription of the book's intro.

TRANSCRIPTION (first 90 seconds of audiobook):
\"\"\"{transcript[:1500]}\"\"\"

{f"FOLDER HINT (may be wrong, use only if transcript is unclear): {folder_hint}" if folder_hint else ""}

The narrator usually announces the book at the start. Extract:
1. TITLE - The book's title
2. AUTHOR - Who WROTE the book (not the narrator!)
3. NARRATOR - Who is READING/performing the book
4. SERIES - Series name if mentioned (null if standalone)
5. SERIES_NUM - Book number in series if mentioned (null if not)

IMPORTANT:
- The AUTHOR is the writer, NOT the narrator/reader
- If narrator says "written by X" or "by X", X is the author
- If narrator says "read by Y" or "narrated by Y", Y is the narrator
- Many intros say "Title by Author, narrated by Narrator"

Return ONLY valid JSON:
{{"title": "Book Title", "author": "Author Name", "narrator": "Narrator Name", "series": "Series Name or null", "series_num": "1 or null", "confidence": "high/medium/low"}}

If you cannot identify the book from the transcript, return:
{{"title": null, "author": null, "narrator": null, "series": null, "series_num": null, "confidence": "none", "reason": "why"}}"""

    try:
        # Try local Ollama FIRST (no cold starts, no rate limits)
        ollama_url = config.get('ollama_url', 'http://localhost:11434')
        ollama_model = config.get('ollama_model', 'qwen2.5:0.5b')  # Tiny model (912MB VRAM)

        try:
            ollama_response = requests.post(
                f"{ollama_url}/api/generate",
                json={
                    'model': ollama_model,
                    'prompt': prompt,
                    'stream': False,
                    'options': {'temperature': 0.1}
                },
                timeout=60
            )

            if ollama_response.status_code == 200:
                ollama_text = ollama_response.json().get('response', '')
                result = parse_json_response(ollama_text)
                if result and result.get('author') and result.get('title'):
                    logger.info(f"[LAYER 1/AUDIO] Local LLM parsed: {result.get('author', '?')} - {result.get('title', '?')}")
                    return result
                else:
                    logger.debug(f"[LAYER 1/AUDIO] Local LLM returned incomplete result, trying external APIs")
        except requests.exceptions.RequestException as e:
            logger.debug(f"[LAYER 1/AUDIO] Local Ollama not available: {e}")

        # Fallback to Gemini
        if config.get('gemini_api_key'):
            api_key = config.get('gemini_api_key')
            model = config.get('gemini_model', 'gemini-2.0-flash')

            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                params={'key': api_key},
                json={'contents': [{'parts': [{'text': prompt}]}]},
                timeout=30
            )

            if response.status_code == 200:
                text = response.json().get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
                result = parse_json_response(text)
                if result:
                    logger.info(f"[LAYER 1/AUDIO] Gemini parsed: {result.get('author', '?')} - {result.get('title', '?')}")
                    return result

        # Fallback to OpenRouter
        if config.get('openrouter_api_key'):
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    'Authorization': f"Bearer {config['openrouter_api_key']}",
                    'Content-Type': 'application/json'
                },
                json={
                    'model': config.get('openrouter_model', 'google/gemini-flash-1.5'),
                    'messages': [{'role': 'user', 'content': prompt}]
                },
                timeout=30
            )

            if response.status_code == 200:
                text = response.json().get('choices', [{}])[0].get('message', {}).get('content', '')
                result = parse_json_response(text)
                if result:
                    logger.info(f"[LAYER 1/AUDIO] AI parsed: {result.get('author', '?')} - {result.get('title', '?')}")
                    return result

    except Exception as e:
        logger.warning(f"[LAYER 1/AUDIO] AI parsing failed: {e}")

    return None


def process_layer_1_audio(config, limit=None):
    """Wrapper for extracted layer function - passes app-level dependencies."""
    def update_status(status_text):
        update_processing_status("current", status_text)

    def set_book(author, title, stage=""):
        set_current_book(author, title, stage)

    def get_circuit_breaker(api_name):
        return API_CIRCUIT_BREAKER.get(api_name, {})

    return _process_layer_1_audio_raw(
        config=config,
        get_db=get_db,
        identify_ebook_from_filename=identify_ebook_from_filename,
        identify_audio_with_bookdb=identify_audio_with_bookdb,
        transcribe_audio_intro=transcribe_audio_intro,
        parse_transcript_with_ai=parse_transcript_with_ai,
        is_circuit_open=is_circuit_open,
        get_circuit_breaker=get_circuit_breaker,
        load_config=load_config,
        build_new_path=build_new_path,
        update_processing_status=update_status,
        set_current_book=set_book,
        limit=limit
    )


def process_layer_1_api(config, limit=None):
    """Wrapper for extracted layer function - passes app-level dependencies."""
    def set_book(author, title, stage=""):
        set_current_book(author, title, stage)

    return _process_layer_1_api_raw(
        config=config,
        get_db=get_db,
        gather_all_api_candidates=gather_all_api_candidates,
        limit=limit,
        set_current_book=set_book
    )


def process_sl_requeue_verification(config, limit=None):
    """Wrapper for SL requeue verification - re-checks books after nightly merge."""
    return _process_sl_requeue_verification_raw(
        config=config,
        get_db=get_db,
        search_bookdb=search_bookdb,
        limit=limit
    )


def process_queue(config, limit=None, verification_layer=2):
    """Wrapper for extracted layer function - passes app-level dependencies."""
    def set_book(author, title, stage=""):
        set_current_book(author, title, stage)

    return _process_queue_raw(
        config=config,
        get_db=get_db,
        check_rate_limit=check_rate_limit,
        call_ai=call_ai,
        detect_multibook_vs_chapters=detect_multibook_vs_chapters,
        auto_save_narrator=auto_save_narrator,
        standardize_initials=standardize_initials,
        extract_series_from_title=extract_series_from_title,
        is_placeholder_author=is_placeholder_author,
        build_new_path=build_new_path,
        is_drastic_author_change=is_drastic_author_change,
        verify_drastic_change=verify_drastic_change,
        analyze_audio_for_credits=analyze_audio_for_credits,
        compare_book_folders=compare_book_folders,
        sanitize_path_component=sanitize_path_component,
        extract_narrator_from_folder=extract_narrator_from_folder,
        build_metadata_for_embedding=build_metadata_for_embedding,
        embed_tags_for_path=embed_tags_for_path,
        BookProfile=BookProfile,
        audio_extensions=AUDIO_EXTENSIONS,
        limit=limit,
        verification_layer=verification_layer,
        set_current_book=set_book
    )


def process_layer_3_audio(config, limit=None, verification_layer=3):
    """Wrapper for extracted layer function - passes app-level dependencies."""
    return _process_layer_3_audio_raw(
        config=config,
        get_db=get_db,
        find_audio_files=find_audio_files,
        analyze_audio_for_credits=analyze_audio_for_credits,
        auto_save_narrator=auto_save_narrator,
        contribute_audio_extraction=contribute_audio_extraction,
        standardize_initials=standardize_initials,
        limit=limit,
        verification_layer=verification_layer
    )


# process_layer_4_content moved to library_manager/pipeline/layer_content.py


def apply_fix(history_id):
    """Apply a pending fix from history."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    fix = c.fetchone()

    if not fix:
        conn.close()
        return False, "Fix not found"

    # Issue #69: Handle history entries with missing paths
    # Some older code paths created history entries without old_path/new_path
    book_id = fix['book_id']

    # Get old_path - fall back to books table if None
    if fix['old_path']:
        old_path = Path(fix['old_path'])
    else:
        c.execute('SELECT path FROM books WHERE id = ?', (book_id,))
        book_row = c.fetchone()
        if not book_row or not book_row['path']:
            conn.close()
            return False, "Cannot determine source path - book not found"
        old_path = Path(book_row['path'])
        logger.info(f"[APPLY FIX] old_path was None, using book path: {old_path}")

    # Get new_path - compute from metadata if None
    if fix['new_path']:
        new_path = Path(fix['new_path'])
    else:
        config = load_config()
        library_paths = config.get('library_paths', [])
        if not library_paths:
            conn.close()
            return False, "Cannot determine destination - no library paths configured"

        # Build new path from fix metadata
        # Detect language from title for multi-language naming
        fix_title = fix['new_title'] or fix['old_title']
        lang_code = detect_title_language(fix_title) if fix_title else None
        new_path = build_new_path(
            Path(library_paths[0]),
            fix['new_author'] or fix['old_author'],
            fix_title,
            series=fix['new_series'] if fix['new_series'] else None,
            series_num=fix['new_series_num'] if fix['new_series_num'] else None,
            narrator=fix['new_narrator'] if fix['new_narrator'] else None,
            year=fix['new_year'] if fix['new_year'] else None,
            edition=fix['new_edition'] if fix['new_edition'] else None,
            variant=fix['new_variant'] if fix['new_variant'] else None,
            language_code=lang_code,
            config=config
        )
        if not new_path:
            conn.close()
            return False, "Cannot build destination path - invalid author/title"
        new_path = Path(new_path)
        logger.info(f"[APPLY FIX] new_path was None, computed: {new_path}")

    # Issue #49: Check if this is a watch folder item
    c.execute('SELECT source_type FROM books WHERE id = ?', (book_id,))
    book_row = c.fetchone()
    source_type = book_row['source_type'] if book_row and book_row['source_type'] else 'library'
    is_watch_folder_item = (source_type == 'watch_folder')

    # CRITICAL SAFETY: Validate paths before any file operations
    config = load_config()
    library_paths = [Path(p).resolve() for p in config.get('library_paths', [])]
    watch_folder = config.get('watch_folder', '').strip()

    # Check old_path is in a library (or watch folder for watch folder items)
    old_in_library = False
    old_in_watch_folder = False
    for lib in library_paths:
        try:
            old_path.resolve().relative_to(lib)
            old_in_library = True
            break
        except ValueError:
            continue

    # Issue #49: Also check if old_path is in watch folder
    if watch_folder and not old_in_library:
        try:
            old_path.resolve().relative_to(Path(watch_folder).resolve())
            old_in_watch_folder = True
        except ValueError:
            pass

    # Check new_path is in a library or output folder (this is where the book goes)
    new_in_library = False
    for lib in library_paths:
        try:
            new_path.resolve().relative_to(lib)
            new_in_library = True
            break
        except ValueError:
            continue

    # Issue #135: Also accept output folder as valid destination
    if not new_in_library:
        watch_output_folder = config.get('watch_output_folder', '').strip()
        if watch_output_folder:
            try:
                new_path.resolve().relative_to(Path(watch_output_folder).resolve())
                new_in_library = True
            except ValueError:
                pass

    # Issue #49: Allow watch folder items to have old_path in watch folder
    old_path_valid = old_in_library or (is_watch_folder_item and old_in_watch_folder)

    if not old_path_valid or not new_in_library:
        error_msg = f"SAFETY BLOCK: Path outside library! old_in_lib={old_in_library}, old_in_watch={old_in_watch_folder}, new_in_lib={new_in_library}"
        logger.error(error_msg)
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

    # Check new_path has reasonable depth (at least 2 components: Author/Title)
    for lib in library_paths:
        try:
            relative = new_path.resolve().relative_to(lib)
            if len(relative.parts) < 2:
                error_msg = f"SAFETY BLOCK: Path too shallow ({len(relative.parts)} levels) - would dump at author level"
                logger.error(error_msg)
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg
            break
        except ValueError:
            continue

    if not old_path.exists():
        # Issue #64: Try current book path as fallback (book may have been moved)
        c.execute('SELECT path FROM books WHERE id = ?', (book_id,))
        current_book = c.fetchone()
        fallback_found = False
        if current_book and current_book['path'] and Path(current_book['path']).exists():
            fallback_path = Path(current_book['path'])
            if fallback_path != old_path:
                logger.warning(f"[APPLY FIX] old_path {old_path} missing, using current book path: {fallback_path}")
                old_path = fallback_path
                # Update history with the correct old_path for future reference
                c.execute('UPDATE history SET old_path = ? WHERE id = ?', (str(old_path), history_id))
                fallback_found = True

        if not fallback_found:
            error_msg = f"Source no longer exists: {old_path}"
            c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                     ('error', error_msg, history_id))
            conn.commit()
            conn.close()
            return False, error_msg

    try:
        import shutil

        # Check if we're moving a file (ebook/loose file/single m4b) vs a folder
        is_file_move = old_path.is_file()

        # If moving a single file, ensure new_path includes the filename with extension
        # (build_new_path returns a folder path, but for single files we need to include the filename)
        if is_file_move:
            # Check if new_path looks like a folder (no extension or doesn't match audio extension)
            audio_extensions = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}
            if new_path.suffix.lower() not in audio_extensions:
                # new_path is a folder, we need to create folder and put file inside
                file_dest = new_path / old_path.name
                logger.info(f"Single file move: {old_path.name} -> {file_dest}")
            else:
                file_dest = new_path

            if file_dest.exists():
                error_msg = f"Destination file already exists: {file_dest.name}"
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg

            # Create destination folder and move file
            file_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(file_dest))
            # Update new_path to the folder for embedding later
            new_path = file_dest.parent
        elif new_path.exists():
            # Moving a folder - check if destination has files
            existing_files = list(new_path.iterdir())
            if existing_files:
                # DON'T MERGE - this is likely a different narrator version
                error_msg = "Destination folder already exists with files - possible different narrator version"
                c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                         ('error', error_msg, history_id))
                conn.commit()
                conn.close()
                return False, error_msg
            else:
                # Destination is empty folder - safe to use it
                shutil.move(str(old_path), str(new_path.parent / (new_path.name + "_temp")))
                new_path.rmdir()
                (new_path.parent / (new_path.name + "_temp")).rename(new_path)
        else:
            # Destination doesn't exist - create parent folders and move
            new_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(new_path))

        # Clean up empty parent
        try:
            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                old_path.parent.rmdir()
        except OSError:
            pass

        # Update book record
        # Issue #49: For watch folder items, also update source_type to 'library' since it's now in the library
        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?, source_type = 'library'
                     WHERE id = ?''',
                 (str(new_path), fix['new_author'], fix['new_title'], 'fixed', fix['book_id']))

        # Update history status
        c.execute('UPDATE history SET status = ? WHERE id = ?', ('fixed', history_id))

        # Issue #79: Remove from queue - this was missing, causing stuck queue items
        c.execute('DELETE FROM queue WHERE book_id = ?', (fix['book_id'],))

        # Embed metadata tags if enabled
        embed_status = None
        embed_error = None
        if config.get('metadata_embedding_enabled', False):
            try:
                embed_metadata = build_metadata_for_embedding(
                    author=fix['new_author'],
                    title=fix['new_title'],
                    series=fix['new_series'] if fix['new_series'] else None,
                    series_num=fix['new_series_num'] if fix['new_series_num'] else None,
                    narrator=fix['new_narrator'] if fix['new_narrator'] else None,
                    year=fix['new_year'] if fix['new_year'] else None,
                    edition=fix['new_edition'] if fix['new_edition'] else None,
                    variant=fix['new_variant'] if fix['new_variant'] else None
                )
                embed_result = embed_tags_for_path(
                    new_path,
                    embed_metadata,
                    create_backup=config.get('metadata_embedding_backup_sidecar', True),
                    overwrite=config.get('metadata_embedding_overwrite_managed', True)
                )
                if embed_result['success']:
                    embed_status = 'ok'
                    logger.info(f"Embedded tags in {embed_result['files_processed']} files at {new_path}")
                else:
                    embed_status = 'error'
                    embed_error = embed_result.get('error') or '; '.join(embed_result.get('errors', []))[:500]
                    logger.warning(f"Tag embedding failed for {new_path}: {embed_error}")
            except Exception as embed_e:
                embed_status = 'error'
                embed_error = str(embed_e)[:500]
                logger.error(f"Tag embedding exception for {new_path}: {embed_e}")

            # Update history with embed status
            c.execute('UPDATE history SET embed_status = ?, embed_error = ? WHERE id = ?',
                     (embed_status, embed_error, history_id))

        conn.commit()
        conn.close()
        return True, "Fix applied successfully"
    except Exception as e:
        error_msg = str(e)
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

# ============== BACKGROUND WORKER ==============

# Worker state is managed by library_manager.worker module
# processing_status is accessed via get_processing_status() from the module

def process_all_queue(config):
    """Wrapper for extracted orchestrator - passes app-level dependencies."""
    def get_circuit_breaker(api_name):
        return API_CIRCUIT_BREAKER.get(api_name, {})

    return _process_all_queue_raw(
        config=config,
        get_db=get_db,
        load_config=load_config,
        is_circuit_open=is_circuit_open,
        get_circuit_breaker=get_circuit_breaker,
        check_rate_limit=check_rate_limit,
        process_layer_1_audio=process_layer_1_audio,
        process_layer_3_audio=process_layer_3_audio,
        process_layer_1_api=process_layer_1_api,
        process_queue=process_queue,
        process_sl_requeue_verification=process_sl_requeue_verification
    )



# ============================================================================
# WATCH FOLDER FUNCTIONALITY
# ============================================================================

# Track processed watch folder items to avoid reprocessing
watch_folder_processed = set()
watch_folder_last_scan = 0

def get_watch_folder_items(watch_folder: str, min_age_seconds: int = 30) -> list:
    """
    Scan watch folder for audiobook folders/files ready for processing.
    Returns list of paths that are old enough (not still downloading).
    """
    items = []
    watch_path = Path(watch_folder)

    if not watch_path.exists():
        logger.warning(f"Watch folder does not exist: {watch_folder}")
        return items

    current_time = time.time()

    # Look for audiobook folders (contain audio files)
    audio_extensions = {'.mp3', '.m4a', '.m4b', '.flac', '.ogg', '.opus', '.wav', '.aac'}

    for item in watch_path.iterdir():
        item_path = str(item.resolve())

        # Skip if already processed
        if item_path in watch_folder_processed:
            continue

        # Check if folder contains audio files or is an audio file
        has_audio = False
        newest_mtime = 0

        if item.is_dir():
            for f in item.rglob('*'):
                if f.is_file():
                    if f.suffix.lower() in audio_extensions:
                        has_audio = True
                    try:
                        mtime = f.stat().st_mtime
                        if mtime > newest_mtime:
                            newest_mtime = mtime
                    except:
                        pass
        elif item.is_file() and item.suffix.lower() in audio_extensions:
            has_audio = True
            try:
                newest_mtime = item.stat().st_mtime
            except:
                pass

        if not has_audio:
            continue

        # Check file age - skip if too recent (still downloading)
        file_age = current_time - newest_mtime
        if file_age < min_age_seconds:
            logger.debug(f"Watch folder: Skipping {item.name} - too recent ({file_age:.0f}s < {min_age_seconds}s)")
            continue

        items.append(item_path)

    return items


def move_to_output_folder(source_path: str, output_folder: str, author: str, title: str,
                          series: str = None, series_num = None,
                          use_hard_links: bool = False, delete_empty: bool = True) -> tuple:
    """
    Move or hard-link an audiobook to the output folder with proper naming.
    Returns (success: bool, new_path: str, error: str or None)

    Issue #57: Now supports series organization - builds path as:
    - output/Author/Series/# - Title (when series and series_num provided)
    - output/Author/Series/Title (when only series provided)
    - output/Author/Title (when no series)
    """
    source = Path(source_path)
    output = Path(output_folder)

    if not source.exists():
        return False, None, f"Source does not exist: {source_path}"

    if not output.exists():
        try:
            output.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return False, None, f"Cannot create output folder: {e}"

    # Sanitize author and title for filesystem
    safe_author = sanitize_path_component(author) if author else "Unknown"
    safe_title = sanitize_path_component(title) if title else source.name
    safe_series = sanitize_path_component(series) if series else None

    # Build destination path with series support (Issue #57)
    # Format: output/Author/Series/## - Title or output/Author/Title
    # Merijeek: ABS compatibility - pad single-digit numbers
    if safe_series:
        if series_num:
            # Zero-pad series numbers for ABS compatibility
            try:
                num = float(str(series_num).replace(',', '.'))
                formatted_num = f"{int(num):02d}" if num == int(num) else str(series_num)
            except (ValueError, TypeError):
                formatted_num = str(series_num)
            title_folder = f"{formatted_num} - {safe_title}"
        else:
            title_folder = safe_title
        dest_folder = output / safe_author / safe_series / title_folder
    else:
        dest_folder = output / safe_author / safe_title

    # Check for existing destination
    partial_move_detected = False
    if dest_folder.exists():
        # Issue #76: Check if this is a partial/interrupted move before creating Version B
        # If destination files are a subset of source files, complete the move instead
        if source.is_dir():
            source_files = {f.name for f in source.rglob('*') if f.is_file()}
            dest_files = {f.name for f in dest_folder.rglob('*') if f.is_file()}

            if dest_files and source_files and dest_files.issubset(source_files):
                # Destination is a partial move - complete it instead of creating Version B
                missing_files = source_files - dest_files
                if missing_files:
                    logger.info(f"[WATCH] Partial move detected: {len(dest_files)} files at dest, {len(missing_files)} remaining in source")
                    partial_move_detected = True
                    # Don't create Version B - we'll complete the move below

        if not partial_move_detected:
            # Add version suffix - handle series path structure
            version = 2
            while True:
                if safe_series:
                    if series_num:
                        versioned_title = f"{series_num} - {safe_title} [Version {chr(64+version)}]"
                    else:
                        versioned_title = f"{safe_title} [Version {chr(64+version)}]"
                    versioned = output / safe_author / safe_series / versioned_title
                else:
                    versioned = output / safe_author / f"{safe_title} [Version {chr(64+version)}]"
                if not versioned.exists():
                    dest_folder = versioned
                    break
                version += 1
                if version > 26:
                    return False, None, f"Too many versions exist for {safe_author}/{safe_title}"

    try:
        # Issue #76: Try atomic directory move first (prevents partial moves on interruption)
        # Only works on same filesystem and when not using hard links and destination doesn't exist
        atomic_move_done = False
        if not use_hard_links and not dest_folder.exists() and source.is_dir():
            try:
                # Ensure parent exists
                dest_folder.parent.mkdir(parents=True, exist_ok=True)
                # Atomic move - uses os.rename internally on same filesystem
                shutil.move(str(source), str(dest_folder))
                atomic_move_done = True
                logger.debug(f"[WATCH] Atomic directory move: {source.name} -> {dest_folder}")
            except OSError as e:
                # Cross-filesystem or other error - fall back to file-by-file
                logger.debug(f"[WATCH] Atomic move failed ({e}), using file-by-file")

        if not atomic_move_done:
            dest_folder.mkdir(parents=True, exist_ok=True)

        # Track if we fell back to copy (need to delete originals afterward)
        used_copy_fallback = False
        files_to_delete = []

        if atomic_move_done:
            # Atomic move succeeded - nothing more to do for the files
            pass
        elif source.is_file():
            # Single file - move/link to destination folder
            dest_file = dest_folder / source.name
            if use_hard_links:
                try:
                    os.link(source, dest_file)
                except OSError as e:
                    if "Invalid cross-device link" in str(e) or e.errno == 18:
                        # Cross-filesystem - fall back to copy, then delete original
                        logger.warning(f"Hard link failed (cross-filesystem), falling back to copy+delete: {source.name}")
                        shutil.copy2(source, dest_file)
                        used_copy_fallback = True
                        files_to_delete.append(source)
                    else:
                        raise
            else:
                shutil.move(str(source), str(dest_file))
        else:
            # Directory - move/link all files
            for src_file in source.rglob('*'):
                if src_file.is_file():
                    rel_path = src_file.relative_to(source)
                    dest_file = dest_folder / rel_path

                    # Issue #76: Skip files that already exist at destination (partial move completion)
                    if dest_file.exists():
                        logger.debug(f"[WATCH] Skipping already-moved file: {src_file.name}")
                        continue

                    dest_file.parent.mkdir(parents=True, exist_ok=True)

                    if use_hard_links:
                        try:
                            os.link(src_file, dest_file)
                        except OSError as e:
                            if "Invalid cross-device link" in str(e) or e.errno == 18:
                                logger.warning(f"Hard link failed, copy+delete: {src_file.name}")
                                shutil.copy2(src_file, dest_file)
                                used_copy_fallback = True
                                files_to_delete.append(src_file)
                            else:
                                raise
                    else:
                        shutil.move(str(src_file), str(dest_file))

            # Clean up empty source folder if not using hard links OR if we used copy fallback
            if (not use_hard_links or used_copy_fallback) and delete_empty:
                try:
                    # Remove empty directories bottom-up
                    for dirpath, dirnames, filenames in os.walk(str(source), topdown=False):
                        if not filenames and not dirnames:
                            os.rmdir(dirpath)
                    if source.exists() and not any(source.iterdir()):
                        source.rmdir()
                except Exception as e:
                    logger.debug(f"Could not clean up empty folder {source}: {e}")

        # Delete originals if we used copy fallback (handles both single files and directories)
        if used_copy_fallback and delete_empty:
            for f in files_to_delete:
                try:
                    if f.exists():
                        f.unlink()
                        logger.debug(f"Deleted source after copy fallback: {f}")
                except Exception as e:
                    logger.warning(f"Could not delete source {f}: {e}")

        return True, str(dest_folder), None

    except Exception as e:
        logger.error(f"Move/link error for {source_path}: {e}")
        return False, None, str(e)


def process_watch_folder(config: dict) -> int:
    """
    Process items in the watch folder.
    Returns number of items processed.
    """
    global watch_folder_processed, watch_folder_last_scan

    watch_folder = config.get('watch_folder', '').strip()
    output_folder = config.get('watch_output_folder', '').strip()
    use_hard_links = config.get('watch_use_hard_links', False)
    delete_empty = config.get('watch_delete_empty_folders', True)
    min_age = config.get('watch_min_file_age_seconds', 30)

    if not watch_folder:
        return 0

    # Default output to first library path
    if not output_folder:
        library_paths = config.get('library_paths', [])
        if not library_paths:
            logger.warning("Watch folder: No output folder and no library paths configured")
            return 0
        output_folder = library_paths[0]

    logger.info(f"=== WATCH FOLDER SCAN: {watch_folder} ===")

    items = get_watch_folder_items(watch_folder, min_age)
    if not items:
        logger.debug("Watch folder: No new items to process")
        return 0

    logger.info(f"Watch folder: Found {len(items)} new items to process")
    processed = 0

    conn = get_db()
    c = conn.cursor()

    for item_path in items:
        try:
            item = Path(item_path)
            logger.info(f"Watch folder: Processing {item.name}")

            # Issue #57: Use parent folder as author hint if item is in a subfolder
            # e.g., /watch/Peter F. Hamilton/02 Night Without Stars.mp3 -> author hint = "Peter F. Hamilton"
            watch_path_obj = Path(watch_folder)
            parent_folder = item.parent
            folder_author_hint = None
            if parent_folder != watch_path_obj and parent_folder.parent == watch_path_obj:
                # Item is in a direct subfolder of watch folder - use folder name as author hint
                folder_author_hint = parent_folder.name
                if not is_placeholder_author(folder_author_hint):
                    logger.info(f"Watch folder: Using parent folder as author hint: '{folder_author_hint}'")

            # Extract author/title from the folder/file name
            author_hint, title_part = extract_author_title(item.stem if item.is_file() else item.name)

            # Prefer folder author hint over filename parsing
            original_author = folder_author_hint if folder_author_hint else (author_hint if author_hint else 'Unknown')
            author = original_author
            title = title_part if title_part else item.name
            original_title = title

            # Try API lookups for better identification
            # Issue #57: Fix argument order - gather_all_api_candidates(title, author, config)
            # Also try searching with just the full filename if author-title parsing might be wrong
            needs_verification = False
            api_author = None
            api_title = None
            api_series = None
            api_series_num = None
            series = None
            series_num = None
            try:
                # First try with parsed author/title
                candidates = gather_all_api_candidates(title, author, config)

                # If no good matches and author looks suspicious, try with full filename as title
                # Normalize confidence to 0-100 scale for comparison
                def norm_conf(c):
                    raw = c.get('confidence', 0) if c else 0
                    return raw * 100 if raw <= 1 else raw
                if (not candidates or norm_conf(candidates[0]) < 60):
                    # The filename might be "Title - Author" or just "Title", not "Author - Title"
                    # Try searching with the full original name as the title
                    full_name = item.stem if item.is_file() else item.name
                    full_candidates = gather_all_api_candidates(full_name, None, config)
                    if full_candidates and norm_conf(full_candidates[0]) > norm_conf(candidates[0] if candidates else None):
                        candidates = full_candidates
                        logger.debug(f"Watch folder: Full filename search gave better results for '{full_name}'")

                if candidates:
                    # Use best match
                    best = candidates[0]
                    # Handle confidence as either 0-1 (fraction) or 0-100 (percentage)
                    raw_confidence = best.get('confidence', 0)
                    confidence = raw_confidence * 100 if raw_confidence <= 1 else raw_confidence
                    if confidence >= 60:
                        api_author = best.get('author', author)
                        api_title = best.get('title', title)
                        api_series = best.get('series')
                        api_series_num = best.get('series_num')

                        # Issue #57: Check if API author is drastically different from our hint
                        # This catches cases like "Night Without Stars" matching wrong author
                        if api_author and original_author and not is_placeholder_author(original_author):
                            # We have a real author hint - verify if API disagrees
                            author_similarity = calculate_title_similarity(original_author.lower(), api_author.lower())
                            if author_similarity < 0.5:
                                # API found different author - needs verification
                                logger.info(f"Watch folder: API author '{api_author}' differs from hint '{original_author}' - verifying...")
                                needs_verification = True

                        # Issue #76: Extract and match series info properly
                        # "Expeditionary Force Book 14 - Match Game" should match "Expeditionary Force" series
                        if not needs_verification:
                            # Use proper series extraction
                            extracted = extract_series_from_title(item.name)
                            query_series_name, query_series_num, query_title = extracted

                            if query_series_name:
                                # Query has series info - check if API result matches
                                if api_series:
                                    # Both have series - check if they match
                                    series_similarity = calculate_title_similarity(
                                        query_series_name.lower(),
                                        api_series.lower()
                                    )
                                    if series_similarity < 0.5:
                                        logger.warning(f"Watch folder: Series mismatch - query has '{query_series_name}', result has '{api_series}'")
                                        needs_verification = True
                                else:
                                    # Query has series but result doesn't - suspicious
                                    logger.warning(f"Watch folder: Query has series '{query_series_name}' #{query_series_num} but result has no series")
                                    logger.warning(f"  Query: '{item.name}' -> series={query_series_name}, num={query_series_num}, title={query_title}")
                                    logger.warning(f"  Result: '{api_author} - {api_title}' (no series info)")
                                    needs_verification = True

                        # Issue #57: Same-title-different-author detection
                        # If we have NO author hint and multiple APIs returned different authors
                        # for similar titles, this could be a common title with multiple books
                        if not needs_verification and is_placeholder_author(original_author) and len(candidates) > 1:
                            # Check if different APIs found different authors for similar titles
                            unique_authors = set()
                            for c in candidates:
                                c_title = c.get('title', '')
                                c_author = c.get('author', '')
                                # Only count if title is similar to what we searched
                                if c_author and calculate_title_similarity(title.lower(), c_title.lower()) > 0.6:
                                    unique_authors.add(c_author.lower())
                            if len(unique_authors) > 1:
                                # Multiple authors for same/similar title - ambiguous, needs verification
                                logger.warning(f"Watch folder: Multiple authors found for '{title}': {unique_authors} - flagging for review")
                                needs_verification = True

                        if not needs_verification:
                            author = api_author
                            title = api_title
                            series = api_series
                            series_num = api_series_num
                            series_info = f" [{series} #{series_num}]" if series and series_num else (f" [{series}]" if series else "")
                            logger.info(f"Watch folder: Identified as {author} - {title}{series_info}")

            except Exception as e:
                logger.debug(f"Watch folder: API lookup failed, using path analysis: {e}")

            # Issue #57: Verify drastic author changes before accepting
            if needs_verification and api_author and api_title:
                try:
                    verification = verify_drastic_change(
                        item.name,  # original input
                        original_author,  # original author (our hint)
                        original_title,  # original title
                        api_author,  # proposed author from API
                        api_title,  # proposed title from API
                        config
                    )
                    if verification and verification.get('verified'):
                        # Verification passed - use the verified result
                        author = verification.get('author', api_author)
                        title = verification.get('title', api_title)
                        # Issue #57: Preserve series info from API when verification passes
                        series = api_series
                        series_num = api_series_num
                        series_info = f" [{series} #{series_num}]" if series and series_num else (f" [{series}]" if series else "")
                        logger.info(f"Watch folder: Verified change to {author} - {title}{series_info}")
                    else:
                        # Verification failed - keep original hint, flag for attention
                        reason = verification.get('reasoning', 'Verification failed') if verification else 'No verification result'
                        logger.warning(f"Watch folder: Rejected API match '{api_author}' for '{item.name}' - {reason}")
                        # Keep original author hint if we had one, otherwise use Unknown
                        author = original_author if not is_placeholder_author(original_author) else 'Unknown'
                        title = original_title
                except Exception as e:
                    logger.warning(f"Watch folder: Verification failed for '{item.name}': {e}")
                    # On verification error, be conservative - use original hint
                    author = original_author if not is_placeholder_author(original_author) else 'Unknown'
                    title = original_title

            # Issue #57: Apply author initials standardization if enabled
            if config.get('standardize_author_initials', False) and author:
                author = standardize_initials(author)

            # Issue #40: Check if author is still unknown/placeholder after API lookup
            # If so, flag for user attention instead of auto-processing
            author_is_placeholder = is_placeholder_author(author)
            if author_is_placeholder:
                logger.warning(f"Watch folder: Unknown author for '{title}' - will require user review")

            # Move to output folder (Issue #57: pass series info for proper organization)
            success, new_path, error = move_to_output_folder(
                item_path, output_folder, author, title,
                series=series, series_num=series_num,
                use_hard_links=use_hard_links, delete_empty=delete_empty
            )

            if success:
                logger.info(f"Watch folder: Moved to {new_path}")
                watch_folder_processed.add(item_path)
                processed += 1

                # Add to books table
                # Issue #40: If author is unknown, mark as needs_attention for user review
                try:
                    if author_is_placeholder:
                        # Unknown author - requires user intervention before processing
                        # Issue #57: Include source_type for watch folder tracking
                        c.execute('''INSERT OR REPLACE INTO books
                                     (path, current_author, current_title, status, error_message, source_type, added_at, updated_at)
                                     VALUES (?, ?, ?, 'needs_attention', ?, 'watch_folder', datetime('now'), datetime('now'))''',
                                  (new_path, author, title, 'Watch folder: Could not determine author - please review and correct'))
                        logger.info(f"Watch folder: Flagged for attention (unknown author): {title}")
                    else:
                        # Known author - normal processing
                        # Issue #57: Include source_type for watch folder tracking
                        c.execute('''INSERT OR REPLACE INTO books
                                     (path, current_author, current_title, status, source_type, added_at, updated_at)
                                     VALUES (?, ?, ?, 'pending', 'watch_folder', datetime('now'), datetime('now'))''',
                                  (new_path, author, title))
                        # Issue #126: Auto-enqueue for full pipeline processing
                        book_id = c.lastrowid
                        c.execute('''INSERT OR IGNORE INTO queue (book_id, reason, priority)
                                    VALUES (?, ?, ?)''',
                                 (book_id, 'watch_folder_new', 3))
                        c.execute('UPDATE books SET verification_layer = 1 WHERE id = ?', (book_id,))
                            logger.info(f"Watch folder: Auto-enqueued for processing: {author}/{title}")
                    conn.commit()
                except Exception as e:
                    logger.debug(f"Watch folder: Could not add to books table: {e}")
            else:
                logger.error(f"Watch folder: Failed to move {item.name}: {error}")
                # Issue #49: Track failed items in the database so user can see and fix them
                # Add to watch_folder_processed to prevent infinite retry loop
                watch_folder_processed.add(item_path)
                try:
                    # Check if this item is already tracked
                    c.execute('SELECT id FROM books WHERE path = ?', (item_path,))
                    existing = c.fetchone()
                    if existing:
                        # Update existing record with error
                        c.execute('''UPDATE books SET status = ?, error_message = ?, source_type = ?, updated_at = datetime('now')
                                     WHERE id = ?''',
                                  ('watch_folder_error', f'Watch folder: {error}', 'watch_folder', existing['id']))
                    else:
                        # Insert new record for the failed item
                        c.execute('''INSERT INTO books
                                     (path, current_author, current_title, status, error_message, source_type, added_at, updated_at)
                                     VALUES (?, ?, ?, 'watch_folder_error', ?, 'watch_folder', datetime('now'), datetime('now'))''',
                                  (item_path, author, title, f'Watch folder: {error}'))
                    conn.commit()
                    logger.info(f"Watch folder: Tracked failure for user review: {item.name}")
                except Exception as db_err:
                    logger.debug(f"Watch folder: Could not track failure in DB: {db_err}")

        except Exception as e:
            logger.error(f"Watch folder: Error processing {item_path}: {e}")

    conn.close()
    watch_folder_last_scan = time.time()
    logger.info(f"Watch folder: Processed {processed}/{len(items)} items")
    return processed


def start_worker():
    """Wrapper for extracted worker start - passes app-level dependencies."""
    _start_worker_raw(
        load_config=load_config,
        scan_library=scan_library,
        process_all_queue_func=process_all_queue,
        process_watch_folder=process_watch_folder
    )


def stop_worker():
    """Stop the background worker."""
    _stop_worker_raw()


def is_worker_running():
    """Check if worker is actually running."""
    return _is_worker_running_raw()


@app.context_processor
def inject_worker_status():
    """Inject worker_running into all templates automatically."""
    return {'worker_running': is_worker_running()}

# ============== ROUTES ==============

@app.route('/')
def dashboard():
    """Main dashboard."""
    # Check if first-run setup is needed
    config = load_config()
    if needs_setup(config):
        return redirect('/setup')

    conn = get_db()
    c = conn.cursor()

    # Get counts (Issue #64: exclude series_folder and multi_book_files containers)
    c.execute("SELECT COUNT(*) as count FROM books WHERE status NOT IN ('series_folder', 'multi_book_files')")
    total_books = c.fetchone()['count']

    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_size = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'fixed'")
    fixed_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'verified'")
    verified_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending_fixes = c.fetchone()['count']

    # Get recent history (use LEFT JOIN in case book was deleted)
    c.execute('''SELECT h.*, b.path FROM history h
                 LEFT JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 15''')
    recent_history = c.fetchall()

    # Get stats for last 7 days
    c.execute('''SELECT date, scanned, queued, fixed, api_calls FROM stats
                 ORDER BY date DESC LIMIT 7''')
    daily_stats = c.fetchall()

    conn.close()

    config = load_config()

    return render_template('dashboard.html',
                          total_books=total_books,
                          queue_size=queue_size,
                          fixed_count=fixed_count,
                          verified_count=verified_count,
                          pending_fixes=pending_fixes,
                          recent_history=recent_history,
                          daily_stats=daily_stats,
                          config=config,
                          worker_running=is_worker_running())


@app.route('/setup')
def setup_wizard():
    """First-run setup wizard for new users."""
    config = load_config()
    # If already configured, redirect to library (unless force=1 for screenshots/testing)
    if not needs_setup(config) and request.args.get('force') != '1':
        return redirect('/library')
    return render_template('setup_wizard.html', config=config)


@app.route('/api/setup/complete', methods=['POST'])
def complete_setup():
    """Save initial setup configuration.

    IMPORTANT: This MERGES with existing config, never overwrites.
    If user has existing settings, they are preserved.
    """
    data = request.json
    config = load_config()  # Load existing config first
    secrets = load_secrets()

    # MERGE library paths - add new ones, keep existing
    existing_paths = config.get('library_paths', [])
    new_paths = data.get('library_paths', [])
    for path in new_paths:
        if path and path not in existing_paths:
            existing_paths.append(path)
    config['library_paths'] = existing_paths

    # Only update AI provider if user selected one in wizard
    if data.get('ai_provider'):
        config['ai_provider'] = data['ai_provider']

    # Only save API keys if provided (don't clear existing)
    if data.get('gemini_api_key'):
        secrets['gemini_api_key'] = data['gemini_api_key']
    if data.get('openrouter_api_key'):
        secrets['openrouter_api_key'] = data['openrouter_api_key']
    if data.get('ollama_url'):
        config['ollama_url'] = data['ollama_url']
    if data.get('ollama_model'):
        config['ollama_model'] = data['ollama_model']

    # Only update toggles if explicitly set in wizard
    if 'auto_fix' in data:
        config['auto_fix'] = data['auto_fix']
    if 'ebook_management' in data:
        config['ebook_management'] = data['ebook_management']
    if 'trust_the_process' in data:
        config['trust_the_process'] = data['trust_the_process']

    # Mark setup as completed (prevents re-showing wizard)
    config['setup_completed'] = True

    save_config(config)
    save_secrets(secrets)

    logger.info(f"Setup wizard completed. Library paths: {config['library_paths']}")
    return jsonify({'success': True, 'redirect': '/library'})


@app.route('/orphans')
def orphans_page():
    """Redirect to unified Library view with orphan filter."""
    return redirect('/library?filter=orphan')

@app.route('/queue')
def queue_page():
    """Queue page with Multi-Edit support."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT q.id, q.reason, q.added_at,
                        b.id as book_id, b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 ORDER BY q.priority, q.added_at''')
    queue_items = [dict(row) for row in c.fetchall()]
    conn.close()

    return render_template('queue.html', queue_items=queue_items)

@app.route('/history')
def history_page():
    """History of all fixes."""
    conn = get_db()
    c = conn.cursor()

    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', None)
    per_page = 50
    offset = (page - 1) * per_page

    # Get duplicate count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'duplicate'")
    duplicate_count = c.fetchone()['count']

    # Get needs_attention count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'needs_attention'")
    needs_attention_count = c.fetchone()['count']

    # Get error count for the UI
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
    error_count = c.fetchone()['count']

    # Build query based on status filter
    # Join with books table to get user_locked status
    if status_filter == 'pending':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
        total = c.fetchone()['count']
        c.execute('''SELECT h.*, b.user_locked FROM history h
                     LEFT JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'pending_fix'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'duplicate':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'duplicate'")
        total = c.fetchone()['count']
        c.execute('''SELECT h.*, b.user_locked FROM history h
                     LEFT JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'duplicate'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'attention':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'needs_attention'")
        total = c.fetchone()['count']
        c.execute('''SELECT h.*, b.user_locked FROM history h
                     LEFT JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'needs_attention'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    elif status_filter == 'error':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
        total = c.fetchone()['count']
        c.execute('''SELECT h.*, b.user_locked FROM history h
                     LEFT JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'error'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    else:
        c.execute('SELECT COUNT(*) as count FROM history')
        total = c.fetchone()['count']
        c.execute('''SELECT h.*, b.user_locked FROM history h
                     LEFT JOIN books b ON h.book_id = b.id
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    rows = c.fetchall()
    conn.close()

    # Convert to dicts and add is_drastic flag
    history_items = []
    for row in rows:
        item = dict(row)
        item['is_drastic'] = is_drastic_author_change(item.get('old_author'), item.get('new_author'))
        history_items.append(item)

    total_pages = (total + per_page - 1) // per_page

    return render_template('history.html',
                          history_items=history_items,
                          page=page,
                          total_pages=total_pages,
                          total=total,
                          status_filter=status_filter,
                          duplicate_count=duplicate_count,
                          needs_attention_count=needs_attention_count,
                          error_count=error_count)

@app.route('/settings', methods=['GET', 'POST'])
def settings_page():
    """Settings page."""
    if request.method == 'POST':
        # Load current config
        config = load_config()

        # Update config values
        config['library_paths'] = [p.strip() for p in request.form.get('library_paths', '').split('\n') if p.strip()]
        config['ai_provider'] = request.form.get('ai_provider', 'openrouter')
        config['openrouter_model'] = request.form.get('openrouter_model', 'google/gemma-3n-e4b-it:free')
        config['gemini_model'] = request.form.get('gemini_model', 'gemini-1.5-flash')
        config['ollama_url'] = request.form.get('ollama_url', 'http://localhost:11434').strip()
        config['ollama_model'] = request.form.get('ollama_model', 'llama3.2:3b').strip()
        config['scan_interval_hours'] = int(request.form.get('scan_interval_hours', 6))
        config['batch_size'] = int(request.form.get('batch_size', 3))
        # Clamp rate limit to safe range (10-500) to prevent API bans
        user_rate = int(request.form.get('max_requests_per_hour', 30))
        config['max_requests_per_hour'] = max(10, min(user_rate, 500))
        config['auto_fix'] = 'auto_fix' in request.form
        config['trust_the_process'] = 'trust_the_process' in request.form
        config['protect_author_changes'] = 'protect_author_changes' in request.form
        config['enabled'] = 'enabled' in request.form
        config['series_grouping'] = 'series_grouping' in request.form
        config['ebook_management'] = 'ebook_management' in request.form
        config['ebook_library_mode'] = request.form.get('ebook_library_mode', 'merge')
        # Verification layer settings (added beta.43)
        config['enable_api_lookups'] = 'enable_api_lookups' in request.form
        config['enable_ai_verification'] = 'enable_ai_verification' in request.form
        config['enable_audio_analysis'] = 'enable_audio_analysis' in request.form
        config['enable_content_analysis'] = 'enable_content_analysis' in request.form  # Issue #65: Layer 4
        config['whisper_model'] = request.form.get('whisper_model', 'base')  # Issue #77: was missing
        config['layer4_openrouter_model'] = request.form.get('layer4_openrouter_model', 'google/gemma-3n-e4b-it:free')
        # Handle both old and new config names for backwards compatibility
        use_sl = 'use_skaldleita_for_audio' in request.form or 'use_bookdb_for_audio' in request.form
        config['use_skaldleita_for_audio'] = use_sl
        config.pop('use_bookdb_for_audio', None)  # Remove deprecated key
        config['enable_voice_id'] = 'enable_voice_id' in request.form  # Skaldleita narrator voice ID
        config['deep_scan_mode'] = 'deep_scan_mode' in request.form
        config['profile_confidence_threshold'] = int(request.form.get('profile_confidence_threshold', 85))
        config['skip_confirmations'] = 'skip_confirmations' in request.form
        config['anonymous_error_reporting'] = 'anonymous_error_reporting' in request.form
        config['error_reporting_include_titles'] = 'error_reporting_include_titles' in request.form
        config['metadata_embedding_enabled'] = 'metadata_embedding_enabled' in request.form
        config['metadata_embedding_overwrite_managed'] = 'metadata_embedding_overwrite_managed' in request.form
        config['metadata_embedding_backup_sidecar'] = 'metadata_embedding_backup_sidecar' in request.form
        # Language and appearance settings
        config['ui_language'] = request.form.get('ui_language', 'en')  # UI translation language
        config['ui_theme'] = request.form.get('ui_theme', 'default')  # UI theme (default, skaldleita)
        config['preferred_language'] = request.form.get('preferred_language', 'en')
        config['strict_language_matching'] = 'strict_language_matching' in request.form
        config['preserve_original_titles'] = 'preserve_original_titles' in request.form
        config['detect_language_from_audio'] = 'detect_language_from_audio' in request.form
        # Multi-language naming settings
        config['multilang_naming_mode'] = request.form.get('multilang_naming_mode', 'native')
        config['language_tag_enabled'] = 'language_tag_enabled' in request.form
        config['language_tag_format'] = request.form.get('language_tag_format', 'bracket_full')
        config['language_tag_position'] = request.form.get('language_tag_position', 'after_title')
        # google_books_api_key is now stored in secrets only (security fix)
        config['update_channel'] = request.form.get('update_channel', 'stable')
        config['naming_format'] = request.form.get('naming_format', 'author/title')
        config['custom_naming_template'] = request.form.get('custom_naming_template', '{author}/{title}').strip()
        # Watch folder settings
        config['watch_mode'] = 'watch_mode' in request.form
        config['watch_folder'] = request.form.get('watch_folder', '').strip()
        config['watch_output_folder'] = request.form.get('watch_output_folder', '').strip()
        config['watch_use_hard_links'] = 'watch_use_hard_links' in request.form
        config['watch_interval_seconds'] = int(request.form.get('watch_interval_seconds', 60))
        config['watch_delete_empty_folders'] = 'watch_delete_empty_folders' in request.form
        config['watch_min_file_age_seconds'] = int(request.form.get('watch_min_file_age_seconds', 30))
        # Author initials setting (Issue #54)
        config['standardize_author_initials'] = 'standardize_author_initials' in request.form
        # Strip "Unabridged" from titles (Issue #92)
        config['strip_unabridged'] = 'strip_unabridged' in request.form
        # Community contributions setting
        config['contribute_to_community'] = 'contribute_to_community' in request.form
        # P2P cache setting (Issue #62)
        config['enable_p2p_cache'] = 'enable_p2p_cache' in request.form

        # Provider chain settings - parse comma-separated values into lists
        audio_chain_str = request.form.get('audio_provider_chain', 'bookdb,gemini').strip()
        config['audio_provider_chain'] = [p.strip() for p in audio_chain_str.split(',') if p.strip()]
        text_chain_str = request.form.get('text_provider_chain', 'gemini,openrouter').strip()
        config['text_provider_chain'] = [p.strip() for p in text_chain_str.split(',') if p.strip()]

        # Save config (without secrets)
        save_config(config)

        # Save secrets separately (preserving existing secrets like abs_api_token)
        # Only update API keys if user provided a new value (security: keys are no longer in HTML)
        secrets = load_secrets()
        new_openrouter_key = request.form.get('openrouter_api_key', '').strip()
        new_gemini_key = request.form.get('gemini_api_key', '').strip()
        new_google_books_key = request.form.get('google_books_api_key', '').strip()
        new_bookdb_key = request.form.get('bookdb_api_key', '').strip()

        # Only overwrite existing keys if user entered a new value
        if new_openrouter_key:
            secrets['openrouter_api_key'] = new_openrouter_key
        if new_gemini_key:
            secrets['gemini_api_key'] = new_gemini_key
        if new_google_books_key:
            secrets['google_books_api_key'] = new_google_books_key
        if new_bookdb_key:
            secrets['bookdb_api_key'] = new_bookdb_key
        save_secrets(secrets)

        return redirect(url_for('settings_page'))

    config = load_config()
    secrets = load_secrets()
    # Pass actual API key values to template (hidden by default, eye toggle to reveal)
    # This is a local/self-hosted app - users need to verify their keys were saved correctly
    config['gemini_api_key'] = secrets.get('gemini_api_key', '')
    config['openrouter_api_key'] = secrets.get('openrouter_api_key', '')
    config['google_books_api_key'] = secrets.get('google_books_api_key', '')
    config['bookdb_api_key'] = secrets.get('bookdb_api_key', '')
    return render_template('settings.html', config=config, version=APP_VERSION)


# ============== PATH DIAGNOSTIC ==============

@app.route('/api/check_path', methods=['POST'])
def api_check_path():
    """
    Diagnostic endpoint to help users debug Docker volume mount issues.
    Shows what the container can actually see at a given path.
    """
    data = request.get_json() or {}
    path = data.get('path', '').strip()

    if not path:
        return jsonify({'success': False, 'error': 'No path provided'})

    result = {
        'path': path,
        'exists': os.path.exists(path),
        'is_directory': False,
        'is_file': False,
        'readable': False,
        'contents': [],
        'error': None,
        'suggestion': None
    }

    if not result['exists']:
        # Path doesn't exist - likely a Docker mount issue
        result['error'] = f"Path does not exist: {path}"
        result['suggestion'] = (
            "This path is not visible to the container. "
            "In Docker, you must mount host paths in your docker-compose.yml:\n\n"
            f"volumes:\n  - /your/host/path:{path}\n\n"
            "Then use the CONTAINER path (right side) in Settings, not the host path."
        )

        # Check if common mount points exist
        common_mounts = ['/audiobooks', '/data', '/media', '/books', '/library']
        existing_mounts = [m for m in common_mounts if os.path.exists(m)]
        if existing_mounts:
            result['available_mounts'] = existing_mounts
            result['suggestion'] += f"\n\nAvailable mount points: {', '.join(existing_mounts)}"

        return jsonify(result)

    result['is_directory'] = os.path.isdir(path)
    result['is_file'] = os.path.isfile(path)
    result['readable'] = os.access(path, os.R_OK)

    if not result['readable']:
        result['error'] = f"Path exists but is not readable (permission denied)"
        result['suggestion'] = "Check file permissions. The container user may not have access."
        return jsonify(result)

    if result['is_directory']:
        try:
            entries = sorted(os.listdir(path))[:50]  # Limit to first 50 entries
            result['contents'] = []
            result['total_entries'] = len(os.listdir(path))

            for entry in entries:
                entry_path = os.path.join(path, entry)
                entry_info = {
                    'name': entry,
                    'is_dir': os.path.isdir(entry_path),
                    'is_file': os.path.isfile(entry_path)
                }
                if entry_info['is_file']:
                    try:
                        entry_info['size'] = os.path.getsize(entry_path)
                    except:
                        entry_info['size'] = 0
                result['contents'].append(entry_info)

            if result['total_entries'] == 0:
                result['suggestion'] = "Directory exists but is empty. Are your audiobooks in a subdirectory?"
            else:
                result['success'] = True

        except Exception as e:
            result['error'] = f"Could not list directory: {e}"
    elif result['is_file']:
        result['error'] = "This is a file, not a directory. Library paths should be directories."
        result['suggestion'] = "Use the parent directory instead."

    result['success'] = result.get('success', result['exists'] and result['readable'] and result['is_directory'])
    return jsonify(result)


# ============== API ENDPOINTS ==============

@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Trigger a library scan."""
    # Issue #61: Check if scan already in progress
    if scan_in_progress:
        return jsonify({
            'success': False,
            'error': 'Scan already in progress',
            'message': 'A scan is already running. Please wait for it to complete.'
        }), 409  # HTTP 409 Conflict

    config = load_config()
    checked, scanned, queued = scan_library(config)
    return jsonify({
        'success': True,
        'checked': checked,      # Total book folders examined
        'scanned': scanned,      # New books added to tracking
        'queued': queued         # Books needing fixes
    })


@app.route('/api/scan/status', methods=['GET'])
def api_scan_status():
    """Check if a scan is currently in progress. Issue #61."""
    return jsonify({
        'scanning': scan_in_progress
    })


@app.route('/api/whisper-status', methods=['GET'])
def api_whisper_status():
    """Check if faster-whisper is installed and model is ready."""
    config = load_config()
    model_name = config.get('whisper_model', 'base')

    # Check if faster-whisper is installed
    try:
        import faster_whisper
        installed = True
    except ImportError:
        installed = False
        return jsonify({
            'installed': False,
            'model': model_name,
            'model_ready': False
        })

    # Check if model is downloaded (check HuggingFace cache)
    import os
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    model_dir = f"models--Systran--faster-whisper-{model_name}"
    model_ready = os.path.isdir(os.path.join(cache_dir, model_dir))

    return jsonify({
        'installed': installed,
        'model': model_name,
        'model_ready': model_ready
    })


@app.route('/api/install-whisper', methods=['POST'])
def api_install_whisper():
    """Install faster-whisper package via pip."""
    import subprocess
    import sys

    try:
        # Issue #63: Use DATA_DIR for pip install since /app may not be writable in Docker
        # DATA_DIR is the mounted volume (/data or /config) that's always writable
        pip_cache = os.path.join(str(DATA_DIR), '.cache', 'pip')
        pip_local = os.path.join(str(DATA_DIR), '.local')
        os.makedirs(pip_cache, exist_ok=True)
        os.makedirs(pip_local, exist_ok=True)

        # Set up environment for pip to use our writable directories
        env = os.environ.copy()
        env['HOME'] = str(DATA_DIR)  # Use writable data directory
        env['PIP_CACHE_DIR'] = pip_cache
        env['PYTHONUSERBASE'] = pip_local

        # Install faster-whisper with --user flag to install to PYTHONUSERBASE
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '--user', 'faster-whisper'],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            env=env
        )

        if result.returncode == 0:
            # Add user site-packages to path if not already there
            user_site = os.path.join(pip_local, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
            if user_site not in sys.path:
                sys.path.insert(0, user_site)

            return jsonify({
                'success': True,
                'message': 'faster-whisper installed successfully. Refresh the page to use it.'
            })
        else:
            error_msg = result.stderr[:500] if result.stderr else 'Unknown error'
            logger.error(f"[WHISPER] Install failed: {error_msg}")
            return jsonify({
                'success': False,
                'error': f'Install failed: {error_msg}'
            })

    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Installation timed out after 5 minutes'
        })
    except Exception as e:
        logger.error(f"[WHISPER] Install error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/deep_verify', methods=['POST'])
def api_deep_verify():
    """
    Deep Verification Mode - "Hail Mary" full library audit.

    Queues ALL books for API verification regardless of how "clean" they look.
    This catches cases where folder structure appears correct but author
    attribution is actually wrong.

    WARNING: This is expensive and time-consuming!
    """
    config = load_config()

    # Get stats first to show what will be affected
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT COUNT(*) FROM books
                 WHERE (user_locked IS NULL OR user_locked = 0)
                   AND status NOT IN ('series_folder', 'multi_book_files', 'needs_split', 'needs_attention')''')
    total_books = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM queue")
    current_queue = c.fetchone()[0]

    conn.close()

    # Run deep verification
    result = deep_verify_all_books(config)

    # Estimate time (rough: ~30 seconds per book for full API cycle)
    estimated_minutes = (result['queued'] * 30) // 60
    estimated_hours = estimated_minutes // 60
    estimated_minutes = estimated_minutes % 60

    return jsonify({
        'success': True,
        'queued': result['queued'],
        'already_verified': result['already_verified'],
        'total_books': result['total'],
        'previous_queue_size': current_queue,
        'estimated_time': f"{estimated_hours}h {estimated_minutes}m" if estimated_hours else f"{estimated_minutes}m",
        'message': f"Deep verification queued {result['queued']} books for full API verification"
    })


@app.route('/api/chaos_scan', methods=['POST'])
def api_chaos_scan():
    """
    Handle chaotic libraries with loose files dumped in root.
    Analyzes, groups, and identifies loose audio files.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])

    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'}), 400

    all_results = []
    for lib_path in library_paths:
        if os.path.exists(lib_path):
            results = handle_chaos_library(lib_path, config)
            all_results.extend(results)

    # Summarize
    identified = sum(1 for r in all_results if r.get('confidence') in ['high', 'medium'])
    needs_review = sum(1 for r in all_results if r.get('confidence') == 'low')
    failed = sum(1 for r in all_results if r.get('confidence') == 'none' or r.get('identification') == 'failed')

    return jsonify({
        'success': True,
        'groups': all_results,
        'summary': {
            'total_groups': len(all_results),
            'identified': identified,
            'needs_review': needs_review,
            'failed': failed
        }
    })

@app.route('/api/search_progress', methods=['GET'])
def api_search_progress():
    """
    Get current progress of ongoing search/scan operations.
    Returns queue position, percent complete, and current item being processed.
    """
    state = search_progress.get_state()
    return jsonify({
        'success': True,
        'active': state['active'],
        'operation': state['operation'],
        'percent': state['percent'],
        'processed': state['processed'],
        'total': state['total'],
        'current_item': state['current_item'],
        'queue_position': state['queue_position'],
        'queue': state['queue'][:10]  # Show next 10 in queue
    })

@app.route('/api/chaos_apply', methods=['POST'])
def api_chaos_apply():
    """
    Apply chaos handler results - create folders and move files.
    Expects JSON with groups to apply.
    """
    import shutil

    data = request.get_json()
    if not data or 'groups' not in data:
        return jsonify({'success': False, 'error': 'No groups provided'}), 400

    config = load_config()
    library_paths = config.get('library_paths', [])
    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'}), 400

    lib_root = Path(library_paths[0])
    applied = 0
    errors = []
    skipped = []  # Items that couldn't be identified - left alone on disk

    for group in data['groups']:
        author = group.get('author', 'Unknown Author')
        title = group.get('title')
        files = group.get('files', [])
        confidence = group.get('confidence', 'none')
        identification = group.get('identification', '')

        if not title or not files:
            continue

        # Skip unidentified items - leave them alone on disk
        # Don't move files to "Unknown Author" folder - that's not helpful
        if author in ('Unknown Author', 'Unknown', None, '') or confidence == 'none' or identification == 'failed':
            skipped.append({
                'title': title or f"Unknown ({len(files)} files)",
                'files': files,
                'reason': 'Could not identify - left in place for manual review'
            })
            logger.info(f"CHAOS: Skipping unidentified group '{title}' - left in place")
            continue

        # Sanitize path components
        safe_author = sanitize_path_component(author)
        safe_title = sanitize_path_component(title)
        if not safe_author or not safe_title:
            errors.append(f"Invalid author/title: {author} / {title}")
            continue

        # Create target folder
        target_folder = lib_root / safe_author / safe_title

        try:
            target_folder.mkdir(parents=True, exist_ok=True)

            # Move files
            for file_path in files:
                src = Path(file_path)
                if src.exists() and src.is_file():
                    dst = target_folder / src.name
                    shutil.move(str(src), str(dst))
                    logger.info(f"CHAOS: Moved {src.name} -> {target_folder}")

            applied += 1

        except Exception as e:
            errors.append(f"Error with {title}: {str(e)}")
            logger.error(f"CHAOS APPLY ERROR: {e}")

    return jsonify({
        'success': True,
        'applied': applied,
        'skipped': skipped,  # Items left in place (couldn't identify)
        'skipped_count': len(skipped),
        'errors': errors
    })

@app.route('/api/deep_rescan', methods=['POST'])
def api_deep_rescan():
    """Deep re-scan: Reset all books and re-queue for fresh metadata lookup."""
    conn = get_db()
    c = conn.cursor()

    # Clear queue first
    c.execute('DELETE FROM queue')

    # Reset book statuses to force re-checking, BUT skip 'protected' books (user undid these)
    c.execute("UPDATE books SET status = 'pending' WHERE status != 'protected'")

    # Get count of protected books
    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'protected'")
    protected_count = c.fetchone()['count']

    # Get all non-protected books and add to queue
    c.execute("SELECT id, path FROM books WHERE status != 'protected'")
    books = c.fetchall()

    queued = 0
    for book in books:
        # Add to queue for re-processing
        c.execute('INSERT INTO queue (book_id, added_at) VALUES (?, ?)',
                  (book['id'], datetime.now().isoformat()))
        queued += 1

    conn.commit()
    conn.close()

    msg = f'Queued {queued} books for fresh metadata verification'
    if protected_count > 0:
        msg += f' ({protected_count} protected books skipped)'

    logger.info(f"Deep re-scan: {msg}")
    return jsonify({
        'success': True,
        'queued': queued,
        'protected': protected_count,
        'message': msg
    })


@app.route('/api/health_scan', methods=['POST'])
def api_health_scan():
    """
    Scan library for corrupt/incomplete audio files.
    Uses ffprobe to verify each file can be read properly.
    Returns list of problematic files grouped by book folder.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])

    if not library_paths:
        return jsonify({'success': False, 'error': 'No library paths configured'})

    corrupt_files = []
    total_checked = 0
    total_duration = 0

    for lib_path in library_paths:
        lib_path = Path(lib_path)
        if not lib_path.exists():
            continue

        # Find all audio files
        audio_files = find_audio_files(str(lib_path))
        logger.info(f"Health scan: checking {len(audio_files)} audio files in {lib_path}")

        for audio_file in audio_files:
            total_checked += 1
            health = check_audio_file_health(audio_file)

            if not health['valid']:
                # Get folder info
                file_path = Path(audio_file)
                folder = file_path.parent
                size_mb = file_path.stat().st_size / (1024 * 1024) if file_path.exists() else 0

                corrupt_files.append({
                    'file': str(audio_file),
                    'folder': str(folder),
                    'folder_name': folder.name,
                    'filename': file_path.name,
                    'size_mb': round(size_mb, 1),
                    'error': health['error']
                })
            elif health['duration']:
                total_duration += health['duration']

    # Group by folder for easier review
    folders_with_issues = {}
    for cf in corrupt_files:
        folder = cf['folder']
        if folder not in folders_with_issues:
            folders_with_issues[folder] = {
                'folder': folder,
                'folder_name': cf['folder_name'],
                'files': [],
                'total_size_mb': 0
            }
        folders_with_issues[folder]['files'].append({
            'filename': cf['filename'],
            'size_mb': cf['size_mb'],
            'error': cf['error']
        })
        folders_with_issues[folder]['total_size_mb'] += cf['size_mb']

    result = {
        'success': True,
        'total_checked': total_checked,
        'total_healthy_duration_hours': round(total_duration / 3600, 1),
        'corrupt_count': len(corrupt_files),
        'folders_with_issues': list(folders_with_issues.values())
    }

    logger.info(f"Health scan complete: {total_checked} files checked, {len(corrupt_files)} corrupt")
    return jsonify(result)


@app.route('/api/delete_corrupt', methods=['POST'])
def api_delete_corrupt():
    """
    Delete a corrupt folder or file.
    Requires 'path' in request body - must be within a library path.
    """
    config = load_config()
    library_paths = config.get('library_paths', [])
    data = request.json if request.is_json else {}

    target_path = data.get('path')
    if not target_path:
        return jsonify({'success': False, 'error': 'No path provided'})

    target = Path(target_path)

    # Security: verify path is within a library path
    is_safe = False
    for lib_path in library_paths:
        try:
            target.relative_to(lib_path)
            is_safe = True
            break
        except ValueError:
            continue

    if not is_safe:
        return jsonify({'success': False, 'error': 'Path not within library'})

    if not target.exists():
        return jsonify({'success': False, 'error': 'Path does not exist'})

    try:
        import shutil
        if target.is_dir():
            shutil.rmtree(target)
            logger.info(f"Deleted corrupt folder: {target}")
        else:
            target.unlink()
            logger.info(f"Deleted corrupt file: {target}")

        return jsonify({'success': True, 'message': f'Deleted: {target}'})
    except Exception as e:
        logger.error(f"Failed to delete {target}: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/process', methods=['POST'])
def api_process():
    """Process the queue using layered processing."""
    config = load_config()
    data = request.json if request.is_json else {}
    process_all = data.get('all', False)
    limit = data.get('limit')

    logger.info(f"API process called: all={process_all}, limit={limit}")

    # Update status to show we're processing
    update_processing_status('active', True)

    if process_all:
        # Process entire queue in batches
        processed, fixed = process_all_queue(config)
    else:
        # Use layered processing even for limited batches
        # Layer 1: API lookups first
        total_processed = 0
        total_fixed = 0

        if config.get('enable_api_lookups', True):
            l1_processed, l1_resolved = process_layer_1_api(config, limit)
            total_processed += l1_processed
            logger.info(f"[LAYER 1] Processed {l1_processed}, resolved {l1_resolved}")

        # Layer 2: AI verification for items that passed through Layer 1
        if config.get('enable_ai_verification', True):
            l2_processed, l2_fixed = process_queue(config, limit)
            total_processed += l2_processed
            total_fixed += l2_fixed

        # Layer 3: Audio analysis (if enabled)
        if config.get('enable_audio_analysis', False):
            l3_processed, l3_fixed = process_layer_3_audio(config, limit)
            total_processed += l3_processed
            total_fixed += l3_fixed

        processed, fixed = total_processed, total_fixed

    # Issue #57: Calculate verified count (processed items that didn't need fixing)
    verified = processed - fixed

    # Check remaining queue size for status message
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM queue')
    remaining = c.fetchone()[0]
    conn.close()

    # Clear active status when done
    update_processing_status('active', False)
    clear_current_book()

    # Build helpful status message
    if remaining == 0:
        status = 'complete'
        message = f'Queue complete! {fixed} renamed, {verified} already correct'
    else:
        status = 'partial'
        message = f'Processed batch: {fixed} renamed, {verified} already correct. {remaining} remaining in queue.'

    return jsonify({
        'success': True,
        'processed': processed,
        'fixed': fixed,
        'verified': verified,
        'remaining': remaining,
        'status': status,
        'message': message
    })


# Background processing thread state
_bg_processing_thread = None
_bg_processing_active = False

@app.route('/api/process_background', methods=['POST'])
def api_process_background():
    """Start background processing that updates status in real-time.

    Unlike /api/process which blocks, this returns immediately and
    processes in a background thread so the status bar can update live.
    """
    global _bg_processing_thread, _bg_processing_active

    # Check if already processing
    if _bg_processing_active:
        return jsonify({
            'success': False,
            'message': 'Background processing already running'
        })

    def process_in_background():
        global _bg_processing_active
        _bg_processing_active = True
        try:
            config = load_config()
            # Issue #137: Use full pipeline (process_all_queue) instead of just Layers 1+2
            # This runs audio analysis, Skaldleita identification, AI verification, etc.
            # Status bar updates happen inside process_all_queue via update_processing_status
            processed, fixed = process_all_queue(config)
            logger.info(f"Background processing complete: {processed} processed, {fixed} fixed")
        except Exception as e:
            logger.error(f"Background processing error: {e}", exc_info=True)
        finally:
            update_processing_status('active', False)
            update_processing_status('layer', 0)
            update_processing_status('layer_name', 'Idle')
            clear_current_book()
            _bg_processing_active = False

    import threading
    _bg_processing_thread = threading.Thread(target=process_in_background, daemon=True)
    _bg_processing_thread.start()

    return jsonify({
        'success': True,
        'message': 'Background processing started. Watch the status bar for updates.'
    })


@app.route('/api/process_status')
def api_process_status():
    """Get current processing status."""
    return jsonify(get_processing_status())


@app.route('/api/live_status')
def api_live_status():
    """Get comprehensive live status for the status bar.

    Returns worker state, processing status, queue depth, and recent activity
    all in one efficient call for the persistent status bar.
    """
    import time as time_module

    # Get processing status
    status = get_processing_status()

    # Get queue depth
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_count = c.fetchone()['count']

    # Get pending fixes count
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending_fixes = c.fetchone()['count']

    # Get recent activity (last 5 processed books)
    c.execute('''SELECT new_author, new_title, status, fixed_at
                 FROM history
                 ORDER BY fixed_at DESC
                 LIMIT 5''')
    recent = [dict(row) for row in c.fetchall()]
    conn.close()

    # Build response
    worker_running = is_worker_running()

    # Determine the display state
    if status.get('active'):
        state = 'processing'
    elif worker_running:
        state = 'idle'
    else:
        state = 'stopped'

    # Calculate time since last activity
    last_activity_time = status.get('last_activity_time', 0)
    if last_activity_time:
        seconds_ago = int(time_module.time() - last_activity_time)
        if seconds_ago < 60:
            time_ago = f"{seconds_ago}s ago"
        elif seconds_ago < 3600:
            time_ago = f"{seconds_ago // 60}m ago"
        else:
            time_ago = f"{seconds_ago // 3600}h ago"
    else:
        time_ago = ""

    return jsonify({
        'state': state,
        'worker_running': worker_running,
        'processing': {
            'active': status.get('active', False),
            'layer': status.get('layer', 0),
            'layer_name': status.get('layer_name', 'Idle'),
            'current_stage': status.get('current', ''),
            'current_book': status.get('current_book', ''),
            'current_author': status.get('current_author', ''),
            'processed': status.get('processed', 0),
            'total': status.get('total', 0),
            # NEW: Detailed provider/API tracking
            'current_provider': status.get('current_provider', ''),
            'current_step': status.get('current_step', ''),
            'provider_chain': status.get('provider_chain', []),
            'provider_index': status.get('provider_index', 0),
            'api_latency_ms': status.get('api_latency_ms', 0),
            'confidence': status.get('confidence', 0),
            'is_free_api': status.get('is_free_api', True),
        },
        'queue': {
            'count': queue_count,
            'pending_fixes': pending_fixes,
        },
        'last_activity': status.get('last_activity', ''),
        'last_activity_time_ago': time_ago,
        'recent': recent[:3],  # Last 3 for the dropdown
    })


@app.route('/api/apply_fix/<int:history_id>', methods=['POST'])
def api_apply_fix(history_id):
    """Apply a specific fix."""
    success, message = apply_fix(history_id)
    return jsonify({'success': success, 'message': message})

@app.route('/api/reject_fix/<int:history_id>', methods=['POST'])
def api_reject_fix(history_id):
    """Reject a pending fix - delete it and mark book as OK."""
    conn = get_db()
    c = conn.cursor()

    # Get the history entry
    c.execute('SELECT book_id FROM history WHERE id = ?', (history_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'success': False, 'error': 'Fix not found'})

    book_id = row['book_id']

    # Delete the history entry
    c.execute('DELETE FROM history WHERE id = ?', (history_id,))

    # Mark book as verified/OK so it doesn't get re-queued
    c.execute("UPDATE books SET status = 'verified' WHERE id = ?", (book_id,))

    conn.commit()
    conn.close()

    logger.info(f"Rejected fix {history_id}, book {book_id} marked as verified")
    return jsonify({'success': True})

@app.route('/api/dismiss_error/<int:history_id>', methods=['POST'])
def api_dismiss_error(history_id):
    """Dismiss an error entry - just delete it from history."""
    conn = get_db()
    c = conn.cursor()

    # Get the history entry
    c.execute('SELECT book_id, status FROM history WHERE id = ?', (history_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'success': False, 'error': 'Entry not found'})

    # Delete the history entry
    c.execute('DELETE FROM history WHERE id = ?', (history_id,))

    # If the book still exists, mark it as verified so it doesn't keep erroring
    if row['book_id']:
        c.execute("UPDATE books SET status = 'verified' WHERE id = ?", (row['book_id'],))

    conn.commit()
    conn.close()

    logger.info(f"Dismissed error entry {history_id}")
    return jsonify({'success': True})

@app.route('/api/apply_all_pending', methods=['POST'])
def api_apply_all_pending():
    """Apply all pending fixes."""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM history WHERE status = 'pending_fix'")
    pending = c.fetchall()
    conn.close()

    applied = 0
    errors = 0
    for row in pending:
        success, _ = apply_fix(row['id'])
        if success:
            applied += 1
        else:
            errors += 1

    return jsonify({
        'success': True,
        'applied': applied,
        'errors': errors,
        'message': f'Applied {applied} fixes, {errors} errors'
    })


@app.route('/api/reject_all_pending', methods=['POST'])
def api_reject_all_pending():
    """Reject all pending fixes, resetting books to pending status."""
    conn = get_db()
    c = conn.cursor()

    # Get count first
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'No pending fixes to reject', 'rejected': 0})

    # Reset books to pending status (NOT verified - user just rejected the proposed fix)
    # Reset verification_layer so they can be re-processed if desired
    c.execute('''UPDATE books SET status = 'pending', verification_layer = 0
                 WHERE id IN (SELECT book_id FROM history WHERE status = 'pending_fix')''')

    # Delete all pending fix entries
    c.execute("DELETE FROM history WHERE status = 'pending_fix'")
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Rejected {count} pending fixes (books reset to pending)',
        'rejected': count
    })


@app.route('/api/remove_from_queue/<int:queue_id>', methods=['POST'])
def api_remove_from_queue(queue_id):
    """Remove an item from the queue."""
    conn = get_db()
    c = conn.cursor()

    # Get book_id first
    c.execute('SELECT book_id FROM queue WHERE id = ?', (queue_id,))
    row = c.fetchone()
    if row:
        c.execute('DELETE FROM queue WHERE id = ?', (queue_id,))
        c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
        conn.commit()

    conn.close()
    return jsonify({'success': True})


@app.route('/api/remove_book/<int:book_id>', methods=['POST'])
def api_remove_book(book_id):
    """Remove a book entry from the library database (does NOT delete files on disk)."""
    conn = get_db()
    c = conn.cursor()

    # Get book info for logging
    c.execute('SELECT path FROM books WHERE id = ?', (book_id,))
    book = c.fetchone()
    if not book:
        conn.close()
        return jsonify({'success': False, 'error': 'Book not found'}), 404

    path = book['path']

    # Delete from all related tables
    c.execute('DELETE FROM queue WHERE book_id = ?', (book_id,))
    c.execute('DELETE FROM history WHERE book_id = ?', (book_id,))
    c.execute('DELETE FROM books WHERE id = ?', (book_id,))

    conn.commit()
    conn.close()

    logger.info(f"[REMOVE] Removed book from library database: {path}")
    return jsonify({'success': True, 'message': 'Book removed from library'})


@app.route('/api/remove_books_bulk', methods=['POST'])
def api_remove_books_bulk():
    """Remove multiple book entries from the library database (does NOT delete files on disk)."""
    data = request.get_json()
    book_ids = data.get('book_ids', [])

    if not book_ids:
        return jsonify({'success': False, 'error': 'No book IDs provided'}), 400

    conn = get_db()
    c = conn.cursor()

    removed = 0
    for book_id in book_ids:
        c.execute('SELECT path FROM books WHERE id = ?', (book_id,))
        book = c.fetchone()
        if book:
            c.execute('DELETE FROM queue WHERE book_id = ?', (book_id,))
            c.execute('DELETE FROM history WHERE book_id = ?', (book_id,))
            c.execute('DELETE FROM books WHERE id = ?', (book_id,))
            removed += 1
            logger.info(f"[REMOVE] Bulk removed: {book['path']}")

    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Removed {removed} book(s) from library',
        'removed': removed
    })


@app.route('/api/clear_queue', methods=['POST'])
def api_clear_queue():
    """Clear all items from the queue, marking them as verified."""
    conn = get_db()
    c = conn.cursor()

    # Get count first
    c.execute('SELECT COUNT(*) as count FROM queue')
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'Queue already empty', 'cleared': 0})

    # Reset queued books to pending status (NOT verified - they weren't actually verified!)
    # Also reset verification_layer so they can be re-processed if re-scanned
    c.execute('''UPDATE books SET status = 'pending', verification_layer = 0
                 WHERE id IN (SELECT book_id FROM queue)''')

    # Clear the queue
    c.execute('DELETE FROM queue')
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Cleared {count} items from queue (reset to pending)',
        'cleared': count
    })


@app.route('/api/find_drastic_changes')
def api_find_drastic_changes():
    """Find history items where author changed drastically - potential mistakes."""
    conn = get_db()
    c = conn.cursor()

    # Get all fixed items where old and new path differ
    c.execute('''SELECT * FROM history
                 WHERE status = 'fixed' AND old_path != new_path
                 ORDER BY fixed_at DESC''')
    items = c.fetchall()
    conn.close()

    drastic_items = []
    for item in items:
        if is_drastic_author_change(item['old_author'], item['new_author']):
            drastic_items.append({
                'id': item['id'],
                'old_author': item['old_author'],
                'old_title': item['old_title'],
                'new_author': item['new_author'],
                'new_title': item['new_title'],
                'fixed_at': item['fixed_at']
            })

    return jsonify({
        'count': len(drastic_items),
        'items': drastic_items[:50]  # Limit to 50 for UI
    })

@app.route('/api/undo_all_drastic', methods=['POST'])
def api_undo_all_drastic():
    """Undo all drastic author changes."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get all fixed items
    c.execute('''SELECT * FROM history
                 WHERE status = 'fixed' AND old_path != new_path''')
    items = c.fetchall()

    undone = 0
    errors = 0

    for item in items:
        if not is_drastic_author_change(item['old_author'], item['new_author']):
            continue

        old_path = item['old_path']
        new_path = item['new_path']

        # Check if paths exist correctly
        if not os.path.exists(new_path):
            continue  # Already moved or doesn't exist
        if os.path.exists(old_path):
            continue  # Original location already exists

        try:
            shutil.move(new_path, old_path)
            c.execute('''UPDATE history SET status = 'undone', error_message = 'Auto-undone: drastic author change'
                         WHERE id = ?''', (item['id'],))
            c.execute('''UPDATE books SET
                         current_author = ?, current_title = ?, path = ?, status = 'protected'
                         WHERE id = ?''',
                      (item['old_author'], item['old_title'], old_path, item['book_id']))
            undone += 1
            logger.info(f"Auto-undone drastic change: {item['new_author']} -> {item['old_author']}")
        except Exception as e:
            errors += 1
            logger.error(f"Failed to undo {item['id']}: {e}")

    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'undone': undone,
        'errors': errors,
        'message': f'Undone {undone} drastic changes, {errors} errors'
    })

@app.route('/api/undo/<int:history_id>', methods=['POST'])
def api_undo(history_id):
    """Undo a fix - rename folder back to original name and restore original tags."""
    import shutil
    from audio_tagging import restore_tags_from_sidecar

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    old_path = record['old_path']
    new_path = record['new_path']

    # Check if the new_path exists (current location)
    if not os.path.exists(new_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Current path not found: {new_path}'
        }), 404

    # Check if old_path already exists (would cause conflict)
    if os.path.exists(old_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Original path already exists: {old_path}'
        }), 409

    try:
        new_path_obj = Path(new_path)
        
        # Determine folder for sidecar (if new_path is a file, parent has sidecar)
        if new_path_obj.is_file():
            sidecar_folder = new_path_obj.parent
        else:
            sidecar_folder = new_path_obj

        # Restore original tags from sidecar backup before moving
        tags_restored = False
        tags_message = ""
        try:
            restore_result = restore_tags_from_sidecar(
                sidecar_folder,
                delete_sidecar_on_success=True  # Clean up sidecar after successful restore
            )
            if restore_result['files_restored'] > 0:
                tags_restored = True
                tags_message = f" Tags restored for {restore_result['files_restored']} file(s)."
                logger.info(f"Undo: Restored tags for {restore_result['files_restored']} files")
            elif restore_result.get('error'):
                tags_message = f" (Tag restore note: {restore_result['error']})"
        except Exception as tag_err:
            logger.warning(f"Could not restore tags during undo: {tag_err}")
            tags_message = " (Tags could not be restored)"

        # Rename back to original
        shutil.move(new_path, old_path)
        logger.info(f"Undo: Renamed '{new_path}' back to '{old_path}'")

        # Clean up empty parent folder if we moved a file
        if new_path_obj.is_file():
            try:
                parent = new_path_obj.parent
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    logger.info(f"Undo: Removed empty folder {parent}")
            except OSError:
                pass

        # Update history record
        c.execute('''UPDATE history SET status = 'undone', error_message = ?
                     WHERE id = ?''', (f'Manually undone by user{tags_message}', history_id))

        # Update book record back to original - use 'protected' status so deep rescan won't re-queue
        c.execute('''UPDATE books SET
                     current_author = ?, current_title = ?, path = ?, status = 'protected'
                     WHERE id = ?''',
                  (record['old_author'], record['old_title'], old_path, record['book_id']))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Undone! Renamed back to: {record['old_author']} / {record['old_title']}{tags_message}",
            'tags_restored': tags_restored
        })

    except Exception as e:
        conn.close()
        logger.error(f"Undo failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/remove_duplicate/<int:history_id>', methods=['POST'])
def api_remove_duplicate(history_id):
    """Remove a confirmed duplicate - deletes the source folder (old_path) since destination is properly named."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    if record['status'] != 'duplicate':
        conn.close()
        return jsonify({'success': False, 'error': 'This record is not marked as a duplicate'}), 400

    old_path = record['old_path']  # This is the duplicate to remove
    new_path = record['new_path']  # This is the properly named copy to keep

    # Safety checks
    if not os.path.exists(old_path):
        # Already removed - just update status
        c.execute('UPDATE history SET status = ? WHERE id = ?', ('duplicate_removed', history_id))
        c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Duplicate was already removed'})

    if not os.path.exists(new_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'The properly named copy no longer exists: {new_path}. Refusing to delete.'
        }), 400

    # Re-verify it's still a duplicate before deleting
    comparison = compare_book_folders(old_path, new_path)
    if not (comparison['identical'] or comparison['same_book']):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Files have changed - no longer appears to be a duplicate (overlap: {comparison["overlap_ratio"]:.0%}). Please review manually.'
        }), 400

    try:
        # Get size before removal for logging
        total_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())

        # Remove the duplicate folder
        shutil.rmtree(old_path)
        logger.info(f"Removed duplicate: {old_path} ({total_size // 1024 // 1024}MB)")

        # Clean up empty parent folder (e.g., Unknown/)
        parent = Path(old_path).parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
                logger.info(f"Removed empty parent folder: {parent}")
        except OSError:
            pass

        # Update records
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('duplicate_removed', f'Removed {total_size // 1024 // 1024}MB duplicate', history_id))
        c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Removed duplicate ({total_size // 1024 // 1024}MB freed)',
            'removed_path': old_path,
            'kept_path': new_path
        })

    except Exception as e:
        conn.close()
        logger.error(f"Failed to remove duplicate: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/replace_corrupt/<int:history_id>', methods=['POST'])
def api_replace_corrupt(history_id):
    """Replace corrupt destination with valid source - delete corrupt, move source to dest path."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get the history record
    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    record = c.fetchone()

    if not record:
        conn.close()
        return jsonify({'success': False, 'error': 'History record not found'}), 404

    if record['status'] != 'corrupt_dest':
        conn.close()
        return jsonify({'success': False, 'error': 'This record is not marked as corrupt_dest'}), 400

    old_path = record['old_path']  # Valid source
    new_path = record['new_path']  # Corrupt destination

    # Safety checks
    if not os.path.exists(old_path):
        conn.close()
        return jsonify({
            'success': False,
            'error': f'Valid source no longer exists: {old_path}'
        }), 400

    try:
        # Get sizes for logging
        source_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())

        # Delete the corrupt destination if it exists
        if os.path.exists(new_path):
            dest_size = sum(f.stat().st_size for f in Path(new_path).rglob('*') if f.is_file())
            shutil.rmtree(new_path)
            logger.info(f"Deleted corrupt destination: {new_path} ({dest_size // 1024 // 1024}MB)")

        # Create parent directories if needed
        Path(new_path).parent.mkdir(parents=True, exist_ok=True)

        # Move the valid source to the destination
        shutil.move(old_path, new_path)
        logger.info(f"Moved valid source to destination: {old_path} -> {new_path}")

        # Clean up empty parent folder
        parent = Path(old_path).parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
                logger.info(f"Removed empty parent folder: {parent}")
        except OSError:
            pass

        # Update records
        c.execute('''UPDATE history SET status = ?, error_message = ? WHERE id = ?''',
                 ('fixed', f'Replaced corrupt destination with valid source ({source_size // 1024 // 1024}MB)', history_id))
        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ? WHERE id = ?''',
                 (new_path, record['new_author'], record['new_title'], 'fixed', record['book_id']))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Replaced corrupt destination with valid source ({source_size // 1024 // 1024}MB)',
            'new_path': new_path
        })

    except Exception as e:
        conn.close()
        logger.error(f"Failed to replace corrupt destination: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/remove_all_duplicates', methods=['POST'])
def api_remove_all_duplicates():
    """Remove all confirmed duplicates in one operation."""
    import shutil

    conn = get_db()
    c = conn.cursor()

    # Get all duplicate records
    c.execute("SELECT * FROM history WHERE status = 'duplicate'")
    duplicates = c.fetchall()

    if not duplicates:
        conn.close()
        return jsonify({'success': True, 'message': 'No duplicates to remove', 'removed': 0})

    removed = 0
    skipped = 0
    errors = []
    total_freed = 0

    for record in duplicates:
        old_path = record['old_path']
        new_path = record['new_path']

        # Skip if already removed
        if not os.path.exists(old_path):
            c.execute('UPDATE history SET status = ? WHERE id = ?', ('duplicate_removed', record['id']))
            c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
            removed += 1
            continue

        # Skip if destination no longer exists
        if not os.path.exists(new_path):
            skipped += 1
            errors.append(f"Skipped {old_path}: destination {new_path} no longer exists")
            continue

        # Re-verify
        comparison = compare_book_folders(old_path, new_path)
        if not (comparison['identical'] or comparison['same_book']):
            skipped += 1
            errors.append(f"Skipped {old_path}: no longer appears to be duplicate")
            continue

        try:
            total_size = sum(f.stat().st_size for f in Path(old_path).rglob('*') if f.is_file())
            shutil.rmtree(old_path)
            total_freed += total_size

            # Clean up empty parent
            parent = Path(old_path).parent
            try:
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
            except OSError:
                pass

            c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                     ('duplicate_removed', f'Removed {total_size // 1024 // 1024}MB duplicate', record['id']))
            c.execute('DELETE FROM books WHERE id = ?', (record['book_id'],))
            removed += 1

        except Exception as e:
            skipped += 1
            errors.append(f"Failed to remove {old_path}: {e}")

    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'removed': removed,
        'skipped': skipped,
        'freed_mb': total_freed // 1024 // 1024,
        'errors': errors[:10] if errors else []  # Limit errors in response
    })


@app.route('/api/clear_all_errors', methods=['POST'])
def api_clear_all_errors():
    """Clear all error entries from history."""
    conn = get_db()
    c = conn.cursor()

    # Get count before clearing
    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'error'")
    count = c.fetchone()['count']

    if count == 0:
        conn.close()
        return jsonify({'success': True, 'message': 'No errors to clear', 'cleared': 0})

    # Delete all error entries
    c.execute("DELETE FROM history WHERE status = 'error'")
    conn.commit()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Cleared {count} error entries',
        'cleared': count
    })


@app.route('/api/stats')
def api_stats():
    """Get current stats."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT COUNT(*) as count FROM books')
    total = c.fetchone()['count']

    # Issue #131: Count only processable queue items (matching process_queue filters)
    c.execute('''SELECT COUNT(*) as count FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.status NOT IN ('verified', 'fixed', 'series_folder', 'multi_book_files', 'needs_attention')
                   AND (b.user_locked IS NULL OR b.user_locked = 0)''')
    queue = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'fixed'")
    fixed = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'verified'")
    verified = c.fetchone()['count']

    conn.close()

    return jsonify({
        'total_books': total,
        'queue_size': queue,
        'fixed': fixed,
        'pending_fixes': pending,
        'verified': verified,
        'worker_running': is_worker_running(),
        'processing': get_processing_status()
    })

@app.route('/api/queue')
def api_queue():
    """Get current queue items as JSON."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT q.id, q.reason, q.added_at,
                        b.id as book_id, b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 ORDER BY q.priority, q.added_at''')
    items = [dict(row) for row in c.fetchall()]

    conn.close()
    return jsonify({'items': items, 'count': len(items)})


@app.route('/api/error_reports')
def api_error_reports():
    """Get stored anonymous error reports (for debug menu)."""
    reports = get_error_reports()
    return jsonify({
        'success': True,
        'reports': reports,
        'count': len(reports)
    })


@app.route('/api/error_reports/clear', methods=['POST'])
def api_clear_error_reports():
    """Clear all stored error reports."""
    clear_error_reports()
    return jsonify({'success': True, 'message': 'Error reports cleared'})


@app.route('/api/error_reports/send', methods=['POST'])
def api_send_error_reports():
    """Send error reports to the developer via email."""
    reports = get_error_reports()

    if not reports:
        return jsonify({
            'success': False,
            'error': 'No error reports to send'
        })

    # Get optional user message from request
    data = request.get_json() or {}
    user_message = data.get('message', '').strip()

    result = send_error_report_email(reports, user_message if user_message else None)
    return jsonify(result)


@app.route('/api/analyze_path', methods=['POST'])
def api_analyze_path():
    """
    Analyze a path to understand its structure (Author/Series/Book).
    Uses smart analysis: script first, Gemini AI for ambiguous cases.

    POST body: {"path": "/path/to/folder"}
    """
    data = request.get_json() or {}
    path = data.get('path')

    if not path:
        return jsonify({'error': 'path is required'}), 400

    config = load_config()
    lib_paths = config.get('library_paths', [])

    # Find which library this path belongs to
    library_root = None
    for lib in lib_paths:
        if path.startswith(lib):
            library_root = lib
            break

    if not library_root:
        # Try parent folders
        p = Path(path)
        for lib in lib_paths:
            if str(p).startswith(lib) or str(p.parent).startswith(lib):
                library_root = lib
                break

    if not library_root and lib_paths:
        library_root = lib_paths[0]  # Default to first library

    if not library_root:
        return jsonify({'error': 'No library paths configured'}), 400

    result = smart_analyze_path(path, library_root, config)
    return jsonify(result)


# NOTE: Removed /api/structure_reversed endpoints in beta.69
# The reversed structure detection was causing false positives (Issue #52).
# Items that need attention now go through normal API lookup flow and end up
# in "Needs Attention" if APIs can't find matches. Users can fix manually there.


@app.route('/api/worker/start', methods=['POST'])
def api_start_worker():
    """Start background worker."""
    start_worker()
    return jsonify({'success': True})

@app.route('/api/worker/stop', methods=['POST'])
def api_stop_worker():
    """Stop background worker."""
    stop_worker()
    return jsonify({'success': True})


@app.route('/api/logs')
def api_logs():
    """Get recent log entries."""
    try:
        log_file = DATA_DIR / 'app.log'
        if log_file.exists():
            with open(log_file, 'r') as f:
                # Read last 100 lines
                lines = f.readlines()[-100:]
                return jsonify({'logs': [line.strip() for line in lines]})
        return jsonify({'logs': []})
    except Exception as e:
        return jsonify({'logs': [f'Error reading logs: {e}']})


@app.route('/api/clear_history', methods=['POST'])
def api_clear_history():
    """Clear all history entries."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('DELETE FROM history')
        conn.commit()
        conn.close()
        logger.info("History cleared by user")
        return jsonify({'success': True, 'message': 'History cleared'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/recent_history')
def api_recent_history():
    """Get recent history items for live updates."""
    conn = get_db()
    c = conn.cursor()
    # Use LEFT JOIN in case book was deleted or book_id is NULL
    c.execute('''SELECT h.*, b.path FROM history h
                 LEFT JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 15''')
    items = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({'items': items})


@app.route('/api/recent_activity')
def api_recent_activity():
    """Get last 15 processed books with full info including series.
    Shows ALL recently processed books - including pending items that have been touched.
    Excludes series_folder, loose_file, needs_split - users want to see actual book processing."""
    conn = get_db()
    c = conn.cursor()
    # Show all recently updated books (excluding folder/file markers)
    # This shows books AS they're being processed, not just when complete
    c.execute('''SELECT id, current_author, current_title, status, profile, updated_at, verification_layer
                 FROM books
                 WHERE status NOT IN ('series_folder', 'loose_file', 'needs_split', 'skipped')
                 AND verification_layer > 0
                 ORDER BY updated_at DESC
                 LIMIT 15''')
    rows = c.fetchall()
    conn.close()

    items = []
    for row in rows:
        profile_data = {}
        if row['profile']:
            try:
                profile_data = json.loads(row['profile'])
            except:
                pass

        # Extract series info from profile
        series = profile_data.get('series', {}).get('value', '') or ''
        series_num = profile_data.get('series_num', {}).get('value', '')
        narrator = profile_data.get('narrator', {}).get('value', '') or ''
        confidence = profile_data.get('overall_confidence', 0)
        author = row['current_author'] or ''

        # Format book number (B1, B2, etc.) - strip .0 from whole numbers
        book_num_str = ''
        if series_num:
            try:
                num = float(series_num)
                if num == int(num):
                    book_num_str = f"B{int(num)}"
                else:
                    book_num_str = f"B{series_num}"
            except (ValueError, TypeError):
                book_num_str = f"B{series_num}"

        # Check if author-narrated (compare normalized names)
        is_author_narrated = False
        narrator_display = narrator
        if narrator and author:
            # Normalize for comparison: lowercase, remove common suffixes
            def normalize_name(name):
                n = name.lower().strip()
                for suffix in [' jr', ' jr.', ' sr', ' sr.', ' iii', ' ii', ' phd', ' ph.d.']:
                    n = n.replace(suffix, '')
                return n

            author_norm = normalize_name(author)
            narrator_norm = normalize_name(narrator)

            # Check if narrator contains author name or vice versa
            if author_norm in narrator_norm or narrator_norm in author_norm:
                is_author_narrated = True
                narrator_display = "📖 Author"
            # Check last name match for "FirstName LastName" patterns
            elif ' ' in author and ' ' in narrator:
                author_last = author_norm.split()[-1]
                narrator_last = narrator_norm.split()[-1]
                if author_last == narrator_last and len(author_last) > 2:
                    is_author_narrated = True
                    narrator_display = "📖 Author"

        # Map status to user-friendly display
        layer = row['verification_layer'] if 'verification_layer' in row.keys() else 0
        if row['status'] == 'verified':
            status_display = "✅ OK"
        elif row['status'] == 'needs_fix':
            status_display = "🔧 Rename"
        elif row['status'] == 'needs_attention':
            status_display = "⚠️ Review"
        elif row['status'] == 'error':
            status_display = "❌ Error"
        elif row['status'] == 'pending':
            # Show which layer it's at
            layer_names = {1: "🎤 Audio", 2: "🤖 AI", 3: "📚 API", 4: "📁 Folder"}
            status_display = layer_names.get(layer, f"⏳ L{layer}")
        else:
            status_display = row['status']

        items.append({
            'id': row['id'],
            'author': author or 'Unknown',
            'title': row['current_title'] or 'Unknown',
            'series': series,
            'book_num': book_num_str,
            'narrator': narrator,
            'narrator_display': narrator_display,
            'is_author_narrated': is_author_narrated,
            'status': row['status'],
            'status_display': status_display,
            'confidence': confidence,
            'updated_at': row['updated_at']
        })

    return jsonify({'items': items})


@app.route('/api/orphans')
def api_orphans():
    """Find orphan audio files (files sitting directly in author folders)."""
    config = load_config()
    orphans = []

    for lib_path in config.get('library_paths', []):
        lib_orphans = find_orphan_audio_files(lib_path, config)
        orphans.extend(lib_orphans)

    return jsonify({
        'count': len(orphans),
        'orphans': orphans
    })


@app.route('/api/orphans/organize', methods=['POST'])
def api_organize_orphan():
    """Organize orphan files into a proper book folder."""
    data = request.json
    author_path = data.get('author_path')
    book_title = data.get('book_title')
    files = data.get('files', [])

    if not author_path or not book_title or not files:
        return jsonify({'success': False, 'error': 'Missing required fields'})

    config = load_config()
    success, message = organize_orphan_files(author_path, book_title, files, config)

    return jsonify({
        'success': success,
        'message': message
    })


@app.route('/api/orphans/organize_all', methods=['POST'])
def api_organize_all_orphans():
    """Auto-organize all detected orphan files using metadata."""
    config = load_config()
    results = {'organized': 0, 'errors': 0, 'details': []}

    for lib_path in config.get('library_paths', []):
        orphans = find_orphan_audio_files(lib_path, config)

        for orphan in orphans:
            if orphan['detected_title'] == 'Unknown Album':
                results['errors'] += 1
                results['details'].append(f"Skipped {orphan['author']}: unknown title")
                continue

            success, message = organize_orphan_files(
                orphan['author_path'],
                orphan['detected_title'],
                orphan['files'],
                config
            )

            if success:
                results['organized'] += 1
                results['details'].append(f"Organized: {orphan['author']}/{orphan['detected_title']}")
            else:
                results['errors'] += 1
                results['details'].append(f"Error: {orphan['author']}: {message}")

    # Issue #57: Auto-scan after organizing to pick up newly created book folders
    # This ensures the database reflects the new folder structure
    scan_results = {'checked': 0, 'scanned': 0, 'queued': 0}
    if results['organized'] > 0:
        try:
            checked, scanned, queued = scan_library(config)
            scan_results = {'checked': checked, 'scanned': scanned, 'queued': queued}
            logger.info(f"Post-organize scan: checked={checked}, scanned={scanned}, queued={queued}")
        except Exception as e:
            logger.error(f"Post-organize scan failed: {e}")

    return jsonify({
        'success': True,
        'organized': results['organized'],
        'errors': results['errors'],
        'details': results['details'][:20],  # Limit details
        'scan': scan_results  # Include scan results so UI knows items were added
    })


@app.route('/api/library')
def api_library():
    """
    Unified library view - returns ALL items (books, orphans, pending fixes, queue)
    with filter counts. This powers the unified Library page.
    """
    config = load_config()
    conn = get_db()
    c = conn.cursor()

    # Get filter and pagination params
    status_filter = request.args.get('filter', 'all')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page

    items = []

    # Get search parameter
    search_query = request.args.get('search', '').strip()

    # === COUNTS for filter chips ===
    counts = {
        'all': 0,
        'pending': 0,
        'orphan': 0,
        'queue': 0,
        'fixed': 0,
        'verified': 0,
        'error': 0,
        'attention': 0,
        'locked': 0,
        # Issue #53: Media type counts
        'audiobook_only': 0,
        'ebook_only': 0,
        'both_formats': 0
    }

    # Get media type filter (Issue #53)
    media_filter = request.args.get('media', 'all')  # 'all', 'audiobook', 'ebook', 'both'

    # Count books by status (Issue #64: exclude series_folder and multi_book_files containers)
    c.execute("SELECT COUNT(*) FROM books WHERE status NOT IN ('series_folder', 'multi_book_files')")
    counts['all'] = c.fetchone()[0]

    # Issue #53: Count by media type
    c.execute("SELECT COUNT(*) FROM books WHERE media_type = 'audiobook' OR media_type IS NULL")
    counts['audiobook_only'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM books WHERE media_type = 'ebook'")
    counts['ebook_only'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM books WHERE media_type = 'both'")
    counts['both_formats'] = c.fetchone()[0]

    # Issue #79: Use JOIN to match the fetch query - only count items with existing books
    c.execute('''SELECT COUNT(*) FROM history h
                 JOIN books b ON h.book_id = b.id
                 WHERE h.status = 'pending_fix' ''')
    counts['pending'] = c.fetchone()[0]

    # Issue #36: Queue count should exclude series_folder and multi_book_files
    c.execute('''SELECT COUNT(*) FROM queue q
                 JOIN books b ON q.book_id = b.id
                 WHERE b.status NOT IN ('series_folder', 'multi_book_files', 'verified', 'fixed')''')
    counts['queue'] = c.fetchone()[0]

    # Issue #79: Use JOIN to match the fetch query - only count items with existing books
    c.execute('''SELECT COUNT(*) FROM history h
                 JOIN books b ON h.book_id = b.id
                 WHERE h.status = 'fixed' ''')
    counts['fixed'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM books WHERE status = 'verified'")
    counts['verified'] = c.fetchone()[0]

    # Issue #79: Use JOIN to match the fetch query - only count items with existing books
    c.execute('''SELECT COUNT(*) FROM history h
                 JOIN books b ON h.book_id = b.id
                 WHERE h.status IN ('error', 'duplicate', 'corrupt_dest')''')
    counts['error'] = c.fetchone()[0]

    # Issue #79: Use JOIN to match the fetch query - only count items with existing books
    c.execute('''SELECT COUNT(*) FROM history h
                 JOIN books b ON h.book_id = b.id
                 WHERE h.status = 'needs_attention' ''')
    attention_history = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM books WHERE status IN ('needs_attention', 'structure_reversed', 'watch_folder_error')")
    attention_books = c.fetchone()[0]
    counts['attention'] = attention_history + attention_books

    # Count user-locked books
    c.execute("SELECT COUNT(*) FROM books WHERE user_locked = 1")
    counts['locked'] = c.fetchone()[0]

    # Count orphans (detected on-the-fly)
    orphan_list = []
    for lib_path in config.get('library_paths', []):
        orphan_list.extend(find_orphan_audio_files(lib_path))
    counts['orphan'] = len(orphan_list)

    # Update 'all' count to include orphans
    counts['all'] += counts['orphan']

    # === FETCH ITEMS based on filter ===
    if status_filter == 'orphan':
        # Return orphans as items
        for idx, orphan in enumerate(orphan_list[offset:offset + per_page]):
            items.append({
                'id': f'orphan_{idx}',
                'type': 'orphan',
                'author': orphan['author'],
                'title': orphan['detected_title'],
                'path': orphan['author_path'],
                'status': 'orphan',
                'file_count': orphan['file_count'],
                'files': orphan['files'],
                'author_path': orphan['author_path']
            })

    elif status_filter == 'pending':
        # Items with pending fixes
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path, b.current_author, b.current_title
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'pending_fix'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'pending_fix',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': 'pending_fix',
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'queue':
        # Items in the processing queue
        # Issue #36: Filter out series_folder and multi_book_files - they should never appear in queue
        c.execute('''SELECT q.id as queue_id, q.reason, q.added_at, q.priority,
                            b.id as book_id, b.path, b.current_author, b.current_title, b.status
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE b.status NOT IN ('series_folder', 'multi_book_files', 'verified', 'fixed')
                     ORDER BY q.priority, q.added_at
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['queue_id'],
                'type': 'queue',
                'book_id': row['book_id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': 'in_queue',
                'reason': row['reason'],
                'priority': row['priority'],
                'added_at': row['added_at']
            })

    elif status_filter == 'fixed':
        # Successfully fixed items
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'fixed'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'fixed',
                'book_id': row['book_id'],
                'author': row['new_author'],
                'title': row['new_title'],
                'old_author': row['old_author'],
                'old_title': row['old_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': 'fixed',
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'verified':
        # Verified/OK books - include profile for source display
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, profile, confidence, user_locked
                     FROM books
                     WHERE status = 'verified'
                     ORDER BY updated_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            item = {
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': 'verified',
                'confidence': row['confidence'] or 0,
                'user_locked': row['user_locked'] == 1
            }
            # Parse profile to get verification sources
            if row['profile']:
                try:
                    profile_data = json.loads(row['profile'])
                    # Collect all unique sources used
                    all_sources = set()
                    for field_name in ['author', 'title', 'narrator', 'series']:
                        if field_name in profile_data and profile_data[field_name].get('sources'):
                            all_sources.update(profile_data[field_name]['sources'])
                    item['sources'] = list(all_sources)
                    item['verification_layers'] = profile_data.get('verification_layers_used', [])
                except:
                    pass
            items.append(item)

    elif status_filter == 'error':
        # Error items from history
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status IN ('error', 'duplicate', 'corrupt_dest')
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'error',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message'],
                'fixed_at': row['fixed_at']
            })

    elif status_filter == 'attention':
        # Items needing attention
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     WHERE h.status = 'needs_attention'
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'attention',
                'book_id': row['book_id'],
                'author': row['old_author'],
                'title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'path': row['path'],
                'status': 'needs_attention',
                'error_message': row['error_message']
            })
        # Also get books with structure issues or watch folder errors
        c.execute('''SELECT id, path, current_author, current_title, status, error_message, source_type
                     FROM books
                     WHERE status IN ('needs_attention', 'structure_reversed', 'watch_folder_error')
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book_attention',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message'],
                'source_type': row['source_type'] if row['source_type'] else 'library'
            })

    elif status_filter == 'locked':
        # User-locked books - books where user has manually set metadata
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, user_locked
                     FROM books
                     WHERE user_locked = 1
                     ORDER BY updated_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'user_locked': True
            })

    # Issue #53: Media type filters
    elif status_filter == 'audiobook_only':
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, user_locked, media_type
                     FROM books
                     WHERE media_type = 'audiobook' OR media_type IS NULL
                     ORDER BY current_author, current_title
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'user_locked': row['user_locked'] == 1,
                'media_type': row['media_type'] or 'audiobook'
            })

    elif status_filter == 'ebook_only':
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, user_locked, media_type
                     FROM books
                     WHERE media_type = 'ebook'
                     ORDER BY current_author, current_title
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'user_locked': row['user_locked'] == 1,
                'media_type': 'ebook'
            })

    elif status_filter == 'both_formats':
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, user_locked, media_type
                     FROM books
                     WHERE media_type = 'both'
                     ORDER BY current_author, current_title
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'user_locked': row['user_locked'] == 1,
                'media_type': 'both'
            })

    elif status_filter == 'search' and search_query:
        # Search across all books by author or title
        search_pattern = f'%{search_query}%'
        c.execute('''SELECT id, path, current_author, current_title, status, updated_at, user_locked, media_type
                     FROM books
                     WHERE current_author LIKE ? OR current_title LIKE ?
                     ORDER BY current_author, current_title
                     LIMIT ? OFFSET ?''', (search_pattern, search_pattern, per_page, offset))
        for row in c.fetchall():
            items.append({
                'id': row['id'],
                'type': 'book',
                'book_id': row['id'],
                'author': row['current_author'],
                'title': row['current_title'],
                'path': row['path'],
                'status': row['status'],
                'user_locked': row['user_locked'] == 1,
                'media_type': row['media_type'] or 'audiobook'
            })
        # Get total for search results
        c.execute('''SELECT COUNT(*) FROM books
                     WHERE current_author LIKE ? OR current_title LIKE ?''',
                  (search_pattern, search_pattern))
        counts['search'] = c.fetchone()[0]

    else:  # 'all' - show everything mixed
        # Get recent history items (includes pending, fixed, errors)
        c.execute('''SELECT h.id, h.book_id, h.old_author, h.old_title, h.new_author, h.new_title,
                            h.old_path, h.new_path, h.status, h.fixed_at, h.error_message,
                            b.path, b.current_author, b.current_title, b.user_locked
                     FROM history h
                     JOIN books b ON h.book_id = b.id
                     ORDER BY h.fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
        for row in c.fetchall():
            item_type = 'pending_fix' if row['status'] == 'pending_fix' else \
                        'fixed' if row['status'] == 'fixed' else \
                        'error' if row['status'] in ('error', 'duplicate', 'corrupt_dest') else 'history'
            items.append({
                'id': row['id'],
                'type': item_type,
                'book_id': row['book_id'],
                'author': row['old_author'] if row['status'] == 'pending_fix' else row['new_author'],
                'title': row['old_title'] if row['status'] == 'pending_fix' else row['new_title'],
                'old_author': row['old_author'],
                'old_title': row['old_title'],
                'new_author': row['new_author'],
                'new_title': row['new_title'],
                'old_path': row['old_path'],
                'new_path': row['new_path'],
                'path': row['path'],
                'status': row['status'],
                'error_message': row['error_message'],
                'fixed_at': row['fixed_at'],
                'user_locked': row['user_locked'] == 1
            })

    conn.close()

    # Calculate total for pagination
    if status_filter == 'orphan':
        total = counts['orphan']
    elif status_filter == 'pending':
        total = counts['pending']
    elif status_filter == 'queue':
        total = counts['queue']
    elif status_filter == 'fixed':
        total = counts['fixed']
    elif status_filter == 'verified':
        total = counts['verified']
    elif status_filter == 'error':
        total = counts['error']
    elif status_filter == 'attention':
        total = counts['attention']
    elif status_filter == 'locked':
        total = counts['locked']
    elif status_filter == 'search':
        total = counts.get('search', 0)
    # Issue #53: Media type filters
    elif status_filter == 'audiobook_only':
        total = counts['audiobook_only']
    elif status_filter == 'ebook_only':
        total = counts['ebook_only']
    elif status_filter == 'both_formats':
        total = counts['both_formats']
    else:
        total = counts['all']

    total_pages = (total + per_page - 1) // per_page if total > 0 else 1

    return jsonify({
        'success': True,
        'items': items,
        'counts': counts,
        'filter': status_filter,
        'search': search_query,
        'page': page,
        'per_page': per_page,
        'total': total,
        'total_pages': total_pages,
        'skip_confirmations': config.get('skip_confirmations', False)
    })


@app.route('/library')
def library_page():
    """Unified library view - the main page."""
    config = load_config()
    return render_template('library.html',
                          config=config,
                          worker_running=is_worker_running())


@app.route('/api/settings/skip_confirmations', methods=['POST'])
def api_skip_confirmations():
    """Toggle skip confirmations setting."""
    data = request.json
    value = data.get('value', False)

    config = load_config()
    config['skip_confirmations'] = value
    save_config(config)

    return jsonify({'success': True, 'value': value})


@app.route('/api/version')
def api_version():
    """Return current app version."""
    return jsonify({
        'version': APP_VERSION,
        'repo': GITHUB_REPO
    })

@app.route('/api/check_update')
def api_check_update():
    """Check GitHub for newer version based on update channel."""
    config = load_config()
    channel = config.get('update_channel', 'stable')

    try:
        headers = {'Accept': 'application/vnd.github.v3+json'}

        if channel == 'nightly':
            # Check latest commit on main branch
            url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/main"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'Repository not found or not published yet'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            data = resp.json()
            latest_sha = data.get('sha', '')[:7]
            commit_msg = data.get('commit', {}).get('message', '')[:200]
            commit_date = data.get('commit', {}).get('committer', {}).get('date', '')[:10]
            commit_url = data.get('html_url', '')

            # For nightly, check if we have a local commit hash stored
            local_commit = config.get('local_commit_sha', '')

            return jsonify({
                'update_available': latest_sha != local_commit if local_commit else True,
                'current': APP_VERSION + (f' ({local_commit})' if local_commit else ''),
                'latest': f'main@{latest_sha}',
                'latest_date': commit_date,
                'channel': channel,
                'release_url': commit_url,
                'release_notes': commit_msg,
                'message': 'Tracking latest commits on main branch' if not local_commit else None
            })

        elif channel == 'beta':
            # Check all releases including pre-releases
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found (repo may not be published yet)'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            releases = resp.json()
            if not releases:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found'
                })

            # Get the latest release (first in list, includes pre-releases)
            latest = releases[0]
            latest_version = latest.get('tag_name', '').lstrip('v')
            release_url = latest.get('html_url', '')
            release_notes = latest.get('body', '')[:500]
            is_prerelease = latest.get('prerelease', False)

            update_available = _compare_versions(APP_VERSION, latest_version)

            return jsonify({
                'update_available': update_available,
                'current': APP_VERSION,
                'latest': latest_version + (' (beta)' if is_prerelease else ''),
                'channel': channel,
                'release_url': release_url,
                'release_notes': release_notes if update_available else None
            })

        else:  # stable (default)
            # Check only stable releases (not pre-releases)
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = requests.get(url, timeout=5, headers=headers)

            if resp.status_code == 404:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'message': 'No releases found (repo may not be published yet)'
                })

            if resp.status_code != 200:
                return jsonify({
                    'update_available': False,
                    'current': APP_VERSION,
                    'channel': channel,
                    'error': f'GitHub API error: {resp.status_code}'
                })

            data = resp.json()
            latest_version = data.get('tag_name', '').lstrip('v')
            release_url = data.get('html_url', '')
            release_notes = data.get('body', '')[:500]

            update_available = _compare_versions(APP_VERSION, latest_version)

            return jsonify({
                'update_available': update_available,
                'current': APP_VERSION,
                'latest': latest_version,
                'channel': channel,
                'release_url': release_url,
                'release_notes': release_notes if update_available else None
            })

    except Exception as e:
        logger.debug(f"Update check failed: {e}")
        return jsonify({
            'update_available': False,
            'current': APP_VERSION,
            'channel': channel,
            'error': str(e)
        })

def _compare_versions(current, latest):
    """Compare semantic versions. Returns True if latest > current."""
    def parse_version(v):
        # Handle versions like "1.0.0-beta.1"
        import re
        match = re.match(r'(\d+)\.(\d+)\.(\d+)', v)
        if match:
            return tuple(int(x) for x in match.groups())
        return (0, 0, 0)

    return parse_version(latest) > parse_version(current)

@app.route('/api/perform_update', methods=['POST'])
def api_perform_update():
    """Perform a git pull to update the application."""
    import subprocess

    # Get the app directory (where this script is located)
    app_dir = os.path.dirname(os.path.abspath(__file__))

    # Determine target branch based on update channel
    config = load_config()
    channel = config.get('update_channel', 'stable')
    target_branch = {
        'stable': 'main',
        'beta': 'develop',
        'nightly': 'develop'  # nightly also uses develop for now
    }.get(channel, 'main')

    try:
        # First check if we're in a git repo
        result = subprocess.run(
            ['git', 'rev-parse', '--git-dir'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            return jsonify({
                'success': False,
                'error': 'Not a git repository. Manual update required.',
                'instructions': 'Download the latest release from GitHub and replace your installation.'
            })

        # Get current commit before update
        before = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=app_dir, capture_output=True, text=True, timeout=10
        ).stdout.strip()

        # Fetch all branches
        fetch_result = subprocess.run(
            ['git', 'fetch', '--all'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=60
        )

        # Checkout the target branch based on update channel
        checkout_result = subprocess.run(
            ['git', 'checkout', target_branch],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=30
        )

        if checkout_result.returncode != 0:
            return jsonify({
                'success': False,
                'error': f'Failed to switch to {target_branch} branch',
                'details': checkout_result.stderr or checkout_result.stdout,
                'instructions': f'You may have local changes. Try: git stash && git checkout {target_branch}'
            })

        # Pull latest from target branch
        pull_result = subprocess.run(
            ['git', 'pull', 'origin', target_branch, '--ff-only'],
            cwd=app_dir,
            capture_output=True,
            text=True,
            timeout=60
        )

        # Get new commit after update
        after = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=app_dir, capture_output=True, text=True, timeout=10
        ).stdout.strip()

        if pull_result.returncode != 0:
            return jsonify({
                'success': False,
                'error': 'Git pull failed',
                'details': pull_result.stderr or pull_result.stdout,
                'instructions': 'You may have local changes. Try: git stash && git pull'
            })

        updated = before != after

        return jsonify({
            'success': True,
            'updated': updated,
            'before': before,
            'after': after,
            'branch': target_branch,
            'channel': channel,
            'output': pull_result.stdout,
            'message': f'Updated to {channel} ({target_branch})! Restart the app to apply changes.' if updated else f'Already up to date on {channel} ({target_branch}).',
            'restart_required': updated
        })

    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'error': 'Update timed out. Check your network connection.'
        })
    except Exception as e:
        logger.error(f"Update failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/restart', methods=['POST'])
def api_restart():
    """Restart the application (for systemd managed services)."""
    import subprocess

    try:
        # Check if running under systemd
        ppid = os.getppid()
        result = subprocess.run(['ps', '-p', str(ppid), '-o', 'comm='],
                               capture_output=True, text=True, timeout=5)

        if 'systemd' in result.stdout:
            # We're running under systemd, restart via systemctl
            # This will kill this process, but systemd will restart it
            subprocess.Popen(['sudo', 'systemctl', 'restart', 'library-manager.service'],
                           start_new_session=True)
            return jsonify({
                'success': True,
                'message': 'Restarting via systemd...'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Not running under systemd. Please restart manually.',
                'instructions': 'Stop the current process and start it again.'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/bug_report')
def api_bug_report():
    """Generate a bug report with system info and sanitized config."""
    import platform
    import sys

    config = load_config()

    # Test API connections instead of showing keys
    api_status = {}

    # Gemini
    if config.get('gemini_api_key'):
        try:
            resp = requests.get(
                f"https://generativelanguage.googleapis.com/v1beta/models?key={config['gemini_api_key']}",
                timeout=5
            )
            api_status['gemini'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['gemini'] = 'connection failed'
    else:
        api_status['gemini'] = 'not configured'

    # Google Books
    if config.get('google_books_api_key'):
        try:
            resp = requests.get(
                f"https://www.googleapis.com/books/v1/volumes?q=test&maxResults=1&key={config['google_books_api_key']}",
                timeout=5
            )
            api_status['google_books'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['google_books'] = 'connection failed'
    else:
        api_status['google_books'] = 'not configured'

    # OpenRouter
    if config.get('openrouter_api_key'):
        try:
            resp = requests.get(
                "https://openrouter.ai/api/v1/models",
                headers={"Authorization": f"Bearer {config['openrouter_api_key']}"},
                timeout=5
            )
            api_status['openrouter'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['openrouter'] = 'connection failed'
    else:
        api_status['openrouter'] = 'not configured'

    # Audiobookshelf
    if config.get('abs_url') and config.get('abs_api_token'):
        try:
            resp = requests.get(
                f"{config['abs_url'].rstrip('/')}/api/libraries",
                headers={"Authorization": f"Bearer {config['abs_api_token']}"},
                timeout=5
            )
            api_status['audiobookshelf'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
        except:
            api_status['audiobookshelf'] = 'connection failed'
    else:
        api_status['audiobookshelf'] = 'not configured'

    # BookDB
    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')
    try:
        resp = requests.get(f"{bookdb_url}/health", timeout=5)
        api_status['bookdb'] = 'connected' if resp.status_code == 200 else f'error ({resp.status_code})'
    except:
        api_status['bookdb'] = 'connection failed'

    # Build safe config - only include non-sensitive settings
    safe_config = {}
    # Settings that are safe to share (no paths, no keys, no personal info)
    safe_keys = [
        'naming_format', 'series_grouping', 'auto_fix', 'protect_author_changes',
        'scan_interval_hours', 'batch_size', 'max_requests_per_hour',
        'enable_api_lookups', 'enable_ai_verification', 'enable_audio_analysis',
        'deep_scan_mode', 'profile_confidence_threshold', 'skip_confirmations',
        'ai_provider', 'openrouter_model', 'ollama_model', 'update_channel',
        'enable_ebooks', 'embed_metadata', 'library_language'
    ]
    for key in safe_keys:
        if key in config:
            safe_config[key] = config[key]

    # Show library path count, not actual paths
    safe_config['library_paths_count'] = len(config.get('library_paths', []))

    # Get database stats
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM books')
    total_books = c.fetchone()['count']
    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_size = c.fetchone()['count']
    c.execute('SELECT COUNT(*) as count FROM history')
    history_count = c.fetchone()['count']
    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'error'")
    error_count = c.fetchone()['count']
    conn.close()

    # Get recent error/warning logs - sanitize paths
    log_file = DATA_DIR / 'app.log'
    recent_errors = []
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()[-200:]
            for line in lines:
                if 'ERROR' in line or 'WARNING' in line:
                    # Sanitize paths - replace anything that looks like a full path
                    sanitized = re.sub(r'/[a-zA-Z0-9_\-./]+/([^/\s]+)', r'[path]/\1', line.strip())
                    # Also sanitize Windows paths
                    sanitized = re.sub(r'[A-Z]:\\[a-zA-Z0-9_\-\\]+\\([^\\]+)', r'[path]\\\1', sanitized)
                    recent_errors.append(sanitized)
            recent_errors = recent_errors[-30:]

    # Build report
    report = f"""## Bug Report - Library Manager

### System Info
- **Python:** {sys.version.split()[0]}
- **Platform:** {platform.system()} {platform.release()}
- **App Version:** {APP_VERSION}

### API Connection Status
- **Gemini:** {api_status['gemini']}
- **Google Books:** {api_status['google_books']}
- **OpenRouter:** {api_status['openrouter']}
- **Audiobookshelf:** {api_status['audiobookshelf']}
- **BookDB:** {api_status['bookdb']}

### Configuration
```json
{json.dumps(safe_config, indent=2)}
```

### Database Stats
- Total Books: {total_books}
- Queue Size: {queue_size}
- History Entries: {history_count}
- Books with Errors: {error_count}

### Recent Errors/Warnings (paths sanitized)
```
{chr(10).join(recent_errors) if recent_errors else 'No recent errors'}
```

### Description
[Please describe the issue you're experiencing]

### Steps to Reproduce
1. [First step]
2. [Second step]
3. [What happened vs what you expected]
"""

    return jsonify({'report': report})


# ============== OLLAMA INTEGRATION ==============

@app.route('/api/test_ollama', methods=['POST'])
def api_test_ollama():
    """Test connection to Ollama server."""
    data = request.get_json() or {}
    ollama_url = data.get('ollama_url', 'http://localhost:11434').strip().rstrip('/')

    result = test_ollama_connection({'ollama_url': ollama_url})
    return jsonify(result)


@app.route('/api/ollama_models', methods=['POST'])
def api_ollama_models():
    """Get list of available models from Ollama server."""
    data = request.get_json() or {}
    ollama_url = data.get('ollama_url', 'http://localhost:11434').strip().rstrip('/')

    models = get_ollama_models({'ollama_url': ollama_url})
    if models:
        return jsonify({
            'success': True,
            'models': models
        })
    else:
        return jsonify({
            'success': False,
            'models': [],
            'error': 'Could not fetch models from Ollama server'
        })


# ============== SKALDLEITA INSTANCE REGISTRATION ==============

@app.route('/api/skaldleita/register', methods=['POST'])
def api_skaldleita_register():
    """Register this Library Manager instance with Skaldleita and get an API key."""
    data = request.get_json() or {}
    email = data.get('email', '').strip().lower()

    if not email or '@' not in email:
        return jsonify({'success': False, 'error': 'Valid email address required'})

    config = load_config()
    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')
    instance_id = get_instance_id()

    # Get library stats for registration metadata
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM books')
            total_books = c.fetchone()[0]
    except Exception as e:
        logger.warning(f"Could not get book count for registration: {e}")
        total_books = 0

    try:
        resp = requests.post(
            f"{bookdb_url}/api/register-instance",
            json={
                'instance_id': instance_id,
                'email': email,
                'app_version': APP_VERSION,
                'total_books': total_books,
                'library_name': data.get('library_name', '')
            },
            headers={
                'User-Agent': f'LibraryManager/{APP_VERSION}',
                'Content-Type': 'application/json'
            },
            timeout=30
        )

        if resp.status_code == 200:
            result = resp.json()
            if result.get('api_key'):
                # Save the key to secrets
                try:
                    secrets = load_secrets()
                    secrets['bookdb_api_key'] = result['api_key']
                    save_secrets(secrets)
                except Exception as e:
                    logger.error(f"Failed to save API key to secrets: {e}")
                    return jsonify({'success': False, 'error': 'Got key but failed to save it locally'})

                # Save registration info
                save_instance_data({
                    'registered_email': email,
                    'registered_at': datetime.now().isoformat()
                })

                # Don't return the API key in response - email-only delivery for security
                # Key is already saved to secrets above, so it's auto-applied
                return jsonify({
                    'success': True,
                    'message': result.get('message', 'API key registered and emailed!'),
                    'is_existing': result.get('is_existing', False),
                    'email_sent': result.get('email_sent', False)
                })
            else:
                return jsonify({'success': False, 'error': result.get('error', 'Registration failed')})
        else:
            return jsonify({'success': False, 'error': f'Registration failed (HTTP {resp.status_code})'})

    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Skaldleita server timeout - please try again'})
    except requests.exceptions.ConnectionError:
        return jsonify({'success': False, 'error': 'Cannot connect to Skaldleita'})
    except Exception as e:
        logger.error(f"Skaldleita registration error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/skaldleita/validate', methods=['POST'])
def api_skaldleita_validate():
    """Validate the current Skaldleita API key."""
    config = load_config()
    secrets = load_secrets()
    api_key = secrets.get('bookdb_api_key', '')

    if not api_key:
        return jsonify({'success': False, 'valid': False, 'error': 'No API key configured'})

    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')

    try:
        resp = requests.get(
            f"{bookdb_url}/api/validate-key",
            headers={'X-API-Key': api_key, 'User-Agent': f'LibraryManager/{APP_VERSION}'},
            timeout=10
        )

        if resp.status_code == 200:
            data = resp.json()
            return jsonify({
                'success': True,
                'valid': data.get('valid', False),
                'rate_limit': data.get('rate_limit', 1000),  # Default to API key rate limit
                'email': data.get('email', '')
            })
        else:
            return jsonify({'success': True, 'valid': False, 'error': 'Invalid or expired API key'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/instance/info', methods=['GET'])
def api_instance_info():
    """Get instance information including ID."""
    instance_data = get_instance_data()
    return jsonify({
        'instance_id': get_instance_id(),
        'registered_email': instance_data.get('registered_email'),
        'registered_at': instance_data.get('registered_at'),
        'version': APP_VERSION
    })


# ============== API CONNECTION TESTING ==============

@app.route('/api/test_bookdb', methods=['POST'])
def api_test_bookdb():
    """Test connection to BookDB."""
    config = load_config()
    bookdb_url = config.get('bookdb_url', 'https://bookdb.deucebucket.com')

    try:
        resp = requests.get(f"{bookdb_url}/stats", timeout=5)
        if resp.status_code == 200:
            try:
                data = resp.json()
                return jsonify({
                    'success': True,
                    'books_count': data.get('books', 0),
                    'authors_count': data.get('authors', 0),
                    'series_count': data.get('series', 0),
                    'message': f"Connected - {data.get('books', 0):,} books, {data.get('authors', 0):,} authors"
                })
            except:
                return jsonify({
                    'success': False,
                    'error': 'BookDB responded but returned invalid data'
                })
        else:
            return jsonify({
                'success': False,
                'error': f'BookDB returned status {resp.status_code}'
            })
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Cannot connect to BookDB - is it running?'
        })
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'BookDB connection timed out'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'BookDB error: {str(e)}'
        })


@app.route('/api/clear_api_key', methods=['POST'])
def api_clear_api_key():
    """Clear a specific API key from secrets."""
    data = request.get_json() or {}
    key_name = data.get('key_name', '')

    # Whitelist of allowed keys to clear
    allowed_keys = ['gemini_api_key', 'openrouter_api_key', 'google_books_api_key', 'bookdb_api_key']

    if key_name not in allowed_keys:
        return jsonify({
            'success': False,
            'error': f'Invalid key name: {key_name}'
        })

    try:
        secrets = load_secrets()
        if key_name in secrets:
            del secrets[key_name]
            save_secrets(secrets)
            logger.info(f"Cleared API key: {key_name}")
            return jsonify({
                'success': True,
                'message': f'{key_name} has been removed'
            })
        else:
            return jsonify({
                'success': True,
                'message': f'{key_name} was not configured'
            })
    except Exception as e:
        logger.error(f"Error clearing API key {key_name}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/test_gemini', methods=['POST'])
def api_test_gemini():
    """Test Gemini API connection."""
    config = load_config()
    api_key = config.get('gemini_api_key', '')

    if not api_key:
        return jsonify({
            'success': False,
            'error': 'No Gemini API key configured'
        })

    try:
        model = config.get('gemini_model', 'gemini-2.0-flash')
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": "Reply with just the word 'connected'"}]}]},
            timeout=10
        )

        if resp.status_code == 200:
            return jsonify({
                'success': True,
                'model': model,
                'message': 'Gemini API connected'
            })
        else:
            error_msg = resp.json().get('error', {}).get('message', f'Status {resp.status_code}')
            return jsonify({
                'success': False,
                'error': error_msg
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/test_openrouter', methods=['POST'])
def api_test_openrouter():
    """Test OpenRouter API connection."""
    config = load_config()
    api_key = config.get('openrouter_api_key', '')
    model = config.get('openrouter_model', 'google/gemma-3n-e4b-it:free')
    result = test_openrouter_connection(api_key, model)
    return jsonify(result)


@app.route('/api/reset_database', methods=['POST'])
def api_reset_database():
    """Reset the database and trigger a fresh scan."""
    try:
        conn = get_db()
        c = conn.cursor()

        # Clear all tables
        c.execute('DELETE FROM books')
        c.execute('DELETE FROM queue')
        c.execute('DELETE FROM history')
        c.execute('DELETE FROM stats')

        conn.commit()
        conn.close()

        # Trigger a scan
        config = load_config()
        if config.get('library_paths'):
            import threading
            def run_scan():
                scan_library(config)
            threading.Thread(target=run_scan, daemon=True).start()

        return jsonify({
            'success': True,
            'message': 'Database reset. Scan started.'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/cleanup-garbage', methods=['POST'])
def api_cleanup_garbage():
    """Remove garbage entries from database (Issue #88).

    Removes @eaDir, #recycle, .AppleDouble and other system folder entries
    that were accidentally scanned.
    """
    try:
        from library_manager.database import cleanup_garbage_entries
        removed_count = cleanup_garbage_entries()
        return jsonify({
            'success': True,
            'removed': removed_count,
            'message': f'Removed {removed_count} garbage entries' if removed_count > 0 else 'No garbage entries found'
        })
    except Exception as e:
        logger.error(f"Cleanup garbage error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/api/cleanup_duplicate_history', methods=['POST'])
def api_cleanup_duplicate_history():
    """Remove duplicate history entries (Issue #79).

    When books are processed multiple times through the pipeline, duplicate
    history entries can accumulate. This cleans them up, keeping only the
    most recent entry per book_id + status combination.
    """
    try:
        removed_count = cleanup_duplicate_history_entries()
        return jsonify({
            'success': True,
            'removed': removed_count,
            'message': f'Removed {removed_count} duplicate history entries' if removed_count > 0 else 'No duplicate entries found'
        })
    except Exception as e:
        logger.error(f"Cleanup duplicate history error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })


# ============== AUDIOBOOKSHELF INTEGRATION ==============

def get_abs_client():
    """Get configured ABS client or None if not configured."""
    from abs_client import ABSClient
    config = load_config()
    abs_url = config.get('abs_url', '').strip()
    abs_token = config.get('abs_api_token', '').strip()
    if abs_url and abs_token:
        return ABSClient(abs_url, abs_token)
    return None


@app.route('/abs')
def abs_dashboard():
    """ABS integration dashboard - user progress tracking."""
    config = load_config()
    abs_connected = bool(config.get('abs_url') and config.get('abs_api_token'))
    return render_template('abs_dashboard.html',
                           config=config,
                           abs_connected=abs_connected,
                           version=APP_VERSION)


@app.route('/api/abs/test', methods=['POST'])
def api_abs_test():
    """Test ABS connection."""
    data = request.get_json() or {}
    abs_url = data.get('url', '').strip()
    abs_token = data.get('token', '').strip()

    if not abs_url or not abs_token:
        return jsonify({'success': False, 'error': 'URL and API token required'})

    from abs_client import ABSClient
    client = ABSClient(abs_url, abs_token)
    result = client.test_connection()
    return jsonify(result)


@app.route('/api/abs/connect', methods=['POST'])
def api_abs_connect():
    """Save ABS connection settings."""
    data = request.get_json() or {}
    abs_url = data.get('url', '').strip()
    abs_token = data.get('token', '').strip()

    if not abs_url or not abs_token:
        return jsonify({'success': False, 'error': 'URL and API token required'})

    # Test connection first
    from abs_client import ABSClient
    client = ABSClient(abs_url, abs_token)
    result = client.test_connection()

    if result.get('success'):
        # Save URL to config
        config = load_config()
        config['abs_url'] = abs_url
        save_config(config)

        # Save token to secrets (preserving existing secrets)
        secrets = load_secrets()
        secrets['abs_api_token'] = abs_token
        save_secrets(secrets)

        return jsonify({'success': True, 'message': f"Connected as {result.get('username')}"})
    else:
        return jsonify({'success': False, 'error': result.get('error', 'Connection failed')})


@app.route('/api/abs/users')
def api_abs_users():
    """Get all ABS users."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    users = client.get_users()
    return jsonify({
        'success': True,
        'users': [{'id': u.id, 'username': u.username, 'type': u.type} for u in users]
    })


@app.route('/api/abs/libraries')
def api_abs_libraries():
    """Get all ABS libraries."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    libraries = client.get_libraries()
    return jsonify({'success': True, 'libraries': libraries})


@app.route('/api/abs/library/<library_id>/progress')
def api_abs_library_progress(library_id):
    """Get all items in library with user progress."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    items = client.get_library_with_all_progress(library_id)

    # Simplify for JSON
    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'duration': media.get('duration', 0),
            'user_progress': item.get('user_progress', {}),
            'progress_summary': item.get('progress_summary', {})
        })

    return jsonify({'success': True, 'items': result})


@app.route('/api/abs/archivable/<library_id>')
def api_abs_archivable(library_id):
    """Get items safe to archive (everyone finished, no one in progress)."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    min_users = request.args.get('min_users', 1, type=int)
    items = client.get_archivable_items(library_id, min_users_finished=min_users)

    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'users_finished': item.get('progress_summary', {}).get('users_finished', 0)
        })

    return jsonify({'success': True, 'items': result, 'count': len(result)})


@app.route('/api/abs/untouched/<library_id>')
def api_abs_untouched(library_id):
    """Get items no one has started."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    items = client.get_untouched_items(library_id)

    result = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        result.append({
            'id': item.get('id'),
            'title': metadata.get('title', 'Unknown'),
            'author': metadata.get('authorName', 'Unknown'),
            'added_at': item.get('addedAt')
        })

    return jsonify({'success': True, 'items': result, 'count': len(result)})


# ============== USER GROUPS (for ABS progress rules) ==============

GROUPS_PATH = DATA_DIR / 'user_groups.json'

DEFAULT_GROUPS_DATA = {
    'user_groups': [],      # Groups of ABS users (e.g., "Twilight Readers": [wife, daughter1, daughter2])
    'rules': [],            # Archive rules tied to user groups
    'author_assignments': {},  # author_name -> group_id (smart assign)
    'genre_assignments': {},   # genre -> group_id (smart assign)
    'keep_forever': {          # Never flag these for archive
        'items': [],           # specific item IDs
        'authors': [],         # author names
        'series': []           # series names
    },
    'exclude_from_rules': {    # Exclude from auto-rules (but can still manually archive)
        'authors': [],
        'genres': []
    }
}


def load_groups():
    """Load user groups configuration."""
    if GROUPS_PATH.exists():
        try:
            with open(GROUPS_PATH) as f:
                data = json.load(f)
                # Merge with defaults for any missing keys
                for key, default in DEFAULT_GROUPS_DATA.items():
                    if key not in data:
                        data[key] = default
                return data
        except:
            pass
    return DEFAULT_GROUPS_DATA.copy()


def save_groups(groups):
    """Save user groups configuration."""
    with open(GROUPS_PATH, 'w') as f:
        json.dump(groups, f, indent=2)


@app.route('/api/abs/groups')
def api_abs_groups():
    """Get all groups."""
    return jsonify(load_groups())


@app.route('/api/abs/groups/user', methods=['POST'])
def api_abs_create_user_group():
    """Create a user group."""
    data = request.get_json() or {}
    name = data.get('name', '').strip()
    user_ids = data.get('user_ids', [])

    if not name:
        return jsonify({'success': False, 'error': 'Group name required'})

    groups = load_groups()
    groups['user_groups'].append({
        'id': str(len(groups['user_groups']) + 1),
        'name': name,
        'user_ids': user_ids
    })
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/groups/user/<group_id>', methods=['DELETE'])
def api_abs_delete_user_group(group_id):
    """Delete a user group."""
    groups = load_groups()
    groups['user_groups'] = [g for g in groups['user_groups'] if g['id'] != group_id]
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/groups/rule', methods=['POST'])
def api_abs_create_rule():
    """Create an archive rule.

    Example rule:
    {
        "name": "Archive when family done",
        "user_group_id": "1",  # which users must finish
        "action": "archive",   # what to do
        "enabled": true
    }
    """
    data = request.get_json() or {}

    groups = load_groups()
    groups['rules'].append({
        'id': str(len(groups['rules']) + 1),
        'name': data.get('name', 'Unnamed Rule'),
        'user_group_id': data.get('user_group_id'),
        'action': data.get('action', 'archive'),
        'enabled': data.get('enabled', True)
    })
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/check_rules/<library_id>')
def api_abs_check_rules(library_id):
    """Check which items match archive rules (with smart assignments)."""
    client = get_abs_client()
    if not client:
        return jsonify({'success': False, 'error': 'ABS not configured'})

    groups = load_groups()
    items = client.get_library_with_all_progress(library_id)

    # Build lookups
    user_groups = {g['id']: set(g['user_ids']) for g in groups.get('user_groups', [])}
    author_assignments = groups.get('author_assignments', {})
    genre_assignments = groups.get('genre_assignments', {})
    keep_forever = groups.get('keep_forever', {})
    exclude_from_rules = groups.get('exclude_from_rules', {})

    matches = []
    for item in items:
        media = item.get('media', {})
        metadata = media.get('metadata', {})
        item_id = item.get('id')
        title = metadata.get('title', 'Unknown')
        author = metadata.get('authorName', 'Unknown')
        genres = metadata.get('genres', [])
        series_name = metadata.get('seriesName', '')

        # Check keep forever - skip if protected
        if item_id in keep_forever.get('items', []):
            continue
        if author.lower() in [a.lower() for a in keep_forever.get('authors', [])]:
            continue
        if series_name and series_name.lower() in [s.lower() for s in keep_forever.get('series', [])]:
            continue

        # Check exclude from rules
        if author.lower() in [a.lower() for a in exclude_from_rules.get('authors', [])]:
            continue
        if any(g.lower() in [eg.lower() for eg in exclude_from_rules.get('genres', [])] for g in genres):
            continue

        # Determine which group should handle this item (smart assignment)
        assigned_group_id = None

        # Check author assignment first
        for assigned_author, group_id in author_assignments.items():
            if assigned_author.lower() in author.lower():
                assigned_group_id = group_id
                break

        # Check genre assignment if no author match
        if not assigned_group_id:
            for genre in genres:
                if genre.lower() in [g.lower() for g in genre_assignments.keys()]:
                    assigned_group_id = genre_assignments.get(genre)
                    break

        # Check all rules (both assigned and general)
        user_progress = item.get('user_progress', {})
        finished_users = {uid for uid, p in user_progress.items() if p.get('is_finished')}

        for rule in groups.get('rules', []):
            if not rule.get('enabled'):
                continue

            rule_group_id = rule.get('user_group_id')
            group_users = user_groups.get(rule_group_id, set())

            if not group_users:
                continue

            # If item has smart assignment, only use that group's rules
            if assigned_group_id and rule_group_id != assigned_group_id:
                continue

            # Check if all group members finished
            if group_users.issubset(finished_users):
                matches.append({
                    'rule_name': rule.get('name'),
                    'action': rule.get('action'),
                    'item_id': item_id,
                    'title': title,
                    'author': author,
                    'smart_assigned': assigned_group_id is not None
                })
                break  # Only match one rule per item

    return jsonify({'success': True, 'matches': matches, 'count': len(matches)})


# ============== SMART ASSIGNMENTS ==============

@app.route('/api/abs/assign/author', methods=['POST'])
def api_abs_assign_author():
    """Assign an author to a user group for smart rules."""
    data = request.get_json() or {}
    author = data.get('author', '').strip()
    group_id = data.get('group_id', '').strip()

    if not author or not group_id:
        return jsonify({'success': False, 'error': 'Author and group_id required'})

    groups = load_groups()
    groups['author_assignments'][author] = group_id
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Assigned "{author}" to group'})


@app.route('/api/abs/assign/author/<author>', methods=['DELETE'])
def api_abs_unassign_author(author):
    """Remove author assignment."""
    groups = load_groups()
    if author in groups.get('author_assignments', {}):
        del groups['author_assignments'][author]
        save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/assign/genre', methods=['POST'])
def api_abs_assign_genre():
    """Assign a genre to a user group for smart rules."""
    data = request.get_json() or {}
    genre = data.get('genre', '').strip()
    group_id = data.get('group_id', '').strip()

    if not genre or not group_id:
        return jsonify({'success': False, 'error': 'Genre and group_id required'})

    groups = load_groups()
    groups['genre_assignments'][genre] = group_id
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Assigned genre "{genre}" to group'})


@app.route('/api/abs/assign/genre/<genre>', methods=['DELETE'])
def api_abs_unassign_genre(genre):
    """Remove genre assignment."""
    groups = load_groups()
    if genre in groups.get('genre_assignments', {}):
        del groups['genre_assignments'][genre]
        save_groups(groups)
    return jsonify({'success': True})


# ============== KEEP FOREVER / EXCLUDE ==============

@app.route('/api/abs/keep', methods=['POST'])
def api_abs_keep_forever():
    """Add item/author/series to keep forever list."""
    data = request.get_json() or {}
    item_type = data.get('type')  # 'item', 'author', 'series'
    value = data.get('value', '').strip()

    if not item_type or not value:
        return jsonify({'success': False, 'error': 'Type and value required'})

    groups = load_groups()
    keep = groups.get('keep_forever', {'items': [], 'authors': [], 'series': []})

    if item_type == 'item' and value not in keep['items']:
        keep['items'].append(value)
    elif item_type == 'author' and value not in keep['authors']:
        keep['authors'].append(value)
    elif item_type == 'series' and value not in keep['series']:
        keep['series'].append(value)

    groups['keep_forever'] = keep
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Added to keep forever: {value}'})


@app.route('/api/abs/keep', methods=['DELETE'])
def api_abs_remove_keep():
    """Remove from keep forever list."""
    data = request.get_json() or {}
    item_type = data.get('type')
    value = data.get('value', '').strip()

    groups = load_groups()
    keep = groups.get('keep_forever', {'items': [], 'authors': [], 'series': []})

    if item_type == 'item' and value in keep['items']:
        keep['items'].remove(value)
    elif item_type == 'author' and value in keep['authors']:
        keep['authors'].remove(value)
    elif item_type == 'series' and value in keep['series']:
        keep['series'].remove(value)

    groups['keep_forever'] = keep
    save_groups(groups)
    return jsonify({'success': True})


@app.route('/api/abs/exclude', methods=['POST'])
def api_abs_exclude():
    """Add author/genre to exclude from auto-rules."""
    data = request.get_json() or {}
    item_type = data.get('type')  # 'author', 'genre'
    value = data.get('value', '').strip()

    if not item_type or not value:
        return jsonify({'success': False, 'error': 'Type and value required'})

    groups = load_groups()
    exclude = groups.get('exclude_from_rules', {'authors': [], 'genres': []})

    if item_type == 'author' and value not in exclude['authors']:
        exclude['authors'].append(value)
    elif item_type == 'genre' and value not in exclude['genres']:
        exclude['genres'].append(value)

    groups['exclude_from_rules'] = exclude
    save_groups(groups)
    return jsonify({'success': True, 'message': f'Excluded from rules: {value}'})


@app.route('/api/abs/exclude', methods=['DELETE'])
def api_abs_remove_exclude():
    """Remove from exclude list."""
    data = request.get_json() or {}
    item_type = data.get('type')
    value = data.get('value', '').strip()

    groups = load_groups()
    exclude = groups.get('exclude_from_rules', {'authors': [], 'genres': []})

    if item_type == 'author' and value in exclude['authors']:
        exclude['authors'].remove(value)
    elif item_type == 'genre' and value in exclude['genres']:
        exclude['genres'].remove(value)

    groups['exclude_from_rules'] = exclude
    save_groups(groups)
    return jsonify({'success': True})


# ============== MANUAL BOOK MATCHING ==============

# Use the public BookBucket API - same as metadata pipeline
# No API key required - the search endpoints are public

@app.route('/api/search_bookdb')
def api_search_bookdb():
    """Search BookBucket for books/series to manually match.
    Uses the public /search endpoint - no API key required.
    Falls back to Google Books if BookDB is unavailable or returns no results.
    """
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'all')  # 'books', 'series', or 'all'
    author = request.args.get('author', '').strip()
    limit = min(int(request.args.get('limit', 20)), 50)

    if not query or len(query) < 2:
        return jsonify({'error': 'Query must be at least 2 characters', 'results': []})

    # Keep original query for series number extraction
    original_query = query

    # Extract series number from ORIGINAL query (e.g., "Horus Heresy Book 36" or "#36")
    # Do this BEFORE cleaning so we capture the book number
    extracted_series_num = None
    extracted_series_name = None
    series_patterns = [
        r'(?:book|#|no\.?|number)\s*(\d+)',  # "Book 36", "#36", "No. 36"
        r'(\d+)(?:st|nd|rd|th)\s+book',       # "36th book"
        r'^\s*(\d+)\s*[-–]\s*\w',             # "5 - Title" at start (Issue #38)
    ]
    for pattern in series_patterns:
        match = re.search(pattern, original_query, re.IGNORECASE)
        if match:
            extracted_series_num = int(match.group(1))
            break

    # Also try to extract series name (text before "book N" or similar)
    series_name_match = re.match(r'^(.+?)\s+(?:book|#|no\.?)\s*\d+', original_query, re.IGNORECASE)
    if series_name_match:
        extracted_series_name = series_name_match.group(1).strip()

    # Issue #38: Clean the query to strip leading book numbers like "5 - " or "01. "
    # This prevents book numbers from polluting search results
    # Preserves actual titles like "1984" (number without separator)
    clean_query = clean_search_title(query)
    if clean_query and len(clean_query) >= 2:
        query = clean_query

    bookdb_error = None
    results = []

    # Try BookDB first
    try:
        params = {'q': query, 'limit': limit}
        if author:
            params['author'] = author

        # Use public /search endpoint (no auth required)
        if search_type == 'all':
            endpoint = f"{BOOKDB_API_URL}/search"
        else:
            endpoint = f"{BOOKDB_API_URL}/search/{search_type}"

        # Longer timeout for cold start (embedding model can take 45-60s to load)
        resp = requests.get(endpoint, params=params, timeout=60)

        if resp.status_code == 200:
            results = resp.json()
            # Enrich results with extracted series info if they lack it
            if results and extracted_series_num:
                for result in results:
                    if not result.get('series_position'):
                        result['series_position'] = extracted_series_num
                        result['series_position_source'] = 'extracted_from_query'
                    if not result.get('series_name') and extracted_series_name:
                        result['series_name'] = extracted_series_name
                        result['series_name_source'] = 'extracted_from_query'
            if results:
                return jsonify({
                    'results': results,
                    'count': len(results),
                    'source': 'bookdb',
                    'extracted_series_num': extracted_series_num,
                    'extracted_series_name': extracted_series_name
                })
            # No results from BookDB - will try fallback
            bookdb_error = 'No results found in BookDB'
        else:
            bookdb_error = f'BookDB API error: {resp.status_code}'

    except requests.exceptions.ConnectionError:
        bookdb_error = 'BookDB server temporarily unavailable (not your issue - our server)'
    except requests.exceptions.Timeout:
        bookdb_error = 'BookDB server timeout (not your issue - our server is slow/down)'
    except Exception as e:
        logger.error(f"BookBucket search error: {e}")
        bookdb_error = str(e)

    # Fallback to Google Books
    try:
        logger.info(f"Manual match fallback to Google Books for '{query}' (BookDB: {bookdb_error})")
        config = load_config()
        google_key = config.get('google_books_api_key')

        import urllib.parse
        search_query = query
        if author:
            search_query += f" inauthor:{author}"

        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(search_query)}&maxResults={limit}"
        if google_key:
            url += f"&key={google_key}"

        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('items', [])

            google_results = []
            for item in items:
                vol = item.get('volumeInfo', {})
                authors = vol.get('authors', [])
                if not vol.get('title') or not authors:
                    continue

                # Extract series info from subtitle
                series_name = None
                series_num = None
                subtitle = vol.get('subtitle', '')
                if subtitle:
                    match = re.search(r'^A\s+(.+?)\s+Novel$', subtitle, re.IGNORECASE)
                    if match:
                        series_name = match.group(1)
                    match = re.search(r'Book\s+(\d+)\s+of\s+(.+)', subtitle, re.IGNORECASE)
                    if match:
                        series_num = int(match.group(1))
                        series_name = match.group(2)
                    match = re.search(r'(.+?)\s+(?:Book|#)\s*(\d+)', subtitle, re.IGNORECASE)
                    if match:
                        series_name = match.group(1)
                        series_num = int(match.group(2))

                # Use extracted series info if we didn't find any in the subtitle
                final_series_name = series_name or extracted_series_name
                final_series_num = series_num or extracted_series_num

                google_results.append({
                    'type': 'book',
                    'name': vol.get('title', ''),
                    'title': vol.get('title', ''),
                    'author_name': authors[0] if authors else '',
                    'year_published': vol.get('publishedDate', '')[:4] if vol.get('publishedDate') else None,
                    'series_name': final_series_name,
                    'series_position': final_series_num,
                    'series_position_source': 'extracted_from_query' if (final_series_num and not series_num) else None,
                    'description': vol.get('description', '')[:500] if vol.get('description') else None,
                    'source': 'googlebooks'
                })

            if google_results:
                return jsonify({
                    'results': google_results,
                    'count': len(google_results),
                    'source': 'googlebooks',
                    'fallback_reason': bookdb_error,
                    'extracted_series_num': extracted_series_num,
                    'extracted_series_name': extracted_series_name
                })

    except Exception as e:
        logger.error(f"Google Books fallback error: {e}")

    # Both failed
    return jsonify({
        'error': f'Search unavailable - this is a server issue on our end, not yours. Try again in a few minutes. ({bookdb_error})',
        'results': [],
        'server_issue': True
    })


@app.route('/api/bookdb_stats')
def api_bookdb_stats():
    """Get BookBucket database statistics (book/author/series counts).
    Uses public /stats endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/stats", timeout=5)
        if resp.status_code == 200:
            return jsonify(resp.json())
        return jsonify({'error': f'BookBucket API error: {resp.status_code}'})
    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/book_detail/<int:book_id>')
def api_book_detail(book_id):
    """
    Get full book details from BookBucket + ABS status.
    Used for hover cards and detail modals.
    Uses public endpoint - no API key required.
    """
    try:
        # Fetch full book details from BookBucket
        resp = requests.get(f"{BOOKDB_API_URL}/book/{book_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Book not found (status {resp.status_code})'})

        book = resp.json()

        # Try to find matching item in ABS by title/author
        abs_status = []
        include_abs = request.args.get('include_abs', 'false').lower() == 'true'

        if include_abs and book.get('title'):
            try:
                # Get ABS client
                abs_client = get_abs_client()
                if abs_client:
                    # Search ABS by title
                    libraries = abs_client.get_libraries()
                    title_lower = book.get('title', '').lower()
                    author_lower = (book.get('author_name') or '').lower()

                    for lib in libraries:
                        items_data = abs_client.get_library_items(lib['id'], include_progress=False, limit=0)
                        items = items_data.get('results', [])

                        for item in items:
                            media = item.get('media', {})
                            metadata = media.get('metadata', {})
                            item_title = (metadata.get('title') or '').lower()
                            item_author = (metadata.get('authorName') or '').lower()

                            # Simple fuzzy match - title contains search term
                            if title_lower in item_title or item_title in title_lower:
                                # Check author too if we have it
                                if not author_lower or author_lower in item_author or item_author in author_lower:
                                    # Found a match! Get user progress
                                    item_id = item.get('id')
                                    library_with_progress = abs_client.get_library_with_all_progress(lib['id'])

                                    for lib_item in library_with_progress:
                                        if lib_item.get('id') == item_id:
                                            user_progress = lib_item.get('user_progress', {})
                                            for user_id, progress in user_progress.items():
                                                abs_status.append({
                                                    'username': progress.get('username', 'Unknown'),
                                                    'progress': round(progress.get('progress', 0) * 100),
                                                    'is_finished': progress.get('is_finished', False),
                                                    'library_name': lib.get('name', 'Library')
                                                })
                                            break
                                    break
            except Exception as e:
                logger.warning(f"ABS lookup failed: {e}")

        return jsonify({
            'book': book,
            'abs_status': abs_status
        })

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Book detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/author_detail/<int:author_id>')
def api_author_detail(author_id):
    """
    Get author details from BookBucket.
    Used for hover cards on author search results.
    Uses public endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/author/{author_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Author not found (status {resp.status_code})'})

        author = resp.json()
        return jsonify({'author': author})

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Author detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/series_detail/<int:series_id>')
def api_series_detail(series_id):
    """
    Get series details from BookBucket.
    Used for hover cards on series search results.
    Uses public endpoint - no API key required.
    """
    try:
        resp = requests.get(f"{BOOKDB_API_URL}/series/{series_id}", timeout=10)

        if resp.status_code != 200:
            return jsonify({'error': f'Series not found (status {resp.status_code})'})

        series = resp.json()
        return jsonify({'series': series})

    except requests.exceptions.ConnectionError:
        return jsonify({'error': 'BookBucket API not available'})
    except Exception as e:
        logger.error(f"Series detail error: {e}")
        return jsonify({'error': str(e)})


@app.route('/api/manual_match', methods=['POST'])
def api_manual_match():
    """
    Save a manual match for a book in the queue.
    Accepts custom author/title OR a selected BookDB result.
    Creates a pending_fix entry in history for review.
    """
    try:
        data = request.get_json() or {}
        queue_id = data.get('queue_id')

        # Manual entry fields
        new_author = data.get('author', '').strip()
        new_title = data.get('title', '').strip()

        # Or BookDB selection
        bookdb_result = data.get('bookdb_result')  # Full result object from search

        if not queue_id:
            return jsonify({'success': False, 'error': 'queue_id required'})

        conn = get_db()
        c = conn.cursor()

        # Get the queue item with book info
        c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                            b.path, b.current_author, b.current_title
                     FROM queue q
                     JOIN books b ON q.book_id = b.id
                     WHERE q.id = ?''', (queue_id,))
        item = c.fetchone()
        if not item:
            conn.close()
            return jsonify({'success': False, 'error': 'Queue item not found'})

        book_id = item['book_id']
        old_path = item['path']
        old_author = item['current_author']
        old_title = item['current_title']

        # Determine new values from BookDB result if provided
        new_series = None
        new_series_num = None
        new_narrator = None
        new_year = None
        if bookdb_result:
            new_author = bookdb_result.get('author_name') or new_author
            new_title = bookdb_result.get('name') or bookdb_result.get('title') or new_title
            new_series = bookdb_result.get('series_name')
            new_series_num = bookdb_result.get('series_position')
            new_year = bookdb_result.get('year_published')

        # Also accept series fields directly (for manual entry without BookDB selection)
        if not new_series and data.get('series_name'):
            new_series = data.get('series_name')
        if not new_series_num and data.get('series_position'):
            new_series_num = data.get('series_position')

        if not new_author or not new_title:
            conn.close()
            return jsonify({'success': False, 'error': 'Author and title required'})

        # Find which library this book belongs to
        config = load_config()

        # Issue #57: Apply author initials standardization if enabled
        if config.get('standardize_author_initials', False) and new_author:
            new_author = standardize_initials(new_author)
        lib_path = None

        # Issue #57: Check if book is from watch folder and should go to watch_output_folder
        watch_folder = config.get('watch_folder', '').strip()
        watch_output_folder = config.get('watch_output_folder', '').strip()
        if watch_folder and watch_output_folder:
            try:
                watch_path = Path(watch_folder).resolve()
                old_path_resolved = Path(old_path).resolve()
                try:
                    old_path_resolved.relative_to(watch_path)
                    # Book is in watch folder - use output folder as target
                    lib_path = Path(watch_output_folder)
                    logger.info(f"Manual match: routing watch folder book to output folder {lib_path}")
                except ValueError:
                    pass  # Not in watch folder
            except Exception as e:
                logger.debug(f"Watch folder path check failed: {e}")

        # If not from watch folder, find which library it belongs to
        if lib_path is None:
            for lp in config.get('library_paths', []):
                lp_path = Path(lp)
                try:
                    Path(old_path).relative_to(lp_path)
                    lib_path = lp_path
                    break
                except ValueError:
                    continue

        if lib_path is None:
            lib_path = Path(old_path).parent.parent

        # Detect language from title for multi-language naming
        lang_code = detect_title_language(new_title) if new_title else None

        # Build the new path
        new_path = build_new_path(lib_path, new_author, new_title,
                                  series=new_series, series_num=new_series_num,
                                  narrator=new_narrator, year=new_year,
                                  language_code=lang_code, config=config)

        if new_path is None:
            conn.close()
            return jsonify({'success': False, 'error': 'Could not build valid path for this metadata'})

        # Issue #57: Delete any existing pending/error/needs_attention entries for this book
        # When user manually matches, the old entries are superseded by the new fix
        # Also clear duplicate/corrupt_dest errors (Merijeek: manual fix should clear ALL error states)
        c.execute("DELETE FROM history WHERE book_id = ? AND status IN ('error', 'needs_attention', 'duplicate', 'corrupt_dest')", (book_id,))

        # Issue #79: Use helper function to prevent duplicate history entries
        insert_history_entry(
            c, book_id, old_author, old_title,
            new_author, new_title, old_path, str(new_path), 'pending_fix',
            new_narrator=new_narrator, new_series=new_series,
            new_series_num=str(new_series_num) if new_series_num else None,
            new_year=str(new_year) if new_year else None
        )

        # Update book status
        c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', book_id))

        # Remove from queue
        c.execute('DELETE FROM queue WHERE id = ?', (queue_id,))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Saved: {old_author}/{old_title} → {new_author}/{new_title}',
            'new_author': new_author,
            'new_title': new_title
        })
    except Exception as e:
        logger.error(f"Error in manual_match: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/edit_book', methods=['POST'])
def api_edit_book():
    """
    Edit a book's metadata and lock it from future changes.
    Can edit from history (pending or fixed) or directly from books table.
    User edits are "cemented" - the system will never overwrite them.
    """
    try:
        data = request.get_json() or {}
        history_id = data.get('history_id')
        book_id = data.get('book_id')

        # Manual entry fields
        new_author = data.get('author', '').strip()
        new_title = data.get('title', '').strip()

        # Or BookDB selection
        bookdb_result = data.get('bookdb_result')

        if not history_id and not book_id:
            return jsonify({'success': False, 'error': 'history_id or book_id required'})

        conn = get_db()
        c = conn.cursor()

        # Get the book info
        if history_id:
            c.execute('''SELECT h.id as history_id, h.book_id, h.old_path, h.new_path,
                                h.old_author, h.old_title, h.status as history_status,
                                b.path, b.current_author, b.current_title, b.source_type
                         FROM history h
                         JOIN books b ON h.book_id = b.id
                         WHERE h.id = ?''', (history_id,))
            item = c.fetchone()
            if not item:
                conn.close()
                return jsonify({'success': False, 'error': 'History item not found'})
            book_id = item['book_id']
            old_path = item['old_path'] or item['path']
            old_author = item['old_author'] or item['current_author']
            old_title = item['old_title'] or item['current_title']
            history_status = item['history_status']
        else:
            c.execute('SELECT id, path, current_author, current_title, source_type FROM books WHERE id = ?', (book_id,))
            item = c.fetchone()
            if not item:
                conn.close()
                return jsonify({'success': False, 'error': 'Book not found'})
            old_path = item['path']
            old_author = item['current_author']
            old_title = item['current_title']
            history_status = None

            # Issue #75: If there's an existing pending_fix entry, preserve its original old_author/old_title
            # This ensures multiple edits show "Original → Latest" not "Previous edit → Latest"
            c.execute('''SELECT old_author, old_title, old_path FROM history
                         WHERE book_id = ? AND status = 'pending_fix'
                         ORDER BY id DESC LIMIT 1''', (book_id,))
            existing_pending = c.fetchone()
            if existing_pending:
                old_author = existing_pending['old_author'] or old_author
                old_title = existing_pending['old_title'] or old_title
                old_path = existing_pending['old_path'] or old_path

        # Determine new values from BookDB result if provided
        new_series = None
        new_series_num = None
        new_narrator = None
        new_year = None
        if bookdb_result:
            new_author = bookdb_result.get('author_name') or new_author
            new_title = bookdb_result.get('name') or bookdb_result.get('title') or new_title
            new_series = bookdb_result.get('series_name')
            new_series_num = bookdb_result.get('series_position')
            new_year = bookdb_result.get('year_published')

        # Also accept series fields directly
        if not new_series and data.get('series_name'):
            new_series = data.get('series_name')
        if not new_series_num and data.get('series_position'):
            new_series_num = data.get('series_position')

        if not new_author or not new_title:
            conn.close()
            return jsonify({'success': False, 'error': 'Author and title required'})

        # Find which library this book belongs to (or should go to for watch folder items)
        config = load_config()

        # Issue #57: Apply author initials standardization if enabled
        if config.get('standardize_author_initials', False) and new_author:
            new_author = standardize_initials(new_author)

        lib_path = None
        # Issue #51: sqlite3.Row doesn't have .get() - use bracket access with fallback
        source_type = item['source_type'] if item['source_type'] else 'library'

        # Issue #49: For watch folder items, use the output folder as destination
        if source_type == 'watch_folder':
            # Watch folder items go to watch_output_folder or first library path
            output_folder = config.get('watch_output_folder', '').strip()
            if output_folder:
                lib_path = Path(output_folder)
            elif config.get('library_paths'):
                lib_path = Path(config['library_paths'][0])
            else:
                conn.close()
                return jsonify({'success': False, 'error': 'No output folder configured for watch folder items'})
        else:
            # Normal library item - find which library it belongs to
            for lp in config.get('library_paths', []):
                lp_path = Path(lp)
                try:
                    Path(old_path).relative_to(lp_path)
                    lib_path = lp_path
                    break
                except ValueError:
                    continue

            if lib_path is None:
                lib_path = Path(old_path).parent.parent

        # Detect language from title for multi-language naming
        lang_code = detect_title_language(new_title) if new_title else None

        # Build the new path
        new_path = build_new_path(lib_path, new_author, new_title,
                                  series=new_series, series_num=new_series_num,
                                  narrator=new_narrator, year=new_year,
                                  language_code=lang_code, config=config)

        if new_path is None:
            conn.close()
            return jsonify({'success': False, 'error': 'Could not build valid path for this metadata'})

        # Issue #57: Delete any existing pending/error/needs_attention entries for this book
        # When user manually edits, the old entries are superseded by the new fix
        # Also clear duplicate/corrupt_dest errors (Merijeek: manual fix should clear ALL error states)
        c.execute("DELETE FROM history WHERE book_id = ? AND status IN ('error', 'needs_attention', 'duplicate', 'corrupt_dest')", (book_id,))

        # Issue #79: Use helper function to prevent duplicate history entries
        insert_history_entry(
            c, book_id, old_author, old_title,
            new_author, new_title, old_path, str(new_path), 'pending_fix',
            new_narrator=new_narrator, new_series=new_series,
            new_series_num=str(new_series_num) if new_series_num else None,
            new_year=str(new_year) if new_year else None
        )

        # Update book status and LOCK it - user has set metadata, never change it
        c.execute('UPDATE books SET status = ?, user_locked = 1, current_author = ?, current_title = ? WHERE id = ?',
                  ('pending_fix', new_author, new_title, book_id))

        # Remove from queue if present
        c.execute('DELETE FROM queue WHERE book_id = ?', (book_id,))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Saved and locked: {old_author}/{old_title} → {new_author}/{new_title}',
            'new_author': new_author,
            'new_title': new_title,
            'locked': True
        })
    except Exception as e:
        logger.error(f"Error in edit_book: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/unlock_book/<int:book_id>', methods=['POST'])
def api_unlock_book(book_id):
    """
    Unlock a book so it can be re-processed by the system.
    Removes the user_locked flag.
    """
    try:
        conn = get_db()
        c = conn.cursor()

        c.execute('SELECT id, path, current_author, current_title, user_locked FROM books WHERE id = ?', (book_id,))
        book = c.fetchone()

        if not book:
            conn.close()
            return jsonify({'success': False, 'error': 'Book not found'})

        if not book['user_locked']:
            conn.close()
            return jsonify({'success': False, 'error': 'Book is not locked'})

        # Unlock the book
        c.execute('UPDATE books SET user_locked = 0 WHERE id = ?', (book_id,))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Unlocked: {book["current_author"]}/{book["current_title"]} - will be re-processed on next scan'
        })
    except Exception as e:
        logger.error(f"Error unlocking book: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ============== BACKUP & RESTORE ==============

import zipfile
import io
from datetime import datetime

BACKUP_FILES = ['config.json', 'secrets.json', 'library.db', 'user_groups.json']

@app.route('/api/backup')
def api_backup():
    """Download a backup of all settings and database."""
    try:
        # Create in-memory zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename in BACKUP_FILES:
                filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
                if filepath.exists():
                    zf.write(filepath, filename)
                    logger.info(f"Backup: Added {filename}")

            # Add metadata
            metadata = {
                'backup_date': datetime.now().isoformat(),
                'version': APP_VERSION,
                'files': [f for f in BACKUP_FILES if (DATA_DIR / f).exists()]
            }
            zf.writestr('backup_metadata.json', json.dumps(metadata, indent=2))

        zip_buffer.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'library_manager_backup_{timestamp}.zip'

        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/restore', methods=['POST'])
def api_restore():
    """Restore settings and database from a backup zip."""
    if 'backup' not in request.files:
        return jsonify({'success': False, 'error': 'No backup file provided'})

    backup_file = request.files['backup']
    if not backup_file.filename.endswith('.zip'):
        return jsonify({'success': False, 'error': 'Backup must be a .zip file'})

    try:
        # Create a timestamped backup of current state first
        current_backup_dir = DATA_DIR / 'backups' / f'pre_restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        current_backup_dir.mkdir(parents=True, exist_ok=True)

        for filename in BACKUP_FILES:
            filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
            if filepath.exists():
                import shutil
                shutil.copy2(filepath, current_backup_dir / filename)

        # Extract the uploaded backup
        restored = []
        skipped = []

        with zipfile.ZipFile(backup_file, 'r') as zf:
            # Check for metadata
            if 'backup_metadata.json' in zf.namelist():
                meta = json.loads(zf.read('backup_metadata.json'))
                logger.info(f"Restoring backup from {meta.get('backup_date', 'unknown')}")

            for filename in BACKUP_FILES:
                if filename in zf.namelist():
                    # Extract to data directory (persistent)
                    target_path = DATA_DIR / filename
                    with zf.open(filename) as src:
                        with open(target_path, 'wb') as dst:
                            dst.write(src.read())
                    restored.append(filename)
                    logger.info(f"Restored: {filename}")
                else:
                    skipped.append(filename)

        return jsonify({
            'success': True,
            'message': f'Restored {len(restored)} files. Please restart the app to apply changes.',
            'restored': restored,
            'skipped': skipped,
            'pre_restore_backup': str(current_backup_dir)
        })

    except zipfile.BadZipFile:
        return jsonify({'success': False, 'error': 'Invalid zip file'})
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/backup/info')
def api_backup_info():
    """Get info about what would be backed up."""
    files_info = []
    total_size = 0

    for filename in BACKUP_FILES:
        filepath = DATA_DIR / filename  # Use DATA_DIR for persistent files
        if filepath.exists():
            size = filepath.stat().st_size
            total_size += size
            files_info.append({
                'name': filename,
                'size': size,
                'size_human': f'{size / 1024:.1f} KB' if size > 1024 else f'{size} bytes',
                'modified': datetime.fromtimestamp(filepath.stat().st_mtime).isoformat()
            })

    return jsonify({
        'files': files_info,
        'total_size': total_size,
        'total_size_human': f'{total_size / 1024:.1f} KB' if total_size > 1024 else f'{total_size} bytes'
    })


# ============== MAIN ==============

def _setup_user_packages():
    """Add user site-packages to path for runtime-installed packages (like Whisper)."""
    # This allows packages installed via /api/install-whisper to be found
    # Issue #63: Check both old location (/app/.local) and new location (DATA_DIR/.local)
    locations = [
        os.path.join(str(DATA_DIR), '.local'),  # New location (writable in Docker)
        os.path.join(os.path.dirname(__file__), '.local'),  # Legacy location
    ]
    for pip_local in locations:
        user_site = os.path.join(pip_local, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
        if os.path.isdir(user_site) and user_site not in sys.path:
            sys.path.insert(0, user_site)
            logger.debug(f"Added user site-packages to path: {user_site}")


if __name__ == '__main__':
    _setup_user_packages()  # Allow runtime-installed packages (Issue #63)
    migrate_legacy_config()  # Migrate from old location if needed (Issue #23)
    init_config()  # Create config files if they don't exist
    init_db()
    cleanup_garbage_entries()  # Remove @eaDir, #recycle, etc. from database (Issue #88)
    cleanup_duplicate_history_entries()  # Remove duplicate history entries (Issue #79)
    start_worker()
    port = int(os.environ.get('PORT', 5757))
    app.run(host='0.0.0.0', port=port, debug=False)
