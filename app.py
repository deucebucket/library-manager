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

APP_VERSION = "0.9.0-beta.6"
GITHUB_REPO = "deucebucket/library-manager"  # Your GitHub repo

# Versioning Guide:
# 0.9.0-beta.1  = Initial beta (basic features)
# 0.9.0-beta.2  = Garbage filtering, series grouping, dismiss errors
# 0.9.0-beta.3  = UI cleanup - merged Advanced/Tools tabs
# 0.9.0-beta.4  = Current (improved series detection, DB locking fix, system folder filtering)
# 0.9.0-rc.1    = Release candidate (feature complete, final testing)
# 1.0.0         = First stable release (everything works!)

import os
import sys
import json
import time
import sqlite3
import threading
import logging
import requests
import re
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for


# ============== SMART MATCHING UTILITIES ==============

def calculate_title_similarity(title1, title2):
    """
    Calculate word overlap similarity between two titles.
    Returns a score from 0.0 to 1.0
    """
    if not title1 or not title2:
        return 0.0

    # Normalize: lowercase, remove punctuation, split into words
    def normalize(t):
        t = t.lower()
        t = re.sub(r'[^\w\s]', ' ', t)
        words = set(t.split())
        # Remove common stop words that don't help matching
        stop_words = {'the', 'a', 'an', 'of', 'and', 'or', 'in', 'to', 'for', 'by', 'part', 'book', 'volume'}
        return words - stop_words

    words1 = normalize(title1)
    words2 = normalize(title2)

    if not words1 or not words2:
        return 0.0

    # Calculate Jaccard similarity (intersection over union)
    intersection = words1 & words2
    union = words1 | words2

    return len(intersection) / len(union) if union else 0.0


def extract_series_from_title(title):
    """
    Extract series name and number from title patterns like:
    - "The Firefly Series, Book 8: Coup de Grâce" -> (Firefly, 8, Coup de Grâce)
    - "The Firefly Series, Book 8꞉ Firefly꞉ Coup de Grâce" -> (Firefly, 8, Firefly: Coup de Grâce)
    - "Mistborn Book 1: The Final Empire" -> (Mistborn, 1, The Final Empire)
    - "The Expanse #3 - Abaddon's Gate" -> (The Expanse, 3, Abaddon's Gate)
    """
    # Normalize colon-like characters (Windows uses ꞉ instead of : in filenames)
    normalized = title.replace('꞉', ':').replace('：', ':')  # U+A789 and full-width colon

    # Pattern: "Series Name, Book N: Title" or "Series Name Book N: Title"
    # Also handles "The X Series, Book N: Title"
    match = re.search(r'^(?:The\s+)?(.+?)\s*(?:Series)?,?\s*Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        # Clean up series name (remove trailing "Series" if it got in)
        series = re.sub(r'\s*Series\s*$', '', series, flags=re.IGNORECASE)
        return series, int(match.group(2)), match.group(3).strip()

    # Pattern: "Series #N - Title" or "Series #N: Title"
    match = re.search(r'^(.+?)\s*#(\d+)\s*[:\s-]+(.+)$', normalized)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N - Title"
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*[:\s-]+(.+)$', normalized, re.IGNORECASE)
    if match:
        return match.group(1).strip(), int(match.group(2)), match.group(3).strip()

    # Pattern: "Series Book N" at END (no subtitle) - e.g., "Dark One Book 1"
    # Series name = title before "Book N", actual title = same as series
    match = re.search(r'^(.+?)\s+Book\s+(\d+)\s*$', normalized, re.IGNORECASE)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series  # Title = series name

    # Pattern: "Series #N" at END (no subtitle) - e.g., "Mistborn #1"
    match = re.search(r'^(.+?)\s*#(\d+)\s*$', normalized)
    if match:
        series = match.group(1).strip()
        return series, int(match.group(2)), series

    # Pattern: "Title (Book N)" - book number in parentheses at end
    # e.g., "Ivypool's Heart (Book 17)" -> extract number, title stays same
    match = re.search(r'^(.+?)\s*\(Book\s+(\d+)\)\s*$', normalized, re.IGNORECASE)
    if match:
        title_clean = match.group(1).strip()
        return None, int(match.group(2)), title_clean  # Series unknown, just got number

    return None, None, title


def is_garbage_match(original_title, suggested_title, threshold=0.3):
    """
    Check if an API suggestion is garbage (very low title similarity).
    Returns True if the match should be rejected.

    Examples that should be rejected:
    - "Chapter 19" -> "College Accounting, Chapters 1-9" (only matches "chapter")
    - "Death Genesis" -> "The Darkborn AfterLife Genesis" (only matches "genesis")
    - "Mr. Murder" -> "Frankenstein" (no overlap)

    Threshold of 0.3 means at least 30% word overlap required.
    """
    similarity = calculate_title_similarity(original_title, suggested_title)

    # If original is very short (1-2 words), be more lenient
    orig_words = len([w for w in original_title.lower().split() if len(w) > 2])
    if orig_words <= 2 and similarity >= 0.2:
        return False

    if similarity < threshold:
        logger.info(f"Garbage match rejected: '{original_title}' vs '{suggested_title}' (similarity: {similarity:.2f})")
        return True

    return False


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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/deucebucket/library-manager/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Silence Flask's HTTP request logging (only show errors)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)
app.secret_key = 'library-manager-secret-key-2024'

# ============== CONFIGURATION ==============

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'library.db'
CONFIG_PATH = BASE_DIR / 'config.json'
SECRETS_PATH = BASE_DIR / 'secrets.json'

DEFAULT_CONFIG = {
    "library_paths": [],  # Empty by default - user configures via Settings
    "ai_provider": "openrouter",  # "openrouter" or "gemini"
    "openrouter_model": "google/gemma-3n-e4b-it:free",
    "gemini_model": "gemini-2.0-flash",
    "scan_interval_hours": 6,
    "batch_size": 3,
    "max_requests_per_hour": 30,
    "auto_fix": False,
    "protect_author_changes": True,  # Require approval if author changes completely
    "enabled": True,
    "update_channel": "beta",  # "stable", "beta", or "nightly"
    "naming_format": "author/title"  # "author/title", "author - title", "author/series/title"
}

DEFAULT_SECRETS = {
    "openrouter_api_key": "",
    "gemini_api_key": ""
}


def init_config():
    """Create default config files if they don't exist."""
    if not CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        logger.info(f"Created default config at {CONFIG_PATH}")

    if not SECRETS_PATH.exists():
        with open(SECRETS_PATH, 'w') as f:
            json.dump(DEFAULT_SECRETS, f, indent=2)
        logger.info(f"Created default secrets at {SECRETS_PATH}")

# ============== DATABASE ==============

def init_db():
    """Initialize SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Books table - tracks all scanned books
    c.execute('''CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY,
        path TEXT UNIQUE,
        current_author TEXT,
        current_title TEXT,
        status TEXT DEFAULT 'pending',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Add error_message column if it doesn't exist (migration)
    try:
        c.execute('ALTER TABLE books ADD COLUMN error_message TEXT')
    except:
        pass  # Column already exists

    # Queue table - books needing AI analysis
    c.execute('''CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY,
        book_id INTEGER,
        priority INTEGER DEFAULT 5,
        reason TEXT,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')

    # History table - all fixes made
    c.execute('''CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY,
        book_id INTEGER,
        old_author TEXT,
        old_title TEXT,
        new_author TEXT,
        new_title TEXT,
        old_path TEXT,
        new_path TEXT,
        status TEXT DEFAULT 'pending_fix',
        error_message TEXT,
        fixed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (book_id) REFERENCES books(id)
    )''')

    # Add status and error_message columns if they don't exist (migration)
    try:
        c.execute("ALTER TABLE history ADD COLUMN status TEXT DEFAULT 'pending_fix'")
    except:
        pass
    try:
        c.execute('ALTER TABLE history ADD COLUMN error_message TEXT')
    except:
        pass

    # Stats table - daily stats
    c.execute('''CREATE TABLE IF NOT EXISTS stats (
        id INTEGER PRIMARY KEY,
        date TEXT UNIQUE,
        scanned INTEGER DEFAULT 0,
        queued INTEGER DEFAULT 0,
        fixed INTEGER DEFAULT 0,
        verified INTEGER DEFAULT 0,
        api_calls INTEGER DEFAULT 0
    )''')

    conn.commit()
    conn.close()

def get_db():
    """Get database connection with timeout to avoid lock issues."""
    conn = sqlite3.connect(DB_PATH, timeout=30)  # Wait up to 30 seconds for lock
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')  # Better concurrent access
    return conn

# ============== CONFIG ==============

def load_config():
    """Load configuration and secrets from files."""
    config = DEFAULT_CONFIG.copy()

    # Load main config
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            logger.warning(f"Error loading config: {e}")

    # Load secrets (API keys)
    if SECRETS_PATH.exists():
        try:
            with open(SECRETS_PATH) as f:
                secrets = json.load(f)
                config.update(secrets)
        except Exception as e:
            logger.warning(f"Error loading secrets: {e}")

    return config

def save_config(config):
    """Save configuration to file (excludes secrets)."""
    # Separate secrets from config
    secrets_keys = ['openrouter_api_key', 'gemini_api_key']
    config_only = {k: v for k, v in config.items() if k not in secrets_keys}

    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_only, f, indent=2)


def save_secrets(secrets):
    """Save API keys to secrets file."""
    with open(SECRETS_PATH, 'w') as f:
        json.dump(secrets, f, indent=2)

# ============== DRASTIC CHANGE DETECTION ==============

def is_drastic_author_change(old_author, new_author):
    """
    Check if an author change is "drastic" (completely different person)
    vs just formatting (case change, initials expanded, etc.)

    Returns True if the change is drastic and should require approval.
    """
    if not old_author or not new_author:
        return False

    # Normalize for comparison
    old_norm = old_author.lower().strip()
    new_norm = new_author.lower().strip()

    # Placeholder authors - going FROM these to a real author is NOT drastic
    placeholder_authors = {'unknown', 'various', 'various authors', 'va', 'n/a', 'none',
                           'audiobook', 'audiobooks', 'ebook', 'ebooks', 'book', 'books',
                           'author', 'authors', 'narrator', 'untitled', 'no author'}
    if old_norm in placeholder_authors:
        return False  # Finding the real author is always good

    # If they're the same after normalization, not drastic
    if old_norm == new_norm:
        return False

    # Extract key words (remove common prefixes/suffixes)
    def get_name_parts(name):
        # Remove punctuation and split
        import re
        clean = re.sub(r'[^\w\s]', ' ', name.lower())
        parts = [p for p in clean.split() if len(p) > 1]
        return set(parts)

    old_parts = get_name_parts(old_author)
    new_parts = get_name_parts(new_author)

    # If no overlap at all, definitely drastic
    if not old_parts.intersection(new_parts):
        # Check for initials match (e.g., "J.R.R. Tolkien" vs "Tolkien")
        # Get last names (usually the longest word or last word)
        old_last = max(old_parts, key=len) if old_parts else ""
        new_last = max(new_parts, key=len) if new_parts else ""

        if old_last and new_last and (old_last in new_last or new_last in old_last):
            return False  # Probably same person

        return True  # Completely different

    # Some overlap - check how much
    overlap = len(old_parts.intersection(new_parts))
    total = max(len(old_parts), len(new_parts))

    # If less than 30% overlap, consider it drastic
    if total > 0 and overlap / total < 0.3:
        return True

    return False

# ============== BOOK METADATA APIs ==============

# Rate limiting for each API (last call timestamp)
# Based on research:
# - Audnexus: No docs, small project - 1 req/sec max
# - OpenLibrary: Had issues with high traffic - 1 req/sec
# - Google Books: ~1000/day free = ~40/hour - 1 req/2sec
# - Hardcover: Beta API, be conservative - 1 req/2sec
API_RATE_LIMITS = {
    'audnexus': {'last_call': 0, 'min_delay': 1.5},      # 1.5 sec between calls
    'openlibrary': {'last_call': 0, 'min_delay': 1.5},   # 1.5 sec between calls
    'googlebooks': {'last_call': 0, 'min_delay': 2.5},   # 2.5 sec between calls (stricter)
    'hardcover': {'last_call': 0, 'min_delay': 2.5},     # 2.5 sec between calls (beta)
}
API_RATE_LOCK = threading.Lock()

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


def build_new_path(lib_path, author, title, series=None, series_num=None, narrator=None, year=None, config=None):
    """Build a new path based on the naming format configuration.

    Audiobookshelf-compatible format (when series_grouping enabled):
    - Narrator in curly braces: {Ray Porter}
    - Series number prefix: "1 - Title"
    - Year in parentheses: (2003)
    """
    naming_format = config.get('naming_format', 'author/title') if config else 'author/title'
    series_grouping = config.get('series_grouping', False) if config else False

    # Build title folder name
    title_folder = title

    # Add series number prefix if series grouping enabled and we have series info
    if series_grouping and series and series_num:
        title_folder = f"{series_num} - {title}"

    # Add year if present
    if year:
        title_folder = f"{title_folder} ({year})"

    # Add narrator - curly braces for ABS format, parentheses otherwise
    if narrator:
        if series_grouping:
            # ABS format uses curly braces for narrator
            title_folder = f"{title_folder} {{{narrator}}}"
        else:
            # Legacy format uses parentheses
            title_folder = f"{title_folder} ({narrator})"

    if naming_format == 'author - title':
        # Flat structure: Author - Title (single folder)
        folder_name = f"{author} - {title_folder}"
        return lib_path / folder_name
    elif series_grouping and series:
        # Series grouping enabled AND book has series: Author/Series/Title
        return lib_path / author / series / title_folder
    else:
        # Default: Author/Title (two-level)
        return lib_path / author / title_folder


def clean_search_title(messy_name):
    """Clean up a messy filename to extract searchable title."""
    import re
    # Remove common junk patterns
    clean = messy_name
    # Remove bracketed content like [bitsearch.to], [64k], [r1.1]
    clean = re.sub(r'\[.*?\]', '', clean)
    # Remove parenthetical junk like (Unabridged), (2019)
    clean = re.sub(r'\((?:Unabridged|Abridged|\d{4}|MP3|M4B|EPUB|PDF|64k|128k|r\d+\.\d+).*?\)', '', clean, flags=re.IGNORECASE)
    # Remove file extensions
    clean = re.sub(r'\.(mp3|m4b|m4a|epub|pdf|mobi)$', '', clean, flags=re.IGNORECASE)
    # Remove "by Author" at the end temporarily for searching
    clean = re.sub(r'\s+by\s+[\w\s]+$', '', clean, flags=re.IGNORECASE)
    # Remove leading/trailing junk
    clean = clean.strip(' -_.')
    return clean

def search_openlibrary(title, author=None):
    """Search OpenLibrary for book metadata. Free, no API key needed."""
    rate_limit_wait('openlibrary')
    try:
        import urllib.parse
        query = urllib.parse.quote(title)
        url = f"https://openlibrary.org/search.json?title={query}&limit=5"
        if author:
            url += f"&author={urllib.parse.quote(author)}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        docs = data.get('docs', [])

        if not docs:
            return None

        # Get the best match (first result usually best)
        best = docs[0]
        result = {
            'title': best.get('title', ''),
            'author': best.get('author_name', [''])[0] if best.get('author_name') else '',
            'year': best.get('first_publish_year'),
            'source': 'openlibrary'
        }

        # Only return if we got useful data
        if result['title'] and result['author']:
            logger.info(f"OpenLibrary found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"OpenLibrary search failed: {e}")
        return None

def search_google_books(title, author=None, api_key=None):
    """Search Google Books for book metadata."""
    rate_limit_wait('googlebooks')
    try:
        import urllib.parse
        query = title
        if author:
            query += f" inauthor:{author}"

        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5"
        if api_key:
            url += f"&key={api_key}"

        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        items = data.get('items', [])

        if not items:
            return None

        # Get best match
        best = items[0].get('volumeInfo', {})
        authors = best.get('authors', [])

        # Try to extract series from subtitle (e.g., "A Mistborn Novel", "Book 2 of The Expanse")
        series_name = None
        series_num = None
        subtitle = best.get('subtitle', '')
        if subtitle:
            # "A Mistborn Novel" -> Mistborn
            match = re.search(r'^A\s+(.+?)\s+Novel$', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
            # "Book 2 of The Expanse" -> The Expanse, 2
            match = re.search(r'Book\s+(\d+)\s+of\s+(.+)', subtitle, re.IGNORECASE)
            if match:
                series_num = int(match.group(1))
                series_name = match.group(2)
            # "The Expanse Book 2" or "Mistborn #1"
            match = re.search(r'(.+?)\s+(?:Book|#)\s*(\d+)', subtitle, re.IGNORECASE)
            if match:
                series_name = match.group(1)
                series_num = int(match.group(2))

        result = {
            'title': best.get('title', ''),
            'author': authors[0] if authors else '',
            'year': best.get('publishedDate', '')[:4] if best.get('publishedDate') else None,
            'series': series_name,
            'series_num': series_num,
            'source': 'googlebooks'
        }

        if result['title'] and result['author']:
            logger.info(f"Google Books found: {result['author']} - {result['title']}" +
                       (f" (Series: {series_name})" if series_name else ""))
            return result
        return None
    except Exception as e:
        logger.debug(f"Google Books search failed: {e}")
        return None

def search_audnexus(title, author=None):
    """Search Audnexus API for audiobook metadata. Pulls from Audible."""
    rate_limit_wait('audnexus')
    try:
        import urllib.parse
        # Audnexus search endpoint
        query = title
        if author:
            query = f"{title} {author}"

        url = f"https://api.audnex.us/books?title={urllib.parse.quote(query)}"

        resp = requests.get(url, timeout=10, headers={'Accept': 'application/json'})
        if resp.status_code != 200:
            return None

        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        # Get best match
        best = data[0]
        result = {
            'title': best.get('title', ''),
            'author': best.get('authors', [{}])[0].get('name', '') if best.get('authors') else '',
            'year': best.get('releaseDate', '')[:4] if best.get('releaseDate') else None,
            'narrator': best.get('narrators', [{}])[0].get('name', '') if best.get('narrators') else None,
            'source': 'audnexus'
        }

        if result['title'] and result['author']:
            logger.info(f"Audnexus found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"Audnexus search failed: {e}")
        return None

def search_hardcover(title, author=None):
    """Search Hardcover.app API for book metadata."""
    rate_limit_wait('hardcover')
    try:
        import urllib.parse
        # Hardcover GraphQL API
        query = title
        if author:
            query = f"{title} {author}"

        # Hardcover uses GraphQL
        graphql_query = {
            "query": """
                query SearchBooks($query: String!) {
                    search(query: $query, limit: 5) {
                        books {
                            title
                            contributions { author { name } }
                            releaseYear
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

        result = {
            'title': best.get('title', ''),
            'author': author_name,
            'year': best.get('releaseYear'),
            'source': 'hardcover'
        }

        if result['title'] and result['author']:
            logger.info(f"Hardcover found: {result['author']} - {result['title']}")
            return result
        return None
    except Exception as e:
        logger.debug(f"Hardcover search failed: {e}")
        return None

def extract_author_title(messy_name):
    """Try to extract author and title from a folder name like 'Author - Title' or 'Author/Title'."""
    import re

    # Common separators: " - ", " / ", " _ "
    separators = [' - ', ' / ', ' _ ', ' – ']  # includes en-dash

    for sep in separators:
        if sep in messy_name:
            parts = messy_name.split(sep, 1)
            if len(parts) == 2:
                author = parts[0].strip()
                title = parts[1].strip()
                # Basic validation - author shouldn't be too long or look like a title
                if len(author) < 50 and not re.search(r'\d{4}|book|vol|part|\[', author, re.I):
                    return author, title

    # No separator found - just return the whole thing as title
    return None, messy_name

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

    # 1. Try Audnexus first (best for audiobooks, pulls from Audible)
    result = validate_result(search_audnexus(clean_title, author=author_hint), clean_title)
    if result:
        return result

    # 2. Try OpenLibrary (free, huge database)
    result = validate_result(search_openlibrary(clean_title, author=author_hint), clean_title)
    if result:
        return result

    # 3. Try Google Books
    google_key = config.get('google_books_api_key')
    result = validate_result(search_google_books(clean_title, author=author_hint, api_key=google_key), clean_title)
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
    Now with garbage match filtering.
    """
    candidates = []
    clean_title = clean_search_title(title)

    if not clean_title or len(clean_title) < 3:
        return candidates

    # Search each API and collect all results
    apis = [
        ('Audnexus', search_audnexus),
        ('OpenLibrary', search_openlibrary),
        ('GoogleBooks', lambda t, a: search_google_books(t, a, config.get('google_books_api_key') if config else None)),
        ('Hardcover', search_hardcover),
    ]

    for api_name, search_func in apis:
        try:
            # Search with author hint
            result = search_func(clean_title, author)
            if result:
                # Filter garbage matches
                suggested_title = result.get('title', '')
                if is_garbage_match(clean_title, suggested_title):
                    logger.debug(f"REJECTED garbage from {api_name}: '{clean_title}' -> '{suggested_title}'")
                else:
                    result['search_query'] = f"{author} - {clean_title}" if author else clean_title
                    candidates.append(result)

            # Also search without author (might find different results)
            if author:
                result_no_author = search_func(clean_title, None)
                if result_no_author:
                    suggested_title = result_no_author.get('title', '')
                    if is_garbage_match(clean_title, suggested_title):
                        logger.debug(f"REJECTED garbage from {api_name}: '{clean_title}' -> '{suggested_title}'")
                    elif result_no_author.get('author') != (result.get('author') if result else None):
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

    return unique_candidates


def build_verification_prompt(original_input, original_author, original_title, proposed_author, proposed_title, candidates):
    """
    Build a verification prompt that shows ALL API candidates and asks AI to vote.
    """
    candidate_list = ""
    for i, c in enumerate(candidates, 1):
        candidate_list += f"  CANDIDATE_{i}: {c.get('author', 'Unknown')} - {c.get('title', 'Unknown')} (from {c.get('source', 'Unknown')})\n"

    if not candidate_list:
        candidate_list = "  No API results found.\n"

    return f"""You are a book metadata verification expert. A drastic author change was detected and needs your verification.

ORIGINAL INPUT: {original_input}
  - Current Author: {original_author}
  - Current Title: {original_title}

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

If the proposed title shares LESS THAN HALF of its significant words with the original title, it is WRONG.

YOUR TASK:
Analyze whether the proposed change is CORRECT or WRONG. Consider:

1. TITLE MATCHING FIRST - Is this even the same book?
   - At least 50% of significant words must match
   - "Mr. Murder" and "Dean Koontz's Frankenstein" = WRONG (0% match!)
   - "Midnight Texas 3" and "Night Shift" = CORRECT if Night Shift is book 3 of Midnight Texas

2. AUTHOR MATCHING: Does the original author name match or partially match any candidate?
   - "Boyett" matches "Steven Boyett" (same person, use full name)
   - "Boyett" does NOT match "John Dickson Carr" (different person!)
   - "A.C. Crispin" matches "A. C. Crispin" or "Ann C. Crispin" (same person)

3. TRUST THE INPUT: If original has a real author name, KEEP that author unless clearly wrong.

4. FIND THE BEST MATCH: Pick the candidate whose author MATCHES or EXTENDS the original.

RESPOND WITH JSON ONLY:
{{
  "decision": "CORRECT" or "WRONG" or "UNCERTAIN",
  "recommended_author": "The correct author name",
  "recommended_title": "The correct title",
  "reasoning": "Brief explanation of why",
  "confidence": "HIGH" or "MEDIUM" or "LOW"
}}

DECISION RULES:
- If titles are completely different books = WRONG (don't just keyword match!)
- If original author matches a candidate (like Boyett -> Steven Boyett) = CORRECT
- If proposed author is completely different person AND same title = WRONG
- If uncertain = UNCERTAIN

When in doubt, say WRONG. It's better to leave a book unfixed than to rename it to the wrong thing."""


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

    # Call AI for verification
    provider = config.get('ai_provider', 'openrouter')
    try:
        if provider == 'gemini' and config.get('gemini_api_key'):
            verification = call_gemini(prompt, config)  # Already returns parsed dict
        elif config.get('openrouter_api_key'):
            verification = call_openrouter(prompt, config)  # Already returns parsed dict
        else:
            logger.error("No API key for verification!")
            return None

        if not verification:
            return None

        # Result is already parsed by call_gemini/call_openrouter

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
    for i, name in enumerate(messy_names):
        item_text = f"ITEM_{i+1}: {name}"
        # Add API lookup result if available
        if api_results and i < len(api_results) and api_results[i]:
            result = api_results[i]
            item_text += f"\n  -> API found: {result['author']} - {result['title']} (from {result['source']})"
        items.append(item_text)
    names_list = "\n".join(items)

    return f"""You are a book metadata expert. For each filename, identify the REAL author and title.

{names_list}

MOST IMPORTANT RULE - TRUST THE EXISTING AUTHOR:
If the input is already in "Author / Title" or "Author - Title" format with a human name as author:
- KEEP THAT AUTHOR unless you're 100% certain it's wrong
- Many books have the SAME TITLE by DIFFERENT AUTHORS
- Example: "The Hollow Man" exists by BOTH Steven Boyett AND John Dickson Carr - different books!
- Example: "Yellow" by Aron Beauregard is NOT "The King in Yellow" by Chambers!
- If API returns a DIFFERENT AUTHOR for the same title, TRUST THE INPUT AUTHOR

WHEN TO CHANGE THE AUTHOR:
- Only if the "author" in input is clearly NOT an author name (e.g., "Bastards Series", "Unknown", "Various")
- Only if the author/title are swapped (e.g., "Mistborn / Brandon Sanderson" -> swap them)
- Only if it's clearly gibberish

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
    """Extract JSON from AI response."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())

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
    provider = config.get('ai_provider', 'openrouter')

    # Use selected provider
    if provider == 'gemini' and config.get('gemini_api_key'):
        return call_gemini(prompt, config)
    elif config.get('openrouter_api_key'):
        return call_openrouter(prompt, config)
    else:
        logger.error("No API key configured!")
        return None


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


def call_openrouter(prompt, config):
    """Call OpenRouter API."""
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {config['openrouter_api_key']}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://deucebucket.com",
                "X-Title": "Library Metadata Manager"
            },
            json={
                "model": config.get('openrouter_model', 'google/gemma-3n-e4b-it:free'),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1
            },
            timeout=90
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if text:
                return parse_json_response(text)
        else:
            error_msg = explain_http_error(resp.status_code, "OpenRouter")
            logger.warning(f"OpenRouter: {error_msg}")
            # Try to get more detail from response
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"OpenRouter detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("OpenRouter: Request timed out after 90 seconds")
    except requests.exceptions.ConnectionError:
        logger.error("OpenRouter: Connection failed - check your internet")
    except Exception as e:
        logger.error(f"OpenRouter: {e}")
    return None


def call_gemini(prompt, config, retry_count=0):
    """Call Google Gemini API directly with automatic retry on rate limit."""
    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', 'gemini-2.0-flash')

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1}
            },
            timeout=90
        )

        if resp.status_code == 200:
            result = resp.json()
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text:
                return parse_json_response(text)
        elif resp.status_code == 429 and retry_count < 3:
            # Rate limit - parse retry time and wait
            error_msg = explain_http_error(resp.status_code, "Gemini")
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
                    # Try to parse "Please retry in X.XXXs" from message
                    import re
                    match = re.search(r'retry in (\d+\.?\d*)s', detail)
                    if match:
                        wait_time = float(match.group(1)) + 5  # Add 5 sec buffer
                        logger.info(f"Gemini: Waiting {wait_time:.0f} seconds before retry...")
                        time.sleep(wait_time)
                        return call_gemini(prompt, config, retry_count + 1)
            except:
                pass
            # Default wait if we can't parse the time
            wait_time = 45 * (retry_count + 1)
            logger.info(f"Gemini: Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
            return call_gemini(prompt, config, retry_count + 1)
        else:
            error_msg = explain_http_error(resp.status_code, "Gemini")
            logger.warning(f"Gemini: {error_msg}")
            try:
                detail = resp.json().get('error', {}).get('message', '')
                if detail:
                    logger.warning(f"Gemini detail: {detail}")
            except:
                pass
    except requests.exceptions.Timeout:
        logger.error("Gemini: Request timed out after 90 seconds")
    except requests.exceptions.ConnectionError:
        logger.error("Gemini: Connection failed - check your internet")
    except Exception as e:
        logger.error(f"Gemini: {e}")
    return None

# ============== DEEP SCANNER ==============

import re
import hashlib

# Audio file extensions we care about
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.opus', '.wma', '.aac'}
EBOOK_EXTENSIONS = {'.epub', '.pdf', '.mobi', '.azw3'}

# Patterns for disc/chapter folders (these are NOT book titles)
DISC_CHAPTER_PATTERNS = [
    r'^(disc|disk|cd|part|chapter|ch)\s*\d+',  # "Disc 1", "Part 2", "Chapter 3"
    r'^\d+\s*[-_]\s*(disc|disk|cd|part|chapter)',  # "1 - Disc", "01_Chapter"
    r'^(side)\s*[ab12]',  # "Side A", "Side 1"
    r'.+\s*-\s*(disc|disk|cd)\s*\d+$',  # "Book Name - Disc 01"
]

# Junk patterns to clean from titles
JUNK_PATTERNS = [
    r'\[bitsearch\.to\]',
    r'\[rarbg\]',
    r'\(unabridged\)',
    r'\(abridged\)',
    r'\(audiobook\)',
    r'\(audio\)',
    r'\(graphicaudio\)',
    r'\(uk version\)',
    r'\(us version\)',
    r'\[EN\]',
    r'\(r\d+\.\d+\)',  # (r1.0), (r1.1)
    r'\[\d+\]',  # [64420]
    r'\{\d+mb\}',  # {388mb}
    r'\{\d+\.\d+gb\}',  # {1.29gb}
    r'\d+k\s+\d+\.\d+\.\d+',  # 64k 13.31.36
    r'128k|64k|192k|320k',  # bitrate
    r'\.epub$|\.pdf$|\.mobi$',  # file extensions in folder names
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


def find_orphan_audio_files(lib_path):
    """Find audio files sitting directly in author folders (not in book subfolders)."""
    orphans = []

    for author_dir in Path(lib_path).iterdir():
        if not author_dir.is_dir():
            continue

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
    """Create a book folder and move orphan files into it."""
    import shutil

    author_dir = Path(author_path)

    # Clean up the book title for folder name
    clean_title = book_title

    # Remove format/quality junk from title
    clean_title = re.sub(r'\s*\((?:Unabridged|Abridged|MP3|M4B|64k|128k|HQ|Complete|Full|Retail)\)', '', clean_title, flags=re.IGNORECASE)
    clean_title = re.sub(r'\s*\[.*?\]', '', clean_title)  # Remove bracketed content
    clean_title = re.sub(r'[<>:"/\\|?*]', '', clean_title)  # Remove illegal chars
    clean_title = clean_title.strip()

    if not clean_title:
        return False, "Could not determine book title"

    book_dir = author_dir / clean_title

    # Check if folder already exists
    if book_dir.exists():
        # Check if it's empty or has files
        existing = list(book_dir.iterdir())
        if existing:
            return False, f"Folder already exists with {len(existing)} items: {book_dir}"
    else:
        book_dir.mkdir(parents=True)

    # Move files
    moved = 0
    errors = []
    for file_path in files:
        try:
            src = Path(file_path)
            if src.exists():
                dest = book_dir / src.name
                shutil.move(str(src), str(dest))
                moved += 1
        except Exception as e:
            errors.append(f"{file_path}: {e}")

    if errors:
        return False, f"Moved {moved} files, {len(errors)} errors: {errors[0]}"

    logger.info(f"Organized {moved} orphan files into: {book_dir}")
    return True, f"Created {book_dir.name} with {moved} files"


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

    for pattern in JUNK_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            issues.append(f"junk: {pattern}")
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    # Clean up extra whitespace and dashes
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[-_\s]+|[-_\s]+$', '', cleaned)
    cleaned = re.sub(r'\s*-\s*$', '', cleaned)

    return cleaned, issues


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


def find_audio_files(directory):
    """Recursively find all audio files in directory."""
    audio_files = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext in AUDIO_EXTENSIONS:
                audio_files.append(os.path.join(root, f))
    return audio_files


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


def deep_scan_library(config):
    """
    Deep scan library - the AUTISTIC LIBRARIAN approach.
    Finds ALL issues, duplicates, and structural problems.
    """
    conn = get_db()
    c = conn.cursor()

    scanned = 0
    queued = 0
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

        # Second pass: Analyze folder structure
        for author_dir in lib_path.iterdir():
            if not author_dir.is_dir():
                continue

            author = author_dir.name
            author_issues = analyze_author(author)

            # Check if "author" folder is actually a book (has audio files directly)
            direct_audio = [f for f in author_dir.iterdir()
                          if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS]
            if direct_audio:
                # This "author" folder might actually be a book!
                issues_found[str(author_dir)] = author_issues + ["author_folder_has_audio_files"]
                logger.warning(f"Author folder has audio files directly: {author}")

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
                path = str(title_dir)

                # Skip if this looks like a disc/chapter folder
                if is_disc_chapter_folder(title):
                    # But flag the parent!
                    issues_found[str(author_dir)] = issues_found.get(str(author_dir), []) + [f"has_disc_folder:{title}"]
                    continue

                # Check if this is a SERIES folder containing multiple book subfolders
                # If so, skip it - we should process the books inside, not the series folder itself
                subdirs = [d for d in title_dir.iterdir() if d.is_dir()]
                if len(subdirs) >= 2:
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
                    if book_like_count >= 2:
                        # This is a series folder, not a book - skip it
                        logger.info(f"Skipping series folder (contains {book_like_count} book subfolders): {path}")
                        # Mark in database as series_folder so we don't keep checking it
                        c.execute('SELECT id FROM books WHERE path = ?', (path,))
                        existing = c.fetchone()
                        if existing:
                            c.execute('UPDATE books SET status = ? WHERE id = ?', ('series_folder', existing['id']))
                        else:
                            c.execute('''INSERT INTO books (path, current_author, current_title, status)
                                         VALUES (?, ?, ?, 'series_folder')''', (path, author, title))
                        conn.commit()
                        continue

                # Analyze title
                title_issues = analyze_title(title, author)
                cleaned_title, clean_issues = clean_title(title)

                all_issues = author_issues + title_issues + clean_issues

                # Check for nested structure (disc folders inside book folder)
                nested_dirs = [d for d in title_dir.iterdir() if d.is_dir()]
                disc_dirs = [d for d in nested_dirs if is_disc_chapter_folder(d.name)]
                if disc_dirs:
                    all_issues.append(f"has_{len(disc_dirs)}_disc_folders")

                # Check for ebook files mixed with audiobooks
                ebook_files = [f for f in title_dir.rglob('*') if f.suffix.lower() in EBOOK_EXTENSIONS]
                if ebook_files:
                    all_issues.append(f"has_{len(ebook_files)}_ebook_files")

                # Store issues
                if all_issues:
                    issues_found[path] = all_issues

                # Add to database
                c.execute('SELECT id, status FROM books WHERE path = ?', (path,))
                existing = c.fetchone()

                if existing:
                    if existing['status'] in ['verified', 'fixed']:
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
    logger.info(f"Scanned: {scanned} new books")
    logger.info(f"Queued: {queued} books with issues")
    logger.info(f"Total issues found: {len(issues_found)} locations")

    return scanned, queued


def scan_library(config):
    """Wrapper that calls deep scan."""
    return deep_scan_library(config)

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


def process_queue(config, limit=None):
    """Process items in the queue."""
    # Check rate limit first
    allowed, calls_made, max_calls = check_rate_limit(config)
    if not allowed:
        logger.warning(f"Rate limit reached: {calls_made}/{max_calls} calls. Waiting...")
        return 0, 0

    conn = get_db()
    c = conn.cursor()

    batch_size = config.get('batch_size', 3)
    if limit:
        batch_size = min(batch_size, limit)

    logger.info(f"[DEBUG] process_queue called with batch_size={batch_size}, limit={limit} (API: {calls_made}/{max_calls})")

    # Get batch from queue
    c.execute('''SELECT q.id as queue_id, q.book_id, q.reason,
                        b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 ORDER BY q.priority, q.added_at
                 LIMIT ?''', (batch_size,))
    batch = c.fetchall()

    logger.info(f"[DEBUG] Fetched {len(batch)} items from queue")

    if not batch:
        logger.info("[DEBUG] No items in batch, returning 0")
        conn.close()
        return 0, 0  # (processed, fixed)

    # Build messy names for AI
    messy_names = [f"{row['current_author']} - {row['current_title']}" for row in batch]

    logger.info(f"[DEBUG] Processing batch of {len(batch)} items:")
    for i, name in enumerate(messy_names):
        logger.info(f"[DEBUG]   Item {i+1}: {name}")

    results = call_ai(messy_names, config)
    logger.info(f"[DEBUG] AI returned {len(results) if results else 0} results")

    # Update API call stats (INSERT if not exists, then UPDATE to preserve other columns)
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('UPDATE stats SET api_calls = COALESCE(api_calls, 0) + 1 WHERE date = ?', (today,))

    if not results:
        logger.warning("No results from AI")
        conn.commit()
        conn.close()
        return 0, 0  # (processed, fixed)

    processed = 0
    fixed = 0
    for row, result in zip(batch, results):
        new_author = (result.get('author') or '').strip()
        new_title = (result.get('title') or '').strip()
        new_narrator = (result.get('narrator') or '').strip() or None  # None if empty
        new_series = (result.get('series') or '').strip() or None  # Series name
        new_series_num = result.get('series_num')  # Series number (can be int or string like "1" or "Book 1")
        new_year = result.get('year')  # Publication year

        # If AI didn't detect series, try to extract it from title patterns
        # First try the ORIGINAL title (has series info like "The Reckoners, Book 2 - Firefight")
        # Then try the new title as fallback
        if not new_series:
            # Try original title first (most likely to have series pattern)
            original_title = row['current_title']
            extracted_series, extracted_num, extracted_title = extract_series_from_title(original_title)
            if extracted_series:
                new_series = extracted_series
                new_series_num = extracted_num
                # Keep the AI's cleaned title, just add the series info
                logger.info(f"Extracted series from original title: '{extracted_series}' #{extracted_num}")
            else:
                # Got book number but no series name? Check if original "author" is actually a series
                if extracted_num and not new_series:
                    original_author = row['current_author']
                    # Check if original author looks like a series name
                    series_indicators = ['series', 'saga', 'cycle', 'chronicles', 'trilogy', 'collection',
                                         'edition', 'novels', 'books', 'tales', 'adventures', 'mysteries']
                    if any(ind in original_author.lower() for ind in series_indicators):
                        new_series = original_author
                        new_series_num = extracted_num
                        logger.info(f"Using original author as series: '{new_series}' #{new_series_num}")

            # Fallback: try the new title
            if not new_series and new_title:
                extracted_series, extracted_num, extracted_title = extract_series_from_title(new_title)
                if extracted_series:
                    new_series = extracted_series
                    new_series_num = extracted_num
                    new_title = extracted_title
                    logger.info(f"Extracted series from new title: '{extracted_series}' #{extracted_num} - '{extracted_title}'")

        if not new_author or not new_title:
            # Remove from queue, mark as verified
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
            processed += 1
            logger.info(f"Verified OK (empty result): {row['current_author']}/{row['current_title']}")
            continue

        # Check if fix needed (also check narrator change)
        if new_author != row['current_author'] or new_title != row['current_title'] or new_narrator:
            old_path = Path(row['path'])
            lib_path = old_path.parent.parent
            new_path = build_new_path(lib_path, new_author, new_title,
                                      series=new_series, series_num=new_series_num,
                                      narrator=new_narrator, year=new_year, config=config)

            # Check for drastic author change
            drastic_change = is_drastic_author_change(row['current_author'], new_author)
            protect_authors = config.get('protect_author_changes', True)

            # If drastic change detected, run verification pipeline
            if drastic_change and protect_authors:
                logger.info(f"DRASTIC CHANGE DETECTED: {row['current_author']} -> {new_author}, running verification...")

                # Run verification with all APIs
                original_input = f"{row['current_author']}/{row['current_title']}"
                verification = verify_drastic_change(
                    original_input,
                    row['current_author'], row['current_title'],
                    new_author, new_title,
                    config
                )

                if verification:
                    if verification['verified']:
                        # AI verified the change is correct (or corrected it)
                        new_author = verification['author']
                        new_title = verification['title']
                        # Recheck if it's still drastic after verification
                        drastic_change = is_drastic_author_change(row['current_author'], new_author)
                        logger.info(f"VERIFIED: {row['current_author']} -> {new_author} ({verification['reasoning'][:50]}...)")
                    elif verification['decision'] == 'WRONG':
                        # AI says the change is wrong - use the recommended fix instead
                        new_author = verification['author']
                        new_title = verification['title']
                        drastic_change = is_drastic_author_change(row['current_author'], new_author)
                        logger.info(f"CORRECTED: {row['current_author']} -> {new_author} (was wrong: {verification['reasoning'][:50]}...)")
                    else:
                        # AI is uncertain - block the change
                        logger.warning(f"BLOCKED (uncertain): {row['current_author']} -> {new_author}")
                        # Record as pending_fix for manual review
                        c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message)
                                     VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix', ?)''',
                                 (row['book_id'], row['current_author'], row['current_title'],
                                  new_author, new_title, str(old_path), str(new_path),
                                  f"Uncertain: {verification.get('reasoning', 'needs review')}"))
                        c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                        processed += 1
                        continue
                else:
                    # Verification failed - block the change
                    logger.warning(f"BLOCKED (verification failed): {row['current_author']} -> {new_author}")
                    c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                    c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                    processed += 1
                    continue

                # Recalculate new_path with potentially updated author/title/narrator
                new_path = build_new_path(lib_path, new_author, new_title,
                                          series=new_series, series_num=new_series_num,
                                          narrator=new_narrator, year=new_year, config=config)

            # Only auto-fix if enabled AND NOT a drastic change
            # Drastic changes ALWAYS require manual approval to prevent data loss
            if config.get('auto_fix', False) and not drastic_change:
                # Actually rename the folder
                try:
                    import shutil

                    if new_path.exists():
                        # Destination already exists - check if it has files
                        existing_files = list(new_path.iterdir())
                        if existing_files:
                            # DON'T MERGE - this is likely a different narrator version
                            # Mark as conflict and skip
                            logger.warning(f"CONFLICT: {new_path} already exists with files - skipping to preserve different versions")
                            c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status, error_message)
                                         VALUES (?, ?, ?, ?, ?, ?, ?, 'error', 'Destination exists - possible different narrator version')''',
                                     (row['book_id'], row['current_author'], row['current_title'],
                                      new_author, new_title, str(old_path), str(new_path)))
                            c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                                     ('conflict', 'Destination folder exists with files', row['book_id']))
                            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
                            processed += 1
                            continue
                        else:
                            # Destination is empty folder - safe to use it
                            shutil.move(str(old_path), str(new_path.parent / (new_path.name + "_temp")))
                            new_path.rmdir()
                            (new_path.parent / (new_path.name + "_temp")).rename(new_path)

                        # Clean up empty parent author folder
                        try:
                            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                                old_path.parent.rmdir()
                        except OSError:
                            pass  # Parent not empty, that's fine
                    else:
                        # Destination doesn't exist - simple rename
                        new_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(old_path), str(new_path))

                        # Clean up empty parent author folder
                        try:
                            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                                old_path.parent.rmdir()
                        except OSError:
                            pass  # Parent not empty, that's fine

                    logger.info(f"Fixed: {row['current_author']}/{row['current_title']} -> {new_author}/{new_title}")

                    # Clean up any stale pending entries for this book before recording fix
                    c.execute("DELETE FROM history WHERE book_id = ? AND status = 'pending_fix'", (row['book_id'],))

                    # Record in history
                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'fixed')''',
                             (row['book_id'], row['current_author'], row['current_title'],
                              new_author, new_title, str(old_path), str(new_path)))

                    # Update book record - handle case where another book already has this path
                    try:
                        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?
                                     WHERE id = ?''',
                                 (str(new_path), new_author, new_title, 'fixed', row['book_id']))
                    except sqlite3.IntegrityError:
                        # Path already exists (duplicate book merged) - delete this book record
                        logger.info(f"Merged duplicate: {row['path']} -> existing {new_path}")
                        c.execute('DELETE FROM books WHERE id = ?', (row['book_id'],))

                    fixed += 1
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error fixing {row['path']}: {error_msg}")
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('error', error_msg, row['book_id']))
            else:
                # Drastic change or auto_fix disabled - record as pending for manual review
                logger.info(f"PENDING APPROVAL: {row['current_author']} -> {new_author} (drastic={drastic_change})")
                c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status)
                             VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix')''',
                         (row['book_id'], row['current_author'], row['current_title'],
                          new_author, new_title, str(old_path), str(new_path)))
                c.execute('UPDATE books SET status = ? WHERE id = ?', ('pending_fix', row['book_id']))
                fixed += 1
        else:
            # No fix needed
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
            logger.info(f"Verified OK: {row['current_author']}/{row['current_title']}")

        # Remove from queue
        c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
        processed += 1

    # Update stats (INSERT if not exists first)
    c.execute('INSERT OR IGNORE INTO stats (date) VALUES (?)', (today,))
    c.execute('UPDATE stats SET fixed = COALESCE(fixed, 0) + ? WHERE date = ?', (fixed, today))

    conn.commit()
    conn.close()

    logger.info(f"[DEBUG] Batch complete: {processed} processed, {fixed} fixed")
    return processed, fixed

def apply_fix(history_id):
    """Apply a pending fix from history."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT * FROM history WHERE id = ?', (history_id,))
    fix = c.fetchone()

    if not fix:
        conn.close()
        return False, "Fix not found"

    old_path = Path(fix['old_path'])
    new_path = Path(fix['new_path'])

    if not old_path.exists():
        error_msg = f"Source folder no longer exists: {old_path}"
        c.execute('UPDATE history SET status = ?, error_message = ? WHERE id = ?',
                 ('error', error_msg, history_id))
        conn.commit()
        conn.close()
        return False, error_msg

    try:
        import shutil

        if new_path.exists():
            # Destination already exists - check if it has files
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
            # Simple rename
            new_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(new_path))

        # Clean up empty parent
        try:
            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                old_path.parent.rmdir()
        except OSError:
            pass

        # Update book record
        c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?
                     WHERE id = ?''',
                 (str(new_path), fix['new_author'], fix['new_title'], 'fixed', fix['book_id']))

        # Update history status
        c.execute('UPDATE history SET status = ? WHERE id = ?', ('fixed', history_id))

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

worker_thread = None
worker_running = False
processing_status = {"active": False, "processed": 0, "total": 0, "current": "", "errors": []}

def process_all_queue(config):
    """Process ALL items in the queue in batches, respecting rate limits."""
    global processing_status

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as count FROM queue')
    total = c.fetchone()['count']
    conn.close()

    if total == 0:
        logger.info("Queue is empty, nothing to process")
        return 0, 0  # (total_processed, total_fixed)

    # Calculate delay between batches based on rate limit
    max_per_hour = config.get('max_requests_per_hour', 30)
    # Spread requests across the hour: 3600 seconds / max_requests
    min_delay = max(2, 3600 // max_per_hour)  # At least 2 seconds
    logger.info(f"Rate limit: {max_per_hour}/hour, delay between batches: {min_delay}s")

    processing_status = {"active": True, "processed": 0, "total": total, "current": "", "errors": []}
    logger.info(f"=== STARTING PROCESS ALL: {total} items in queue ===")

    total_processed = 0
    total_fixed = 0
    batch_num = 0
    rate_limit_hits = 0

    while True:
        # Reload config each batch so settings changes take effect immediately
        config = load_config()

        # Check rate limit before processing
        allowed, calls_made, max_calls = check_rate_limit(config)
        if not allowed:
            rate_limit_hits += 1
            wait_time = min(300 * rate_limit_hits, 1800)  # 5 min, 10 min, 15 min... max 30 min
            logger.info(f"Rate limit reached ({calls_made}/{max_calls}), waiting {wait_time//60} minutes... (hit #{rate_limit_hits})")
            processing_status["current"] = f"Rate limited, waiting {wait_time//60}min... ({calls_made}/{max_calls})"
            time.sleep(wait_time)
            continue

        batch_num += 1
        logger.info(f"--- Processing batch {batch_num} (API: {calls_made}/{max_calls}) ---")

        processed, fixed = process_queue(config)

        if processed == 0:
            # Check if queue is actually empty or if there was an error
            conn = get_db()
            c = conn.cursor()
            c.execute('SELECT COUNT(*) as count FROM queue')
            remaining = c.fetchone()['count']
            conn.close()

            if remaining == 0:
                logger.info("Queue is now empty")
                break
            else:
                # Could be rate limit or API error
                logger.warning(f"No items processed but {remaining} remain")
                processing_status["errors"].append(f"Batch {batch_num}: No items processed, {remaining} remain")
                # Wait and retry once
                time.sleep(10)
                continue

        total_processed += processed
        total_fixed += fixed
        processing_status["processed"] = total_processed
        processing_status["current"] = f"Batch {batch_num}: {processed} processed"
        logger.info(f"Batch {batch_num} complete: {processed} processed, {fixed} fixed, {total_processed}/{total} total")

        # Rate limiting delay between batches
        logger.debug(f"Waiting {min_delay}s before next batch...")
        time.sleep(min_delay)

    processing_status["active"] = False
    logger.info(f"=== PROCESS ALL COMPLETE: {total_processed} processed, {total_fixed} fixed ===")
    return total_processed, total_fixed

def background_worker():
    """Background worker that periodically scans and processes."""
    global worker_running

    logger.info("Background worker thread started")

    while worker_running:
        config = load_config()

        if config.get('enabled', True):
            try:
                logger.debug("Worker: Starting scan cycle")
                # Scan library
                scan_library(config)

                # Process queue if auto_fix is enabled
                if config.get('auto_fix', False):
                    logger.debug("Worker: Auto-fix enabled, processing queue")
                    process_all_queue(config)
            except Exception as e:
                logger.error(f"Worker error: {e}", exc_info=True)

        # Sleep for scan interval
        interval = config.get('scan_interval_hours', 6) * 3600
        logger.debug(f"Worker: Sleeping for {interval} seconds")
        for _ in range(int(interval / 10)):
            if not worker_running:
                break
            time.sleep(10)

    logger.info("Background worker thread stopped")

def start_worker():
    """Start the background worker."""
    global worker_thread, worker_running

    if worker_thread and worker_thread.is_alive():
        logger.info("Worker already running")
        return

    worker_running = True
    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()
    logger.info("Background worker started")

def stop_worker():
    """Stop the background worker."""
    global worker_running
    worker_running = False
    logger.info("Background worker stop requested")

def is_worker_running():
    """Check if worker is actually running."""
    global worker_thread, worker_running
    return worker_running and worker_thread is not None and worker_thread.is_alive()

@app.context_processor
def inject_worker_status():
    """Inject worker_running into all templates automatically."""
    return {'worker_running': is_worker_running()}

# ============== ROUTES ==============

@app.route('/')
def dashboard():
    """Main dashboard."""
    conn = get_db()
    c = conn.cursor()

    # Get counts
    c.execute('SELECT COUNT(*) as count FROM books')
    total_books = c.fetchone()['count']

    c.execute('SELECT COUNT(*) as count FROM queue')
    queue_size = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'fixed'")
    fixed_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM books WHERE status = 'verified'")
    verified_count = c.fetchone()['count']

    c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
    pending_fixes = c.fetchone()['count']

    # Get recent history
    c.execute('''SELECT h.*, b.path FROM history h
                 JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 10''')
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
                          worker_running=worker_running)

@app.route('/orphans')
def orphans_page():
    """Orphan files management page."""
    return render_template('orphans.html')

@app.route('/queue')
def queue_page():
    """Queue management page."""
    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT q.id, q.reason, q.added_at,
                        b.id as book_id, b.path, b.current_author, b.current_title
                 FROM queue q
                 JOIN books b ON q.book_id = b.id
                 ORDER BY q.priority, q.added_at''')
    queue_items = c.fetchall()

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

    # Build query based on status filter
    if status_filter == 'pending':
        c.execute("SELECT COUNT(*) as count FROM history WHERE status = 'pending_fix'")
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     WHERE status = 'pending_fix'
                     ORDER BY fixed_at DESC
                     LIMIT ? OFFSET ?''', (per_page, offset))
    else:
        c.execute('SELECT COUNT(*) as count FROM history')
        total = c.fetchone()['count']
        c.execute('''SELECT * FROM history
                     ORDER BY fixed_at DESC
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
                          status_filter=status_filter)

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
        config['scan_interval_hours'] = int(request.form.get('scan_interval_hours', 6))
        config['batch_size'] = int(request.form.get('batch_size', 3))
        config['max_requests_per_hour'] = int(request.form.get('max_requests_per_hour', 30))
        config['auto_fix'] = 'auto_fix' in request.form
        config['protect_author_changes'] = 'protect_author_changes' in request.form
        config['enabled'] = 'enabled' in request.form
        config['series_grouping'] = 'series_grouping' in request.form
        config['google_books_api_key'] = request.form.get('google_books_api_key', '').strip() or None
        config['update_channel'] = request.form.get('update_channel', 'stable')
        config['naming_format'] = request.form.get('naming_format', 'author/title')

        # Save config (without secrets)
        save_config(config)

        # Save secrets separately
        secrets = {
            'openrouter_api_key': request.form.get('openrouter_api_key', ''),
            'gemini_api_key': request.form.get('gemini_api_key', '')
        }
        save_secrets(secrets)

        return redirect(url_for('settings_page'))

    config = load_config()
    return render_template('settings.html', config=config, version=APP_VERSION)

# ============== API ENDPOINTS ==============

@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Trigger a library scan."""
    config = load_config()
    scanned, queued = scan_library(config)
    return jsonify({'success': True, 'scanned': scanned, 'queued': queued})

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

@app.route('/api/process', methods=['POST'])
def api_process():
    """Process the queue."""
    config = load_config()
    data = request.json if request.is_json else {}
    process_all = data.get('all', False)
    limit = data.get('limit')

    logger.info(f"API process called: all={process_all}, limit={limit}")

    if process_all:
        # Process entire queue in batches
        processed, fixed = process_all_queue(config)
    else:
        processed, fixed = process_queue(config, limit)

    return jsonify({'success': True, 'processed': processed, 'fixed': fixed})

@app.route('/api/process_status')
def api_process_status():
    """Get current processing status."""
    return jsonify(processing_status)

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
    """Undo a fix - rename folder back to original name."""
    import shutil

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
        # Rename back to original
        shutil.move(new_path, old_path)
        logger.info(f"Undo: Renamed '{new_path}' back to '{old_path}'")

        # Update history record
        c.execute('''UPDATE history SET status = 'undone', error_message = 'Manually undone by user'
                     WHERE id = ?''', (history_id,))

        # Update book record back to original - use 'protected' status so deep rescan won't re-queue
        c.execute('''UPDATE books SET
                     current_author = ?, current_title = ?, path = ?, status = 'protected'
                     WHERE id = ?''',
                  (record['old_author'], record['old_title'], old_path, record['book_id']))

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': f"Undone! Renamed back to: {record['old_author']} / {record['old_title']}"
        })

    except Exception as e:
        conn.close()
        logger.error(f"Undo failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stats')
def api_stats():
    """Get current stats."""
    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT COUNT(*) as count FROM books')
    total = c.fetchone()['count']

    c.execute('SELECT COUNT(*) as count FROM queue')
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
        'processing': processing_status
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
        log_file = BASE_DIR / 'app.log'
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


@app.route('/api/reset_database', methods=['POST'])
def api_reset_database():
    """Reset entire database - DANGER!"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('DELETE FROM queue')
        c.execute('DELETE FROM history')
        c.execute('DELETE FROM books')
        c.execute('DELETE FROM stats')
        conn.commit()
        conn.close()
        logger.warning("DATABASE RESET by user!")
        return jsonify({'success': True, 'message': 'Database reset complete'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/recent_history')
def api_recent_history():
    """Get recent history items for live updates."""
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT h.*, b.path FROM history h
                 JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC LIMIT 15''')
    items = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({'items': items})


@app.route('/api/orphans')
def api_orphans():
    """Find orphan audio files (files sitting directly in author folders)."""
    config = load_config()
    orphans = []

    for lib_path in config.get('library_paths', []):
        lib_orphans = find_orphan_audio_files(lib_path)
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
        orphans = find_orphan_audio_files(lib_path)

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

    return jsonify({
        'success': True,
        'organized': results['organized'],
        'errors': results['errors'],
        'details': results['details'][:20]  # Limit details
    })


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

@app.route('/api/bug_report')
def api_bug_report():
    """Generate a bug report with system info and sanitized config."""
    import platform
    import sys

    # Get config (sanitize API keys)
    config = load_config()
    safe_config = {k: v for k, v in config.items()}
    if safe_config.get('openrouter_api_key'):
        safe_config['openrouter_api_key'] = '***REDACTED***'
    if safe_config.get('gemini_api_key'):
        safe_config['gemini_api_key'] = '***REDACTED***'

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

    # Get recent error/warning logs
    log_file = BASE_DIR / 'app.log'
    recent_errors = []
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()[-200:]
            recent_errors = [l.strip() for l in lines if 'ERROR' in l or 'WARNING' in l][-30:]

    # Build report
    report = f"""## Bug Report - Library Manager

### System Info
- **Python:** {sys.version}
- **Platform:** {platform.system()} {platform.release()}
- **App Version:** {APP_VERSION}

### Configuration
```json
{json.dumps(safe_config, indent=2)}
```

### Database Stats
- Total Books: {total_books}
- Queue Size: {queue_size}
- History Entries: {history_count}
- Books with Errors: {error_count}

### Recent Errors/Warnings
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


# ============== MAIN ==============

if __name__ == '__main__':
    init_config()  # Create config files if they don't exist
    init_db()
    start_worker()
    app.run(host='0.0.0.0', port=5060, debug=False)
