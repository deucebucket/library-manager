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

import os
import sys
import json
import time
import sqlite3
import threading
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for

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
    "enabled": True
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
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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

# ============== AI API ==============

def build_prompt(messy_names):
    """Build the parsing prompt for AI."""
    items = []
    for i, name in enumerate(messy_names):
        items.append(f"ITEM_{i+1}: {name}")
    names_list = "\n".join(items)

    return f"""Parse these book filenames. Extract author and title.

{names_list}

RULES:
- Author names are people (e.g. "Adrian Tchaikovsky", "Dean Koontz", "Cormac McCarthy")
- Titles are book names (e.g. "Service Model", "The Funhouse", "Stella Maris")
- IMPORTANT: Keep series info in the title! "Book 2", "Book 6", "Part 1" etc MUST stay in the title
  - "Trailer Park Elves, Book 2" -> title should be "Trailer Park Elves, Book 2" NOT just "Trailer Park Elves"
  - "The Expanse 3" -> title should include the "3"
- Remove junk: [bitsearch.to], version numbers [r1.1], quality [64k], format suffixes (EPUB, MP3)
- "Author - Title" format: first part is usually author
- "Title by Author" format: author comes after "by"
- Years like 1999 go in year field, not author
- For "LastName, FirstName" format, author is "FirstName LastName"
- Keep ALL co-authors (e.g. "Michael Dalton, Adam Lance" stays as-is)

Return JSON array. Each object MUST have "item" matching the ITEM_N label:
[
  {{"item": "ITEM_1", "author": "Author Name", "title": "Book Title", "series": null, "series_num": null, "year": null}}
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
    """Call AI API to parse book names."""
    prompt = build_prompt(messy_names)
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


def call_gemini(prompt, config):
    """Call Google Gemini API directly."""
    try:
        api_key = config.get('gemini_api_key')
        model = config.get('gemini_model', 'gemini-1.5-flash')

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
        else:
            error_msg = explain_http_error(resp.status_code, "Gemini")
            logger.warning(f"Gemini: {error_msg}")
            # Try to get more detail from response
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

    # Year in author name
    if re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', author):
        issues.append("year_in_author")

    # Looks like a book title (common title words)
    title_words = ['the', 'of', 'and', 'a', 'in', 'to', 'for', 'book', 'series', 'volume']
    author_words = author.lower().split()
    if any(w in author_words for w in title_words):
        issues.append("title_words_in_author")

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

    # Update daily stats
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('''INSERT OR REPLACE INTO stats (date, scanned, queued)
                 VALUES (?, COALESCE((SELECT scanned FROM stats WHERE date = ?), 0) + ?,
                         COALESCE((SELECT queued FROM stats WHERE date = ?), 0) + ?)''',
              (today, today, scanned, today, queued))
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

    # Update API call stats
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute('''INSERT OR REPLACE INTO stats (date, api_calls)
                 VALUES (?, COALESCE((SELECT api_calls FROM stats WHERE date = ?), 0) + 1)''',
              (today, today))

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

        if not new_author or not new_title:
            # Remove from queue, mark as verified
            c.execute('DELETE FROM queue WHERE id = ?', (row['queue_id'],))
            c.execute('UPDATE books SET status = ? WHERE id = ?', ('verified', row['book_id']))
            processed += 1
            logger.info(f"Verified OK (empty result): {row['current_author']}/{row['current_title']}")
            continue

        # Check if fix needed
        if new_author != row['current_author'] or new_title != row['current_title']:
            old_path = Path(row['path'])
            lib_path = old_path.parent.parent
            new_author_dir = lib_path / new_author
            new_path = new_author_dir / new_title

            if config.get('auto_fix', False):
                # Actually rename the folder
                try:
                    if new_path.exists():
                        # Merge into existing
                        for item in old_path.iterdir():
                            dest = new_path / item.name
                            if not dest.exists():
                                item.rename(dest)
                        old_path.rmdir()
                        if not any(old_path.parent.iterdir()):
                            old_path.parent.rmdir()
                    else:
                        new_author_dir.mkdir(parents=True, exist_ok=True)
                        old_path.rename(new_path)
                        if not any(old_path.parent.iterdir()):
                            old_path.parent.rmdir()

                    logger.info(f"Fixed: {row['current_author']}/{row['current_title']} -> {new_author}/{new_title}")

                    # Record in history
                    c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path, status)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, 'fixed')''',
                             (row['book_id'], row['current_author'], row['current_title'],
                              new_author, new_title, str(old_path), str(new_path)))

                    # Update book record
                    c.execute('''UPDATE books SET path = ?, current_author = ?, current_title = ?, status = ?
                                 WHERE id = ?''',
                             (str(new_path), new_author, new_title, 'fixed', row['book_id']))

                    fixed += 1
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error fixing {row['path']}: {error_msg}")
                    c.execute('UPDATE books SET status = ?, error_message = ? WHERE id = ?',
                             ('error', error_msg, row['book_id']))
            else:
                # Just record the suggested fix
                c.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title, old_path, new_path)
                             VALUES (?, ?, ?, ?, ?, ?, ?)''',
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

    # Update stats
    c.execute('''UPDATE stats SET fixed = COALESCE(fixed, 0) + ? WHERE date = ?''',
              (fixed, today))

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
        if new_path.exists():
            # Merge
            for item in old_path.iterdir():
                dest = new_path / item.name
                if not dest.exists():
                    item.rename(dest)
            old_path.rmdir()
            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                old_path.parent.rmdir()
        else:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            old_path.rename(new_path)
            if old_path.parent.exists() and not any(old_path.parent.iterdir()):
                old_path.parent.rmdir()

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
    per_page = 50
    offset = (page - 1) * per_page

    c.execute('SELECT COUNT(*) as count FROM history')
    total = c.fetchone()['count']

    c.execute('''SELECT h.*, b.status FROM history h
                 JOIN books b ON h.book_id = b.id
                 ORDER BY h.fixed_at DESC
                 LIMIT ? OFFSET ?''', (per_page, offset))
    history_items = c.fetchall()

    conn.close()

    total_pages = (total + per_page - 1) // per_page

    return render_template('history.html',
                          history_items=history_items,
                          page=page,
                          total_pages=total_pages,
                          total=total)

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
        config['enabled'] = 'enabled' in request.form

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
    return render_template('settings.html', config=config)

# ============== API ENDPOINTS ==============

@app.route('/api/scan', methods=['POST'])
def api_scan():
    """Trigger a library scan."""
    config = load_config()
    scanned, queued = scan_library(config)
    return jsonify({'success': True, 'scanned': scanned, 'queued': queued})

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
- **App Version:** 1.0.0

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
