"""Database operations for Library Manager."""
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_db_path = None


def set_db_path(path):
    """Set the database path. Called during app initialization."""
    global _db_path
    _db_path = path


def init_db(db_path=None):
    """Initialize SQLite database."""
    path = db_path or _db_path
    if not path:
        raise ValueError("Database path not set. Call set_db_path() first.")

    conn = sqlite3.connect(path, timeout=30)
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

    # Add user_locked column - when True, user has manually set metadata and it should not be changed
    try:
        c.execute('ALTER TABLE books ADD COLUMN user_locked INTEGER DEFAULT 0')
    except:
        pass  # Column already exists

    # Add source_type column - tracks where the book came from ('library' or 'watch_folder')
    # Used to handle watch folder items that failed to move and need special processing
    try:
        c.execute("ALTER TABLE books ADD COLUMN source_type TEXT DEFAULT 'library'")
    except:
        pass  # Column already exists

    # Add media_type column - tracks what formats exist ('audiobook', 'ebook', 'both')
    # Issue #53: Used for filtering library by format
    try:
        c.execute("ALTER TABLE books ADD COLUMN media_type TEXT DEFAULT 'audiobook'")
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

    # Add metadata columns for embedding (migration)
    metadata_columns = [
        'new_narrator TEXT',
        'new_series TEXT',
        'new_series_num TEXT',
        'new_year TEXT',
        'new_edition TEXT',
        'new_variant TEXT',
        'embed_status TEXT',
        'embed_error TEXT'
    ]
    for col_def in metadata_columns:
        try:
            c.execute(f'ALTER TABLE history ADD COLUMN {col_def}')
        except:
            pass  # Column already exists

    # Add profile columns for Book Profile system (migration)
    profile_columns = [
        ('books', 'profile TEXT'),          # Full JSON profile
        ('books', 'confidence INTEGER DEFAULT 0'),  # Overall confidence score
        ('books', 'verification_layer INTEGER DEFAULT 0')  # 0=pending, 1=API, 2=AI, 3=audio, 4=complete
    ]
    for table, col_def in profile_columns:
        try:
            c.execute(f'ALTER TABLE {table} ADD COLUMN {col_def}')
        except:
            pass  # Column already exists

    # Add folder_triage column - categorizes folder name quality (clean/messy/garbage)
    # Issue #110: Used to decide whether to trust path-derived hints
    try:
        c.execute("ALTER TABLE books ADD COLUMN folder_triage TEXT DEFAULT 'clean'")
    except:
        pass  # Column already exists

    # Issue #168: Retry tracking columns - prevent re-searching unresolved books every scan
    retry_columns = [
        ('attempt_count', 'INTEGER DEFAULT 0'),      # Full processing cycles completed
        ('last_attempted', 'TIMESTAMP'),              # When last processed (for backoff)
        ('max_layer_reached', 'INTEGER DEFAULT 0'),   # Highest layer ever reached (prevents resetting)
    ]
    for col_name, col_type in retry_columns:
        try:
            c.execute(f'ALTER TABLE books ADD COLUMN {col_name} {col_type}')
        except:
            pass  # Column already exists

    # Backfill: existing needs_attention books already went through at least one cycle
    try:
        c.execute('''UPDATE books SET attempt_count = 1
                     WHERE status = 'needs_attention' AND (attempt_count IS NULL OR attempt_count = 0)''')
    except:
        pass

    # Issue #110: File validation columns - track whether audio files are valid
    validation_columns = [
        ('validation_status', "TEXT"),           # NULL=not validated, 'valid', 'invalid', 'skipped'
        ('validation_reason', "TEXT"),           # Why it failed (e.g., 'too_short', 'corrupt', 'no_audio_stream')
    ]
    for col_name, col_type in validation_columns:
        try:
            c.execute(f'ALTER TABLE books ADD COLUMN {col_name} {col_type}')
        except:
            pass  # Column already exists

    # Issue #280: languages column - JSON array of ISO 639-1 codes
    # (primary first) for bilingual/multi-language books
    try:
        c.execute('ALTER TABLE books ADD COLUMN languages TEXT')
    except:
        pass  # Column already exists

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

    # Issue #208: Persistent watch-folder dedup
    # Was an in-memory set(), wiped on restart, which caused the watch worker
    # to re-submit the same failing file every cycle (ate ~48% of Skaldleita
    # traffic from a single LM instance before server-side cache absorbed it).
    c.execute('''CREATE TABLE IF NOT EXISTS watch_folder_processed (
        path TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        outcome TEXT,
        error_message TEXT
    )''')

    # Issue #281: Per-series language overrides ("language lock")
    # series_key is the normalized (lowercase, whitespace-stripped) series name
    # used for case-insensitive matching; series_name keeps the display form.
    c.execute('''CREATE TABLE IF NOT EXISTS series_language_overrides (
        id INTEGER PRIMARY KEY,
        series_name TEXT,
        series_key TEXT UNIQUE,
        language_code TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # Issue #289: indexes for hot query paths (dashboard counts, queue lookups,
    # layer pipeline fetches). books.path is already indexed via UNIQUE.
    c.execute('CREATE INDEX IF NOT EXISTS idx_history_status ON history(status)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_history_book_id ON history(book_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_queue_book_id ON queue(book_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_books_verification_layer ON books(verification_layer)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_books_status ON books(status)')

    conn.commit()
    conn.close()

    # Initialize hook tables (Issue #166)
    from library_manager.hooks import init_hook_tables
    init_hook_tables(path)

    # Initialize plugin metrics table (Issue #189)
    from library_manager.plugins import init_plugin_metrics_table
    init_plugin_metrics_table(path)


def watch_folder_is_processed(path, db_path=None):
    """Return True if the watch-folder path has already been handled.

    Issue #208: replaces the in-memory set. Survives restarts so the worker
    doesn't re-submit the same failing file every scan cycle.
    """
    p = db_path or _db_path
    if not p:
        return False
    conn = sqlite3.connect(p, timeout=30)
    try:
        c = conn.execute(
            'SELECT 1 FROM watch_folder_processed WHERE path = ? LIMIT 1',
            (path,)
        )
        return c.fetchone() is not None
    finally:
        conn.close()


def watch_folder_mark_processed(path, outcome, error_message=None, db_path=None, conn=None):
    """Record that a watch-folder path has been handled.

    outcome: 'moved' | 'move_failed' | 'unknown_author' | 'aborted_by_server'
    Issue #208.

    Issue #215: pass the watch worker's own connection as ``conn`` so the
    write rides the worker's transaction instead of opening a second
    connection that stalls for the full busy_timeout against either the
    scan worker's write lock or the worker's own uncommitted transaction.
    When ``conn`` is given we commit on it; the worker owns transaction
    lifetime for its item either way.
    """
    sql = '''INSERT OR REPLACE INTO watch_folder_processed
             (path, processed_at, outcome, error_message)
             VALUES (?, CURRENT_TIMESTAMP, ?, ?)'''
    if conn is not None:
        conn.execute(sql, (path, outcome, error_message))
        conn.commit()
        return
    p = db_path or _db_path
    if not p:
        return
    own_conn = sqlite3.connect(p, timeout=30)
    try:
        own_conn.execute(sql, (path, outcome, error_message))
        own_conn.commit()
    finally:
        own_conn.close()


def cleanup_garbage_entries(db_path=None):
    """Remove garbage entries from database on startup.

    This catches entries that were scanned before filtering was added,
    like Synology @eaDir folders, macOS .AppleDouble, etc.
    """
    path = db_path or _db_path
    if not path:
        return 0

    # System folder patterns that should never be authors or titles
    garbage_patterns = {
        # Synology
        '@eadir', '#recycle', '@syno', '@tmp',
        # macOS
        '.appledouble', '__macosx', '.ds_store', '.spotlight', '.fseventsd', '.trashes',
        # Windows
        '$recycle.bin', 'system volume information', 'thumbs.db',
        # Linux/General
        '.trash', '.cache', '.metadata', '.thumbnails',
        # Common system folders
        'metadata', 'tmp', 'temp', 'cache', 'config', 'data', 'logs', 'log',
        'backup', 'backups', '.streams', 'streams'
    }

    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Find garbage entries
    c.execute('SELECT id, path, current_author, current_title FROM books')
    rows = c.fetchall()

    garbage_ids = []
    for row in rows:
        author = (row['current_author'] or '').lower().strip()
        title = (row['current_title'] or '').lower().strip()

        is_garbage = False
        reason = None

        # Check exact matches
        if author in garbage_patterns:
            is_garbage = True
            reason = f"garbage author: {author}"
        elif title in garbage_patterns:
            is_garbage = True
            reason = f"garbage title: {title}"
        # Check prefix patterns (folders starting with @ # or .)
        elif author.startswith('@') or author.startswith('#'):
            is_garbage = True
            reason = f"system prefix author: {author}"
        elif title.startswith('@') or title.startswith('#'):
            is_garbage = True
            reason = f"system prefix title: {title}"
        # Check for hidden folders as author (but allow titles starting with . for edge cases)
        elif author.startswith('.') and len(author) > 1:
            is_garbage = True
            reason = f"hidden folder author: {author}"

        if is_garbage:
            garbage_ids.append(row['id'])
            logger.info(f"[CLEANUP] Removing garbage entry: {reason} - {row['path']}")

    # Delete garbage entries
    if garbage_ids:
        placeholders = ','.join('?' * len(garbage_ids))
        c.execute(f'DELETE FROM queue WHERE book_id IN ({placeholders})', garbage_ids)
        c.execute(f'DELETE FROM history WHERE book_id IN ({placeholders})', garbage_ids)
        c.execute(f'DELETE FROM books WHERE id IN ({placeholders})', garbage_ids)
        conn.commit()
        logger.info(f"[CLEANUP] Removed {len(garbage_ids)} garbage entries from database")

    conn.close()
    return len(garbage_ids)


def get_db(db_path=None):
    """Get database connection with timeout to avoid lock issues."""
    path = db_path or _db_path
    if not path:
        raise ValueError("Database path not set. Call set_db_path() first.")

    conn = sqlite3.connect(path, timeout=30)  # Wait up to 30 seconds for lock
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')  # Better concurrent access
    conn.execute('PRAGMA busy_timeout=30000')  # 30s SQLite-level busy wait
    return conn


def cleanup_duplicate_history_entries(db_path=None):
    """Remove duplicate history entries on startup (Issue #79).

    Duplicates occur when:
    1. A book is processed multiple times through different layers
    2. Rescans create new entries without cleaning up old ones
    3. Race conditions between scan, watch folder, and requeue processing

    This keeps only the most recent entry per book_id + status combination.
    """
    path = db_path or _db_path
    if not path:
        return 0

    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Find duplicates: same book_id + status but different IDs
    # Keep only the most recent (highest ID or most recent fixed_at)
    c.execute('''
        SELECT book_id, status, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
        FROM history
        GROUP BY book_id, status
        HAVING cnt > 1
    ''')
    duplicates = c.fetchall()

    removed = 0
    for dup in duplicates:
        ids = [int(x) for x in dup['ids'].split(',')]
        # Keep the highest ID (most recent), delete the rest
        keep_id = max(ids)
        delete_ids = [i for i in ids if i != keep_id]

        if delete_ids:
            placeholders = ','.join('?' * len(delete_ids))
            c.execute(f'DELETE FROM history WHERE id IN ({placeholders})', delete_ids)
            removed += len(delete_ids)
            logger.info(f"[CLEANUP] Removed {len(delete_ids)} duplicate history entries for book_id={dup['book_id']} status={dup['status']}")

    if removed > 0:
        conn.commit()
        logger.info(f"[CLEANUP] Total: Removed {removed} duplicate history entries")

    conn.close()
    return removed


def insert_history_entry(cursor, book_id, old_author, old_title, new_author, new_title,
                         old_path, new_path, status, error_message=None,
                         new_narrator=None, new_series=None, new_series_num=None,
                         new_year=None, new_edition=None, new_variant=None):
    """Insert a history entry with deduplication (Issue #79).

    This function prevents duplicate history entries by:
    1. Deleting any existing entries for the same book_id + status
    2. Then inserting the new entry

    This is the ONLY function that should insert into the history table.
    All pipeline layers and app.py should use this instead of direct INSERT.

    Args:
        cursor: SQLite cursor (caller manages connection/commit)
        book_id: The book's ID
        old_author, old_title: Original values
        new_author, new_title: New proposed values
        old_path, new_path: Original and proposed paths
        status: One of 'pending_fix', 'fixed', 'needs_attention', 'error', 'duplicate', 'corrupt_dest'
        error_message: Optional error/reason message
        new_narrator, new_series, new_series_num: Optional metadata
        new_year, new_edition, new_variant: Optional metadata
    """
    # Delete any existing entry for this book_id + status combination
    # This prevents duplicates when a book is re-processed
    cursor.execute("DELETE FROM history WHERE book_id = ? AND status = ?", (book_id, status))

    # Insert the new entry
    cursor.execute('''INSERT INTO history (book_id, old_author, old_title, new_author, new_title,
                                           old_path, new_path, status, error_message,
                                           new_narrator, new_series, new_series_num,
                                           new_year, new_edition, new_variant)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (book_id, old_author, old_title, new_author, new_title,
                    old_path, new_path, status, error_message,
                    new_narrator, new_series, new_series_num,
                    new_year, new_edition, new_variant))


def should_requeue_book(book_row, max_retries=3):
    """Decide whether a book should be re-added to the processing queue.

    Issue #168: Prevents re-searching unresolved books every scan cycle.
    Called by deep_scan_library() before inserting into queue.

    Args:
        book_row: sqlite3.Row or dict with book columns (status, user_locked,
                  profile, attempt_count, last_attempted, max_layer_reached,
                  verification_layer)
        max_retries: Maximum processing attempts before giving up (0=unlimited)

    Returns:
        (should_queue: bool, reset_layer_to: int or None)
        reset_layer_to is the layer to set, or None to keep current layer
    """
    status = book_row.get('status', 'pending') if isinstance(book_row, dict) else book_row['status']
    user_locked = book_row.get('user_locked', 0) if isinstance(book_row, dict) else book_row['user_locked']
    profile = book_row.get('profile', None) if isinstance(book_row, dict) else book_row['profile']
    attempt_count = book_row.get('attempt_count', 0) if isinstance(book_row, dict) else book_row['attempt_count']
    last_attempted = book_row.get('last_attempted', None) if isinstance(book_row, dict) else book_row['last_attempted']
    max_layer = book_row.get('max_layer_reached', 0) if isinstance(book_row, dict) else book_row['max_layer_reached']

    # Coerce NULLs
    attempt_count = attempt_count or 0
    max_layer = max_layer or 0

    # Never requeue these statuses
    skip_statuses = {'user_locked', 'needs_attention', 'needs_split', 'series_folder', 'multi_book_files', 'validation_failed'}
    if status in skip_statuses:
        return (False, None)

    # Skip user-locked books
    if user_locked:
        return (False, None)

    # Skip verified/fixed books that have a real profile
    if status in ('verified', 'fixed'):
        has_profile = profile and len(str(profile)) > 2
        if has_profile:
            return (False, None)

    # Skip if exceeded max retries (0 = unlimited)
    if max_retries > 0 and attempt_count >= max_retries:
        logger.debug(f"Skipping book (attempt_count={attempt_count} >= max_retries={max_retries})")
        return (False, None)

    # Skip if within exponential backoff window: 24h * 2^attempt_count
    if attempt_count > 0 and last_attempted:
        try:
            if isinstance(last_attempted, str):
                last_attempted = datetime.fromisoformat(last_attempted)
            backoff_hours = 24 * (2 ** min(attempt_count, 5))  # Cap at 768h (~32 days)
            next_eligible = last_attempted + timedelta(hours=backoff_hours)
            if datetime.now() < next_eligible:
                logger.debug(f"Skipping book (backoff: next eligible {next_eligible})")
                return (False, None)
        except (ValueError, TypeError):
            pass  # Bad timestamp, allow requeue

    # Determine layer reset: don't reset if book already progressed past layer 1
    if max_layer > 1:
        # Resume from where it left off (or layer 1 if it's at 0)
        current_layer = book_row.get('verification_layer', 0) if isinstance(book_row, dict) else book_row['verification_layer']
        reset_layer = current_layer if current_layer and current_layer > 0 else 1
        return (True, reset_layer)
    else:
        return (True, 1)


def _normalize_series_key(series_name):
    """Normalize a series name for case-insensitive override matching."""
    if not series_name:
        return ''
    return ' '.join(str(series_name).lower().split())


def set_series_language_override(series_name, language_code, db_path=None):
    """Set (or update) the language lock for a series (Issue #281).

    Args:
        series_name: Display name of the series (matched case-insensitively)
        language_code: ISO 639-1 code from LANGUAGE_NAMES, or 'auto'/None to
            remove the override and fall back to normal detection
        db_path: Optional database path (defaults to the app database)

    Raises:
        ValueError: If the language code is not a known ISO 639-1 code
    """
    path = db_path or _db_path
    if not path:
        raise ValueError("Database path not set. Call set_db_path() first.")

    series_name = (series_name or '').strip()
    series_key = _normalize_series_key(series_name)
    if not series_key:
        raise ValueError("Series name is required")

    code = (language_code or '').lower().strip()
    if code in ('', 'auto'):
        delete_series_language_override(series_name, db_path=db_path)
        return

    from library_manager.utils.path_safety import LANGUAGE_NAMES
    if code not in LANGUAGE_NAMES:
        raise ValueError(f"Unknown language code: {language_code}")

    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute(
            '''INSERT INTO series_language_overrides (series_name, series_key, language_code)
               VALUES (?, ?, ?)
               ON CONFLICT(series_key) DO UPDATE SET
                   series_name = excluded.series_name,
                   language_code = excluded.language_code,
                   updated_at = CURRENT_TIMESTAMP''',
            (series_name, series_key, code)
        )
        conn.commit()
    finally:
        conn.close()


def get_series_language_override(series_name, db_path=None):
    """Return the locked language code for a series, or None if unset/auto."""
    path = db_path or _db_path
    if not path:
        return None
    series_key = _normalize_series_key(series_name)
    if not series_key:
        return None
    conn = sqlite3.connect(path, timeout=30)
    try:
        c = conn.execute(
            'SELECT language_code FROM series_language_overrides WHERE series_key = ? LIMIT 1',
            (series_key,)
        )
        row = c.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None  # Table may not exist yet on older databases
    finally:
        conn.close()


def get_all_series_language_overrides(db_path=None):
    """Return all series language overrides as a list of dicts."""
    path = db_path or _db_path
    if not path:
        return []
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.execute(
            'SELECT series_name, language_code, updated_at FROM series_language_overrides ORDER BY series_name'
        )
        return [dict(row) for row in c.fetchall()]
    except sqlite3.OperationalError:
        return []  # Table may not exist yet on older databases
    finally:
        conn.close()


def delete_series_language_override(series_name, db_path=None):
    """Remove the language override for a series. Returns True if one existed."""
    path = db_path or _db_path
    if not path:
        return False
    series_key = _normalize_series_key(series_name)
    if not series_key:
        return False
    conn = sqlite3.connect(path, timeout=30)
    try:
        cursor = conn.execute(
            'DELETE FROM series_language_overrides WHERE series_key = ?',
            (series_key,)
        )
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def _normalize_language_list(languages):
    """Normalize an ordered language list: lowercase, deduped, order preserved."""
    cleaned = []
    for code in languages or []:
        normalized = str(code).lower().strip() if code else ''
        if normalized and normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned


def set_book_languages(book_id, languages, db_path=None):
    """Set the full language list for a book (Issue #280).

    Args:
        book_id: books table row id
        languages: Ordered list of ISO 639-1 codes, primary first. An empty
            list clears the list (book falls back to single-language behavior).
        db_path: Optional database path (defaults to the app database)

    Raises:
        ValueError: If any code is not a known ISO 639-1 code
    """
    path = db_path or _db_path
    if not path:
        raise ValueError("Database path not set. Call set_db_path() first.")

    import json as _json
    from library_manager.utils.path_safety import LANGUAGE_NAMES

    cleaned = _normalize_language_list(languages)
    for code in cleaned:
        if code not in LANGUAGE_NAMES:
            raise ValueError(f"Unknown language code: {code}")

    languages_json = _json.dumps(cleaned) if cleaned else None

    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.execute(
            'UPDATE books SET languages = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
            (languages_json, book_id)
        )
        if c.rowcount == 0:
            raise ValueError(f"Book not found: {book_id}")
        # Keep the profile JSON in sync: primary language + full list
        row = conn.execute('SELECT profile FROM books WHERE id = ?', (book_id,)).fetchone()
        if row and row['profile']:
            try:
                profile = _json.loads(row['profile'])
            except ValueError:
                profile = None
            if isinstance(profile, dict):
                profile['languages'] = cleaned
                lang_fv = profile.get('language')
                if cleaned and isinstance(lang_fv, dict):
                    lang_fv['value'] = cleaned[0]
                conn.execute(
                    'UPDATE books SET profile = ? WHERE id = ?',
                    (_json.dumps(profile), book_id)
                )
        conn.commit()
    finally:
        conn.close()


def get_book_languages(book_id, db_path=None):
    """Return the ordered language list for a book (primary first).

    Falls back to the primary language stored in the profile JSON for rows
    without a languages column value. Returns [] when nothing is known.
    """
    path = db_path or _db_path
    if not path:
        return []
    import json as _json
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        try:
            row = conn.execute('SELECT languages, profile FROM books WHERE id = ?', (book_id,)).fetchone()
        except sqlite3.OperationalError:
            # Older schema without the languages column
            row = conn.execute('SELECT profile FROM books WHERE id = ?', (book_id,)).fetchone()
        if not row:
            return []
        languages = row['languages'] if 'languages' in row.keys() else None
        if languages:
            try:
                return _normalize_language_list(_json.loads(languages))
            except ValueError:
                return []
        # Fall back to the profile JSON
        if row['profile']:
            try:
                profile = _json.loads(row['profile'])
            except ValueError:
                return []
            if isinstance(profile, dict):
                listed = profile.get('languages')
                if listed:
                    return _normalize_language_list(listed)
                lang = profile.get('language')
                if isinstance(lang, dict):
                    lang = lang.get('value')
                if lang:
                    return [str(lang).lower().strip()]
        return []
    finally:
        conn.close()


def get_language_distribution(db_path=None):
    """Return a {language_code: count} map of detected book languages (Issue #284).

    Reads the profile JSON of every book, extracts the primary language from
    'detected_language' or 'language' (plain string or FieldValue dict),
    normalizes ISO 639-2 codes to 639-1, and skips empty/undetermined values.
    """
    path = db_path or _db_path
    if not path:
        return {}
    import json as _json
    from library_manager.utils.path_safety import ISO_639_2_TO_1

    conn = sqlite3.connect(path, timeout=30)
    try:
        rows = conn.execute(
            'SELECT profile FROM books WHERE profile IS NOT NULL'
        ).fetchall()
    except sqlite3.OperationalError:
        return {}  # Table may not exist yet on older databases
    finally:
        conn.close()

    distribution = {}
    for (profile_json,) in rows:
        try:
            profile = _json.loads(profile_json)
        except ValueError:
            continue
        if not isinstance(profile, dict):
            continue
        # The pipeline persists either 'detected_language' (plain string or
        # FieldValue dict) or 'language' (FieldValue) - accept both, matching
        # _extract_detected_language semantics.
        lang = profile.get('detected_language') or profile.get('language')
        if isinstance(lang, dict):
            lang = lang.get('value')
        if not lang:
            continue
        code = str(lang).lower().strip()
        code = ISO_639_2_TO_1.get(code, code)
        if not code or code == 'und':
            continue
        distribution[code] = distribution.get(code, 0) + 1
    return distribution


__all__ = ['init_db', 'get_db', 'set_db_path', 'cleanup_garbage_entries',
           'cleanup_duplicate_history_entries', 'insert_history_entry',
           'should_requeue_book',
           'set_series_language_override', 'get_series_language_override',
           'get_all_series_language_overrides', 'delete_series_language_override',
           'set_book_languages', 'get_book_languages',
           'get_language_distribution']
