"""Database operations for Library Manager."""
import sqlite3
import logging
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


__all__ = ['init_db', 'get_db', 'set_db_path', 'cleanup_garbage_entries',
           'cleanup_duplicate_history_entries', 'insert_history_entry']
