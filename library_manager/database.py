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


__all__ = ['init_db', 'get_db', 'set_db_path']
