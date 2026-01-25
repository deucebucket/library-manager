"""
P2P Book Cache using Gun.db

Provides decentralized, peer-to-peer caching for BookDB lookups.
When enabled, successful book lookups are cached both locally and shared
with other Library Manager instances via Gun.db relay servers.

This helps when BookDB is temporarily unavailable - users can still get
results from the distributed cache.

Privacy: This is OPT-IN only. Users who don't enable P2P caching will
only use local SQLite cache, and their lookups won't be shared.
"""

import os
import json
import sqlite3
import hashlib
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Try to import Gun.db client - it's optional
GUNDB_AVAILABLE = False
try:
    from pygundb.client import GunClient
    GUNDB_AVAILABLE = True
except ImportError:
    logger.debug("pygundb not installed - P2P cache disabled, using local cache only")

# Default public Gun relay servers
DEFAULT_RELAYS = [
    "wss://gun-manhattan.herokuapp.com/gun",
    "wss://gun-us.herokuapp.com/gun",
]

# Cache namespace to avoid collisions with other Gun.db apps
CACHE_NAMESPACE = "libman_bookdb_v1"


class BookCache:
    """
    Hybrid local + P2P book metadata cache.

    - Always uses local SQLite cache (fast, private)
    - Optionally syncs to P2P network via Gun.db (opt-in)
    - Falls back gracefully when P2P is unavailable
    """

    def __init__(self, data_dir: str, enable_p2p: bool = False, relay_urls: list = None):
        """
        Initialize the book cache.

        Args:
            data_dir: Directory to store local cache database
            enable_p2p: Whether to enable P2P sharing (opt-in)
            relay_urls: Custom Gun.db relay URLs (uses defaults if None)
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "bookdb_cache.db"
        self.enable_p2p = enable_p2p and GUNDB_AVAILABLE
        self.relay_urls = relay_urls or DEFAULT_RELAYS

        # Initialize local SQLite cache
        self._init_local_db()

        # Gun.db client (initialized lazily)
        self._gun_client = None
        self._gun_loop = None
        self._gun_thread = None

        # Stats
        self.stats = {
            'local_hits': 0,
            'local_misses': 0,
            'p2p_hits': 0,
            'p2p_misses': 0,
            'p2p_writes': 0,
        }

        logger.info(f"BookCache initialized: local={self.db_path}, p2p={self.enable_p2p}")

    def _init_local_db(self):
        """Initialize local SQLite cache database."""
        conn = sqlite3.connect(str(self.db_path))
        c = conn.cursor()

        # Book metadata cache
        c.execute('''
            CREATE TABLE IF NOT EXISTS book_cache (
                cache_key TEXT PRIMARY KEY,
                query_title TEXT,
                query_author TEXT,
                result_json TEXT NOT NULL,
                source TEXT DEFAULT 'bookdb',
                confidence REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 1
            )
        ''')

        # Audio identification cache (transcript -> result)
        c.execute('''
            CREATE TABLE IF NOT EXISTS audio_cache (
                transcript_hash TEXT PRIMARY KEY,
                transcript_preview TEXT,
                result_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 1
            )
        ''')

        # Index for cleanup queries
        c.execute('CREATE INDEX IF NOT EXISTS idx_book_cache_accessed ON book_cache(accessed_at)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_audio_cache_accessed ON audio_cache(accessed_at)')

        conn.commit()
        conn.close()

        logger.debug(f"Local cache DB initialized: {self.db_path}")

    def _make_cache_key(self, title: str, author: str = None) -> str:
        """Generate a consistent cache key from title and author."""
        # Normalize: lowercase, strip whitespace, remove punctuation
        normalized = (title or "").lower().strip()
        if author:
            normalized += "|" + (author or "").lower().strip()

        # Hash for consistent key length
        return hashlib.sha256(normalized.encode()).hexdigest()[:32]

    def _make_transcript_hash(self, transcript: str) -> str:
        """Generate a hash from transcript text."""
        # Use first 500 chars to handle slight variations
        normalized = (transcript or "")[:500].lower().strip()
        return hashlib.sha256(normalized.encode()).hexdigest()[:32]

    # ==================== LOCAL CACHE ====================

    def get_local(self, title: str, author: str = None) -> Optional[Dict[str, Any]]:
        """
        Get book metadata from local cache.

        Returns cached result dict or None if not found.
        """
        cache_key = self._make_cache_key(title, author)

        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            c.execute('''
                SELECT result_json, created_at FROM book_cache
                WHERE cache_key = ?
            ''', (cache_key,))

            row = c.fetchone()
            if row:
                # Update access stats
                c.execute('''
                    UPDATE book_cache
                    SET accessed_at = CURRENT_TIMESTAMP, access_count = access_count + 1
                    WHERE cache_key = ?
                ''', (cache_key,))
                conn.commit()

                self.stats['local_hits'] += 1
                result = json.loads(row[0])
                result['_cache_source'] = 'local'
                result['_cached_at'] = row[1]

                logger.debug(f"Local cache hit: {title}")
                conn.close()
                return result

            self.stats['local_misses'] += 1
            conn.close()
            return None

        except Exception as e:
            logger.warning(f"Local cache read error: {e}")
            return None

    def put_local(self, title: str, author: str, result: Dict[str, Any],
                  source: str = 'bookdb', confidence: float = 0.0):
        """
        Store book metadata in local cache.
        """
        cache_key = self._make_cache_key(title, author)

        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            c.execute('''
                INSERT OR REPLACE INTO book_cache
                (cache_key, query_title, query_author, result_json, source, confidence)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (cache_key, title, author, json.dumps(result), source, confidence))

            conn.commit()
            conn.close()

            logger.debug(f"Local cache write: {title} by {author}")

        except Exception as e:
            logger.warning(f"Local cache write error: {e}")

    def get_audio_local(self, transcript: str) -> Optional[Dict[str, Any]]:
        """Get audio identification result from local cache."""
        transcript_hash = self._make_transcript_hash(transcript)

        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            c.execute('''
                SELECT result_json FROM audio_cache WHERE transcript_hash = ?
            ''', (transcript_hash,))

            row = c.fetchone()
            if row:
                c.execute('''
                    UPDATE audio_cache
                    SET accessed_at = CURRENT_TIMESTAMP, access_count = access_count + 1
                    WHERE transcript_hash = ?
                ''', (transcript_hash,))
                conn.commit()
                conn.close()

                result = json.loads(row[0])
                result['_cache_source'] = 'local_audio'
                return result

            conn.close()
            return None

        except Exception as e:
            logger.warning(f"Audio cache read error: {e}")
            return None

    def put_audio_local(self, transcript: str, result: Dict[str, Any]):
        """Store audio identification result in local cache."""
        transcript_hash = self._make_transcript_hash(transcript)
        preview = (transcript or "")[:200]

        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            c.execute('''
                INSERT OR REPLACE INTO audio_cache
                (transcript_hash, transcript_preview, result_json)
                VALUES (?, ?, ?)
            ''', (transcript_hash, preview, json.dumps(result)))

            conn.commit()
            conn.close()

        except Exception as e:
            logger.warning(f"Audio cache write error: {e}")

    # ==================== P2P CACHE (Gun.db) ====================

    def _get_gun_client(self):
        """Get or create Gun.db client."""
        if not self.enable_p2p or not GUNDB_AVAILABLE:
            return None

        if self._gun_client is None:
            try:
                # Gun.db client needs its own event loop in a thread
                self._gun_client = GunClient()
                logger.info("Gun.db P2P client connected")
            except Exception as e:
                logger.warning(f"Failed to connect to Gun.db relay: {e}")
                self._gun_client = None

        return self._gun_client

    async def _get_p2p_async(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Async get from P2P network."""
        client = self._get_gun_client()
        if not client:
            return None

        try:
            # Gun.db path: libman_bookdb_v1/books/{cache_key}
            result = await client.get(f"{CACHE_NAMESPACE}/books/{cache_key}")
            if result:
                self.stats['p2p_hits'] += 1
                return json.loads(result) if isinstance(result, str) else result

            self.stats['p2p_misses'] += 1
            return None

        except Exception as e:
            logger.debug(f"P2P cache read error: {e}")
            return None

    async def _put_p2p_async(self, cache_key: str, data: Dict[str, Any]):
        """Async put to P2P network."""
        client = self._get_gun_client()
        if not client:
            return

        try:
            # Store in Gun.db
            await client.put(f"{CACHE_NAMESPACE}/books/{cache_key}", json.dumps(data))
            self.stats['p2p_writes'] += 1
            logger.debug(f"P2P cache write: {cache_key[:8]}...")

        except Exception as e:
            logger.debug(f"P2P cache write error: {e}")

    def get_p2p(self, title: str, author: str = None) -> Optional[Dict[str, Any]]:
        """
        Get book metadata from P2P network (synchronous wrapper).

        Returns cached result dict or None if not found.
        """
        if not self.enable_p2p:
            return None

        cache_key = self._make_cache_key(title, author)

        try:
            # Run async in thread-safe way
            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(self._get_p2p_async(cache_key))
            loop.close()

            if result:
                result['_cache_source'] = 'p2p'
                logger.info(f"P2P cache hit: {title}")

                # Also store locally for faster future access
                self.put_local(title, author, result, source='p2p_cache')

            return result

        except Exception as e:
            logger.debug(f"P2P get error: {e}")
            return None

    def put_p2p(self, title: str, author: str, result: Dict[str, Any]):
        """
        Store book metadata to P2P network (synchronous wrapper).

        Only shares if P2P is enabled (user opted in).
        """
        if not self.enable_p2p:
            return

        cache_key = self._make_cache_key(title, author)

        # Clean result for sharing (remove local-only fields)
        shareable = {k: v for k, v in result.items() if not k.startswith('_')}
        shareable['_shared_at'] = datetime.utcnow().isoformat()

        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self._put_p2p_async(cache_key, shareable))
            loop.close()

        except Exception as e:
            logger.debug(f"P2P put error: {e}")

    # ==================== UNIFIED INTERFACE ====================

    def get(self, title: str, author: str = None) -> Optional[Dict[str, Any]]:
        """
        Get book metadata from cache (local first, then P2P).

        Returns cached result or None if not found anywhere.
        """
        # Try local first (fastest)
        result = self.get_local(title, author)
        if result:
            return result

        # Try P2P if enabled
        if self.enable_p2p:
            result = self.get_p2p(title, author)
            if result:
                return result

        return None

    def put(self, title: str, author: str, result: Dict[str, Any],
            source: str = 'bookdb', confidence: float = 0.0):
        """
        Store book metadata to cache (local always, P2P if opted in).
        """
        # Always store locally
        self.put_local(title, author, result, source, confidence)

        # Share to P2P if enabled and result is good quality
        if self.enable_p2p and confidence >= 0.5:
            self.put_p2p(title, author, result)

    # ==================== MAINTENANCE ====================

    def cleanup(self, max_age_days: int = 90, max_entries: int = 50000):
        """
        Clean up old cache entries to manage disk space.

        Removes entries not accessed in max_age_days, and if still over
        max_entries, removes least recently accessed.
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            cutoff = datetime.now() - timedelta(days=max_age_days)

            # Delete old book cache entries
            c.execute('''
                DELETE FROM book_cache WHERE accessed_at < ?
            ''', (cutoff.isoformat(),))
            deleted_books = c.rowcount

            # Delete old audio cache entries
            c.execute('''
                DELETE FROM audio_cache WHERE accessed_at < ?
            ''', (cutoff.isoformat(),))
            deleted_audio = c.rowcount

            # If still too many, delete least recently accessed
            c.execute('SELECT COUNT(*) FROM book_cache')
            count = c.fetchone()[0]

            if count > max_entries:
                c.execute('''
                    DELETE FROM book_cache WHERE cache_key IN (
                        SELECT cache_key FROM book_cache
                        ORDER BY accessed_at ASC
                        LIMIT ?
                    )
                ''', (count - max_entries,))

            conn.commit()
            conn.close()

            logger.info(f"Cache cleanup: removed {deleted_books} books, {deleted_audio} audio entries")

        except Exception as e:
            logger.warning(f"Cache cleanup error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()

            c.execute('SELECT COUNT(*) FROM book_cache')
            book_count = c.fetchone()[0]

            c.execute('SELECT COUNT(*) FROM audio_cache')
            audio_count = c.fetchone()[0]

            conn.close()

            return {
                'book_entries': book_count,
                'audio_entries': audio_count,
                'p2p_enabled': self.enable_p2p,
                'gundb_available': GUNDB_AVAILABLE,
                **self.stats
            }

        except Exception as e:
            logger.warning(f"Stats error: {e}")
            return {'error': str(e)}


# Global cache instance (initialized when needed)
_cache_instance: Optional[BookCache] = None


def get_cache(data_dir: str = None, enable_p2p: bool = None) -> BookCache:
    """
    Get or create the global BookCache instance.

    Args:
        data_dir: Data directory (uses default if None)
        enable_p2p: Enable P2P sharing (reads from config if None)

    Returns:
        BookCache instance
    """
    global _cache_instance

    if _cache_instance is None:
        if data_dir is None:
            # Default to same location as other Library Manager data
            data_dir = os.environ.get('DATA_DIR', '/data')

        if enable_p2p is None:
            enable_p2p = os.environ.get('ENABLE_P2P_CACHE', 'false').lower() == 'true'

        _cache_instance = BookCache(data_dir, enable_p2p)

    return _cache_instance


def reset_cache():
    """Reset the global cache instance (for testing)."""
    global _cache_instance
    _cache_instance = None
