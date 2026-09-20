"""Explicitly started, disabled-by-default corrections polling service.

All network work runs outside database transactions. A persistent lease guards
against overlapping processes, and each validated page is durable before the
application callback runs. Callback failure therefore cannot lose corrections.
"""

from datetime import datetime
import threading
import time
import uuid

from library_manager.providers import bookdb

from . import store
from .feed import FeedFailure, fetch_page, source_namespace, validate_since
from .schema import validate_event, validate_page


POLL_INTERVAL = 24 * 60 * 60
RETRY_INTERVAL = 5 * 60
PAGE_CONTINUATION_INTERVAL = 5
LEASE_SECONDS = 120
MAX_PAGES_PER_POLL = 3


class CorrectionsService:
    def __init__(self, get_db, load_config, load_secrets, on_ingested=None, *,
                 fetcher=None, clock=None):
        self.get_db = get_db
        self.load_config = load_config
        self.load_secrets = load_secrets
        self.on_ingested = on_ingested
        self.fetcher = fetcher or fetch_page
        self.clock = clock or time.time
        self._owner = uuid.uuid4().hex
        self._thread = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._force = threading.Event()
        self._poll_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()

    def initialize(self):
        conn = self.get_db()
        try:
            store.initialize_store(conn)
        finally:
            conn.close()

    def _settings(self):
        config = self.load_config()
        enabled = config.get('receive_corrections', False) is True
        source = source_namespace(bookdb.get_bookdb_url(config.get('bookdb_url')))
        return enabled, source

    def _still_enabled(self, source):
        enabled, current = self._settings()
        return enabled and current == source and not self._stop.is_set()

    def summary(self):
        try:
            enabled, source = self._settings()
        except FeedFailure:
            return {'enabled': False, 'status': 'configuration_invalid', 'source': None,
                    'last_error': 'source_invalid', 'bootstrap_required': False,
                    'running': False, 'polling': False, 'cursor_available': False,
                    'has_more': False, 'next_poll_at': 0}
        conn = self.get_db()
        try:
            state = store.checkpoint(conn, source)
        finally:
            conn.close()
        polling = self._poll_lock.locked() or bool(state.lease_owner and state.lease_expires > self.clock())
        status = 'ready'
        if not enabled:
            status = 'disabled'
        elif state.bootstrap_required:
            status = 'reset_required'
        elif bookdb.get_terminal_server_denial():
            status = 'authentication_terminal'
        elif polling:
            status = 'polling'
        elif state.last_error and state.next_poll_at > self.clock():
            status = 'backoff'
        return {'enabled': enabled, 'source': source, 'status': status,
                'cursor_available': state.cursor is not None, 'has_more': state.has_more,
                'bootstrap_required': state.bootstrap_required, 'next_poll_at': state.next_poll_at,
                'last_error': state.last_error, 'generation': state.generation,
                'running': bool(self._thread and self._thread.is_alive()), 'polling': polling}

    def poll_once(self, force=False):
        """One bounded polling batch. Manual force never bypasses retry delays."""
        if not self._poll_lock.acquire(blocking=False):
            return {'status': 'busy', 'pages': 0, 'events': 0}
        conn = None
        source = None
        state = None
        pages = events = 0
        try:
            enabled, source = self._settings()
            if not enabled or self._stop.is_set():
                return {'status': 'disabled', 'pages': 0, 'events': 0}
            if bookdb.get_terminal_server_denial():
                return {'status': 'authentication_terminal', 'pages': 0, 'events': 0}
            conn = self.get_db()
            state = store.acquire_poll(conn, source, self._owner, now=self.clock(),
                                       force=force, lease_seconds=LEASE_SECONDS)
            if state is None:
                return {'status': 'deferred', 'pages': 0, 'events': 0}
            # Reconcile a previous process's committed-but-unhandled page first.
            if self.on_ingested and self._still_enabled(source):
                self.on_ingested(source)
            for _ in range(MAX_PAGES_PER_POLL):
                if not self._still_enabled(source):
                    return {'status': 'configuration_changed', 'pages': pages, 'events': events}
                secrets = self.load_secrets()
                page = self.fetcher(
                    source, api_key=secrets.get('bookdb_api_key'),
                    cursor=state.cursor, since=state.since if state.cursor is None else None,
                    limit=100, now=self.clock(),
                )
                validate_page(page)
                if not self._still_enabled(source):
                    return {'status': 'configuration_changed', 'pages': pages, 'events': events}
                now = self.clock()
                delay = PAGE_CONTINUATION_INTERVAL if page.has_more else POLL_INTERVAL
                state = store.ingest_page(conn, state, page, validator=validate_event,
                                          now=now, next_poll_at=now + delay, lease_seconds=LEASE_SECONDS)
                pages += 1
                events += len(page.corrections)
                if self.on_ingested:
                    self.on_ingested(source)
                if not page.has_more:
                    break
            return {'status': 'updated', 'pages': pages, 'events': events,
                    'has_more': state.has_more}
        except FeedFailure as failure:
            if state is not None and conn is not None:
                delay = POLL_INTERVAL if failure.code in ('feature_unavailable', 'authentication_terminal') else RETRY_INTERVAL
                delay = max(delay, failure.retry_after or 0)
                try:
                    store.record_failure(conn, state, failure, next_poll_at=self.clock() + delay)
                except Exception:
                    # The last page checkpoint remains durable even if reporting
                    # an outage fails. No body or exception text is persisted.
                    pass
            return {'status': 'error', 'error': failure.code, 'pages': pages, 'events': events}
        except Exception:
            if state is not None and conn is not None:
                try:
                    store.record_failure(conn, state, FeedFailure('processing_unavailable'),
                                         next_poll_at=self.clock() + RETRY_INTERVAL)
                except Exception:
                    pass
            return {'status': 'error', 'error': 'processing_unavailable', 'pages': pages, 'events': events}
        finally:
            try:
                if conn is not None:
                    try:
                        if source is not None:
                            store.release_poll(conn, source, self._owner)
                    except Exception:
                        # A lost connection leaves only an expiring lease.
                        pass
                    finally:
                        conn.close()
            finally:
                self._poll_lock.release()

    def reset(self, since=None):
        """Explicit operator-reviewed reset; preserve events in older generations."""
        validate_since(since)
        if since is not None:
            parsed = datetime.fromisoformat(since.replace('Z', '+00:00'))
            if parsed.timestamp() > self.clock():
                raise FeedFailure('since_invalid')
        enabled, source = self._settings()
        if not enabled:
            return {'status': 'disabled'}
        conn = self.get_db()
        try:
            state = store.checkpoint(conn, source)
            if state.lease_owner and state.lease_expires > self.clock():
                return {'status': 'busy'}
            state = store.rebootstrap(conn, state, since=since)
        finally:
            conn.close()
        return {'status': 'reset', 'generation': state.generation}

    def start(self):
        """Explicit lifecycle hook, idempotent; starts no work when opted out."""
        with self._lifecycle_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._force.set()
            self._thread = threading.Thread(target=self._run, name='lm-corrections', daemon=True)
            self._thread.start()

    def request_poll(self):
        """Nonblocking UI action; the normal durable backoff/lease gates apply."""
        if not self._settings()[0]:
            return {'status': 'disabled'}
        self.start()
        self._force.set()
        self._wake.set()
        return {'status': 'scheduled'}

    def stop(self):
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)

    def _run(self):
        while not self._stop.is_set():
            force = self._force.is_set()
            self._force.clear()
            self.poll_once(force=force)
            self._wake.wait(timeout=30)
            self._wake.clear()
