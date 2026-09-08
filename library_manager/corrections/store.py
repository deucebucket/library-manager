"""Source-scoped event ledger and transactional cursor checkpoints.

No book metadata or files are changed here. The caller must supply the approved
backend event-schema validator before any page, including an empty page, can be
accepted. Checkpoint versions prevent stale pollers from advancing progress.
"""

import hashlib
import json
import math

from .feed import (
    CorrectionPage, FeedCheckpoint, FeedFailure, ValidatedCorrection,
    source_namespace, validate_cursor, validate_since,
)


SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS correction_feed_state (
        source TEXT PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0,
        cursor TEXT, bootstrap_since TEXT, has_more INTEGER NOT NULL DEFAULT 0,
        bootstrap_required INTEGER NOT NULL DEFAULT 0,
        next_poll_at REAL NOT NULL DEFAULT 0, last_error TEXT,
        generation INTEGER NOT NULL DEFAULT 0, lease_owner TEXT,
        lease_expires REAL NOT NULL DEFAULT 0
    )''',
    '''CREATE TABLE IF NOT EXISTS correction_feed_events (
        source TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
        event_id TEXT NOT NULL, payload TEXT NOT NULL,
        reconciliation_key TEXT NOT NULL, received_at REAL NOT NULL,
        PRIMARY KEY (source, generation, event_id)
    )''',
)


def _idle_connection(conn):
    if conn.in_transaction:
        raise FeedFailure('transaction_already_active')


def initialize_store(conn):
    """Explicit additive initialization; never commit a caller's transaction."""
    _idle_connection(conn)
    with conn:
        conn.execute('BEGIN IMMEDIATE')
        for statement in SCHEMA:
            conn.execute(statement)


def checkpoint(conn, endpoint):
    source = source_namespace(endpoint)
    row = conn.execute(
        '''SELECT version,cursor,bootstrap_since,has_more,bootstrap_required,
                  next_poll_at,last_error,generation,lease_owner,lease_expires
           FROM correction_feed_state WHERE source=?''',
        (source,),
    ).fetchone()
    if row is None:
        return FeedCheckpoint(source)
    return FeedCheckpoint(source, row[0], row[1], row[2], bool(row[3]), bool(row[4]), row[5], row[6],
                          row[7], row[8], row[9])


def _claim(conn, expected):
    _idle_connection(conn)
    conn.execute('BEGIN IMMEDIATE')
    current = checkpoint(conn, expected.source)
    if current.version != expected.version:
        raise FeedFailure('checkpoint_conflict')
    conn.execute('INSERT OR IGNORE INTO correction_feed_state (source) VALUES (?)',
                 (current.source,))
    return current


def _timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise FeedFailure('timestamp_invalid')
    return value


def reconciliation_key(payload):
    """Semantic reset reconciliation hint; never suppress same-feed events.

    Application decisions compare this across generations only. IDs and dates
    are deliberately absent because a replaced feed can reuse them.
    """
    targets = ('affected_skaldleita_book_ids', 'affected_sl_ids', 'affected_audio_hashes')
    semantic = {field: sorted(set(payload.get(field, []))) for field in targets}
    semantic.update({field: payload.get(field, {}) for field in ('original_result', 'corrected_result')})
    # Generic validator fixtures need distinct keys too; production always has
    # the complete canonical fields checked by schema.validate_event.
    if not any(field in payload for field in targets):
        semantic = payload
    encoded = json.dumps(semantic, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def ingest_page(conn, expected, page, *, validator, now, next_poll_at, lease_seconds=120):
    """Commit validated events and page checkpoint together, or neither.

    ``validator`` must reject an unsupported canonical event schema. The service
    supplies schema.validate_event; this storage boundary has no permissive default.
    """
    if not callable(validator):
        raise FeedFailure('event_validator_required')
    if not isinstance(page, CorrectionPage) or type(page.has_more) is not bool:
        raise FeedFailure('page_invalid')
    validate_cursor(page.next_cursor)
    _timestamp(now)
    _timestamp(next_poll_at)
    if page.has_more and page.next_cursor == expected.cursor:
        raise FeedFailure('cursor_not_advancing')
    events = []
    for raw in page.corrections:
        event = validator(raw)
        if (not isinstance(event, ValidatedCorrection) or not isinstance(event.event_id, str)
                or not event.event_id or len(event.event_id) > 200
                or any(ord(char) < 32 for char in event.event_id)
                or not isinstance(event.payload, dict)):
            raise FeedFailure('event_invalid')
        try:
            payload = json.dumps(event.payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
        except (TypeError, ValueError):
            raise FeedFailure('event_invalid') from None
        events.append((event.event_id, payload, reconciliation_key(event.payload)))
    _idle_connection(conn)
    with conn:
        current = _claim(conn, expected)
        if current.bootstrap_required:
            raise FeedFailure('bootstrap_required')
        for event_id, payload, semantic_key in events:
            existing = conn.execute(
                'SELECT payload FROM correction_feed_events WHERE source=? AND generation=? AND event_id=?',
                (current.source, current.generation, event_id),
            ).fetchone()
            if existing is not None and existing[0] != payload:
                raise FeedFailure('event_identity_conflict')
            conn.execute(
                '''INSERT OR IGNORE INTO correction_feed_events
                   (source,generation,event_id,payload,reconciliation_key,received_at)
                   VALUES (?,?,?,?,?,?)''',
                (current.source, current.generation, event_id, payload, semantic_key, now),
            )
        conn.execute(
            '''UPDATE correction_feed_state SET cursor=?,has_more=?,version=version+1,
               next_poll_at=?,last_error=NULL,lease_expires=? WHERE source=?''',
            (page.next_cursor, int(page.has_more), next_poll_at,
             now + lease_seconds if current.lease_owner else 0, current.source),
        )
    return checkpoint(conn, expected.source)


def record_failure(conn, expected, failure, *, next_poll_at):
    """Persist retry/reset state without changing the last durable cursor."""
    if not isinstance(failure, FeedFailure):
        raise FeedFailure('failure_invalid')
    allowed = {'bootstrap_required', 'authentication_terminal', 'feature_unavailable', 'query_invalid',
               'rate_limited', 'store_unavailable', 'http_error', 'response_too_large',
               'page_invalid', 'cursor_invalid', 'transport_unavailable',
               'event_invalid', 'event_identity_conflict', 'cursor_not_advancing', 'processing_unavailable'}
    if failure.code not in allowed:
        raise FeedFailure('failure_invalid')
    _timestamp(next_poll_at)
    _idle_connection(conn)
    with conn:
        current = _claim(conn, expected)
        conn.execute(
            '''UPDATE correction_feed_state SET last_error=?,next_poll_at=?,
               bootstrap_required=?,version=version+1 WHERE source=?''',
            (failure.code, next_poll_at,
             int(current.bootstrap_required or failure.code == 'bootstrap_required'), current.source),
        )
    return checkpoint(conn, expected.source)


def rebootstrap(conn, expected, *, since=None):
    """Explicit reset: retain old events and start a separate local feed generation."""
    validate_since(since)
    _idle_connection(conn)
    with conn:
        current = _claim(conn, expected)
        conn.execute(
            '''UPDATE correction_feed_state SET cursor=NULL,bootstrap_since=?,has_more=0,
               bootstrap_required=0,next_poll_at=0,last_error=NULL,version=version+1
               ,generation=generation+1,lease_owner=NULL,lease_expires=0
               WHERE source=?''', (since, current.source),
        )
    return checkpoint(conn, expected.source)


def list_events(conn, endpoint, *, limit=1000, after_rowid=0):
    """Return durable catalog events in ingestion order for bounded reconciliation."""
    source = source_namespace(endpoint)
    if type(limit) is not int or not 1 <= limit <= 1000 or type(after_rowid) is not int or after_rowid < 0:
        raise FeedFailure('query_invalid')
    rows = conn.execute(
        '''SELECT rowid,source,generation,event_id,payload,reconciliation_key,received_at
           FROM correction_feed_events WHERE source=? AND rowid>? ORDER BY rowid LIMIT ?''',
        (source, after_rowid, limit),
    ).fetchall()
    return [{'ledger_id': row[0], 'source': row[1], 'generation': row[2], 'event_id': row[3],
             'payload': json.loads(row[4]), 'reconciliation_key': row[5], 'received_at': row[6]}
            for row in rows]


def acquire_poll(conn, endpoint, owner, *, now, force=False, lease_seconds=120):
    """Atomically lease one feed across threads/processes, honoring durable waits."""
    source = source_namespace(endpoint)
    _timestamp(now)
    if not isinstance(owner, str) or not owner or len(owner) > 128:
        raise FeedFailure('lease_invalid')
    _idle_connection(conn)
    with conn:
        conn.execute('BEGIN IMMEDIATE')
        current = checkpoint(conn, source)
        if current.lease_owner and current.lease_expires > now:
            return None
        if current.bootstrap_required:
            return None
        if current.next_poll_at > now and (not force or current.last_error not in (None, 'authentication_terminal')):
            return None
        conn.execute('INSERT OR IGNORE INTO correction_feed_state (source) VALUES (?)', (source,))
        conn.execute(
            '''UPDATE correction_feed_state SET lease_owner=?,lease_expires=?,version=version+1
               WHERE source=?''', (owner, now + lease_seconds, source),
        )
    return checkpoint(conn, source)


def release_poll(conn, endpoint, owner):
    source = source_namespace(endpoint)
    _idle_connection(conn)
    with conn:
        conn.execute('UPDATE correction_feed_state SET lease_owner=NULL,lease_expires=0 WHERE source=? AND lease_owner=?',
                     (source, owner))
