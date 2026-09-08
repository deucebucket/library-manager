#!/usr/bin/env python3
"""Confirmed corrections envelope/auth/checkpoint tests, without network access.

The local event validator below tests storage without assuming a wire schema.
Production polling supplies the separately tested canonical Skaldleita validator.
"""

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.corrections import feed, store
from library_manager.providers import bookdb


SOURCE = 'https://corrections.example.invalid'


def local_validator(raw):
    if set(raw) != {'local_test_id', 'value'} or not isinstance(raw['local_test_id'], str):
        raise feed.FeedFailure('event_invalid')
    return feed.ValidatedCorrection(raw['local_test_id'], dict(raw))


def page(cursor='opaque-next', more=False, value='one', event_id='fixture-1'):
    return feed.CorrectionPage(({'local_test_id': event_id, 'value': value},), cursor, more)


class FakeResponse:
    def __init__(self, payload=None, status=200, headers=None, chunks=None):
        self.status_code = status
        self.headers = headers or {}
        self.chunks = chunks if chunks is not None else [json.dumps(payload).encode()]
        self.closed = False

    def iter_content(self, chunk_size):
        yield from self.chunks

    def close(self):
        self.closed = True


class TransportTests(unittest.TestCase):
    def setUp(self):
        bookdb.clear_terminal_server_denial()

    def tearDown(self):
        bookdb.clear_terminal_server_denial()

    def client(self, response):
        client = Mock()
        client.get.return_value = response
        return client

    def test_bootstrap_cursor_and_shared_signing(self):
        response = FakeResponse({'corrections': [], 'next_cursor': 'opaque-token', 'has_more': False})
        client = self.client(response)
        with patch.object(bookdb, 'get_bookdb_headers', return_value={'fixture': 'signed'}) as headers:
            result = feed.fetch_page(SOURCE + '/', api_key='fixture-personal',
                                     since='2026-09-08T00:00:00Z', session=client)
        self.assertEqual(result, feed.CorrectionPage((), 'opaque-token', False))
        self.assertTrue(response.closed)
        headers.assert_called_once_with('fixture-personal')
        request = client.get.call_args
        self.assertEqual(request.args, (SOURCE + '/api/corrections',))
        self.assertEqual(request.kwargs['params'], {'limit': 100, 'since': '2026-09-08T00:00:00Z'})
        self.assertFalse(request.kwargs['allow_redirects'])
        self.assertTrue(request.kwargs['stream'])
        feed.fetch_page(SOURCE, cursor='opaque/token+==', session=client)
        self.assertEqual(client.get.call_args.kwargs['params'], {'limit': 100, 'cursor': 'opaque/token+=='})

    def test_auth_denial_is_terminal_without_credential_retry(self):
        for status in (401, 403):
            bookdb.clear_terminal_server_denial()
            response = FakeResponse({'detail': {'message': 'fixture denied'}}, status=status)
            client = self.client(response)
            with self.assertRaises(feed.FeedFailure) as first:
                feed.fetch_page(SOURCE, session=client)
            self.assertEqual(first.exception.code, 'authentication_terminal')
            with self.assertRaises(feed.FeedFailure):
                feed.fetch_page(SOURCE, session=client)
            self.assertEqual(client.get.call_count, 1)
            self.assertEqual(bookdb.get_terminal_server_denial()['status_code'], status)

    def test_errors_preserve_meaning_and_retry_guidance(self):
        for status, code in ((426, 'http_error'), (404, 'feature_unavailable'), (422, 'query_invalid'),
                             (429, 'rate_limited'), (503, 'store_unavailable'), (302, 'http_error')):
            client = self.client(FakeResponse(status=status, headers={'Retry-After': '123'}))
            with self.assertRaises(feed.FeedFailure) as failure:
                feed.fetch_page(SOURCE, cursor='retained', session=client)
            self.assertEqual(failure.exception.code, code)
            self.assertEqual(failure.exception.retry_after, 123)
        self.assertEqual(feed._retry_after('Thu, 01 Jan 1970 00:02:00 GMT', 60), 60)

    def test_only_explicit_reset_error_authorizes_rebootstrap_state(self):
        fixture = json.loads((Path(__file__).parent / 'fixtures/corrections-reset-error.json').read_text())
        cases = ((fixture, 'bootstrap_required'),
                 ({'detail': {'code': 'invalid_corrections_cursor', 'reset_required': False}}, 'cursor_invalid'),
                 ({'detail': {'code': 'invalid_corrections_query'}}, 'query_invalid'),
                 ({'detail': {'code': 'corrections_cursor_reset_required', 'reset_required': False}}, 'http_error'))
        for payload, code in cases:
            with self.assertRaises(feed.FeedFailure) as failure:
                feed.fetch_page(SOURCE, cursor='preserve', session=self.client(FakeResponse(payload, status=400)))
            self.assertEqual(failure.exception.code, code)

    def test_retry_after_uses_current_clock_by_default_and_rejects_extremes(self):
        client = self.client(FakeResponse(status=429, headers={
            'Retry-After': 'Thu, 01 Jan 1970 00:02:00 GMT'}))
        with patch.object(feed.time, 'time', return_value=60):
            with self.assertRaises(feed.FeedFailure) as failure:
                feed.fetch_page(SOURCE, session=client)
        self.assertEqual(failure.exception.retry_after, 60)
        self.assertEqual(feed._retry_after('0', 60), 0)
        self.assertEqual(feed._retry_after('3600', 60), 3600)
        for value in ('-1', '9999999999999999999999999', 'invalid', None):
            self.assertIsNone(feed._retry_after(value, 60))

    def test_malformed_and_oversized_pages_never_escape_transport(self):
        for payload in ({'events': [], 'next_cursor': 'a', 'has_more': False},
                        {'corrections': [], 'next_cursor': None, 'has_more': False},
                        {'corrections': [], 'next_cursor': 'a', 'has_more': 1},
                        {'corrections': [None], 'next_cursor': 'a', 'has_more': False}):
            with self.assertRaises(feed.FeedFailure):
                feed.fetch_page(SOURCE, session=self.client(FakeResponse(payload)))
        response = FakeResponse(chunks=[b'x' * (feed.MAX_PAGE_BYTES + 1)])
        with self.assertRaises(feed.FeedFailure) as failure:
            feed.fetch_page(SOURCE, session=self.client(response))
        self.assertEqual(failure.exception.code, 'response_too_large')
        self.assertTrue(response.closed)

    def test_invalid_request_never_sends(self):
        client = Mock()
        for kwargs in ({'cursor': 'a', 'since': '2026-09-08T00:00:00Z'},
                       {'since': '2026-09-08T00:00:00'}, {'limit': True}, {'limit': 501}):
            with self.assertRaises(feed.FeedFailure):
                feed.fetch_page(SOURCE, session=client, **kwargs)
        client.get.assert_not_called()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='corrections-feed-test-')
        self.path = Path(self.temp.name) / 'feed.db'
        self.conn = sqlite3.connect(self.path)
        store.initialize_store(self.conn)

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def ingest(self, incoming, expected=None, validator=local_validator):
        return store.ingest_page(self.conn, expected or store.checkpoint(self.conn, SOURCE),
                                 incoming, validator=validator, now=100, next_poll_at=200)

    def count(self):
        return self.conn.execute('SELECT COUNT(*) FROM correction_feed_events').fetchone()[0]

    def test_page_checkpoint_survives_restart_and_deduplicates(self):
        first = self.ingest(page(more=True))
        self.conn.close()
        self.conn = sqlite3.connect(self.path)
        self.assertEqual(store.checkpoint(self.conn, SOURCE), first)
        last = self.ingest(page(cursor='terminal', more=False))
        self.assertEqual(last.cursor, 'terminal')
        self.assertFalse(last.has_more)
        self.assertEqual(self.count(), 1)
        empty = self.ingest(feed.CorrectionPage((), 'empty-checkpoint', False))
        self.assertEqual(empty.cursor, 'empty-checkpoint')
        self.assertEqual(self.count(), 1)

    def test_schema_validator_is_mandatory_and_page_rejection_is_atomic(self):
        with self.assertRaises(feed.FeedFailure):
            self.ingest(page(), validator=None)
        invalid = feed.CorrectionPage(({'local_test_id': 'valid', 'value': 'ok'}, {'unknown': True}), 'bad', False)
        with self.assertRaises(feed.FeedFailure):
            self.ingest(invalid)
        self.assertEqual(self.count(), 0)
        self.assertEqual(store.checkpoint(self.conn, SOURCE).version, 0)

    def test_database_failure_rolls_back_events_and_checkpoint(self):
        self.conn.execute('''CREATE TRIGGER fixture_fail_checkpoint BEFORE UPDATE ON correction_feed_state
                           BEGIN SELECT RAISE(ABORT, 'fixture checkpoint failure'); END''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.ingest(page())
        self.assertEqual(self.count(), 0)
        self.assertEqual(store.checkpoint(self.conn, SOURCE).version, 0)

    def test_stale_poller_cannot_advance_checkpoint(self):
        stale = store.checkpoint(self.conn, SOURCE)
        fresh = self.ingest(page())
        with self.assertRaises(feed.FeedFailure) as failure:
            self.ingest(page(cursor='stale', event_id='fixture-2'), expected=stale)
        self.assertEqual(failure.exception.code, 'checkpoint_conflict')
        self.assertEqual(store.checkpoint(self.conn, SOURCE), fresh)
        self.assertEqual(self.count(), 1)

    def test_backend_namespace_and_conflicting_event_identity(self):
        original = self.ingest(page())
        other = store.checkpoint(self.conn, 'https://other.example.invalid')
        self.ingest(page(value='different server'), expected=other)
        self.assertEqual(self.count(), 2)
        with self.assertRaises(feed.FeedFailure) as failure:
            self.ingest(page(value='mutated immutable event'))
        self.assertEqual(failure.exception.code, 'event_identity_conflict')
        self.assertEqual(store.checkpoint(self.conn, SOURCE), original)

    def test_error_checkpoint_and_deliberate_rebootstrap_retain_deduplication(self):
        original = self.ingest(page())
        current = original
        for code in ('authentication_terminal', 'feature_unavailable', 'rate_limited', 'store_unavailable', 'bootstrap_required'):
            current = store.record_failure(self.conn, current, feed.FeedFailure(code), next_poll_at=500)
            self.assertEqual(current.cursor, original.cursor)
        self.assertTrue(current.bootstrap_required)
        with self.assertRaises(feed.FeedFailure):
            self.ingest(page(cursor='not-authorized-reset'))
        fresh = store.rebootstrap(self.conn, current, since='2026-09-01T00:00:00Z')
        self.assertIsNone(fresh.cursor)
        self.assertEqual(fresh.since, '2026-09-01T00:00:00Z')
        self.assertFalse(fresh.bootstrap_required)
        self.ingest(page(cursor='new-snapshot'))
        self.assertEqual(self.count(), 2)
        events = store.list_events(self.conn, SOURCE)
        self.assertEqual([event['generation'] for event in events], [0, 1])
        self.assertEqual(events[0]['reconciliation_key'], events[1]['reconciliation_key'])

    def test_nonadvancing_snapshot_and_caller_transaction_are_refused(self):
        current = self.ingest(page())
        with self.assertRaises(feed.FeedFailure):
            self.ingest(page(more=True), expected=current)
        self.conn.execute("INSERT INTO correction_feed_state (source) VALUES ('local-uncommitted')")
        with self.assertRaises(feed.FeedFailure):
            self.ingest(page(cursor='next'))
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertEqual(store.checkpoint(self.conn, SOURCE), current)
        with self.assertRaises(feed.FeedFailure):
            self.ingest(replace(page(), next_cursor=''))


if __name__ == '__main__':
    unittest.main(verbosity=2)
