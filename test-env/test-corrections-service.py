#!/usr/bin/env python3
"""Corrections v1 fixture/schema/polling tests; every transport is local/mock.

Fixtures are from Skaldleita e4feef29c297279ec4082a2dafff588c3a9d6e0a,
docs/fixtures/corrections-{page,empty,reset-error}.json (PR #185).
"""

import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.corrections.feed import CorrectionPage, FeedFailure
from library_manager.corrections.schema import validate_event, validate_page
from library_manager.corrections.service import CorrectionsService, POLL_INTERVAL
from library_manager.corrections import store
from library_manager.providers import bookdb


FIXTURES = Path(__file__).parent / 'fixtures'
SOURCE = 'https://fixture.example.invalid'


def fixture_page(name='corrections-page.json'):
    value = json.loads((FIXTURES / name).read_text())
    return CorrectionPage(tuple(value['corrections']), value['next_cursor'], value['has_more'])


class SchemaTests(unittest.TestCase):
    def test_canonical_full_and_empty_fixture_and_null_omission(self):
        page = validate_page(fixture_page())
        event = validate_event(page.corrections[0])
        self.assertEqual(event.event_id, '1')
        self.assertEqual(event.payload['affected_skaldleita_book_ids'], [42])
        self.assertEqual(event.payload['corrected_result']['skaldleita_book_id'], 84)
        self.assertIsNone(event.payload['corrected_result']['sl_id'])
        self.assertNotIn('sl_id', event.payload['original_result'])
        self.assertEqual(validate_page(fixture_page('corrections-empty.json')).corrections, ())

    def test_aliases_wrong_types_untrusted_identity_and_bounds_rejected(self):
        original = fixture_page().corrections[0]
        edits = [lambda e: e.update(id=True), lambda e: e.update(reason=['manual']),
                 lambda e: e.update(affected_skaldleita_book_ids=['42']),
                 lambda e: e.update(affected_audio_hashes=['A' * 64]),
                 lambda e: e.update(affected_sl_ids=['bad/identifier']),
                 lambda e: e.update(affected_sl_ids=['ok'] * 501),
                 lambda e: e['corrected_result'].update(author_name='alias'),
                 lambda e: e['corrected_result'].update(skaldleita_book_id='84'),
                 lambda e: e['corrected_result'].update(series_position=float('nan')),
                 lambda e: e['corrected_result'].update(title='x' * 1001),
                 lambda e: e['corrected_result'].update(year=True),
                 lambda e: e.update(created_at='2026-09-08')]
        for edit in edits:
            event = copy.deepcopy(original)
            edit(event)
            with self.assertRaises(FeedFailure):
                validate_event(event)
        with self.assertRaises(FeedFailure):
            validate_page(CorrectionPage((original, original), 'cursor', False))


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='corrections-service-')
        self.path = Path(self.temp.name) / 'fixture.db'
        self.config = {'receive_corrections': True, 'bookdb_url': SOURCE}
        self.now = 1000
        self.fetcher = Mock(return_value=fixture_page())
        self.callback = Mock()
        bookdb.clear_terminal_server_denial()
        self.service = self.make_service()
        self.service.initialize()

    def tearDown(self):
        self.service.stop()
        bookdb.clear_terminal_server_denial()
        self.temp.cleanup()

    def db(self):
        return sqlite3.connect(self.path)

    def make_service(self, **kwargs):
        return CorrectionsService(self.db, lambda: dict(self.config), lambda: {'bookdb_api_key': 'fixture'},
                                  on_ingested=kwargs.get('callback', self.callback),
                                  fetcher=kwargs.get('fetcher', self.fetcher), clock=lambda: self.now)

    def state(self, source=SOURCE):
        conn = self.db()
        try:
            return store.checkpoint(conn, source)
        finally:
            conn.close()

    def events(self):
        conn = self.db()
        try:
            return store.list_events(conn, SOURCE)
        finally:
            conn.close()

    def test_disabled_default_has_no_network_and_runtime_enable_works(self):
        del self.config['receive_corrections']
        self.assertEqual(self.service.poll_once()['status'], 'disabled')
        self.fetcher.assert_not_called()
        self.config['receive_corrections'] = True
        self.assertEqual(self.service.poll_once()['status'], 'updated')
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(self.state().next_poll_at, self.now + POLL_INTERVAL)
        self.assertEqual(self.service.poll_once()['status'], 'deferred')
        self.assertEqual(self.fetcher.call_count, 1)
        self.now += POLL_INTERVAL
        self.fetcher.return_value = fixture_page('corrections-empty.json')
        self.assertEqual(self.service.poll_once()['status'], 'updated')
        self.assertEqual(self.fetcher.call_args.kwargs['cursor'], fixture_page().next_cursor)

    def test_failed_later_page_preserves_first_checkpoint_and_resumes(self):
        first = CorrectionPage(fixture_page().corrections, 'first-snapshot', True)
        self.fetcher.side_effect = [first, FeedFailure('store_unavailable')]
        result = self.service.poll_once()
        self.assertEqual(result['error'], 'store_unavailable')
        self.assertEqual(self.state().cursor, 'first-snapshot')
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(self.service.poll_once(force=True)['status'], 'deferred')
        self.now += 300
        self.fetcher.side_effect = None
        self.fetcher.return_value = fixture_page('corrections-empty.json')
        self.assertEqual(self.make_service().poll_once()['status'], 'updated')
        self.assertEqual(self.fetcher.call_args.kwargs['cursor'], 'first-snapshot')

    def test_callback_failure_keeps_page_for_later_durable_reconciliation(self):
        def callback(source):
            if self.events():
                raise RuntimeError('local processing fixture failure')
        self.service.on_ingested = callback
        self.assertEqual(self.service.poll_once()['error'], 'processing_unavailable')
        self.assertEqual(self.state().cursor, fixture_page().next_cursor)
        self.assertEqual(len(self.events()), 1)
        self.now += 300
        self.service.on_ingested = self.callback
        self.fetcher.return_value = fixture_page('corrections-empty.json')
        self.assertEqual(self.service.poll_once()['status'], 'updated')
        self.callback.assert_called_with(SOURCE)

    def test_two_services_lease_and_expired_lease_restart(self):
        conn = self.db()
        store.acquire_poll(conn, SOURCE, 'crashed-worker', now=self.now, lease_seconds=120)
        conn.close()
        self.assertEqual(self.make_service().poll_once(force=True)['status'], 'deferred')
        self.fetcher.assert_not_called()
        self.now += 121
        self.assertEqual(self.make_service().poll_once(force=True)['status'], 'updated')
        self.assertIsNone(self.state().lease_owner)

    def test_concurrent_services_only_one_fetches(self):
        entered, release = threading.Event(), threading.Event()
        def fetch(*args, **kwargs):
            entered.set()
            release.wait(timeout=3)
            return fixture_page()
        first = self.make_service(fetcher=fetch)
        thread = threading.Thread(target=first.poll_once)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=2))
            self.assertEqual(self.service.poll_once(force=True)['status'], 'deferred')
            self.fetcher.assert_not_called()
        finally:
            release.set()
            thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.events()), 1)

    def test_opt_out_or_source_change_during_fetch_never_ingests(self):
        def fetch(*args, **kwargs):
            self.config['receive_corrections'] = False
            return fixture_page()
        self.service.fetcher = fetch
        self.assertEqual(self.service.poll_once()['status'], 'configuration_changed')
        self.assertEqual(self.events(), [])
        self.assertIsNone(self.state().cursor)
        self.config['receive_corrections'] = True
        self.config['bookdb_url'] = 'https://other.example.invalid'
        self.service.fetcher = self.fetcher
        self.assertEqual(self.service.poll_once()['status'], 'updated')
        self.assertIsNone(self.state().cursor)
        self.assertIsNotNone(self.state(self.config['bookdb_url']).cursor)

    def test_explicit_reset_preserves_old_generation_and_allows_reused_id(self):
        self.service.poll_once()
        self.now += POLL_INTERVAL
        self.fetcher.side_effect = FeedFailure('bootstrap_required')
        self.assertEqual(self.service.poll_once()['error'], 'bootstrap_required')
        self.assertTrue(self.service.summary()['bootstrap_required'])
        self.assertEqual(self.service.poll_once(force=True)['status'], 'deferred')
        self.assertEqual(self.service.reset()['generation'], 1)
        self.fetcher.side_effect = None
        event = copy.deepcopy(fixture_page().corrections[0])
        event['corrected_result']['narrator'] = 'Different reviewed correction'
        self.fetcher.return_value = CorrectionPage((event,), 'new-feed', False)
        self.assertEqual(self.service.poll_once()['status'], 'updated')
        events = self.events()
        self.assertEqual([item['generation'] for item in events], [0, 1])
        self.assertNotEqual(events[0]['reconciliation_key'], events[1]['reconciliation_key'])

    def test_bounded_page_batch_and_stoppable_explicit_scheduler(self):
        self.fetcher.side_effect = [CorrectionPage((), str(i), True) for i in range(3)]
        result = self.service.poll_once()
        self.assertEqual(result['pages'], 3)
        self.assertTrue(result['has_more'])
        self.assertEqual(self.state().next_poll_at, self.now + 5)
        self.fetcher.side_effect = None
        self.fetcher.return_value = fixture_page('corrections-empty.json')
        completed = threading.Event()
        self.service.on_ingested = lambda source: completed.set()
        self.service.start()
        self.assertTrue(completed.wait(timeout=2))
        self.service.stop()
        self.assertFalse(self.service.summary()['running'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
