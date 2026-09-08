#!/usr/bin/env python3
"""Corrections UI route validation and nonblocking service integration."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from library_manager.corrections.routes import register_corrections_routes


class RoutesTests(unittest.TestCase):
    def setUp(self):
        self.service = Mock()
        self.application = Mock()
        self.service.summary.return_value = {'enabled': False}
        self.application.summary.return_value = {'counts': {'pending': 1201}, 'pending': 1201}
        app = Flask(__name__)
        register_corrections_routes(app, lambda: (self.service, self.application), lambda: {})
        self.client = app.test_client()

    def test_summary_uses_exact_count(self):
        response = self.client.get('/api/corrections/summary')
        self.assertEqual(response.json['pending'], 1201)

    def test_decision_requires_revision(self):
        for value in ({}, {'expected_revision': True}, {'expected_revision': -1}, []):
            self.assertEqual(self.client.post('/api/corrections/1/apply', json=value).status_code, 400)
        self.application.apply.assert_not_called()

    def test_explicit_decision_and_conflict(self):
        self.application.apply.return_value = {'success': False, 'error': 'stale_revision'}
        response = self.client.post('/api/corrections/1/apply', json={'expected_revision': 2})
        self.assertEqual(response.status_code, 409)
        self.application.apply.assert_called_once_with(1, 2, explicit=True)

    def test_check_is_nonblocking(self):
        self.service.request_poll.return_value = {'status': 'scheduled'}
        self.assertEqual(self.client.post('/api/corrections/check').status_code, 400)
        self.assertEqual(self.client.post('/api/corrections/check', json={}).json['result']['status'], 'scheduled')
        self.service.poll_once.assert_not_called()

    def test_reset_requires_confirmation_and_exposes_busy(self):
        self.assertEqual(self.client.post('/api/corrections/reset', json={}).status_code, 400)
        self.service.reset.assert_not_called()
        self.service.reset.return_value = {'status': 'busy'}
        self.assertEqual(self.client.post('/api/corrections/reset', json={'confirm': True}).status_code, 409)


if __name__ == '__main__':
    unittest.main()
