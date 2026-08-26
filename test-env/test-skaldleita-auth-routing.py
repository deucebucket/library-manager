#!/usr/bin/env python3
"""Skaldleita credentials, routing, and terminal denials fail closed."""
from __future__ import annotations

import json
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from library_manager.providers import bookdb, fingerprint, isbn_lookup  # noqa: E402


class Response:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self.payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.content = json.dumps(self.payload).encode()
        self.text = self.content.decode()

    def json(self):
        return self.payload


class Embedding:
    def tobytes(self):
        return b"voice"

    def tolist(self):
        return [0.0] * 256


def reset():
    bookdb.clear_terminal_server_denial()
    bookdb.get_and_clear_server_abort()
    bookdb.API_CIRCUIT_BREAKER['bookdb']['failures'] = 0
    bookdb.API_CIRCUIT_BREAKER['bookdb']['circuit_open_until'] = 0
    bookdb.API_RATE_LIMITS['bookdb']['last_call'] = 0


def test_headers_choose_one_credential():
    reset()
    with patch.object(bookdb, "get_lm_version", return_value="0.9.0-beta.168"):
        public = bookdb.get_bookdb_headers(None)
        personal = bookdb.get_bookdb_headers("personal-key")
    assert public['X-API-Key'] == bookdb.BOOKDB_PUBLIC_KEY
    assert personal['X-API-Key'] == "personal-key"
    for headers in (public, personal):
        assert headers['User-Agent'] == "LibraryManager/0.9.0-beta.168"
        assert headers['X-LM-Signature']
        assert headers['X-LM-Timestamp']
        assert headers['X-LM-Nonce']


def test_configured_route_and_personal_key():
    reset()
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(200, {"match": False})

    with patch.object(fingerprint.requests, "get", side_effect=get):
        result = fingerprint.lookup_fingerprint(
            "fingerprint", api_key="personal-key",
            bookdb_url="http://127.0.0.1:9876/custom/")
    assert result is None
    assert calls[0][0] == "http://127.0.0.1:9876/custom/api/fingerprint/lookup"
    assert calls[0][1]['headers']['X-API-Key'] == "personal-key"
    assert calls[0][1]['headers']['X-LM-Signature']


def test_403_is_terminal_and_never_downgrades():
    reset()
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(403, {"detail": {
            "code": "client_blocked",
            "message": "This client is blocked.",
            "action": "disable_provider",
        }})

    with patch.object(fingerprint.requests, "get", side_effect=get):
        assert fingerprint.lookup_fingerprint(
            "fingerprint", api_key="rejected-personal-key") is None
        assert fingerprint.lookup_fingerprint("fingerprint", api_key=None) is None

    assert len(calls) == 1
    assert calls[0][1]['headers']['X-API-Key'] == "rejected-personal-key"
    denial = bookdb.get_terminal_server_denial()
    assert denial['code'] == "client_blocked"
    assert denial['status_code'] == 403
    abort = bookdb.get_and_clear_server_abort()
    assert abort['action'] == "abort_task"


def test_401_is_terminal_not_public_fallback():
    reset()
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response(401, {"detail": "API key required"})

    with patch.object(bookdb.requests, "post", side_effect=post):
        assert bookdb.search_bookdb("Title", api_key="bad-personal-key") is None
        assert bookdb.search_bookdb("Title", api_key=None) is None

    assert len(calls) == 1
    assert calls[0][1]['headers']['X-API-Key'] == "bad-personal-key"
    assert bookdb.get_terminal_server_denial()['code'] == "authentication_required"


def test_429_remains_temporary():
    reset()
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(429, {"detail": "slow down"}, {"Retry-After": "60"})

    with patch.object(fingerprint.requests, "get", side_effect=get):
        assert fingerprint.lookup_fingerprint("fingerprint") is None
        assert fingerprint.lookup_fingerprint("fingerprint") is None

    assert len(calls) == 2
    assert bookdb.get_terminal_server_denial() is None


def test_all_fingerprint_voice_routes_use_configured_service():
    reset()
    calls = []
    base = "http://127.0.0.1:9876"

    def get(url, **kwargs):
        calls.append(("GET", url, kwargs))
        return Response(200, {"match": False})

    def post(url, **kwargs):
        calls.append(("POST", url, kwargs))
        return Response(200, {"success": True, "is_new": False})

    with patch.object(fingerprint.requests, "get", side_effect=get), \
            patch.object(fingerprint.requests, "post", side_effect=post), \
            patch.object(fingerprint, "extract_voice_embedding", return_value=Embedding()):
        fingerprint.lookup_fingerprint("fingerprint", bookdb_url=base)
        fingerprint.contribute_fingerprint(
            "fingerprint", 120, {"title": "Book"}, bookdb_url=base)
        fingerprint.lookup_narrator(Embedding(), bookdb_url=base)
        fingerprint.contribute_narrator(Embedding(), "Narrator", bookdb_url=base)
        fingerprint.store_voice_signature(
            "/unused.mp3", "Book", "Author", bookdb_url=base)

    assert [item[1] for item in calls] == [
        f"{base}/api/fingerprint/lookup",
        f"{base}/api/fingerprint",
        f"{base}/api/narrator/lookup",
        f"{base}/api/narrator",
        f"{base}/api/voice",
    ]
    assert all(item[2]['headers']['X-API-Key'] == bookdb.BOOKDB_PUBLIC_KEY
               for item in calls)
    assert all(item[2]['headers']['X-LM-Signature'] for item in calls)


def test_isbn_route_is_signed_and_honors_terminal_denial():
    reset()
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(403, {"detail": {
            "code": "upgrade_required",
            "message": "Upgrade Library Manager.",
        }})

    with patch.object(isbn_lookup.requests, "get", side_effect=get):
        assert isbn_lookup.lookup_isbn(
            "9781234567890",
            api_key="personal-key",
            bookdb_url="http://127.0.0.1:9876/custom/",
        ) is None
        assert isbn_lookup.lookup_isbn("9781234567890") is None

    assert len(calls) == 1
    assert calls[0][0] == "http://127.0.0.1:9876/custom/api/isbn/9781234567890"
    assert calls[0][1]['headers']['X-API-Key'] == "personal-key"
    assert calls[0][1]['headers']['X-LM-Signature']
    assert bookdb.get_terminal_server_denial()['code'] == "upgrade_required"


def test_match_route_uses_configured_service_and_public_key():
    reset()
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response(200, {
            "confidence": 0.9,
            "books": [{"title": "Title", "author_name": "Author"}],
        })

    with patch.object(bookdb.requests, "post", side_effect=post):
        result = bookdb.search_bookdb(
            "Title", bookdb_url="http://127.0.0.1:9876/custom/")

    assert result['title'] == "Title"
    assert calls[0][0] == "http://127.0.0.1:9876/custom/match"
    assert calls[0][1]['headers']['X-API-Key'] == bookdb.BOOKDB_PUBLIC_KEY
    assert calls[0][1]['headers']['X-LM-Signature']


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    for test in tests:
        test()
        print(f"[PASS] {test.__name__}")
    reset()
    print(f"All {len(tests)} Skaldleita auth/routing tests passed.")
