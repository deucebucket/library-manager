#!/usr/bin/env python3
"""Explicit backend identities survive profiles without displacing ASINs."""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.models import book_profile
from library_manager.providers import bookdb
from library_manager.pipeline.layer_api import process_layer_1_api, process_sl_requeue_verification
from library_manager.utils.skaldleita_identity import (
    canonical_skaldleita_book_id, canonical_skaldleita_source_url,
    provider_skaldleita_identity, skaldleita_identity,
)


ENDPOINT = "https://metadata.example.test"
IDENTITY = {"skaldleita_book_id": "123", "skaldleita_source_url": ENDPOINT}
ASIN = "B003ZW6GR8"


class IdentityTests(unittest.TestCase):
    def run_api_candidate(self, candidates, current_author="Test Author", current_title="Test Title", requeue=False):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.db"
            def connect():
                conn = sqlite3.connect(path)
                conn.row_factory = sqlite3.Row
                return conn
            conn = connect()
            conn.executescript("""
                CREATE TABLE books(id INTEGER PRIMARY KEY, path TEXT, current_author TEXT,
                    current_title TEXT, verification_layer INTEGER, profile TEXT,
                    confidence INTEGER, status TEXT, user_locked INTEGER,
                    max_layer_reached INTEGER, error_message TEXT);
                CREATE TABLE queue(id INTEGER PRIMARY KEY, book_id INTEGER, reason TEXT,
                    priority INTEGER, added_at TEXT);
            """)
            conn.execute("INSERT INTO books VALUES(1, ?, ?, ?, 1, ?, 20, 'pending', 0, 1, NULL)",
                         (directory, current_author, current_title, json.dumps({"book_id": ASIN, **IDENTITY})))
            conn.execute("INSERT INTO queue VALUES(1, 1, 'test', 0, '')")
            if requeue:
                conn.execute("UPDATE books SET status='pending_fix', profile=? WHERE id=1",
                             (json.dumps({"book_id": ASIN, **IDENTITY,
                              "sl_requeue": {"requeue_after": "2000-01-01T00:00:00"}}),))
            conn.commit()
            conn.close()
            if requeue:
                process_sl_requeue_verification({}, connect, lambda **kw: candidates)
            else:
                process_layer_1_api({"sl_trust_mode": "legacy"}, connect, lambda *a, **kw: candidates)
            conn = connect()
            row = dict(conn.execute("SELECT * FROM books WHERE id=1").fetchone())
            conn.close()
            return json.loads(row["profile"]), row

    def test_scheduled_reidentification_clears_or_replaces_mapping(self):
        profile, _ = self.run_api_candidate([{"book_id": ASIN}], requeue=True)
        self.assertEqual(skaldleita_identity(profile), {})
        profile, _ = self.run_api_candidate([{"book_id": ASIN, **IDENTITY,
                                            "skaldleita_book_id": 456}], requeue=True)
        self.assertEqual(profile["skaldleita_book_id"], "456")
        self.assertEqual(profile["book_id"], ASIN)

    def test_api_new_candidate_without_any_identifiers_clears_old_mapping(self):
        profile, row = self.run_api_candidate([
            {"source": "bookdb", "author": "Different Author", "title": "Different Book"}
        ])
        self.assertEqual(skaldleita_identity(profile), {})
        self.assertEqual(profile["book_id"], ASIN)
        self.assertEqual(row["verification_layer"], 2)

    def test_api_no_candidate_preserves_existing_identity(self):
        profile, _ = self.run_api_candidate([])
        self.assertEqual(skaldleita_identity(profile), IDENTITY)

    def test_api_new_explicit_identification_replaces_mapping(self):
        profile, _ = self.run_api_candidate([{
            "source": "bookdb", "author": "Different Author", "title": "Different Book",
            "skaldleita_book_id": 456, "skaldleita_source_url": "https://other.test",
        }])
        self.assertEqual(skaldleita_identity(profile), {
            "skaldleita_book_id": "456", "skaldleita_source_url": "https://other.test",
        })

    def test_api_same_book_metadata_verification_preserves_mapping(self):
        profile, row = self.run_api_candidate([{
            "source": "audnexus", "author": "Test Author", "title": "Test Title", "asin": ASIN,
        }])
        self.assertEqual(row["status"], "verified")
        self.assertEqual(skaldleita_identity(profile), IDENTITY)

    def test_numeric_normalization(self):
        for value in (123, "123", "000123"):
            self.assertEqual(canonical_skaldleita_book_id(value), "123")
        for value in (None, True, False, 0, -1, 1.0, "1e3", "１２３", " 123", {}, "1" * 100):
            self.assertIsNone(canonical_skaldleita_book_id(value))

    def test_endpoint_namespace(self):
        self.assertEqual(canonical_skaldleita_source_url("https://METADATA.example.test:443/"), ENDPOINT)
        self.assertEqual(canonical_skaldleita_source_url(ENDPOINT + "/instance/"), ENDPOINT + "/instance")
        self.assertNotEqual(canonical_skaldleita_source_url(ENDPOINT + ":8443"), ENDPOINT)
        for value in ("https://user:secret@example.test", ENDPOINT + "?key=secret",
                      ENDPOINT + "#fragment", "file:///data", "https://example.test:bad"):
            self.assertIsNone(canonical_skaldleita_source_url(value))

    def test_explicit_only_and_locally_bound(self):
        self.assertEqual(provider_skaldleita_identity({"book_id": 123, "id": 123}, ENDPOINT), {})
        self.assertEqual(provider_skaldleita_identity({
            "skaldleita_book_id": 123, "skaldleita_source_url": "https://untrusted.test"
        }, ENDPOINT), IDENTITY)
        self.assertEqual(skaldleita_identity({"skaldleita_book_id": "123"}), {})

    def test_profile_json_roundtrip_and_legacy(self):
        profile = book_profile.BookProfile.from_dict({"book_id": ASIN, **IDENTITY})
        restored = book_profile.BookProfile.from_dict(json.loads(json.dumps(profile.to_dict())))
        self.assertEqual(restored.book_id, ASIN)
        self.assertEqual(skaldleita_identity(restored.to_dict()), IDENTITY)
        legacy = book_profile.BookProfile.from_dict({"book_id": "123"})
        self.assertEqual(legacy.book_id, "123")
        self.assertIsNone(legacy.skaldleita_book_id)
        self.assertEqual(skaldleita_identity(legacy.to_dict()), {})

    def test_profile_database_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "books.db"
            def connect():
                conn = sqlite3.connect(path)
                conn.row_factory = sqlite3.Row
                return conn
            conn = connect()
            conn.execute("CREATE TABLE books(id INTEGER PRIMARY KEY, profile TEXT, confidence INTEGER, updated_at TEXT)")
            conn.execute("INSERT INTO books(id) VALUES(1)")
            conn.commit()
            conn.close()
            with patch.object(book_profile, "_get_db", connect):
                profile = book_profile.BookProfile.from_dict({"book_id": ASIN, **IDENTITY})
                book_profile.save_book_profile(1, profile)
                loaded = book_profile.load_book_profile(1)
            self.assertEqual(loaded.book_id, ASIN)
            self.assertEqual(skaldleita_identity(loaded.to_dict()), IDENTITY)

    def test_candidate_does_not_displace_asin(self):
        profile = book_profile.build_profile_from_sources(api_candidates=[{
            "source": "bookdb", "author": "Test Author", "title": "Test Title",
            "asin": ASIN, "book_id": 123, **IDENTITY,
        }])
        self.assertEqual(profile.book_id, ASIN)
        self.assertEqual(skaldleita_identity(profile.to_dict()), IDENTITY)
        other = book_profile.build_profile_from_sources(api_candidates=[{
            "source": "openlibrary", "author": "Test Author", "title": "Test Title", "id": 123,
        }])
        self.assertIsNone(other.skaldleita_book_id)

    def test_audio_explicit_identity_replaces_candidate(self):
        profile = book_profile.build_profile_from_sources(
            api_candidates=[{"asin": ASIN, **IDENTITY}],
            audio_result={"skaldleita_book_id": "456", "skaldleita_source_url": ENDPOINT},
        )
        self.assertEqual(profile.book_id, ASIN)
        self.assertEqual(profile.skaldleita_book_id, "456")

    def test_audio_reidentification_without_id_clears_but_language_enrichment_preserves(self):
        candidate = {"source": "bookdb", "asin": ASIN, **IDENTITY}
        changed = book_profile.build_profile_from_sources(api_candidates=[candidate],
            audio_result={"author": "Another Author", "title": "Another Book"})
        self.assertIsNone(changed.skaldleita_book_id)
        enriched = book_profile.build_profile_from_sources(api_candidates=[candidate],
            audio_result={"language": "de"})
        self.assertEqual(skaldleita_identity(enriched.to_dict()), IDENTITY)

    def test_later_candidate_same_asin_can_supply_identity_but_other_book_cannot(self):
        first = {"source": "audnexus", "asin": ASIN}
        same = book_profile.build_profile_from_sources(api_candidates=[first, {"asin": ASIN, **IDENTITY}])
        self.assertEqual(skaldleita_identity(same.to_dict()), IDENTITY)
        other = book_profile.build_profile_from_sources(api_candidates=[first, {"asin": "B001234567", **IDENTITY}])
        self.assertIsNone(other.skaldleita_book_id)

    def test_consensus_cannot_attach_losing_candidate_identity(self):
        profile = book_profile.build_profile_from_sources(
            api_candidates=[{"source": "bookdb", "author": "Wrong Author", "title": "Wrong Title", **IDENTITY}],
            audio_result={"author": "Real Author", "title": "Real Title"},
        )
        self.assertIsNone(profile.skaldleita_book_id)

    def test_provider_text_field_and_cache_namespace(self):
        payload = {"confidence": 0.9, "skaldleita_book_id": 123, "book_id": 123,
                   "books": [{"title": "Test Title", "author_name": "Test Author", "asin": ASIN}]}
        response = Mock(status_code=200)
        response.json.return_value = payload
        with patch.object(bookdb, "get_terminal_server_denial", return_value=None), \
                patch.object(bookdb, "rate_limit_wait"), \
                patch.object(bookdb, "get_bookdb_headers", return_value={}), \
                patch.object(bookdb.requests, "post", return_value=response):
            result = bookdb.search_bookdb("Test Title", "Test Author", bookdb_url=ENDPOINT)
        self.assertEqual(result["asin"], ASIN)
        self.assertEqual(result["book_id"], 123)
        self.assertEqual(skaldleita_identity(result), IDENTITY)
        cache = Mock()
        cache.get.return_value = result
        with patch.object(bookdb, "get_terminal_server_denial", return_value=None):
            same = bookdb.search_bookdb("Test Title", bookdb_url=ENDPOINT,
                                      config={"enabled": True}, data_dir="unused", cache_getter=lambda **kw: cache)
            other = bookdb.search_bookdb("Test Title", bookdb_url="https://other.test",
                                       config={"enabled": True}, data_dir="unused", cache_getter=lambda **kw: cache)
        self.assertEqual(skaldleita_identity(same), IDENTITY)
        self.assertEqual(skaldleita_identity(other), {})
        self.assertEqual(other["asin"], ASIN)

    def test_audio_provider_preserves_server_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            audio = Path(directory) / "fixture.mp3"
            audio.write_bytes(b"synthetic source")
            def ffmpeg(command, **kwargs):
                Path(command[-1]).write_bytes(b"synthetic clip" * 100)
                return SimpleNamespace(returncode=0)
            response = Mock(status_code=200)
            response.json.return_value = {
                "author": "Test Author", "title": "Test Title", "source": "database",
                "asin": ASIN, "book_id": 123, "skaldleita_book_id": 123,
            }
            with patch.object(bookdb, "get_terminal_server_denial", return_value=None), \
                    patch.object(bookdb.subprocess, "run", side_effect=ffmpeg), \
                    patch.object(bookdb, "is_voice_embedding_available", return_value=False), \
                    patch.object(bookdb, "get_bookdb_headers", return_value={}), \
                    patch.object(bookdb.requests, "post", return_value=response):
                result = bookdb.identify_audio_with_bookdb(audio, bookdb_url=ENDPOINT)
            self.assertEqual(result["asin"], ASIN)
            self.assertEqual(result["book_id"], 123)
            self.assertEqual(skaldleita_identity(result), IDENTITY)


if __name__ == "__main__":
    unittest.main()
