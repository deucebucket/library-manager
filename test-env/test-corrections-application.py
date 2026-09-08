#!/usr/bin/env python3
"""Real SQLite correction matching, decisions, conflicts and atomic receipts."""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.corrections.application import CorrectionsApplication
from library_manager.corrections.feed import CorrectionPage
from library_manager.corrections.schema import validate_event
from library_manager.corrections.store import checkpoint, ingest_page, initialize_store, rebootstrap


SOURCE = "https://metadata.example.test"


class ApplicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "library.db"
        self.settings = {"bookdb_url": SOURCE, "receive_corrections": True,
                         "auto_apply_corrections": False}
        conn = self.connect()
        conn.executescript("""
            CREATE TABLE books(id INTEGER PRIMARY KEY, profile TEXT, current_author TEXT,
                current_title TEXT, path TEXT, status TEXT, user_locked INTEGER DEFAULT 0,
                updated_at TEXT, languages TEXT);
            CREATE TABLE queue(book_id INTEGER);
            CREATE TABLE history(book_id INTEGER,status TEXT);
        """)
        initialize_store(conn)
        conn.close()
        self.app = CorrectionsApplication(self.connect, lambda: self.settings)
        self.app.initialize()
        self.add_book()

    def tearDown(self):
        self.temp.cleanup()

    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def add_book(self, book_id=1, *, source=SOURCE, canonical="42", locked=0):
        profile = {"book_id": "B012345678", "author": {"value": "Example Author"},
                   "title": {"value": "Example Book"}, "narrator": {"value": "Old Narrator"},
                   "series_num": {"value": "2.0"}, "year": {"value": "2020"},
                   "language": {"value": "en"}, "languages": ["en"]}
        if canonical:
            profile.update(skaldleita_book_id=canonical, skaldleita_source_url=source)
        conn = self.connect()
        conn.execute("INSERT INTO books VALUES(?,?,?,?,?,?,?,?,?)",
                     (book_id, json.dumps(profile), "Example Author", "Example Book",
                      "/synthetic-library/unchanged.mp3", "verified", locked, "before", '["en"]'))
        conn.commit()
        conn.close()

    def event(self, event_id=1, *, before=None, after=None, reason="narrator_mismatch"):
        return {"id": event_id, "created_at": "2026-04-01T00:00:00Z", "reason": reason,
                "affected_skaldleita_book_ids": [42], "affected_sl_ids": [], "affected_audio_hashes": [],
                "original_result": {"narrator": "Old Narrator"} if before is None else before,
                "corrected_result": {"narrator": "Correct Narrator"} if after is None else after}

    def ingest(self, event, source=SOURCE):
        conn = self.connect()
        expected = checkpoint(conn, source)
        ingest_page(conn, expected, CorrectionPage((event,), "checkpoint-" + str(event["id"]), False),
                    validator=validate_event, now=1, next_poll_at=2)
        conn.close()
        self.app.reconcile(source)
        return self.app.list_decisions()[0]

    def book(self, book_id=1):
        conn = self.connect()
        row = dict(conn.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone())
        conn.close()
        row["decoded"] = json.loads(row["profile"])
        return row

    def mutate(self, statement, args=()):
        conn = self.connect()
        conn.execute(statement, args)
        conn.commit()
        conn.close()

    def test_review_apply_preserves_files_and_omitted_metadata(self):
        decision = self.ingest(self.event())
        self.assertTrue(decision["can_apply"])
        self.assertEqual(self.app.summary()["pending"], 1)
        before = self.book()
        self.assertTrue(self.app.apply(decision["id"], decision["revision"])["success"])
        after = self.book()
        self.assertEqual(after["decoded"]["narrator"]["value"], "Correct Narrator")
        for field in ("path", "current_author", "current_title", "status"):
            self.assertEqual(after[field], before[field])
        self.assertEqual(after["decoded"]["book_id"], "B012345678")
        detail = self.app.detail(decision["id"])
        self.assertEqual(detail["status"], "applied")
        self.assertEqual(len(detail["receipts"]), 1)
        self.assertFalse(self.app.apply(decision["id"], 1)["success"])

    def test_auto_apply_has_no_active_outer_transaction(self):
        self.ingest(self.event(after={"narrator": "Middle Narrator"}))
        opened = []
        original_apply = self.app.apply
        calls = []

        def tracked_connect():
            conn = self.connect()
            opened.append(conn)
            return conn

        def checked_apply(*args, **kwargs):
            for conn in opened:
                try:
                    self.assertFalse(conn.in_transaction)
                except sqlite3.ProgrammingError:
                    pass  # Earlier application connections are already closed.
            calls.append(args[0])
            return original_apply(*args, **kwargs)

        self.app.get_db = tracked_connect
        self.app.apply = checked_apply
        self.settings["auto_apply_corrections"] = True
        self.ingest(self.event(2, before={"narrator": "Middle Narrator"},
                               after={"narrator": "Final Narrator"}))
        self.assertEqual(len(calls), 2)  # Durable retry, then newly recorded event.
        self.assertEqual(self.book()["decoded"]["narrator"]["value"], "Final Narrator")

    def test_decision_pages_reach_older_pending_without_duplicates(self):
        conn = self.connect()
        expected = checkpoint(conn, SOURCE)
        events = tuple(self.event(i) for i in range(1, 106))
        ingest_page(conn, expected, CorrectionPage(events, "many-decisions", False),
                    validator=validate_event, now=1, next_poll_at=2)
        conn.close()
        self.app.reconcile(SOURCE)
        self.app.reconcile(SOURCE)
        newest = self.app.list_decisions()[0]
        self.assertTrue(self.app.dismiss(newest["id"], newest["revision"])["success"])
        first = self.app.decision_page(status="pending")
        self.assertEqual(len(first["items"]), 100)
        self.assertTrue(first["has_more"])
        self.assertEqual(first["status_counts"], {"pending": 104, "dismissed": 1})
        # A new arrival between reads must not shift the older page boundary.
        self.ingest(self.event(106))
        second = self.app.decision_page(status="pending", before_id=first["next_before_id"])
        self.assertEqual(len(second["items"]), 4)
        self.assertFalse(second["has_more"])
        self.assertIsNone(second["next_before_id"])
        ids = [item["id"] for item in first["items"] + second["items"]]
        self.assertEqual(len(set(ids)), 104)
        self.assertIn(1, ids)
        self.assertNotIn(newest["id"], ids)
        self.assertEqual(self.app.list_decisions(status="dismissed")[0]["id"], newest["id"])
        # Historical-source decisions remain visible, unlike the active-source dashboard.
        self.settings["bookdb_url"] = "https://other.example.test"
        self.assertEqual(self.app.decision_page()["status_counts"], {"pending": 105, "dismissed": 1})
        self.assertEqual(self.app.summary()["pending"], 0)
        for kwargs in ({"status": "unknown"}, {"status": []}, {"before_id": 0},
                       {"before_id": True}, {"before_id": "1"}, {"before_id": 2**63}, {"limit": True}):
            with self.assertRaises(ValueError):
                self.app.decision_page(**kwargs)

    def test_auto_apply_requires_explicit_optin(self):
        self.settings["auto_apply_corrections"] = True
        decision = self.ingest(self.event())
        self.assertEqual(decision["status"], "applied")
        self.settings["receive_corrections"] = False
        self.assertEqual(self.app.reconcile()["status"], "disabled")
        self.assertFalse(self.app.apply(decision["id"], 2)["success"])

    def test_ordered_same_page_auto_apply_chain_and_conflict_stop(self):
        self.settings["auto_apply_corrections"] = True
        events = [
            self.event(after={"narrator": "Middle Narrator"}),
            self.event(2, before={"narrator": "Middle Narrator"}, after={"narrator": "Final Narrator"}),
            self.event(3, before={"narrator": "Unexpected Narrator"}, after={"narrator": "Must Not Apply"}),
            self.event(4, before={"narrator": "Must Not Apply"}, after={"narrator": "Also Must Not Apply"}),
        ]
        conn = self.connect()
        ingest_page(conn, checkpoint(conn, SOURCE), CorrectionPage(tuple(events), "chain-end", False),
                    validator=validate_event, now=1, next_poll_at=2)
        conn.close()
        self.app.reconcile()
        self.assertEqual(self.book()["decoded"]["narrator"]["value"], "Final Narrator")
        decisions = self.app.list_decisions()
        self.assertEqual([row["status"] for row in decisions], ["conflict", "conflict", "applied", "applied"])

    def test_provenance_missing_or_other_endpoint_is_unmatched(self):
        self.mutate("DELETE FROM books")
        self.add_book(source="https://another-instance.test")
        decision = self.ingest(self.event())
        self.assertEqual(decision["status"], "unmatched")
        self.assertFalse(decision["can_apply"])
        self.mutate("DELETE FROM books")
        self.add_book(canonical=None)
        self.app.reconcile()
        self.assertEqual(self.app.list_decisions()[0]["status"], "unmatched")
        self.mutate("DELETE FROM books")
        self.add_book()
        self.app.reconcile()
        self.assertEqual(self.app.list_decisions()[0]["status"], "pending")

    def test_locks_and_original_mismatch_block_even_review(self):
        self.mutate("UPDATE books SET user_locked=1")
        decision = self.ingest(self.event())
        self.assertIn("user_locked", decision["conflicts"])
        self.assertFalse(self.app.apply(decision["id"], 1)["success"])
        self.mutate("UPDATE books SET user_locked=0")
        other = self.ingest(self.event(2, before={"narrator": "Not Local Narrator"}))
        self.assertIn("original_mismatch:narrator", other["conflicts"])
        self.assertFalse(other["can_apply"])

    def test_field_user_source_is_protected(self):
        profile = self.book()["decoded"]
        profile["narrator"]["sources"] = ["user"]
        self.mutate("UPDATE books SET profile=?", (json.dumps(profile),))
        decision = self.ingest(self.event())
        self.assertIn("user_field:narrator", decision["conflicts"])
        self.assertFalse(decision["can_apply"])

    def test_pending_move_or_queue_blocks_apply(self):
        self.mutate("INSERT INTO history VALUES(1,'pending_fix')")
        decision = self.ingest(self.event())
        self.assertIn("pending_work", decision["conflicts"])
        self.assertFalse(decision["can_apply"])

    def test_concurrent_profile_edit_and_revision_are_rechecked(self):
        decision = self.ingest(self.event())
        self.assertEqual(self.app.apply(decision["id"], 999)["error"], "stale_revision")
        profile = self.book()["decoded"]
        profile["edition"] = {"value": "User changed this meanwhile"}
        self.mutate("UPDATE books SET profile=?", (json.dumps(profile),))
        result = self.app.apply(decision["id"], 1)
        self.assertFalse(result["success"])
        self.assertIn("stale_snapshot", result["conflicts"])

    def test_missing_original_and_multiple_local_rows_require_review(self):
        self.settings["auto_apply_corrections"] = True
        self.add_book(2)
        decision = self.ingest(self.event(before={}))
        self.assertEqual(decision["status"], "pending")
        self.assertIn("missing_original", decision["conflicts"])
        self.assertIn("multiple_local_matches", decision["conflicts"])
        self.assertTrue(self.app.apply(decision["id"], 1)["success"])

    def test_manual_chain_refreshes_revision_only_from_committed_predecessor(self):
        first = self.ingest(self.event(after={"narrator": "Middle Narrator"}))
        second = self.ingest(self.event(2, before={"narrator": "Middle Narrator"},
                                        after={"narrator": "Final Narrator"}))
        self.assertFalse(second["can_apply"])
        self.assertTrue(self.app.apply(first["id"], 1)["success"])
        refreshed = self.app.detail(second["id"])
        self.assertTrue(refreshed["can_apply"])
        self.assertEqual(refreshed["revision"], 2)
        self.assertEqual(self.app.apply(second["id"], 1)["error"], "stale_revision")
        profile = self.book()["decoded"]
        profile["edition"] = {"value": "External edit after predecessor"}
        self.mutate("UPDATE books SET profile=?", (json.dumps(profile),))
        self.assertFalse(self.app.apply(second["id"], 2)["success"])

    def test_manual_chain_can_complete_after_reloading_review(self):
        first = self.ingest(self.event(after={"narrator": "Middle Narrator"}))
        second = self.ingest(self.event(2, before={"narrator": "Middle Narrator"},
                                        after={"narrator": "Final Narrator"}))
        self.app.apply(first["id"], 1)
        refreshed = self.app.detail(second["id"])
        self.assertTrue(self.app.apply(second["id"], refreshed["revision"])["success"])
        self.assertEqual(self.book()["decoded"]["narrator"]["value"], "Final Narrator")

    def test_reviewed_identity_replacement_and_explicit_null(self):
        self.settings["auto_apply_corrections"] = True
        decision = self.ingest(self.event(before={"skaldleita_book_id": 42, "narrator": "Old Narrator"},
            after={"skaldleita_book_id": 84, "narrator": None, "sl_id": None}, reason="wrong_book"))
        self.assertEqual(decision["status"], "pending")
        self.assertTrue(self.app.apply(decision["id"], 1)["success"])
        profile = self.book()["decoded"]
        self.assertEqual(profile["skaldleita_book_id"], "84")
        self.assertEqual(profile["skaldleita_source_url"], SOURCE)
        self.assertIsNone(profile["narrator"]["value"])

    def test_numeric_semantic_comparison_and_null_identity_clear(self):
        decision = self.ingest(self.event(
            before={"series_position": 2, "year": 2020, "skaldleita_book_id": 42},
            after={"series_position": 2.5, "year": 2026, "skaldleita_book_id": None}))
        self.assertTrue(decision["can_apply"])
        self.assertTrue(self.app.apply(decision["id"], 1)["success"])
        self.assertNotIn("skaldleita_book_id", self.book()["decoded"])
        self.assertNotIn("skaldleita_source_url", self.book()["decoded"])

    def test_unsupported_identity_patch_never_partially_applies(self):
        decision = self.ingest(self.event(after={"narrator": "Correct Narrator", "sl_id": "SL-opaque"}))
        self.assertIn("unsupported_sl_id", decision["conflicts"])
        self.assertFalse(self.app.apply(decision["id"], 1)["success"])
        self.assertEqual(self.book()["decoded"]["narrator"]["value"], "Old Narrator")

    def test_receipt_failure_rolls_back_metadata_and_decision(self):
        decision = self.ingest(self.event())
        self.mutate("""CREATE TRIGGER refuse_receipt BEFORE INSERT ON correction_application_receipts
                       BEGIN SELECT RAISE(ABORT,'fixture receipt failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.app.apply(decision["id"], 1)
        self.assertEqual(self.book()["decoded"]["narrator"]["value"], "Old Narrator")
        self.assertEqual(self.app.detail(decision["id"])["status"], "pending")

    def test_dismiss_and_generation_replay_do_not_reapply(self):
        decision = self.ingest(self.event())
        self.assertTrue(self.app.dismiss(decision["id"], 1)["success"])
        conn = self.connect()
        rebootstrap(conn, checkpoint(conn, SOURCE))
        conn.close()
        replayed = self.ingest(self.event())
        self.assertEqual(replayed["status"], "dismissed")
        self.assertFalse(replayed["can_apply"])
        distinct = self.ingest(self.event(2, after={"narrator": "Different Correction"}))
        self.assertEqual(distinct["status"], "pending")

    def test_reset_replay_after_identity_replacement_preserves_original_receipt(self):
        event = self.event(before={"skaldleita_book_id": 42}, after={"skaldleita_book_id": 84},
                           reason="wrong_book")
        decision = self.ingest(event)
        self.assertTrue(self.app.apply(decision["id"], 1)["success"])
        original = self.app.detail(decision["id"])
        book_before_reset = self.book()
        conn = self.connect()
        rebootstrap(conn, checkpoint(conn, SOURCE))
        conn.close()
        replay = self.ingest(event)
        self.assertEqual(replay["status"], "applied")
        self.assertEqual(replay["book_id"], 1)
        self.assertEqual(replay["prior_decision_id"], decision["id"])
        self.assertEqual(replay["receipts"], original["receipts"])
        self.assertEqual(self.app.summary()["pending"], 0)
        self.assertEqual(self.book(), book_before_reset)
        conn = self.connect()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM correction_application_receipts").fetchone()[0], 1)
        conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
