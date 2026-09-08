"""Review and apply source-bound corrections to stored metadata only."""

import hashlib
import json
import math

from library_manager.providers.bookdb import get_bookdb_url
from library_manager.utils.skaldleita_identity import skaldleita_identity
from library_manager.utils.validation import looks_like_asin
from .feed import source_namespace
from .store import list_events


FIELDS = {
    "author": "author", "title": "title", "narrator": "narrator",
    "series": "series", "series_position": "series_num", "language": "language",
    "year": "year",
}
IDENTITY_FIELDS = {"skaldleita_book_id", "sl_id", "asin"}
TERMINAL = {"applied", "dismissed", "superseded"}
AUTO_ONLY = {"missing_original", "identity_change", "identity_reason", "multiple_local_matches"}
SCHEMA = (
    """CREATE TABLE IF NOT EXISTS correction_decisions (
        id INTEGER PRIMARY KEY, source TEXT NOT NULL, generation INTEGER NOT NULL,
        event_id TEXT NOT NULL, reconciliation_key TEXT NOT NULL, book_id INTEGER NOT NULL,
        event_json TEXT NOT NULL, snapshot_json TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL, conflicts_json TEXT NOT NULL, prior_decision_id INTEGER,
        UNIQUE(source,generation,event_id,book_id))""",
    """CREATE TABLE IF NOT EXISTS correction_application_receipts (
        id INTEGER PRIMARY KEY, decision_id INTEGER NOT NULL UNIQUE,
        actor TEXT NOT NULL, before_json TEXT NOT NULL, after_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS correction_application_progress (
        source TEXT PRIMARY KEY, ledger_id INTEGER NOT NULL DEFAULT 0,
        retry_decision_id INTEGER NOT NULL DEFAULT 0)""",
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _profile(book):
    try:
        value = json.loads(book.get("profile") or "{}")
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError):
        return {}


def _value(value):
    return value.get("value") if isinstance(value, dict) else value


def _semantic(field, value):
    value = _value(value)
    if value is None:
        return None
    if field == "year":
        if type(value) is int:
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return value
        return value
    if field == "series_position":
        try:
            number = float(value)
            return number if math.isfinite(number) and not isinstance(value, bool) else value
        except (TypeError, ValueError, OverflowError):
            return value
    if field == "skaldleita_book_id":
        return str(value)
    return value


def _metadata(book):
    profile = _profile(book)
    result = {name: _semantic(name, profile.get(local)) for name, local in FIELDS.items()}
    result["author"] = book.get("current_author")
    result["title"] = book.get("current_title")
    result["skaldleita_book_id"] = skaldleita_identity(profile).get("skaldleita_book_id")
    result["sl_id"] = None  # No established opaque SL-ID provenance in legacy LM profiles.
    identifier = profile.get("book_id")
    result["asin"] = identifier if isinstance(identifier, str) and looks_like_asin(identifier) else None
    return result


def _snapshot(book):
    return {key: book.get(key) for key in (
        "id", "profile", "current_author", "current_title", "path", "status",
        "user_locked", "updated_at", "languages",
    )}


def _stamp(snapshot):
    return hashlib.sha256(_json(snapshot).encode()).hexdigest()


def _book(conn, book_id):
    cursor = conn.execute("SELECT * FROM books WHERE id=?", (book_id,))
    row = cursor.fetchone()
    return dict(zip((column[0] for column in cursor.description), row)) if row else None


def _rows(conn, sql, args=()):
    cursor = conn.execute(sql, args)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _pending_work(conn, book_id):
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "queue" in tables and conn.execute("SELECT 1 FROM queue WHERE book_id=? LIMIT 1", (book_id,)).fetchone():
        return True
    return bool("history" in tables and conn.execute(
        "SELECT 1 FROM history WHERE book_id=? AND status IN ('pending_fix','applying') LIMIT 1",
        (book_id,),
    ).fetchone())


def _conflicts(conn, book, event, source, multiple=False):
    if book is None:
        return ["unmatched"]
    profile = _profile(book)
    identity = skaldleita_identity(profile)
    targets = {str(value) for value in event["affected_skaldleita_book_ids"]}
    conflicts = []
    if identity.get("skaldleita_source_url") != source or identity.get("skaldleita_book_id") not in targets:
        conflicts.append("identity_mismatch")
    if book.get("user_locked"):
        conflicts.append("user_locked")
    if _pending_work(conn, book["id"]):
        conflicts.append("pending_work")
    current = _metadata(book)
    for field, before in event["original_result"].items():
        if _semantic(field, current.get(field)) != _semantic(field, before):
            conflicts.append("original_mismatch:" + field)
    for field, after in event["corrected_result"].items():
        local = FIELDS.get(field)
        entry = profile.get(local) if local else None
        if isinstance(entry, dict):
            sources = entry.get("sources") or []
            weights = entry.get("source_weights") or {}
            if not isinstance(sources, (list, tuple, str)) or not isinstance(weights, dict):
                conflicts.append("invalid_profile:" + field)
            elif entry.get("source") == "user" or "user" in sources or "user" in weights:
                conflicts.append("user_field:" + field)
        if field not in event["original_result"] and _semantic(field, current.get(field)) != _semantic(field, after):
            conflicts.append("missing_original")
        if field in IDENTITY_FIELDS and _semantic(field, current.get(field)) != _semantic(field, after):
            conflicts.append("identity_change")
        if field == "sl_id" and (after is not None or profile.get("sl_id") is not None):
            conflicts.append("unsupported_sl_id")
        if field == "asin" and after is not None and not looks_like_asin(after):
            conflicts.append("invalid_asin")
    if event["reason"] in ("wrong_book", "merged_duplicate"):
        conflicts.append("identity_reason")
    if multiple:
        conflicts.append("multiple_local_matches")
    return sorted(set(conflicts))


def _hard_conflicts(conflicts):
    return [value for value in conflicts if value not in AUTO_ONLY]


def _patched(book, event, source):
    updated = dict(book)
    profile = _profile(book)
    for field, value in event["corrected_result"].items():
        if field in FIELDS:
            local = FIELDS[field]
            profile[local] = {"value": value, "confidence": 100,
                              "sources": ["skaldleita_correction"],
                              "source_weights": {"skaldleita_correction": 100}}
            if field in ("author", "title"):
                updated["current_" + field] = value
            if field == "language":
                profile["detected_language"] = value
                profile["languages"] = [] if value is None else [value]
                if "languages" in updated:
                    updated["languages"] = json.dumps(profile["languages"])
        elif field == "asin":
            if value is not None or looks_like_asin(profile.get("book_id") or ""):
                profile["book_id"] = value
        elif field == "skaldleita_book_id":
            profile.pop("skaldleita_book_id", None)
            profile.pop("skaldleita_source_url", None)
            if value is not None:
                profile["skaldleita_book_id"] = str(value)
                profile["skaldleita_source_url"] = source
        # sl_id=null is a no-op when there is no established opaque identity.
    updated["profile"] = _json(profile)
    return updated


class CorrectionsApplication:
    def __init__(self, get_db, load_config):
        self.get_db = get_db
        self.load_config = load_config

    def _source(self):
        return source_namespace(get_bookdb_url(self.load_config().get("bookdb_url")))

    def initialize(self):
        conn = self.get_db()
        try:
            with conn:
                for statement in SCHEMA:
                    conn.execute(statement)
                columns = {row[1] for row in conn.execute("PRAGMA table_info(correction_application_progress)")}
                if "retry_decision_id" not in columns:
                    conn.execute("ALTER TABLE correction_application_progress ADD COLUMN retry_decision_id INTEGER NOT NULL DEFAULT 0")
                columns = {row[1] for row in conn.execute("PRAGMA table_info(correction_decisions)")}
                if "prior_decision_id" not in columns:
                    conn.execute("ALTER TABLE correction_decisions ADD COLUMN prior_decision_id INTEGER")
        finally:
            conn.close()

    def _record(self, conn, source, event, index):
        payload = event["payload"]
        matched = []
        for target in set(payload["affected_skaldleita_book_ids"]):
            for book_id in index.get(str(target), []):
                # Each preceding correction may have changed this book. Build
                # the comparison under the current transaction, not an old batch.
                row = _book(conn, book_id)
                if row:
                    matched.append(row)
        if not matched:
            # A reviewed identity replacement can remove the old target from
            # current profiles. A reset replay remains historical applied work;
            # do not invent a new match or create a misleading unmatched item.
            prior_applied = _rows(conn, """SELECT * FROM correction_decisions
                WHERE source=? AND generation<>? AND reconciliation_key=?
                  AND status='applied' AND book_id<>0 ORDER BY id""",
                (source, event["generation"], event["reconciliation_key"]))
            if prior_applied:
                for prior in prior_applied:
                    conn.execute("""INSERT OR IGNORE INTO correction_decisions
                        (source,generation,event_id,reconciliation_key,book_id,event_json,
                         snapshot_json,status,conflicts_json,prior_decision_id)
                        VALUES(?,?,?,?,?,?,?,'applied','[]',?)""",
                        (source, event["generation"], event["event_id"], event["reconciliation_key"],
                         prior["book_id"], _json(payload), prior["snapshot_json"],
                         prior["prior_decision_id"] or prior["id"]))
                return []
        if matched:
            conn.execute("""UPDATE correction_decisions SET status='superseded',revision=revision+1
                            WHERE source=? AND generation=? AND event_id=? AND book_id=0 AND status='unmatched'""",
                         (source, event["generation"], event["event_id"]))
        ids = []
        for book in matched or [None]:
            book_id = book["id"] if book else 0
            existing = conn.execute("""SELECT id,status FROM correction_decisions
                WHERE source=? AND generation=? AND event_id=? AND book_id=?""",
                (source, event["generation"], event["event_id"], book_id)).fetchone()
            if existing:
                continue
            conflicts = _conflicts(conn, book, payload, source, len(matched) > 1)
            status = "unmatched" if book is None else ("conflict" if _hard_conflicts(conflicts) else "pending")
            # A replaced feed may reuse event IDs. Reconcile already reviewed
            # semantic content across generations, never silently apply it twice.
            prior = conn.execute("""SELECT status FROM correction_decisions
                WHERE source=? AND generation<>? AND reconciliation_key=? AND book_id=?
                AND status IN ('applied','dismissed') ORDER BY id DESC LIMIT 1""",
                (source, event["generation"], event["reconciliation_key"], book_id)).fetchone()
            if prior:
                status = prior[0]
            cursor = conn.execute("""INSERT INTO correction_decisions
                (source,generation,event_id,reconciliation_key,book_id,event_json,snapshot_json,status,conflicts_json)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                (source, event["generation"], event["event_id"], event["reconciliation_key"], book_id,
                 _json(payload), _json(_snapshot(book) if book else {}), status, _json(conflicts)))
            if status == "pending" and not conflicts:
                ids.append(cursor.lastrowid)
        return ids

    def reconcile(self, source=None):
        config = self.load_config()
        active = self._source()
        if config.get("receive_corrections", False) is not True:
            return {"success": True, "status": "disabled"}
        if source is not None and source_namespace(source) != active:
            return {"success": False, "error": "inactive_source"}
        conn = self.get_db()
        try:
            # One profile scan per bounded reconciliation batch, not per event.
            index = {}
            for row in conn.execute("SELECT id,profile FROM books"):
                identity = skaldleita_identity(row[1])
                if identity.get("skaldleita_source_url") == active:
                    index.setdefault(identity["skaldleita_book_id"], []).append(row[0])
            progress = conn.execute("SELECT ledger_id,retry_decision_id FROM correction_application_progress WHERE source=?", (active,)).fetchone()
            events = list_events(conn, active, after_rowid=progress[0] if progress else 0, limit=100)
            # Retry a durable pending decision after a callback/process failure.
            # Apply's original/snapshot/lock checks still protect intervening edits.
            if config.get("auto_apply_corrections", False) is True:
                for row in _rows(conn, "SELECT id,revision FROM correction_decisions WHERE source=? AND status='pending' ORDER BY id LIMIT 100", (active,)):
                    self.apply(row["id"], row["revision"], explicit=False)
            retry_rows = _rows(conn, """SELECT * FROM correction_decisions WHERE source=? AND status='unmatched'
                AND id>? ORDER BY id LIMIT 100""", (active, progress[1] if progress else 0))
            if not retry_rows:
                retry_rows = _rows(conn, "SELECT * FROM correction_decisions WHERE source=? AND status='unmatched' ORDER BY id LIMIT 100", (active,))
            retries = [{"generation": row["generation"], "event_id": row["event_id"],
                        "reconciliation_key": row["reconciliation_key"], "payload": json.loads(row["event_json"])}
                       for row in retry_rows]
            for event in events + retries:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    auto = self._record(conn, active, event, index)
                    if "ledger_id" in event:
                        conn.execute("""INSERT INTO correction_application_progress(source,ledger_id) VALUES(?,?)
                            ON CONFLICT(source) DO UPDATE SET ledger_id=excluded.ledger_id""", (active, event["ledger_id"]))
                # Drain in immutable event order so B's original values can
                # compare with A's committed metadata, while never forcing a conflict.
                if config.get("auto_apply_corrections", False) is True:
                    for decision_id in auto:
                        self.apply(decision_id, 1, explicit=False)
            if retry_rows:
                with conn:
                    conn.execute("""INSERT INTO correction_application_progress(source,retry_decision_id) VALUES(?,?)
                        ON CONFLICT(source) DO UPDATE SET retry_decision_id=excluded.retry_decision_id""",
                        (active, retry_rows[-1]["id"]))
        finally:
            conn.close()
        return {"success": True, "status": "reconciled", "processed": len(events)}

    def _detail(self, conn, row):
        event = json.loads(row["event_json"])
        snapshot = json.loads(row["snapshot_json"])
        book = _book(conn, row["book_id"]) if row["book_id"] else None
        conflicts = json.loads(row["conflicts_json"])
        if book is not None and row["status"] not in TERMINAL:
            conflicts = sorted(set(conflicts + _conflicts(conn, book, event, row["source"])))
            if _stamp(_snapshot(book)) != _stamp(snapshot):
                conflicts.append("stale_snapshot")
        before = _metadata(snapshot) if snapshot else {}
        after = _metadata(_patched(snapshot, event, row["source"])) if snapshot else {}
        active = row["source"] == self._source() and self.load_config().get("receive_corrections", False) is True
        return {"id": row["id"], "revision": row["revision"], "status": row["status"],
                "book_id": row["book_id"] or None, "title": before.get("title"),
                "reason": event["reason"], "event": event, "before": before, "after": after,
                "conflicts": sorted(set(conflicts)), "source": row["source"],
                "prior_decision_id": row["prior_decision_id"],
                "can_apply": bool(active and book and row["status"] not in TERMINAL and not _hard_conflicts(conflicts)),
                "can_dismiss": row["status"] not in TERMINAL,
                "receipts": _rows(conn, "SELECT id,actor,created_at FROM correction_application_receipts WHERE decision_id=?",
                                  (row["prior_decision_id"] or row["id"],))}

    def detail(self, decision_id):
        conn = self.get_db()
        try:
            rows = _rows(conn, "SELECT * FROM correction_decisions WHERE id=?", (decision_id,))
            return self._detail(conn, rows[0]) if rows else None
        finally:
            conn.close()

    def list_decisions(self, status="all", limit=100):
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("invalid_limit")
        conn = self.get_db()
        try:
            rows = _rows(conn, """SELECT * FROM correction_decisions
                WHERE (?='all' OR status=?) ORDER BY id DESC LIMIT ?""", (status, status, limit))
            return [self._detail(conn, row) for row in rows]
        finally:
            conn.close()

    def list_pending(self):
        return [row for row in self.list_decisions(limit=1000) if row["status"] not in TERMINAL]

    def summary(self):
        conn = self.get_db()
        try:
            counts = dict(conn.execute("SELECT status,COUNT(*) FROM correction_decisions WHERE source=? GROUP BY status", (self._source(),)))
            return {"counts": counts, "pending": sum(count for status, count in counts.items() if status not in TERMINAL)}
        finally:
            conn.close()

    def apply(self, decision_id, expected_revision, explicit=True):
        config = self.load_config()
        if type(explicit) is not bool:
            return {"success": False, "error": "invalid_decision"}
        if config.get("receive_corrections", False) is not True or (not explicit and config.get("auto_apply_corrections", False) is not True):
            return {"success": False, "error": "disabled"}
        conn = self.get_db()
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                rows = _rows(conn, "SELECT * FROM correction_decisions WHERE id=?", (decision_id,))
                if not rows:
                    return {"success": False, "error": "not_found"}
                row = rows[0]
                if type(expected_revision) is not int or row["revision"] != expected_revision:
                    return {"success": False, "error": "stale_revision"}
                detail = self._detail(conn, row)
                if not detail["can_apply"] or (not explicit and detail["conflicts"]):
                    return {"success": False, "error": "review_required", "conflicts": detail["conflicts"]}
                book = _book(conn, row["book_id"])
                after = _patched(book, detail["event"], row["source"])
                columns = ["profile", "current_author", "current_title"]
                if "languages" in after:
                    columns.append("languages")
                conn.execute("UPDATE books SET " + ",".join(name + "=?" for name in columns) +
                             ",updated_at=CURRENT_TIMESTAMP WHERE id=?",
                             tuple(after[name] for name in columns) + (book["id"],))
                after = _book(conn, book["id"])
                conn.execute("""INSERT INTO correction_application_receipts
                    (decision_id,actor,before_json,after_json) VALUES(?,?,?,?)""",
                    (decision_id, "review" if explicit else "automatic", _json(_snapshot(book)), _json(_snapshot(after))))
                conn.execute("UPDATE correction_decisions SET status='applied',revision=revision+1 WHERE id=?", (decision_id,))
                # A later review may depend on this exact preceding correction.
                # Refresh only snapshots equal to our known before-state, never
                # snapshots made stale by an unrelated user/pipeline edit.
                for successor in _rows(conn, """SELECT * FROM correction_decisions
                    WHERE source=? AND book_id=? AND id>? AND status IN ('pending','conflict')""",
                    (row["source"], book["id"], decision_id)):
                    if _stamp(json.loads(successor["snapshot_json"])) != _stamp(_snapshot(book)):
                        continue
                    conflicts = _conflicts(conn, after, json.loads(successor["event_json"]), row["source"],
                                           "multiple_local_matches" in json.loads(successor["conflicts_json"]))
                    if not _hard_conflicts(conflicts):
                        conn.execute("""UPDATE correction_decisions SET snapshot_json=?,conflicts_json=?,
                            status='pending',revision=revision+1 WHERE id=?""",
                            (_json(_snapshot(after)), _json(conflicts), successor["id"]))
            return {"success": True, "status": "applied", "revision": expected_revision + 1}
        finally:
            conn.close()

    def dismiss(self, decision_id, expected_revision):
        conn = self.get_db()
        try:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT status,revision FROM correction_decisions WHERE id=?", (decision_id,)).fetchone()
                if row is None:
                    return {"success": False, "error": "not_found"}
                if type(expected_revision) is not int or row[1] != expected_revision:
                    return {"success": False, "error": "stale_revision"}
                if row[0] in TERMINAL:
                    return {"success": False, "error": "already_decided"}
                conn.execute("UPDATE correction_decisions SET status='dismissed',revision=revision+1 WHERE id=?", (decision_id,))
            return {"success": True, "status": "dismissed", "revision": expected_revision + 1}
        finally:
            conn.close()
