"""Capability-bounded video root moves with verified provider rollback.

The coordinator owns policy and receipts, while injected adapters own product-specific
catalogue and media-server calls.  No title or filesystem path is persisted: receipts
contain only digests, opaque operation state, and verification booleans.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Protocol


class OrganizationRefused(RuntimeError):
    """Raised before mutation when a required authority fact is absent."""


@dataclass(frozen=True)
class PlaybackObservation:
    observed: bool
    busy: bool


@dataclass(frozen=True)
class VideoMovePlan:
    approval_ref: str
    subject_id: str
    provider: str
    provider_id: str
    media_kind: str
    movie_length: bool
    source_root_ref: str
    destination_root_ref: str
    destination_library_ref: str
    approved: bool


class CatalogueAdapter(Protocol):
    async def move_root(self, subject_id: str, root_ref: str, *,
                        move_files: bool) -> bool: ...

    async def root_is(self, subject_id: str, root_ref: str) -> bool: ...


class VisibilityAdapter(Protocol):
    async def visible_in(self, provider: str, provider_id: str,
                         library_ref: str) -> bool: ...


IdleProbe = Callable[[], Awaitable[PlaybackObservation]]


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the additive, path-neutral operation ledger."""
    conn.execute('''CREATE TABLE IF NOT EXISTS video_move_receipts (
        operation_id TEXT PRIMARY KEY,
        operation_key TEXT NOT NULL,
        approval_digest TEXT NOT NULL,
        subject_digest TEXT NOT NULL,
        identity_digest TEXT NOT NULL,
        source_root_digest TEXT NOT NULL,
        destination_root_digest TEXT NOT NULL,
        destination_library_digest TEXT NOT NULL,
        media_kind TEXT NOT NULL,
        move_files INTEGER NOT NULL,
        idle_observed INTEGER NOT NULL,
        destination_verified INTEGER NOT NULL DEFAULT 0,
        library_verified INTEGER NOT NULL DEFAULT 0,
        rollback_verified INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        error_code TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_video_move_active_key
                    ON video_move_receipts(operation_key)
                    WHERE status NOT IN ('rolled_back', 'failed_before_move')''')
    conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_video_move_active_approval
                    ON video_move_receipts(approval_digest)
                    WHERE status NOT IN ('rolled_back', 'failed_before_move')''')
    conn.commit()


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    return 'sha256:' + hashlib.sha256(encoded).hexdigest()


def _validate(plan: VideoMovePlan) -> None:
    if not plan.approved:
        raise OrganizationRefused("owner_approval_required")
    if plan.media_kind != "movie" or not plan.movie_length:
        raise OrganizationRefused("movie_length_movies_only")
    values = (plan.approval_ref, plan.subject_id, plan.provider, plan.provider_id,
              plan.source_root_ref, plan.destination_root_ref,
              plan.destination_library_ref)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise OrganizationRefused("move_identity_incomplete")
    if plan.source_root_ref == plan.destination_root_ref:
        raise OrganizationRefused("source_and_destination_match")


def _insert_receipt(conn: sqlite3.Connection, operation_id: str,
                    plan: VideoMovePlan) -> None:
    operation_key = _digest({
        "subject_id": plan.subject_id,
        "approval_ref": plan.approval_ref,
        "provider": plan.provider,
        "provider_id": plan.provider_id,
        "source_root_ref": plan.source_root_ref,
        "destination_root_ref": plan.destination_root_ref,
    })
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute('''INSERT INTO video_move_receipts (
        operation_id, operation_key, approval_digest, subject_digest, identity_digest, source_root_digest,
        destination_root_digest, destination_library_digest, media_kind,
        move_files, idle_observed, status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 'prepared')''', (
        operation_id,
        operation_key,
        _digest({"approval_ref": plan.approval_ref}),
        _digest({"subject_id": plan.subject_id}),
        _digest({"provider": plan.provider, "provider_id": plan.provider_id}),
        _digest({"root_ref": plan.source_root_ref}),
        _digest({"root_ref": plan.destination_root_ref}),
        _digest({"library_ref": plan.destination_library_ref}),
        plan.media_kind,
        ))
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise OrganizationRefused("move_receipt_exists") from exc


def _update(conn: sqlite3.Connection, operation_id: str, *, status: str,
            destination_verified: bool = False, library_verified: bool = False,
            rollback_verified: bool = False, error_code: Optional[str] = None) -> None:
    conn.execute('''UPDATE video_move_receipts
                    SET status = ?, destination_verified = ?, library_verified = ?,
                        rollback_verified = ?, error_code = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE operation_id = ?''', (
        status, int(destination_verified), int(library_verified),
        int(rollback_verified), error_code, operation_id,
    ))
    conn.commit()


async def execute_move(plan: VideoMovePlan, *, conn: sqlite3.Connection,
                       catalogue: CatalogueAdapter,
                       visibility: VisibilityAdapter,
                       idle_probe: IdleProbe) -> dict:
    """Execute one approved move and return a path/title-free receipt summary."""
    _validate(plan)
    idle = await idle_probe()
    if not isinstance(idle, PlaybackObservation) or not idle.observed:
        raise OrganizationRefused("playback_observation_unavailable")
    if idle.busy:
        raise OrganizationRefused("playback_active")

    ensure_schema(conn)
    operation_id = "vm_" + uuid.uuid4().hex
    _insert_receipt(conn, operation_id, plan)
    destination_verified = False
    library_verified = False
    moved = False
    try:
        moved = bool(await catalogue.move_root(
            plan.subject_id, plan.destination_root_ref, move_files=True))
        if not moved:
            raise RuntimeError("catalogue_move_refused")
        _update(conn, operation_id, status="moved")

        destination_verified = bool(await catalogue.root_is(
            plan.subject_id, plan.destination_root_ref))
        if not destination_verified:
            raise RuntimeError("destination_not_verified")
        _update(conn, operation_id, status="destination_verified",
                destination_verified=True)

        library_verified = bool(await visibility.visible_in(
            plan.provider, plan.provider_id, plan.destination_library_ref))
        if not library_verified:
            raise RuntimeError("library_identity_not_verified")
        _update(conn, operation_id, status="committed",
                destination_verified=True, library_verified=True)
        return {"operation_id": operation_id, "status": "committed",
                "destination_verified": True, "library_verified": True,
                "rollback_verified": False}
    except Exception as exc:
        known_codes = {
            "catalogue_move_refused", "destination_not_verified",
            "library_identity_not_verified",
        }
        error_code = str(exc) if str(exc) in known_codes else type(exc).__name__
        rollback_verified = False
        if moved:
            try:
                restored = bool(await catalogue.move_root(
                    plan.subject_id, plan.source_root_ref, move_files=True))
                rollback_verified = restored and bool(await catalogue.root_is(
                    plan.subject_id, plan.source_root_ref))
            except Exception:
                rollback_verified = False
        status = ("failed_before_move" if not moved else
                  "rolled_back" if rollback_verified else "manual_recovery")
        _update(conn, operation_id, status=status,
                destination_verified=destination_verified,
                library_verified=library_verified,
                rollback_verified=rollback_verified,
                error_code=error_code)
        return {"operation_id": operation_id, "status": status,
                "destination_verified": destination_verified,
                "library_verified": library_verified,
                "rollback_verified": rollback_verified,
                "error_code": error_code}


__all__ = [
    "CatalogueAdapter", "OrganizationRefused", "PlaybackObservation",
    "VideoMovePlan", "VisibilityAdapter", "ensure_schema", "execute_move",
]
