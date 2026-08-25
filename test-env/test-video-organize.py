#!/usr/bin/env python3
"""Phase-4 video organization is gated, verified, and reversible."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import sqlite3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.video.organize import (  # noqa: E402
    OrganizationRefused,
    PlaybackObservation,
    VideoMovePlan,
    execute_move,
)


PLAN = VideoMovePlan(
    subject_id="opaque-catalogue-item",
    provider="tmdb",
    provider_id="opaque-provider-id",
    media_kind="movie",
    movie_length=True,
    source_root_ref="source-root",
    destination_root_ref="documentary-root",
    destination_library_ref="documentary-library",
    approved=True,
)


class Catalogue:
    def __init__(self, *, destination_ok=True, rollback_ok=True, move_ok=True):
        self.root = PLAN.source_root_ref
        self.destination_ok = destination_ok
        self.rollback_ok = rollback_ok
        self.move_ok = move_ok
        self.calls = []

    async def move_root(self, subject_id, root_ref, *, move_files):
        self.calls.append((subject_id, root_ref, move_files))
        if root_ref == PLAN.destination_root_ref and not self.move_ok:
            return False
        if root_ref == PLAN.source_root_ref and not self.rollback_ok:
            return False
        self.root = root_ref
        return True

    async def root_is(self, subject_id, root_ref):
        if root_ref == PLAN.destination_root_ref and not self.destination_ok:
            return False
        if root_ref == PLAN.source_root_ref and not self.rollback_ok:
            return False
        return subject_id == PLAN.subject_id and self.root == root_ref


class Visibility:
    def __init__(self, visible=True):
        self.visible = visible
        self.calls = []

    async def visible_in(self, provider, provider_id, library_ref):
        self.calls.append((provider, provider_id, library_ref))
        return self.visible


async def idle():
    return PlaybackObservation(observed=True, busy=False)


def connection():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def expect_refused(plan, probe=idle):
    conn = connection()
    catalogue = Catalogue()
    try:
        asyncio.run(execute_move(plan, conn=conn, catalogue=catalogue,
                                 visibility=Visibility(), idle_probe=probe))
        raise AssertionError("unsafe plan was accepted")
    except OrganizationRefused:
        pass
    assert catalogue.calls == []
    assert conn.execute("SELECT name FROM sqlite_master WHERE name='video_move_receipts'").fetchone() is None
    conn.close()


def test_policy_refusals():
    expect_refused(replace(PLAN, approved=False))
    expect_refused(replace(PLAN, media_kind="episode", movie_length=False))
    expect_refused(replace(PLAN, movie_length=False))
    expect_refused(replace(PLAN, provider_id=""))

    async def unobserved():
        return PlaybackObservation(observed=False, busy=False)

    async def busy():
        return PlaybackObservation(observed=True, busy=True)

    expect_refused(PLAN, unobserved)
    expect_refused(PLAN, busy)
    print("[PASS] approval, movie-only scope, identity, and idle evidence gate mutation")


def test_committed_receipt_is_private():
    conn = connection()
    catalogue = Catalogue()
    visibility = Visibility()
    result = asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=visibility, idle_probe=idle))
    row = dict(conn.execute("SELECT * FROM video_move_receipts").fetchone())
    assert result["status"] == "committed"
    assert catalogue.calls == [(PLAN.subject_id, PLAN.destination_root_ref, True)]
    assert visibility.calls == [(PLAN.provider, PLAN.provider_id,
                                 PLAN.destination_library_ref)]
    assert row["move_files"] == 1 and row["idle_observed"] == 1
    assert row["destination_verified"] == 1 and row["library_verified"] == 1
    serialized = repr(row)
    for private in (PLAN.subject_id, PLAN.provider_id, PLAN.source_root_ref,
                    PLAN.destination_root_ref, PLAN.destination_library_ref):
        assert private not in serialized
    conn.close()
    print("[PASS] committed move verifies catalogue and exact library identity without private receipt data")


def test_duplicate_operation_is_idempotently_refused():
    conn = connection()
    catalogue = Catalogue()
    asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=Visibility(), idle_probe=idle))
    try:
        asyncio.run(execute_move(
            PLAN, conn=conn, catalogue=catalogue,
            visibility=Visibility(), idle_probe=idle))
        raise AssertionError("duplicate move was accepted")
    except OrganizationRefused as exc:
        assert str(exc) == "move_receipt_exists"
    assert len(catalogue.calls) == 1
    assert conn.execute("SELECT COUNT(*) FROM video_move_receipts").fetchone()[0] == 1
    conn.close()
    print("[PASS] one active/committed receipt prevents duplicate provider mutation")


def test_failed_destination_rolls_back():
    conn = connection()
    catalogue = Catalogue(destination_ok=False)
    result = asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=Visibility(), idle_probe=idle))
    row = dict(conn.execute("SELECT * FROM video_move_receipts").fetchone())
    assert result["status"] == "rolled_back" and result["rollback_verified"]
    assert [call[1] for call in catalogue.calls] == [
        PLAN.destination_root_ref, PLAN.source_root_ref]
    assert row["rollback_verified"] == 1 and row["library_verified"] == 0
    conn.close()
    print("[PASS] failed destination verification restores and verifies the exact source root")


def test_refused_move_records_no_false_rollback():
    conn = connection()
    catalogue = Catalogue(move_ok=False)
    result = asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=Visibility(), idle_probe=idle))
    row = dict(conn.execute("SELECT * FROM video_move_receipts").fetchone())
    assert result["status"] == "failed_before_move"
    assert row["rollback_verified"] == 0
    assert [call[1] for call in catalogue.calls] == [PLAN.destination_root_ref]
    conn.close()
    print("[PASS] provider refusal records a pre-move failure without fake rollback")


def test_failed_library_rolls_back():
    conn = connection()
    catalogue = Catalogue()
    result = asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=Visibility(visible=False), idle_probe=idle))
    row = dict(conn.execute("SELECT * FROM video_move_receipts").fetchone())
    assert result["status"] == "rolled_back" and result["rollback_verified"]
    assert row["destination_verified"] == 1 and row["library_verified"] == 0
    conn.close()
    print("[PASS] absent media-server identity rolls back after destination proof")


def test_unverified_rollback_requires_manual_recovery():
    conn = connection()
    catalogue = Catalogue(destination_ok=False, rollback_ok=False)
    result = asyncio.run(execute_move(
        PLAN, conn=conn, catalogue=catalogue,
        visibility=Visibility(), idle_probe=idle))
    row = dict(conn.execute("SELECT * FROM video_move_receipts").fetchone())
    assert result["status"] == "manual_recovery"
    assert row["rollback_verified"] == 0
    conn.close()
    print("[PASS] unverified rollback is preserved as manual recovery")


if __name__ == "__main__":
    test_policy_refusals()
    test_committed_receipt_is_private()
    test_duplicate_operation_is_idempotently_refused()
    test_refused_move_records_no_false_rollback()
    test_failed_destination_rolls_back()
    test_failed_library_rolls_back()
    test_unverified_rollback_requires_manual_recovery()
    print("All video organization tests passed.")
