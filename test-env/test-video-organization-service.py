#!/usr/bin/env python3
"""The loopback video move surface requires one exact signed plan."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.video.organize import PlaybackObservation  # noqa: E402
from video_organization_service import (  # noqa: E402
    REQUEST_SCHEMA,
    VERIFY_REQUEST_SCHEMA,
    ServiceConfig,
    ThreadingHTTPServer,
    VideoOrganizationService,
    capability_signature,
    make_handler,
    plan_from_payload,
    verification_from_payload,
)


SECRET = b"fixture-capability-secret-at-least-32-bytes"
PAYLOAD = {
    "schema": REQUEST_SCHEMA,
    "approval_ref": "approval-fixture-0001",
    "subject_id": "7",
    "provider": "tmdb",
    "provider_id": "123",
    "media_kind": "movie",
    "runtime_minutes": 90,
    "source_root_id": "movies",
    "destination_root_id": "documentaries",
    "destination_library_id": "documentaries",
}


def verification_payload(operation_id: str) -> dict:
    return {**PAYLOAD, "schema": VERIFY_REQUEST_SCHEMA,
            "operation_id": operation_id}


class Catalogue:
    def __init__(self):
        self.root = "/media/Movies"
        self.calls = []

    async def move_root(self, subject_id, root_ref, *, move_files):
        self.calls.append((subject_id, root_ref, move_files))
        self.root = root_ref
        return True

    async def root_is(self, subject_id, root_ref):
        return subject_id == "7" and self.root == root_ref


class Visibility:
    def __init__(self):
        self.visible = True

    async def visible_in(self, provider, provider_id, library_ref):
        return (self.visible and (provider, provider_id, library_ref)
                == ("tmdb", "123", "jf-docs"))


async def idle():
    return PlaybackObservation(observed=True, busy=False)


def config(root: str) -> ServiceConfig:
    return ServiceConfig(
        database=Path(root) / "receipts.db",
        capability_secret=SECRET,
        roots={"movies": "/media/Movies",
               "documentaries": "/media/Documentaries"},
        libraries={"documentaries": "jf-docs"},
    )


def test_plan_and_capability_boundary():
    with tempfile.TemporaryDirectory(prefix="lm-video-service-") as root:
        cfg = config(root)
        plan = plan_from_payload(PAYLOAD, cfg)
        assert plan.approval_ref == "approval-fixture-0001"
        assert plan.subject_id == "7" and plan.provider_id == "123"
        assert plan.source_root_ref == "/media/Movies"
        assert plan.destination_root_ref == "/media/Documentaries"
        assert plan.destination_library_ref == "jf-docs"
        assert plan.approved and plan.movie_length

        catalogue = Catalogue()
        visibility = Visibility()
        service = VideoOrganizationService(
            cfg, catalogue=catalogue, visibility=visibility, idle_probe=idle)
        status, result = asyncio.run(service.apply(PAYLOAD, "sha256=wrong"))
        assert status == 401 and result["error_code"] == "capability_invalid"
        assert catalogue.calls == [] and not cfg.database.exists()

        absent = verification_payload("vm_" + "0" * 32)
        status, result = asyncio.run(service.verify(
            absent, capability_signature(absent, SECRET)))
        assert status == 409 and result["error_code"] == "move_receipt_absent"
        assert catalogue.calls == [] and not cfg.database.exists()

        signature = capability_signature(PAYLOAD, SECRET)
        status, result = asyncio.run(service.apply(PAYLOAD, signature))
        assert status == 200 and result["status"] == "committed"
        operation_id = result["operation_id"]
        assert catalogue.calls == [("7", "/media/Documentaries", True)]
        assert cfg.database.stat().st_mode & 0o777 == 0o600
        receipt_db = sqlite3.connect(cfg.database)
        receipt_db.row_factory = sqlite3.Row
        row = dict(receipt_db.execute(
            "SELECT * FROM video_move_receipts").fetchone())
        receipt_db.close()
        serialized = repr(row)
        for private in ("approval-fixture-0001", "/media/Movies",
                        "/media/Documentaries", "jf-docs"):
            assert private not in serialized
        status, result = asyncio.run(service.apply(PAYLOAD, signature))
        assert status == 409 and result["error_code"] == "move_receipt_exists"
        assert len(catalogue.calls) == 1
        verify_payload = verification_payload(operation_id)
        parsed_operation, parsed_plan = verification_from_payload(verify_payload, cfg)
        assert parsed_operation == operation_id and parsed_plan == plan
        status, result = asyncio.run(service.verify(verify_payload, "sha256=wrong"))
        assert status == 401 and result["error_code"] == "capability_invalid"
        before_verify = cfg.database.read_bytes()
        status, result = asyncio.run(service.verify(
            verify_payload, capability_signature(verify_payload, SECRET)))
        assert status == 200 and result == {
            "schema": "video.move.verification.v1",
            "operation_id": operation_id,
            "status": "verified",
            "destination_verified": True,
            "library_verified": True,
        }
        assert cfg.database.read_bytes() == before_verify

        mismatched = {**verify_payload, "provider_id": "124"}
        status, result = asyncio.run(service.verify(
            mismatched, capability_signature(mismatched, SECRET)))
        assert status == 409 and result["error_code"] == "move_receipt_mismatch"
        catalogue.root = "/media/Movies"
        status, result = asyncio.run(service.verify(
            verify_payload, capability_signature(verify_payload, SECRET)))
        assert status == 409 and result["error_code"] == "destination_not_verified"
        assert result["destination_verified"] is False
        assert result["library_verified"] is True
        catalogue.root = "/media/Documentaries"
        visibility.visible = False
        status, result = asyncio.run(service.verify(
            verify_payload, capability_signature(verify_payload, SECRET)))
        assert status == 409
        assert result["error_code"] == "library_identity_not_verified"
        assert result["destination_verified"] is True
        assert result["library_verified"] is False
        receipt_db = sqlite3.connect(cfg.database)
        receipt_db.execute(
            "UPDATE video_move_receipts SET status = 'rolled_back' WHERE operation_id = ?",
            (operation_id,))
        receipt_db.commit()
        receipt_db.close()
        visibility.visible = True
        status, result = asyncio.run(service.verify(
            verify_payload, capability_signature(verify_payload, SECRET)))
        assert status == 409 and result["error_code"] == "move_not_committed"
    print("[PASS] exact capability is one-use and receipts remain path/title-free")


def test_http_surface():
    with tempfile.TemporaryDirectory(prefix="lm-video-http-") as root:
        cfg = config(root)
        service = VideoOrganizationService(
            cfg, catalogue=Catalogue(), visibility=Visibility(), idle_probe=idle)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            health = json.load(urlopen(base + "/health"))
            assert health == {"schema": "video.move.health.v1", "status": "ready"}
            body = json.dumps(PAYLOAD, sort_keys=True, separators=(",", ":")).encode()
            request = Request(
                base + "/api/v1/video/move", data=body,
                headers={"Content-Type": "application/json",
                         "X-LM-Video-Capability": capability_signature(PAYLOAD, SECRET)})
            result = json.load(urlopen(request))
            assert result["schema"] == "video.move.result.v1"
            assert result["status"] == "committed"
            verify_body = verification_payload(result["operation_id"])
            encoded_verify = json.dumps(
                verify_body, sort_keys=True, separators=(",", ":")).encode()
            verified = json.load(urlopen(Request(
                base + "/api/v1/video/move/verify", data=encoded_verify,
                headers={"Content-Type": "application/json",
                         "X-LM-Video-Capability": capability_signature(
                             verify_body, SECRET)})))
            assert verified["schema"] == "video.move.verification.v1"
            assert verified["status"] == "verified"
            for method in ("PUT", "PATCH", "DELETE"):
                try:
                    urlopen(Request(base + "/api/v1/video/move", data=b"{}",
                                    method=method))
                    raise AssertionError(f"{method} unexpectedly available")
                except HTTPError as exc:
                    assert exc.code == 405
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()
    print("[PASS] loopback HTTP surface exposes health plus exact signed apply/verify POSTs")


if __name__ == "__main__":
    test_plan_and_capability_boundary()
    test_http_surface()
    print("All video organization service tests passed.")
