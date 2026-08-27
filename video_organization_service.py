"""Loopback-only, capability-bound adapter for verified video root moves."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from typing import Optional

from library_manager.video.adapters import (
    JellyfinPlaybackProbe,
    JellyfinVisibilityAdapter,
    RadarrCatalogueAdapter,
)
from library_manager.video.organize import (
    OrganizationRefused,
    VideoMovePlan,
    ensure_schema,
    execute_move,
    verify_committed_move,
)


MAX_BODY = 64 * 1024
REQUEST_SCHEMA = "video.move.request.v1"
RESPONSE_SCHEMA = "video.move.result.v1"
VERIFY_REQUEST_SCHEMA = "video.move.verify.request.v1"
VERIFY_RESPONSE_SCHEMA = "video.move.verification.v1"
_APPROVAL = re.compile(r"[A-Za-z0-9_.:-]{8,160}")
_OPERATION = re.compile(r"vm_[a-f0-9]{32}")
_RADARR_ID = re.compile(r"[1-9][0-9]{0,9}")
_TMDB_ID = re.compile(r"[1-9][0-9]{0,11}")
_IMDB_ID = re.compile(r"tt[0-9]{5,12}")


class RequestRefused(RuntimeError):
    """A bounded public error code, safe to return without private data."""


class ReceiptDatabaseUnsafe(RuntimeError):
    """The configured receipt ledger cannot be opened as one private file."""


def prepare_receipt_database(path: Path) -> None:
    """Create/tighten the receipt ledger before SQLite can write to it.

    The final path must be one regular, unlinked file. Opening it with
    ``O_NOFOLLOW`` keeps a local symlink from redirecting receipt writes, while
    ``O_NONBLOCK`` prevents a hostile FIFO from hanging service startup.
    """
    if not path.is_absolute():
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NONBLOCK
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        connection = sqlite3.connect(str(path), timeout=60)
        try:
            ensure_schema(connection)
        finally:
            connection.close()
    except ReceiptDatabaseUnsafe:
        raise
    except (OSError, sqlite3.Error):
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe") from None


def canonical_payload(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def capability_signature(payload: dict, secret: bytes) -> str:
    return "sha256=" + hmac.new(secret, canonical_payload(payload), hashlib.sha256).hexdigest()


def verify_capability(payload: dict, signature: str, secret: bytes) -> bool:
    return bool(secret and signature and hmac.compare_digest(
        capability_signature(payload, secret), signature))


def _mapping(name: str) -> dict[str, str]:
    try:
        value = json.loads(os.environ.get(name, "{}"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()
            if isinstance(key, str) and isinstance(item, str) and key and item}


@dataclass(frozen=True)
class ServiceConfig:
    database: Path
    capability_secret: bytes
    roots: dict[str, str]
    libraries: dict[str, str]

    @classmethod
    def from_environment(cls) -> "ServiceConfig":
        database = Path(os.environ.get(
            "LM_VIDEO_DB", "/data/video-move-receipts.db"))
        return cls(
            database=database,
            capability_secret=os.environ.get(
                "LM_VIDEO_CAPABILITY_SECRET", "").encode(),
            roots=_mapping("LM_VIDEO_ROOTS_JSON"),
            libraries=_mapping("LM_VIDEO_LIBRARIES_JSON"),
        )

    def ready(self) -> bool:
        return (self.database.is_absolute() and len(self.capability_secret) >= 32
                and len(self.roots) >= 2 and bool(self.libraries)
                and all(Path(value).is_absolute() for value in self.roots.values())
                and all(bool(value.strip()) for value in self.libraries.values()))


def plan_from_payload(payload: object, config: ServiceConfig) -> VideoMovePlan:
    if not isinstance(payload, dict) or payload.get("schema") != REQUEST_SCHEMA:
        raise RequestRefused("request_schema_invalid")
    allowed = {"schema", "approval_ref", "subject_id", "provider", "provider_id",
               "media_kind", "runtime_minutes", "source_root_id",
               "destination_root_id", "destination_library_id"}
    if set(payload) != allowed:
        raise RequestRefused("request_fields_invalid")
    if (not _APPROVAL.fullmatch(str(payload.get("approval_ref") or ""))
            or not _RADARR_ID.fullmatch(str(payload.get("subject_id") or ""))):
        raise RequestRefused("request_identity_invalid")
    provider = payload.get("provider")
    provider_id = str(payload.get("provider_id") or "")
    if provider not in {"tmdb", "imdb"}:
        raise RequestRefused("request_provider_invalid")
    if not (_TMDB_ID.fullmatch(provider_id) if provider == "tmdb"
            else _IMDB_ID.fullmatch(provider_id)):
        raise RequestRefused("request_identity_invalid")
    if payload.get("media_kind") != "movie":
        raise RequestRefused("movie_length_movies_only")
    runtime = payload.get("runtime_minutes")
    if not isinstance(runtime, int) or isinstance(runtime, bool) or runtime < 40:
        raise RequestRefused("movie_length_movies_only")
    source_id = str(payload.get("source_root_id") or "")
    destination_id = str(payload.get("destination_root_id") or "")
    library_id = str(payload.get("destination_library_id") or "")
    if (source_id not in config.roots or destination_id not in config.roots
            or source_id == destination_id or library_id not in config.libraries):
        raise RequestRefused("configured_scope_invalid")
    return VideoMovePlan(
        approval_ref=str(payload["approval_ref"]),
        subject_id=str(payload["subject_id"]),
        provider=str(payload["provider"]),
        provider_id=str(payload["provider_id"]),
        media_kind="movie",
        movie_length=True,
        source_root_ref=config.roots[source_id],
        destination_root_ref=config.roots[destination_id],
        destination_library_ref=config.libraries[library_id],
        approved=True,
    )


def verification_from_payload(payload: object, config: ServiceConfig
                              ) -> tuple[str, VideoMovePlan]:
    if not isinstance(payload, dict) or payload.get("schema") != VERIFY_REQUEST_SCHEMA:
        raise RequestRefused("request_schema_invalid")
    allowed = {"operation_id", "schema", "approval_ref", "subject_id", "provider",
               "provider_id", "media_kind", "runtime_minutes", "source_root_id",
               "destination_root_id", "destination_library_id"}
    if set(payload) != allowed:
        raise RequestRefused("request_fields_invalid")
    operation_id = str(payload.get("operation_id") or "")
    if not _OPERATION.fullmatch(operation_id):
        raise RequestRefused("operation_identity_invalid")
    move_payload = {key: value for key, value in payload.items()
                    if key != "operation_id"}
    move_payload["schema"] = REQUEST_SCHEMA
    return operation_id, plan_from_payload(move_payload, config)


class VideoOrganizationService:
    def __init__(self, config: ServiceConfig, *, catalogue, visibility,
                 idle_probe) -> None:
        if config.ready():
            prepare_receipt_database(config.database)
        self.config = config
        self.catalogue = catalogue
        self.visibility = visibility
        self.idle_probe = idle_probe

    async def apply(self, payload: object, signature: str) -> tuple[int, dict]:
        if not self.config.ready():
            return 503, {"schema": RESPONSE_SCHEMA, "status": "unavailable",
                         "error_code": "service_configuration_incomplete"}
        if not isinstance(payload, dict) or not verify_capability(
                payload, signature, self.config.capability_secret):
            return 401, {"schema": RESPONSE_SCHEMA, "status": "refused",
                         "error_code": "capability_invalid"}
        try:
            plan = plan_from_payload(payload, self.config)
        except RequestRefused as exc:
            return 400, {"schema": RESPONSE_SCHEMA, "status": "refused",
                         "error_code": str(exc)}
        self.config.database.parent.mkdir(parents=True, exist_ok=True)
        conn: Optional[sqlite3.Connection] = None
        try:
            prepare_receipt_database(self.config.database)
            conn = sqlite3.connect(str(self.config.database), timeout=60)
            result = await execute_move(
                plan, conn=conn, catalogue=self.catalogue,
                visibility=self.visibility, idle_probe=self.idle_probe)
            os.chmod(self.config.database, 0o600)
        except OrganizationRefused as exc:
            return 409, {"schema": RESPONSE_SCHEMA, "status": "refused",
                         "error_code": str(exc)}
        except Exception:
            return 503, {"schema": RESPONSE_SCHEMA, "status": "unavailable",
                         "error_code": "operation_unavailable"}
        finally:
            if conn is not None:
                conn.close()
        status = 200 if result["status"] == "committed" else (
            503 if result["status"] == "manual_recovery" else 409)
        return status, {"schema": RESPONSE_SCHEMA, **result}

    async def verify(self, payload: object, signature: str) -> tuple[int, dict]:
        if not self.config.ready():
            return 503, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "unavailable",
                         "error_code": "service_configuration_incomplete"}
        if not isinstance(payload, dict) or not verify_capability(
                payload, signature, self.config.capability_secret):
            return 401, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "refused",
                         "error_code": "capability_invalid"}
        try:
            operation_id, plan = verification_from_payload(payload, self.config)
        except RequestRefused as exc:
            return 400, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "refused",
                         "error_code": str(exc)}
        if not self.config.database.is_file():
            return 409, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "unverified",
                         "error_code": "move_receipt_absent"}
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(
                self.config.database.resolve().as_uri() + "?mode=ro",
                uri=True, timeout=60)
            conn.row_factory = sqlite3.Row
            result = await verify_committed_move(
                operation_id, plan, conn=conn, catalogue=self.catalogue,
                visibility=self.visibility)
        except OrganizationRefused as exc:
            return 409, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "unverified",
                         "error_code": str(exc)}
        except Exception:
            return 503, {"schema": VERIFY_RESPONSE_SCHEMA, "status": "unavailable",
                         "error_code": "operation_unavailable"}
        finally:
            if conn is not None:
                conn.close()
        status = 200 if result["status"] == "verified" else 409
        return status, {"schema": VERIFY_RESPONSE_SCHEMA, **result}


def make_handler(service: VideoOrganizationService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "LibraryManagerVideoOrganization/1"

        def _send(self, status: int, payload: dict) -> None:
            body = canonical_payload(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/health":
                self._send(404, {"schema": RESPONSE_SCHEMA,
                                 "status": "unavailable",
                                 "error_code": "route_unavailable"})
                return
            self._send(200, {"schema": "video.move.health.v1",
                             "status": "ready" if service.config.ready() else "unavailable"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {"/api/v1/video/move",
                                 "/api/v1/video/move/verify"}:
                self._send(404, {"schema": RESPONSE_SCHEMA,
                                 "status": "unavailable",
                                 "error_code": "route_unavailable"})
                return
            response_schema = (VERIFY_RESPONSE_SCHEMA
                               if self.path.endswith("/verify") else RESPONSE_SCHEMA)
            if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
                self._send(415, {"schema": response_schema, "status": "refused",
                                 "error_code": "content_type_invalid"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length <= 0 or length > MAX_BODY:
                self._send(413, {"schema": response_schema, "status": "refused",
                                 "error_code": "body_size_invalid"})
                return
            try:
                payload = json.loads(self.rfile.read(length))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send(400, {"schema": response_schema, "status": "refused",
                                 "error_code": "json_invalid"})
                return
            call = (service.verify if self.path.endswith("/verify")
                    else service.apply)
            status, result = asyncio.run(call(
                payload, self.headers.get("X-LM-Video-Capability", "")))
            self._send(status, result)

        def do_PUT(self) -> None:  # noqa: N802
            self._send(405, {"schema": RESPONSE_SCHEMA, "status": "refused",
                             "error_code": "method_unavailable"})

        do_PATCH = do_PUT
        do_DELETE = do_PUT

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main() -> None:
    config = ServiceConfig.from_environment()
    radarr_url = os.environ.get("LM_RADARR_URL", "")
    radarr_key = os.environ.get("LM_RADARR_API_KEY", "")
    jellyfin_url = os.environ.get("LM_JELLYFIN_URL", "")
    jellyfin_key = os.environ.get("LM_JELLYFIN_API_KEY", "")
    service = VideoOrganizationService(
        config,
        catalogue=RadarrCatalogueAdapter(radarr_url, radarr_key),
        visibility=JellyfinVisibilityAdapter(jellyfin_url, jellyfin_key),
        idle_probe=JellyfinPlaybackProbe(jellyfin_url, jellyfin_key),
    )
    port = int(os.environ.get("PORT", "5759"))
    ThreadingHTTPServer(("127.0.0.1", port), make_handler(service)).serve_forever()


if __name__ == "__main__":
    main()
