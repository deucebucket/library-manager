"""Loopback-only, capability-bound adapter for verified video root moves."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
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
from typing import Iterator, Optional
from urllib.parse import quote

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
RECEIPT_APPLICATION_ID = 0x4C4D5652  # "LMVR"
RECEIPT_SCHEMA_VERSION = 1
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


def _open_private_parent(path: Path) -> int:
    """Open an absolute, symlink-free parent owned privately by this service."""
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
    directory_flags = (os.O_RDONLY | os.O_DIRECTORY
                       | getattr(os, "O_CLOEXEC", 0)
                       | getattr(os, "O_NOFOLLOW", 0))
    descriptor = os.open("/", directory_flags)
    try:
        for component in path.parent.parts[1:]:
            try:
                child = os.open(component, directory_flags, dir_fd=descriptor)
            except FileNotFoundError:
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                child = os.open(component, directory_flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        metadata = os.fstat(descriptor)
        if (not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) & 0o022):
            raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
        return descriptor
    except ReceiptDatabaseUnsafe:
        os.close(descriptor)
        raise
    except OSError:
        os.close(descriptor)
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe") from None


def _schema_objects(connection: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    return [
        (str(row[0]), str(row[1]), str(row[2]), " ".join(str(row[3]).split()))
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")
    ]


def _expected_schema_objects() -> list[tuple[str, str, str, str]]:
    expected = sqlite3.connect(":memory:")
    try:
        ensure_schema(expected)
        return _schema_objects(expected)
    finally:
        expected.close()


EXPECTED_RECEIPT_SCHEMA = _expected_schema_objects()


def _validate_receipt_schema(connection: sqlite3.Connection) -> None:
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if (application_id != RECEIPT_APPLICATION_ID
            or user_version != RECEIPT_SCHEMA_VERSION
            or _schema_objects(connection) != EXPECTED_RECEIPT_SCHEMA
            or connection.execute("PRAGMA quick_check").fetchone()[0] != "ok"):
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe")


def _initialize_or_validate_schema(
        connection: sqlite3.Connection, *, allow_legacy_initialization: bool) -> None:
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    objects = _schema_objects(connection)
    if application_id == 0 and user_version == 0 and allow_legacy_initialization:
        if objects:
            expected_without_markers = EXPECTED_RECEIPT_SCHEMA
            if objects != expected_without_markers:
                raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
        else:
            ensure_schema(connection)
        connection.execute(f"PRAGMA application_id={RECEIPT_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version={RECEIPT_SCHEMA_VERSION}")
        connection.commit()
    _validate_receipt_schema(connection)


@contextmanager
def secure_receipt_connection(
        path: Path, *, initialize: bool = False,
        readonly: bool = False) -> Iterator[sqlite3.Connection]:
    """Yield SQLite only while its private parent and checked inode stay bound."""
    parent_descriptor = _open_private_parent(path)
    database_descriptor = -1
    connection: Optional[sqlite3.Connection] = None
    try:
        file_flags = (os.O_RDWR | os.O_CREAT
                      | getattr(os, "O_CLOEXEC", 0)
                      | getattr(os, "O_NONBLOCK", 0)
                      | getattr(os, "O_NOFOLLOW", 0))
        database_descriptor = os.open(
            path.name, file_flags, 0o600, dir_fd=parent_descriptor)
        expected = os.fstat(database_descriptor)
        if not stat.S_ISREG(expected.st_mode) or expected.st_nlink != 1:
            raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
        os.fchmod(database_descriptor, 0o600)

        stable_path = f"/proc/self/fd/{parent_descriptor}/{quote(path.name, safe='')}"
        connection = sqlite3.connect(
            f"file:{stable_path}?mode=rw", uri=True, timeout=60)
        observed = os.stat(
            path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        after_open = os.fstat(database_descriptor)
        def identity(value: os.stat_result) -> tuple[int, int, int, int]:
            return (value.st_dev, value.st_ino, value.st_nlink,
                    stat.S_IFMT(value.st_mode))
        if (identity(observed) != identity(expected)
                or identity(after_open) != identity(expected)
                or stat.S_IMODE(observed.st_mode) != 0o600):
            raise ReceiptDatabaseUnsafe("receipt_database_unsafe")
        _initialize_or_validate_schema(
            connection, allow_legacy_initialization=initialize)
        if readonly:
            connection.execute("PRAGMA query_only=ON")
        yield connection
    except ReceiptDatabaseUnsafe:
        raise
    except (OSError, sqlite3.Error):
        raise ReceiptDatabaseUnsafe("receipt_database_unsafe") from None
    finally:
        if connection is not None:
            connection.close()
        if database_descriptor >= 0:
            os.close(database_descriptor)
        os.close(parent_descriptor)


def prepare_receipt_database(path: Path) -> None:
    """Create, mark, and validate a private receipt ledger before serving."""
    try:
        with secure_receipt_connection(path, initialize=True):
            pass
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
        try:
            with secure_receipt_connection(self.config.database) as conn:
                result = await execute_move(
                    plan, conn=conn, catalogue=self.catalogue,
                    visibility=self.visibility, idle_probe=self.idle_probe)
        except OrganizationRefused as exc:
            return 409, {"schema": RESPONSE_SCHEMA, "status": "refused",
                         "error_code": str(exc)}
        except Exception:
            return 503, {"schema": RESPONSE_SCHEMA, "status": "unavailable",
                         "error_code": "operation_unavailable"}
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
        try:
            with secure_receipt_connection(
                    self.config.database, readonly=True) as conn:
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
