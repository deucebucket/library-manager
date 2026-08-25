"""Loopback-only HTTP adapter for the bounded Music inspection engine.

This deliberately does not import the Library Manager Flask application.  The service
surface is two routes, contains no organization verb, and gets one configured root
from its environment.  Network binding is loopback-only even if a caller supplies a
different host value.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os

from library_manager.music.inspect import inspect_payload

MAX_BODY = 1024 * 1024


def configured_roots() -> list[dict[str, str]]:
    root_id = os.environ.get("LM_MUSIC_ROOT_ID", "")
    path = os.environ.get("LM_MUSIC_ROOT_PATH", "")
    return [{"id": root_id, "path": path}] if root_id and os.path.isabs(path) else []


class Handler(BaseHTTPRequestHandler):
    server_version = "LibraryManagerMusicReadOnly/1"

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        if self.path != "/health":
            self._send(404, {"schema": "music.inspection.error.v1",
                             "state": "unavailable", "error": "route_unavailable"})
            return
        self._send(200, {"schema": "music.inspection.health.v1", "state": "ready",
                         "root_configured": len(configured_roots()) == 1})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        if self.path != "/api/v1/music/inspect":
            self._send(404, {"schema": "music.inspection.error.v1",
                             "state": "unavailable", "error": "route_unavailable"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length <= 0 or length > MAX_BODY:
            self._send(413, {"schema": "music.inspection.error.v1",
                             "state": "unavailable", "error": "body_size_invalid"})
            return
        try:
            payload = json.loads(self.rfile.read(length))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send(400, {"schema": "music.inspection.error.v1",
                             "state": "unavailable", "error": "json_invalid"})
            return
        result, status = inspect_payload(payload, configured_roots())
        self._send(status, result)

    def do_PUT(self) -> None:  # noqa: N802
        self._send(405, {"schema": "music.inspection.error.v1",
                         "state": "unavailable", "error": "method_unavailable"})

    do_PATCH = do_PUT
    do_DELETE = do_PUT

    def log_message(self, format: str, *args: object) -> None:
        # Requests contain opaque root-relative subjects; keep them out of logs.
        return


def main() -> None:
    port = int(os.environ.get("PORT", "5758"))
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
