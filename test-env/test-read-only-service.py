#!/usr/bin/env python3
"""The deployed adapter exposes inspection and no organization verbs."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from read_only_music_service import Handler, ThreadingHTTPServer  # noqa: E402


with tempfile.TemporaryDirectory(prefix="lm-read-only-") as root:
    os.environ["LM_MUSIC_ROOT_ID"] = "music"
    os.environ["LM_MUSIC_ROOT_PATH"] = root
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        health = json.load(urlopen(base + "/health"))
        assert health == {"schema": "music.inspection.health.v1", "state": "ready",
                          "root_configured": True}
        body = json.dumps({"schema": "music.inspection.request.v1", "root_id": "music",
                           "relative_path": "missing-album", "identity": {
                               "lidarr_foreign_album_id": "",
                               "lidarr_foreign_artist_id": "",
                               "musicbrainz_album_id": "",
                               "musicbrainz_artist_id": "",
                               "musicbrainz_release_group_id":
                                   "12345678-1234-1234-9234-123456789abc"}}).encode()
        try:
            urlopen(Request(base + "/api/v1/music/inspect", data=body,
                            headers={"Content-Type": "application/json"}))
            raise AssertionError("missing target should not pass")
        except HTTPError as exc:
            assert exc.code == 503
            assert json.load(exc)["schema"] == "music.inspection.error.v1"
        for method in ("PUT", "PATCH", "DELETE"):
            try:
                urlopen(Request(base + "/api/v1/music/inspect", data=b"{}", method=method))
                raise AssertionError(f"{method} unexpectedly available")
            except HTTPError as exc:
                assert exc.code == 405
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()

print("Read-only service tests passed.")
