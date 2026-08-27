#!/usr/bin/env python3
"""Radarr/Jellyfin adapters keep product calls exact and fail closed."""
from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from library_manager.video.adapters import (  # noqa: E402
    JellyfinPlaybackProbe,
    JellyfinVisibilityAdapter,
    RadarrCatalogueAdapter,
)


class Response:
    def __init__(self, body=None, status=200):
        self.body = body
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("fixture response")

    def json(self):
        return self.body


class RadarrSession:
    def __init__(self):
        self.movie = {"id": 7, "path": "/media/Movies/Fixture (2026)",
                      "rootFolderPath": "/media/Movies", "hasFile": True,
                      "movieFile": {"path": "/media/Movies/Fixture (2026)/fixture.mkv"}}
        self.puts = []

    def get(self, url, **kwargs):
        assert url.endswith("/api/v3/movie/7")
        assert kwargs["headers"]["X-Api-Key"] == "secret"
        return Response(dict(self.movie))

    def put(self, url, **kwargs):
        self.puts.append((url, kwargs))
        body = kwargs["json"]
        self.movie = dict(body)
        self.movie["movieFile"] = {
            "path": body["path"] + "/fixture.mkv",
        }
        return Response(dict(self.movie), 202)


class JellyfinSession:
    def __init__(self, *, duplicate=False, malformed=False, malformed_identity=False,
                 sessions=None, status=200, unrelated=0, total_override=None):
        self.duplicate = duplicate
        self.malformed = malformed
        self.malformed_identity = malformed_identity
        self.status = status
        self.sessions = sessions
        self.unrelated = unrelated
        self.total_override = total_override
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if url.endswith("/Sessions"):
            return Response(self.sessions, self.status)
        if self.malformed:
            return Response({"Items": "not-a-list"}, self.status)
        items = [
            {"Id": f"unrelated-{index}", "ProviderIds": {"Tmdb": str(index + 1000)}}
            for index in range(self.unrelated)
        ]
        items.append({"Id": "one", "ProviderIds": (
            [] if self.malformed_identity else {"Tmdb": "123"})})
        if self.duplicate:
            items.append({"Id": "two", "ProviderIds": {"Tmdb": "123"}})
        start = kwargs["params"]["StartIndex"]
        limit = kwargs["params"]["Limit"]
        total = len(items) if self.total_override is None else self.total_override
        return Response({"Items": items[start:start + limit],
                         "TotalRecordCount": total}, self.status)


def test_radarr_move_and_readback():
    session = RadarrSession()
    adapter = RadarrCatalogueAdapter("http://radarr.invalid", "secret", session=session)
    moved = asyncio.run(adapter.move_root("7", "/media/Documentaries", move_files=True))
    verified = asyncio.run(adapter.root_is("7", "/media/Documentaries"))
    assert moved and verified
    _, call = session.puts[0]
    assert call["params"] == {"moveFiles": "true"}
    assert call["json"]["moveFiles"] is True
    assert call["json"]["rootFolderPath"] == "/media/Documentaries"
    assert call["json"]["path"] == "/media/Documentaries/Fixture (2026)"

    session.movie["path"] = "/media/Documentaries-old/Fixture (2026)"
    assert not asyncio.run(adapter.root_is("7", "/media/Documentaries"))
    assert not asyncio.run(adapter.move_root("7", "/media/Movies", move_files=False))
    print("[PASS] Radarr move is exact, file-enabled, and refuses prefix lookalikes")


def test_jellyfin_exact_library_identity():
    session = JellyfinSession()
    adapter = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret", session=session)
    assert asyncio.run(adapter.visible_in("tmdb", "123", "documentary-library"))
    _, call = session.calls[0]
    assert call["headers"] == {"X-Emby-Token": "secret"}
    assert call["params"]["ParentId"] == "documentary-library"
    assert "AnyProviderIdEquals" not in call["params"]
    assert call["params"]["StartIndex"] == 0
    assert call["params"]["Limit"] == 200
    assert call["params"]["IncludeItemTypes"] == "Movie"

    paged_session = JellyfinSession(unrelated=200)
    paged = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret", session=paged_session)
    assert asyncio.run(paged.visible_in("tmdb", "123", "documentary-library"))
    assert [call[1]["params"]["StartIndex"] for call in paged_session.calls] == [0, 200]

    duplicate = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret", session=JellyfinSession(duplicate=True))
    malformed = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret", session=JellyfinSession(malformed=True))
    malformed_identity = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(malformed_identity=True))
    unavailable = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret", session=JellyfinSession(status=503))
    oversized = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(total_override=10_001))
    inconsistent = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(total_override=0))
    truncated = JellyfinVisibilityAdapter(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(total_override=201))
    assert not asyncio.run(duplicate.visible_in("tmdb", "123", "documentary-library"))
    assert not asyncio.run(malformed.visible_in("tmdb", "123", "documentary-library"))
    assert not asyncio.run(malformed_identity.visible_in(
        "tmdb", "123", "documentary-library"))
    assert not asyncio.run(unavailable.visible_in("tmdb", "123", "documentary-library"))
    assert not asyncio.run(oversized.visible_in("tmdb", "123", "documentary-library"))
    assert not asyncio.run(inconsistent.visible_in(
        "tmdb", "123", "documentary-library"))
    assert not asyncio.run(truncated.visible_in("tmdb", "123", "documentary-library"))
    assert not asyncio.run(adapter.visible_in("tvdb", "123", "documentary-library"))
    print("[PASS] Jellyfin verification is exact-library, exact-provider, and fail-closed")


def test_playback_probe_is_authoritative():
    idle = JellyfinPlaybackProbe(
        "http://jellyfin.invalid", "secret", session=JellyfinSession(sessions=[]))
    active = JellyfinPlaybackProbe(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(sessions=[{"NowPlayingItem": {"Id": "fixture"}}]))
    unavailable = JellyfinPlaybackProbe(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(sessions=[], status=503))
    malformed = JellyfinPlaybackProbe(
        "http://jellyfin.invalid", "secret",
        session=JellyfinSession(sessions={"Items": []}))
    assert asyncio.run(idle()).observed and not asyncio.run(idle()).busy
    assert asyncio.run(active()).observed and asyncio.run(active()).busy
    assert not asyncio.run(unavailable()).observed and asyncio.run(unavailable()).busy
    assert not asyncio.run(malformed()).observed and asyncio.run(malformed()).busy
    print("[PASS] playback probe distinguishes authoritative idle from busy/unavailable")


if __name__ == "__main__":
    test_radarr_move_and_readback()
    test_jellyfin_exact_library_identity()
    test_playback_probe_is_authoritative()
    print("All video adapter tests passed.")
