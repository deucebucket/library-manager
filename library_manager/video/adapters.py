"""Narrow product adapters for the provider-neutral video organization core.

Credentials are constructor inputs owned by the embedding service.  Adapters do not
persist them, log response bodies, or import the Library Manager Flask application.
"""
from __future__ import annotations

import asyncio
import posixpath
from typing import Any, Optional

import requests

from library_manager.video.organize import PlaybackObservation


DEFAULT_TIMEOUT = (5, 900)


def _within(path: str, root: str) -> bool:
    normalized_path = posixpath.normpath(str(path or "").replace("\\", "/"))
    normalized_root = posixpath.normpath(str(root or "").replace("\\", "/"))
    return (bool(normalized_path and normalized_root)
            and (normalized_path == normalized_root
                 or normalized_path.startswith(normalized_root.rstrip("/") + "/")))


class RadarrCatalogueAdapter:
    """Move and re-read one exact Radarr movie without title search."""

    def __init__(self, url: str, api_key: str, *,
                 session: Optional[requests.Session] = None) -> None:
        self.url = str(url or "").rstrip("/")
        self.api_key = str(api_key or "")
        self.session = session or requests.Session()
        if not self.url or not self.api_key:
            raise ValueError("radarr_configuration_incomplete")

    @property
    def _headers(self) -> dict:
        return {"X-Api-Key": self.api_key, "Content-Type": "application/json"}

    def _movie(self, subject_id: str) -> Optional[dict]:
        try:
            response = self.session.get(
                f"{self.url}/api/v3/movie/{subject_id}",
                headers=self._headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            value = response.json()
            return value if isinstance(value, dict) else None
        except (requests.RequestException, ValueError):
            return None

    async def move_root(self, subject_id: str, root_ref: str, *,
                        move_files: bool) -> bool:
        if move_files is not True or not subject_id or not root_ref:
            return False

        def apply() -> bool:
            movie = self._movie(subject_id)
            current = str((movie or {}).get("path") or "")
            folder = posixpath.basename(posixpath.normpath(current.replace("\\", "/")))
            if not movie or not folder or folder in {".", "/"}:
                return False
            destination = posixpath.join(root_ref.rstrip("/"), folder)
            body: dict[str, Any] = dict(movie)
            body.update({"rootFolderPath": root_ref, "path": destination,
                         "moveFiles": True})
            try:
                response = self.session.put(
                    f"{self.url}/api/v3/movie/{subject_id}",
                    headers=self._headers, params={"moveFiles": "true"},
                    json=body, timeout=DEFAULT_TIMEOUT)
                response.raise_for_status()
                return response.status_code in (200, 201, 202)
            except requests.RequestException:
                return False

        return await asyncio.to_thread(apply)

    async def root_is(self, subject_id: str, root_ref: str) -> bool:
        def verify() -> bool:
            movie = self._movie(subject_id)
            if not movie or str(movie.get("rootFolderPath") or "") != root_ref:
                return False
            movie_path = str(movie.get("path") or "")
            if not _within(movie_path, root_ref):
                return False
            movie_file = movie.get("movieFile")
            if movie.get("hasFile") is True:
                return (isinstance(movie_file, dict)
                        and _within(str(movie_file.get("path") or ""), root_ref))
            return True

        return await asyncio.to_thread(verify)


class JellyfinVisibilityAdapter:
    """Verify one exact provider identity inside one exact Jellyfin library."""

    PROVIDER_KEYS = {"tmdb": "Tmdb", "imdb": "Imdb"}

    def __init__(self, url: str, api_key: str, *,
                 session: Optional[requests.Session] = None) -> None:
        self.url = str(url or "").rstrip("/")
        self.api_key = str(api_key or "")
        self.session = session or requests.Session()
        if not self.url or not self.api_key:
            raise ValueError("jellyfin_configuration_incomplete")

    async def visible_in(self, provider: str, provider_id: str,
                         library_ref: str) -> bool:
        key = self.PROVIDER_KEYS.get(str(provider or "").casefold())
        if not key or not provider_id or not library_ref:
            return False

        def verify() -> bool:
            try:
                response = self.session.get(
                    f"{self.url}/Items",
                    headers={"X-Emby-Token": self.api_key},
                    params={"ParentId": library_ref, "Recursive": "true",
                            "IncludeItemTypes": "Movie", "Fields": "ProviderIds",
                            "AnyProviderIdEquals": f"{key}.{provider_id}",
                            "Limit": 10},
                    timeout=(5, 45))
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError):
                return False
            items = payload.get("Items") if isinstance(payload, dict) else None
            if not isinstance(items, list):
                return False
            matches = []
            for item in items:
                provider_ids = item.get("ProviderIds") if isinstance(item, dict) else None
                if (isinstance(provider_ids, dict)
                        and str(provider_ids.get(key) or "") == str(provider_id)):
                    matches.append(item)
            return len(matches) == 1

        return await asyncio.to_thread(verify)


class JellyfinPlaybackProbe:
    """Return authoritative busy/idle truth; transport ambiguity stays unobserved."""

    def __init__(self, url: str, api_key: str, *,
                 session: Optional[requests.Session] = None) -> None:
        self.url = str(url or "").rstrip("/")
        self.api_key = str(api_key or "")
        self.session = session or requests.Session()
        if not self.url or not self.api_key:
            raise ValueError("jellyfin_configuration_incomplete")

    async def __call__(self) -> PlaybackObservation:
        def observe() -> PlaybackObservation:
            try:
                response = self.session.get(
                    f"{self.url}/Sessions",
                    headers={"X-Emby-Token": self.api_key}, timeout=(5, 45))
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError):
                return PlaybackObservation(observed=False, busy=True)
            if not isinstance(payload, list) or not all(
                    isinstance(item, dict) for item in payload):
                return PlaybackObservation(observed=False, busy=True)
            busy = any(isinstance(item.get("NowPlayingItem"), dict)
                       for item in payload)
            return PlaybackObservation(observed=True, busy=busy)

        return await asyncio.to_thread(observe)


__all__ = [
    "JellyfinPlaybackProbe", "JellyfinVisibilityAdapter",
    "RadarrCatalogueAdapter",
]
