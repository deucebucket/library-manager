"""TMDb provider — identify movies and TV shows.

Mirrors the book providers (googlebooks.py etc.): pure search functions that take
a user-supplied API key and return normalized candidate dicts. TMDb covers BOTH
movies and TV, so it's the primary video source; TVDB (tvdb.py) is a secondary
for episode-level detail on long-running/anime shows.

Key is user-supplied (Settings), never hardcoded. All functions degrade to [] on
error so the pipeline can fall back to filename/AI data.
"""
from __future__ import annotations
import logging
from typing import Optional

import requests as http

logger = logging.getLogger(__name__)
BASE = "https://api.themoviedb.org/3"
TIMEOUT = 12


def _get(path: str, api_key: str, **params):
    params["api_key"] = api_key
    try:
        r = http.get(f"{BASE}{path}", params=params, timeout=TIMEOUT)
        if r.status_code == 401:
            logger.warning("TMDb: invalid API key")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug("TMDb request failed %s: %s", path, e)
        return None


def search_movie(title: str, year: Optional[int] = None, api_key: str = "",
                 lang: str = "en-US") -> list[dict]:
    """Return candidate movies, best first: {tmdb_id,title,year,popularity,votes,overview,imdb?}."""
    if not api_key or not title:
        return []
    params = {"query": title, "language": lang, "include_adult": "false"}
    if year:
        params["year"] = year
    data = _get("/search/movie", api_key, **params)
    if not data:
        return []
    out = []
    for r in data.get("results", []):
        rd = (r.get("release_date") or "")[:4]
        out.append({
            "tmdb_id": r.get("id"),
            "title": r.get("title"),
            "original_title": r.get("original_title"),
            "year": int(rd) if rd.isdigit() else None,
            "popularity": r.get("popularity", 0),
            "votes": r.get("vote_count", 0),
            "overview": r.get("overview", ""),
        })
    return out


def search_tv(title: str, year: Optional[int] = None, api_key: str = "",
              lang: str = "en-US") -> list[dict]:
    """Return candidate TV shows, best first."""
    if not api_key or not title:
        return []
    params = {"query": title, "language": lang, "include_adult": "false"}
    if year:
        params["first_air_date_year"] = year
    data = _get("/search/tv", api_key, **params)
    if not data:
        return []
    out = []
    for r in data.get("results", []):
        fa = (r.get("first_air_date") or "")[:4]
        out.append({
            "tmdb_id": r.get("id"),
            "title": r.get("name"),
            "original_title": r.get("original_name"),
            "year": int(fa) if fa.isdigit() else None,
            "popularity": r.get("popularity", 0),
            "votes": r.get("vote_count", 0),
            "overview": r.get("overview", ""),
        })
    return out


def external_ids(kind: str, tmdb_id: int, api_key: str) -> dict:
    """imdb_id / tvdb_id for a movie ('movie') or show ('tv')."""
    data = _get(f"/{kind}/{tmdb_id}/external_ids", api_key) or {}
    return {"imdb_id": data.get("imdb_id"), "tvdb_id": data.get("tvdb_id")}


def episode_title(tv_id: int, season: int, episode: int, api_key: str,
                  lang: str = "en-US") -> Optional[str]:
    if not api_key:
        return None
    data = _get(f"/tv/{tv_id}/season/{season}/episode/{episode}", api_key, language=lang)
    return data.get("name") if data else None
