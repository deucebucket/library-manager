"""Identify a video file: messy path -> confirmed VideoProfile.

Pipeline (mirrors the audiobook flow):
  1. Deterministic parse (naming.parse_filename).
  2. If the parse is weak (no year / low confidence) and an AI hook is provided,
     ask the user's model "what movie/show is this?" to recover title+year.
  3. Search TMDb (movie or TV per parse.kind).
  4. Pick the best candidate by Jaccard title similarity + popularity; reject
     garbage (<30% overlap), flag drastic changes for approval.
  5. Enrich: episode title (TV) via TMDb, external ids, ffprobe MediaInfo.

The AI and TMDb calls are injected so this stays unit-testable and provider-
agnostic (the app wires gemini/ollama/openrouter + the user's TMDb key).
"""
from __future__ import annotations
import re
from typing import Callable, Optional

from library_manager.video.naming import parse_filename
from library_manager.models.video_profile import VideoProfile
from library_manager.providers import tmdb as tmdb_provider

GARBAGE_THRESHOLD = 0.30      # < this Jaccard overlap => reject (matches book flow)
DRASTIC_THRESHOLD = 0.55      # < this => keep but require approval

_WORD = re.compile(r"[a-z0-9]+")


def _tokens(s: str) -> set:
    return set(_WORD.findall((s or "").lower()))


def jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _best(cands: list, title: str, year: Optional[int]) -> tuple[Optional[dict], float]:
    best, score = None, 0.0
    for c in cands:
        s = max(jaccard(title, c.get("title", "")),
                jaccard(title, c.get("original_title", "")))
        if year and c.get("year") == year:
            s += 0.15            # year match is a strong signal
        s += min(c.get("popularity", 0) / 1000.0, 0.05)  # gentle tiebreak
        if s > score:
            best, score = c, min(s, 1.0)
    return best, score


def identify_file(
    path: str,
    *,
    tmdb_key: str = "",
    ai_fn: Optional[Callable[[str], dict]] = None,
    probe_fn: Optional[Callable[[str], object]] = None,
    server: str = "plex",
) -> VideoProfile:
    """ai_fn(filename)->{'title','year','kind'} ; probe_fn(path)->MediaInfo."""
    p = parse_filename(path)
    prof = VideoProfile(source_path=path, kind=p.kind, parsed=p, ext=p.ext,
                        title=p.title, year=p.year, season=p.season,
                        episodes=list(p.episodes), episode_title=p.episode_title)

    # 2. AI recovery when the deterministic parse is weak.
    if ai_fn and (p.confidence < 0.5 or not p.title or (p.kind == "movie" and not p.year)):
        try:
            g = ai_fn(p.raw) or {}
            if g.get("title"):
                prof.title = g["title"]
            if g.get("year"):
                prof.year = int(g["year"])
            if g.get("kind") in ("movie", "episode"):
                prof.kind = g["kind"]
            prof.note = "ai-assisted"
        except Exception:
            pass

    if not tmdb_key:
        prof.confidence = p.confidence
        prof.note = (prof.note + "; no tmdb key — filename only").strip("; ")
        return prof

    # 3-4. TMDb match.
    if prof.kind == "episode":
        cands = tmdb_provider.search_tv(prof.title, prof.year, tmdb_key)
    else:
        cands = tmdb_provider.search_movie(prof.title, prof.year, tmdb_key)
    best, score = _best(cands, prof.title, prof.year)
    prof.match_score = score

    if not best or score < GARBAGE_THRESHOLD:
        prof.rejected = bool(cands)        # had candidates but none matched => garbage
        prof.confidence = min(p.confidence, score)
        prof.note = (prof.note + "; no confident TMDb match").strip("; ")
        return prof

    # accept the match; canonicalize title/year
    prof.tmdb_id = best["tmdb_id"]
    prof.title = best["title"] or prof.title
    if prof.kind == "episode":
        prof.show_year = best.get("year")
    else:
        prof.year = best.get("year") or prof.year
    prof.needs_approval = score < DRASTIC_THRESHOLD
    prof.confidence = round((p.confidence + score) / 2, 2)

    # 5. enrich
    ext = tmdb_provider.external_ids("tv" if prof.kind == "episode" else "movie",
                                     prof.tmdb_id, tmdb_key)
    prof.imdb_id, prof.tvdb_id = ext.get("imdb_id"), ext.get("tvdb_id")
    if prof.kind == "episode" and prof.season is not None and prof.episodes:
        et = tmdb_provider.episode_title(prof.tmdb_id, prof.season, prof.episodes[0], tmdb_key)
        if et:
            prof.episode_title = et
    if probe_fn:
        try:
            prof.mediainfo = probe_fn(path)
        except Exception:
            pass
    return prof
