"""Video filename parsing + media-server naming conventions.

The hard part of organizing a video library is two things:
  1. Pulling a clean (title, year, season, episode, quality) out of a messy
     release name like  honey.i.shrunk.the.kids.1989.1080p.BluRay.x265-HUGGY.mkv
  2. Writing it back in the EXACT folder/file layout a given media server
     expects, because Plex / Jellyfin / Emby / Kodi each have their own picky
     rules and a wrong layout means the file doesn't get detected.

This module is media-type aware but server-agnostic: it parses to a neutral
`Parsed` record, then formats to whatever server preset the user picked. AI /
TMDb / TVDB identification (separate modules) refine the parse before formatting;
this module is the deterministic backbone they build on.

Nothing here touches disk — pure string/path logic so it's trivially testable.
"""
from __future__ import annotations
import os
import re
from dataclasses import dataclass, field
from typing import Optional

# ── token vocabularies ────────────────────────────────────────────────────
RES = {
    "2160p": ("2160p", "4k", "uhd", "2160"),
    "1080p": ("1080p", "1080i", "1080"),
    "720p":  ("720p", "720"),
    "480p":  ("480p", "576p", "sd", "dvdrip", "dvd"),
}
SOURCES = ("bluray", "blu-ray", "bdrip", "brrip", "remux", "web-dl", "webdl",
           "webrip", "web", "hdtv", "dvdrip", "dvd", "hdrip", "amzn", "nf",
           "dsnp", "hmax", "atvp", "hulu")
CODECS = ("x265", "x264", "h265", "h264", "hevc", "avc", "xvid", "divx", "av1", "10bit")
EDITIONS = {
    "director's cut": ("directors.cut", "director's.cut", "directors cut", "dc"),
    "extended": ("extended", "extended.cut", "extended.edition"),
    "unrated": ("unrated",),
    "remastered": ("remastered", "remaster"),
    "theatrical": ("theatrical",),
    "imax": ("imax",),
    "uncut": ("uncut",),
}

# SxxExx, SxxExxExx (multi), 1x02, Season 1 Episode 2, absolute "- 123" (anime)
_RE_SE = re.compile(r"[Ss](\d{1,2})[ ._-]?[Ee](\d{1,3})(?:[ ._-]?[Ee](\d{1,3}))?")
_RE_SE_X = re.compile(r"(?<!\d)(\d{1,2})x(\d{2,3})(?!\d)")
_RE_SE_WORD = re.compile(r"season[ ._]?(\d{1,2}).{0,4}episode[ ._]?(\d{1,3})", re.I)
_RE_YEAR = re.compile(r"(?:^|[^\d])((?:19|20)\d{2})(?:[^\d]|$)")
_RE_GROUP = re.compile(r"[-\[]([A-Za-z0-9]+)\]?$")


@dataclass
class Parsed:
    raw: str
    kind: str = "unknown"           # "movie" | "episode" | "unknown"
    title: str = ""
    year: Optional[int] = None
    season: Optional[int] = None
    episodes: list = field(default_factory=list)  # supports multi-ep files
    episode_title: Optional[str] = None
    resolution: Optional[str] = None
    source: Optional[str] = None
    codec: Optional[str] = None
    edition: Optional[str] = None
    group: Optional[str] = None
    ext: str = ""
    confidence: float = 0.0         # 0-1, how sure the parse is

    @property
    def episode(self):
        return self.episodes[0] if self.episodes else None


def _clean_title(s: str) -> str:
    s = re.sub(r"[._]+", " ", s)
    s = re.sub(r"\s*[\(\[][^\)\]]*[\)\]]\s*$", " ", s)  # trailing complete bracket junk
    s = re.sub(r"\s*[\(\[]\s*$", "", s)                  # trailing orphan open bracket
    s = re.sub(r"\s{2,}", " ", s).strip(" -_.([")
    return s


def _find_res(low: str) -> Optional[str]:
    for canon, toks in RES.items():
        if any(re.search(rf"(?<![a-z0-9]){re.escape(t)}(?![a-z0-9])", low) for t in toks):
            return canon
    return None


def _find_token(low: str, vocab) -> Optional[str]:
    for t in vocab:
        if re.search(rf"(?<![a-z0-9]){re.escape(t)}(?![a-z0-9])", low):
            return t
    return None


def _find_edition(low: str) -> Optional[str]:
    for canon, toks in EDITIONS.items():
        if any(t in low for t in toks):
            return canon
    return None


def parse_filename(path: str) -> Parsed:
    """Best-effort deterministic parse of a video file path. AI/TMDb refine later."""
    base = os.path.basename(path)
    stem, ext = os.path.splitext(base)
    p = Parsed(raw=base, ext=ext.lower().lstrip("."))
    low = stem.lower()

    p.resolution = _find_res(low)
    p.source = _find_token(low, SOURCES)
    p.codec = _find_token(low, CODECS)
    p.edition = _find_edition(low)
    gm = _RE_GROUP.search(stem)
    if gm and gm.group(1).lower() not in ("1080p", "720p", "2160p"):
        p.group = gm.group(1)

    # season/episode
    m = _RE_SE.search(stem) or _RE_SE_WORD.search(stem) or _RE_SE_X.search(stem)
    if m:
        p.kind = "episode"
        p.season = int(m.group(1))
        p.episodes = [int(g) for g in m.groups()[1:] if g]
        # title = everything before the SxxExx marker
        head = stem[:m.start()]
        ym = _RE_YEAR.search(head)
        if ym:
            p.year = int(ym.group(1))
            head = head[:ym.start()]
        p.title = _clean_title(head)
        # episode title = text after the marker, stripped of quality tokens
        tail = stem[m.end():]
        tail = re.split(r"(?i)\b(1080p|720p|2160p|480p|bluray|web-?dl|webrip|hdtv|x26[45]|hevc)\b", tail)[0]
        et = _clean_title(tail)
        p.episode_title = et or None
        p.confidence = 0.9 if p.title else 0.5
    else:
        # movie: title (year) [quality...]
        ym = _RE_YEAR.search(stem)
        p.kind = "movie"
        if ym:
            p.year = int(ym.group(1))
            p.title = _clean_title(stem[:ym.start()])
            p.confidence = 0.9 if p.title else 0.4
        else:
            # no year — title is everything up to the first quality/source token
            cut = re.split(r"(?i)\b(1080p|720p|2160p|480p|bluray|web-?dl|webrip|hdtv|x26[45]|hevc|dvdrip)\b", stem)[0]
            p.title = _clean_title(cut)
            p.confidence = 0.35  # low — needs AI/TMDb to confirm year + identity
    return p


# ── media-server naming presets ───────────────────────────────────────────
# Each preset knows how to lay out a confirmed Parsed (post-identification) for
# a given server. Folder + filename templates use {title} {year} {s} {e} {etitle}
# {res} {edition}. Servers differ mainly in: id tags, "Season 0N" vs "Season N",
# specials handling, and whether quality goes in the name.

def _season_dir(server: str, season: int) -> str:
    if season == 0:
        return "Specials" if server in ("plex", "kodi") else "Season 00"
    return f"Season {season:02d}"


def _yeartag(p: Parsed) -> str:
    return f" ({p.year})" if p.year else ""


def _tag_default(server: str, tag_ids) -> bool:
    # Jellyfin/Emby matching is materially improved by id tags; Plex/Kodi match
    # cleanly on name+year, so default them to clean names. Caller can force.
    return (server in ("jellyfin", "emby")) if tag_ids is None else bool(tag_ids)


def format_movie(p: Parsed, server: str = "plex", *, tmdb_id=None, imdb_id=None,
                 tag_ids=None) -> str:
    """Return RELATIVE path:  'Movie (Year)/Movie (Year)[ tags].ext'."""
    folder = f"{p.title}{_yeartag(p)}".strip()
    name = folder
    if p.edition:
        name += f" {{edition-{p.edition.title()}}}" if server in ("plex",) else f" - {p.edition.title()}"
    if _tag_default(server, tag_ids):
        if server in ("jellyfin", "emby"):
            if tmdb_id:
                name += f" [tmdbid-{tmdb_id}]"
            elif imdb_id:
                name += f" [imdbid-{imdb_id}]"
        elif imdb_id:
            name += f" {{imdb-{imdb_id}}}"
    return f"{folder}/{name}.{p.ext}"


def format_episode(p: Parsed, server: str = "plex", *, show_year=None,
                   tvdb_id=None, tag_ids=None) -> str:
    """Return RELATIVE path under the show folder:
    'Show (Year)/Season 01/Show (Year) - S01E02 - Title.ext'."""
    yr = show_year or p.year
    show = f"{p.title}" + (f" ({yr})" if yr else "")
    show_folder = show
    if _tag_default(server, tag_ids) and server in ("jellyfin", "emby") and tvdb_id:
        show_folder += f" [tvdbid-{tvdb_id}]"
    sd = _season_dir(server, p.season or 0)
    ep = "".join(f"E{e:02d}" for e in p.episodes) or "E00"
    epcode = f"S{(p.season or 0):02d}{ep}"
    et = f" - {p.episode_title}" if p.episode_title else ""
    if server in ("jellyfin", "emby"):
        fname = f"{show} {epcode}{et}.{p.ext}"
    else:  # plex, kodi, sonarr-style
        fname = f"{show} - {epcode}{et}.{p.ext}"
    return f"{show_folder}/{sd}/{fname}"


SERVERS = ("plex", "jellyfin", "emby", "kodi")
