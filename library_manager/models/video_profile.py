"""VideoProfile — the confirmed identity of a movie/episode file.

The video equivalent of book_profile: what the file IS (after identification),
plus everything needed to name it for a target media server and to gate risky
renames behind approval.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

from library_manager.video.naming import Parsed, format_movie, format_episode


@dataclass
class VideoProfile:
    source_path: str
    kind: str                              # "movie" | "episode"
    parsed: Optional[Parsed] = None
    title: str = ""
    year: Optional[int] = None
    season: Optional[int] = None
    episodes: list = field(default_factory=list)
    episode_title: Optional[str] = None
    show_year: Optional[int] = None        # for episodes: the SHOW's year
    tmdb_id: Optional[int] = None
    tvdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    mediainfo: object = None               # utils.video.MediaInfo
    match_score: float = 0.0               # 0-1 title similarity to the match
    confidence: float = 0.0                # combined parse+match confidence
    needs_approval: bool = False           # drastic change -> human gate
    rejected: bool = False                 # garbage match -> no rename
    note: str = ""
    ext: str = ""

    def target_path(self, server: str = "plex") -> str:
        """Relative media-server path this file SHOULD live at."""
        p = self.parsed or Parsed(raw=self.source_path)
        p.title = self.title or p.title
        p.year = self.year or p.year
        p.ext = self.ext or p.ext
        if self.kind == "episode":
            p.season = self.season if self.season is not None else p.season
            p.episodes = self.episodes or p.episodes
            p.episode_title = self.episode_title or p.episode_title
            return format_episode(p, server, show_year=self.show_year or self.year,
                                  tvdb_id=self.tvdb_id)
        return format_movie(p, server, tmdb_id=self.tmdb_id, imdb_id=self.imdb_id)
