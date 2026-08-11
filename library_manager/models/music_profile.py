"""Read-only, stable identity records for music library inspection."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Optional


@dataclass(frozen=True)
class TrackIdentity:
    subject: str
    relative_path: str
    artist: str = ""
    title: str = ""
    album: str = ""
    album_artist: str = ""
    disc: Optional[int] = None
    track: Optional[int] = None
    musicbrainz_artist_id: str = ""
    musicbrainz_album_id: str = ""
    musicbrainz_release_group_id: str = ""
    musicbrainz_recording_id: str = ""
    duration_seconds: Optional[float] = None
    codec: str = ""
    sample_rate: Optional[int] = None
    channels: Optional[int] = None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class MusicProfile:
    root_id: str
    subject: str
    state: str
    artist: str = ""
    album: str = ""
    musicbrainz_artist_id: str = ""
    musicbrainz_album_id: str = ""
    musicbrainz_release_group_id: str = ""
    lidarr_foreign_artist_id: str = ""
    lidarr_foreign_album_id: str = ""
    tracks: tuple[TrackIdentity, ...] = ()
    errors: tuple[str, ...] = ()
    evidence_digest: str = ""
    schema: str = "music.inspection.v1"

    def as_dict(self) -> dict:
        return asdict(self)
