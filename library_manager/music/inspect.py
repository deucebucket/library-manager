"""Bounded, read-only inspection of configured music roots."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from pathlib import Path, PurePosixPath
from typing import Callable, Optional

from library_manager.models.music_profile import MusicProfile, TrackIdentity

AUDIO_EXTENSIONS = frozenset({".flac", ".mp3", ".m4a", ".ogg", ".opus", ".wav"})
MAX_FILES = 500
MAX_RELATIVE_BYTES = 1024
_ROOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")


class InspectionUnavailable(Exception):
    pass


def _one(tags: dict, *names: str) -> str:
    for name in names:
        value = tags.get(name)
        if isinstance(value, (list, tuple)):
            value = value[0] if value else ""
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _number(value: str) -> Optional[int]:
    try:
        number = int(str(value).split("/", 1)[0])
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _default_tags(fileobj, _relative: str) -> tuple[dict, dict]:
    from mutagen import File
    media = File(fileobj, easy=True)
    if media is None:
        raise ValueError("unreadable_tags")
    tags = dict(media.tags or {})
    fileobj.seek(0)
    raw = File(fileobj, easy=False)
    raw_tags = dict(getattr(raw, "tags", {}) or {})
    for key, value in raw_tags.items():
        name = str(key).lower()
        if "musicbrainz" in name:
            tags.setdefault(name, value)
    info = getattr(media, "info", None)
    technical = {
        "duration_seconds": getattr(info, "length", None),
        "codec": _codec(info),
        "sample_rate": getattr(info, "sample_rate", None),
        "channels": getattr(info, "channels", None),
    }
    return tags, technical


def _codec(info: object) -> str:
    if info is None:
        return ""
    explicit = getattr(info, "codec", "")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()
    return {
        "mpeginfo": "mp3", "streaminfo": "flac", "oggvorbisinfo": "vorbis",
        "oggopusinfo": "opus", "wavestreaminfo": "pcm",
    }.get(type(info).__name__.lower(), "")


def _safe_relative(value: object) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise InspectionUnavailable("relative_path_invalid")
    if len(value.encode("utf-8")) > MAX_RELATIVE_BYTES:
        raise InspectionUnavailable("relative_path_too_long")
    pure = PurePosixPath(value.replace("\\", "/"))
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise InspectionUnavailable("relative_path_invalid")
    return pure.as_posix()


def _root(configured_roots: list[dict], root_id: object) -> tuple[str, int, os.stat_result]:
    if not isinstance(root_id, str) or not _ROOT_ID.fullmatch(root_id):
        raise InspectionUnavailable("root_id_invalid")
    if not isinstance(configured_roots, list):
        raise InspectionUnavailable("root_config_invalid")
    matches = [r for r in configured_roots if isinstance(r, dict) and r.get("id") == root_id]
    if len(matches) != 1 or set(matches[0]) != {"id", "path"}:
        raise InspectionUnavailable("root_unavailable")
    if not isinstance(matches[0]["path"], (str, os.PathLike)):
        raise InspectionUnavailable("root_config_invalid")
    path = os.path.abspath(os.fspath(matches[0]["path"]))
    root_fd = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        root_fd = os.open(path, flags)
        before = os.fstat(root_fd)
    except (OSError, TypeError, ValueError) as exc:
        if root_fd is not None:
            os.close(root_fd)
        raise InspectionUnavailable("root_unavailable") from exc
    if not stat.S_ISDIR(before.st_mode):
        os.close(root_fd)
        raise InspectionUnavailable("root_unsafe")
    return path, root_fd, before


def _open_target(root_fd: int, relative: str, device: int) -> int:
    current = os.dup(root_fd)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        for component in PurePosixPath(relative).parts:
            next_fd = os.open(component, flags, dir_fd=current)
            os.close(current)
            current = next_fd
            current_stat = os.fstat(current)
            if not stat.S_ISDIR(current_stat.st_mode) or current_stat.st_dev != device:
                raise InspectionUnavailable("target_device_mismatch")
        return current
    except InspectionUnavailable:
        os.close(current)
        raise
    except OSError as exc:
        os.close(current)
        raise InspectionUnavailable("target_unsafe") from exc


def _technical(values: object) -> tuple[dict, bool]:
    if not isinstance(values, dict) or set(values) != {
            "duration_seconds", "codec", "sample_rate", "channels"}:
        return {"duration_seconds": None, "codec": "", "sample_rate": None,
                "channels": None}, False
    duration, codec = values["duration_seconds"], values["codec"]
    sample_rate, channels = values["sample_rate"], values["channels"]
    valid = (isinstance(duration, (int, float)) and not isinstance(duration, bool)
             and math.isfinite(duration) and duration > 0
             and isinstance(codec, str) and 0 < len(codec) <= 32
             and isinstance(sample_rate, int) and not isinstance(sample_rate, bool)
             and 1000 <= sample_rate <= 768000
             and isinstance(channels, int) and not isinstance(channels, bool)
             and 1 <= channels <= 64)
    return values if valid else {"duration_seconds": None, "codec": "",
                                  "sample_rate": None, "channels": None}, valid


def _track(root_id: str, relative: str, tags: object, technical: dict,
           track_errors: list[str]) -> TrackIdentity:
    if not isinstance(tags, dict):
        tags = {}
        track_errors.append("tags_unavailable")
    mb_artist = _one(tags, "musicbrainz_artistid", "musicbrainz artist id",
                     "----:com.apple.itunes:musicbrainz artist id")
    mb_album = _one(tags, "musicbrainz_albumid", "musicbrainz album id",
                    "----:com.apple.itunes:musicbrainz album id")
    mb_group = _one(tags, "musicbrainz_releasegroupid", "musicbrainz release group id",
                    "----:com.apple.itunes:musicbrainz release group id")
    mb_recording = _one(tags, "musicbrainz_trackid", "musicbrainz recording id",
                        "----:com.apple.itunes:musicbrainz track id")
    ids = (mb_artist, mb_album, mb_group, mb_recording)
    if (not mb_artist or not (mb_album or mb_group) or not mb_recording
            or any(value and not _UUID.fullmatch(value) for value in ids)):
        track_errors.append("stable_identity_missing")
    return TrackIdentity(
        subject=_subject(root_id, relative), relative_path=relative,
        artist=_one(tags, "artist"), title=_one(tags, "title"), album=_one(tags, "album"),
        album_artist=_one(tags, "albumartist", "album artist"),
        disc=_number(_one(tags, "discnumber")), track=_number(_one(tags, "tracknumber")),
        musicbrainz_artist_id=mb_artist, musicbrainz_album_id=mb_album,
        musicbrainz_release_group_id=mb_group, musicbrainz_recording_id=mb_recording,
        duration_seconds=technical["duration_seconds"], codec=technical["codec"],
        sample_rate=technical["sample_rate"], channels=technical["channels"],
        errors=tuple(sorted(set(track_errors))))


def _subject(root_id: str, relative: str) -> str:
    return hashlib.sha256(f"{root_id}\0{relative}".encode()).hexdigest()


def _stable(value: str) -> str:
    return value.strip().lower()


def inspect_album(request: dict, configured_roots: list[dict], *,
                  tag_reader: Callable[[object, str], tuple[dict, dict]] = _default_tags,
                  before_finish: Optional[Callable[[], None]] = None,
                  max_files: int = MAX_FILES) -> MusicProfile:
    allowed = {"schema", "root_id", "relative_path", "identity"}
    if not isinstance(request, dict) or set(request) != allowed:
        raise ValueError("request_schema_invalid")
    if request.get("schema") != "music.inspection.request.v1":
        raise ValueError("request_version_unsupported")
    identity = request.get("identity")
    identity_keys = {"musicbrainz_artist_id", "musicbrainz_album_id",
                     "musicbrainz_release_group_id", "lidarr_foreign_artist_id",
                     "lidarr_foreign_album_id"}
    if not isinstance(identity, dict) or set(identity) != identity_keys:
        raise ValueError("identity_schema_invalid")
    if not any(identity.values()) or any(not isinstance(v, str) or (v and not _UUID.fullmatch(v))
                                         for v in identity.values()):
        raise ValueError("identity_invalid")
    if (identity["lidarr_foreign_artist_id"] and identity["musicbrainz_artist_id"]
            and _stable(identity["lidarr_foreign_artist_id"]) !=
            _stable(identity["musicbrainz_artist_id"])):
        raise ValueError("lidarr_artist_identity_mismatch")
    if (identity["lidarr_foreign_album_id"] and identity["musicbrainz_release_group_id"]
            and _stable(identity["lidarr_foreign_album_id"]) !=
            _stable(identity["musicbrainz_release_group_id"])):
        raise ValueError("lidarr_album_identity_mismatch")
    relative = _safe_relative(request.get("relative_path"))
    root, root_fd, root_before = _root(configured_roots, request.get("root_id"))
    try:
        target_fd = _open_target(root_fd, relative, root_before.st_dev)
        target_stat = os.fstat(target_fd)
        errors = []
        tracks = []
        count = 0
        try:
            for current, dirs, files, directory_fd in os.fwalk(
                    ".", topdown=True, follow_symlinks=False, dir_fd=target_fd):
                dirs.sort()
                files.sort()
                safe_dirs = []
                for name in dirs:
                    try:
                        entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                    except OSError as exc:
                        raise InspectionUnavailable("filesystem_unavailable") from exc
                    if stat.S_ISLNK(entry.st_mode) or entry.st_dev != root_before.st_dev:
                        errors.append("unsafe_entry")
                    else:
                        safe_dirs.append(name)
                dirs[:] = safe_dirs
                for name in files:
                    if Path(name).suffix.lower() not in AUDIO_EXTENSIONS:
                        continue
                    count += 1
                    if count > max_files:
                        raise InspectionUnavailable("scan_limit_exceeded")
                    file_fd = None
                    try:
                        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                        file_fd = os.open(name, flags, dir_fd=directory_fd)
                        before = os.fstat(file_fd)
                    except OSError as exc:
                        if file_fd is not None:
                            os.close(file_fd)
                        raise InspectionUnavailable("filesystem_unavailable") from exc
                    if not stat.S_ISREG(before.st_mode) or before.st_dev != root_before.st_dev:
                        os.close(file_fd)
                        errors.append("unsafe_entry")
                        continue
                    rel_dir = "" if current == "." else current[2:].replace(os.sep, "/")
                    rel = "/".join(part for part in (relative, rel_dir, name) if part)
                    track_errors = []
                    try:
                        with os.fdopen(os.dup(file_fd), "rb") as media_file:
                            tags, raw_technical = tag_reader(media_file, rel)
                        after = os.fstat(file_fd)
                        if ((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) !=
                                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)):
                            raise InspectionUnavailable("file_changed_during_scan")
                    except InspectionUnavailable:
                        raise
                    except Exception:
                        tags, raw_technical = {}, {}
                        track_errors.append("tags_unavailable")
                    finally:
                        os.close(file_fd)
                    technical, technical_valid = _technical(raw_technical)
                    if not technical_valid:
                        track_errors.append("technical_evidence_invalid")
                    tracks.append(_track(request["root_id"], rel, tags, technical, track_errors))
        finally:
            os.close(target_fd)
        if before_finish:
            before_finish()
        root_after = os.fstat(root_fd)
        check_fd = _open_target(root_fd, relative, root_before.st_dev)
        try:
            target_after = os.fstat(check_fd)
        finally:
            os.close(check_fd)
        if ((root_after.st_dev, root_after.st_ino) != (root_before.st_dev, root_before.st_ino)
                or (target_after.st_dev, target_after.st_ino) !=
                (target_stat.st_dev, target_stat.st_ino)):
            raise InspectionUnavailable("root_changed_during_scan")
    except InspectionUnavailable:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise InspectionUnavailable("filesystem_unavailable") from exc
    finally:
        os.close(root_fd)
    if not tracks:
        errors.append("no_supported_audio")

    album_ids = {_stable(t.musicbrainz_album_id) for t in tracks if t.musicbrainz_album_id}
    group_ids = {_stable(t.musicbrainz_release_group_id) for t in tracks if t.musicbrainz_release_group_id}
    artist_ids = {_stable(t.musicbrainz_artist_id) for t in tracks if t.musicbrainz_artist_id}
    albums = {_stable(t.album) for t in tracks if t.album}
    album_artists = {_stable(t.album_artist) for t in tracks if t.album_artist}
    if any(len(values) > 1 for values in (album_ids, group_ids, albums, album_artists)):
        errors.append("album_identity_conflict")
    requested = {
        "musicbrainz_artist_id": artist_ids,
        "musicbrainz_album_id": album_ids,
        "musicbrainz_release_group_id": group_ids,
        "lidarr_foreign_artist_id": artist_ids,
        "lidarr_foreign_album_id": group_ids,
    }
    for key, observed in requested.items():
        if identity[key] and _stable(identity[key]) not in observed:
            errors.append("requested_identity_mismatch")
    errors.extend(error for track in tracks for error in track.errors)
    errors = sorted(set(errors))
    state = "complete" if tracks and not errors else "partial"
    evidence = {
        "root_id": request["root_id"], "relative_path": relative, "identity": identity,
        "tracks": [track.__dict__ for track in tracks], "errors": errors,
    }
    digest = hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":"),
                                       default=str).encode()).hexdigest()
    first = tracks[0] if tracks else None
    return MusicProfile(
        root_id=request["root_id"], subject=_subject(request["root_id"], relative), state=state,
        artist=(first.album_artist or first.artist) if first else "", album=first.album if first else "",
        musicbrainz_artist_id=first.musicbrainz_artist_id if first else "",
        musicbrainz_album_id=first.musicbrainz_album_id if first else "",
        musicbrainz_release_group_id=first.musicbrainz_release_group_id if first else "",
        lidarr_foreign_artist_id=identity["lidarr_foreign_artist_id"],
        lidarr_foreign_album_id=identity["lidarr_foreign_album_id"],
        tracks=tuple(tracks), errors=tuple(errors), evidence_digest=digest)


def inspect_payload(body: object, configured_roots: list[dict], **kwargs) -> tuple[dict, int]:
    """Framework-neutral API contract, kept testable without starting Flask."""
    try:
        profile = inspect_album(body, configured_roots, **kwargs)
    except ValueError as exc:
        return {"schema": "music.inspection.error.v1", "state": "unavailable",
                "error": str(exc)}, 400
    except InspectionUnavailable as exc:
        return {"schema": "music.inspection.error.v1", "state": "unavailable",
                "error": str(exc)}, 503
    return profile.as_dict(), 200
