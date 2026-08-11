"""Fixture-only acceptance tests for the read-only Music inspection API."""
from __future__ import annotations

import os
import stat
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from library_manager.music.inspect import InspectionUnavailable, _codec, inspect_album, inspect_payload

fails = []
ARTIST_A = "11111111-1111-4111-8111-111111111111"
ARTIST_B = "22222222-2222-4222-8222-222222222222"
RELEASE_A = "33333333-3333-4333-8333-333333333333"
GROUP_A = "44444444-4444-4444-8444-444444444444"
DELUXE = "55555555-5555-4555-8555-555555555555"


def check(label, condition, detail=""):
    if not condition:
        fails.append(f"{label}: {detail}")


def request(root_id="music", relative="Artist/Album", **identity):
    values = {
        "musicbrainz_artist_id": ARTIST_A,
        "musicbrainz_album_id": RELEASE_A,
        "musicbrainz_release_group_id": GROUP_A,
        "lidarr_foreign_artist_id": ARTIST_A,
        "lidarr_foreign_album_id": GROUP_A,
    }
    values.update(identity)
    return {"schema": "music.inspection.request.v1", "root_id": root_id,
            "relative_path": relative, "identity": values}


def reader(_fileobj, relative):
    stem = Path(relative).stem
    disc, track = stem.split("-")[:2]
    tags = {
        "artist": ["Artist A"], "albumartist": ["Artist A"], "album": ["Album A"],
        "title": [f"Song {track}"], "discnumber": [disc], "tracknumber": [track],
        "musicbrainz_artistid": [ARTIST_A], "musicbrainz_albumid": [RELEASE_A],
        "musicbrainz_releasegroupid": [GROUP_A],
        "musicbrainz_trackid": [f"66666666-6666-4666-8666-{int(disc):02d}{int(track):02d}66666666"],
    }
    return tags, {"duration_seconds": 123.5, "codec": Path(relative).suffix[1:],
                  "sample_rate": 48000, "channels": 2}


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp) / "root"
    album = root / "Artist" / "Album"
    album.mkdir(parents=True)
    formats = ("flac", "mp3", "m4a", "ogg", "opus", "wav")
    for number, ext in enumerate(formats, 1):
        (album / f"1-{number}.{ext}").write_bytes(b"fixture")
    roots = [{"id": "music", "path": str(root)}]

    profile = inspect_album(request(), roots, tag_reader=reader)
    check("complete stable album", profile.state == "complete", profile.errors)
    check("all supported formats", [t.codec for t in profile.tracks] == list(formats))
    check("multidisc fields are typed", profile.tracks[0].disc == 1 and profile.tracks[0].track == 1)
    check("paths are relative", all(not os.path.isabs(t.relative_path) for t in profile.tracks))
    check("opaque subjects hide paths", all("Artist" not in t.subject for t in profile.tracks))
    check("deterministic output", profile == inspect_album(request(), roots, tag_reader=reader))
    class MP4Info:
        codec = "alac"
    class OggOpusInfo:
        pass
    class UnknownInfo:
        pass
    MPEGInfo = type("MPEGInfo", (), {})
    StreamInfo = type("StreamInfo", (), {})
    WaveStreamInfo = type("WaveStreamInfo", (), {})
    check("codec evidence uses explicit stream codec", _codec(MP4Info()) == "alac")
    check("codec evidence has conservative known mappings", _codec(OggOpusInfo()) == "opus")
    check("Mutagen MP3 class maps truthfully", _codec(MPEGInfo()) == "mp3")
    check("Mutagen FLAC class maps truthfully", _codec(StreamInfo()) == "flac")
    check("Mutagen WAV class maps truthfully", _codec(WaveStreamInfo()) == "pcm")
    check("unknown info type never invents codec", _codec(UnknownInfo()) == "")

    try:
        inspect_album(request(relative="../elsewhere"), roots, tag_reader=reader)
        check("traversal refuses", False)
    except InspectionUnavailable as exc:
        check("traversal refuses", str(exc) == "relative_path_invalid", exc)

    outside = Path(tmp) / "outside"
    outside.mkdir()
    (outside / "1-1.flac").write_bytes(b"fixture")
    (root / "link").symlink_to(outside, target_is_directory=True)
    try:
        inspect_album(request(relative="link"), roots, tag_reader=reader)
        check("symlink target refuses", False)
    except InspectionUnavailable as exc:
        check("symlink target refuses", str(exc) == "target_unsafe", exc)

    (root / "Alias").symlink_to(root / "Artist", target_is_directory=True)
    try:
        inspect_album(request(relative="Alias/Album"), roots, tag_reader=reader)
        check("intermediate symlink refuses", False)
    except InspectionUnavailable as exc:
        check("intermediate symlink refuses", str(exc) == "target_unsafe", exc)

    original_fstat = os.fstat
    root_inode = os.stat(root).st_ino
    calls = {"root": 0}
    def device_swap(fd):
        result = original_fstat(fd)
        if result.st_ino == root_inode:
            calls["root"] += 1
            if calls["root"] > 1:
                values = list(result)
                values[2] += 1
                return os.stat_result(values)
        return result
    saved = os.fstat
    os.fstat = device_swap
    try:
        try:
            inspect_album(request(), roots, tag_reader=reader)
            check("root device swap refuses", False)
        except InspectionUnavailable as exc:
            check("root device swap refuses", str(exc) == "root_changed_during_scan", exc)
    finally:
        os.fstat = saved

    changed = {"done": False}
    def mutate():
        if not changed["done"]:
            changed["done"] = True
            os.replace(album, root / "Artist" / "old")
            album.mkdir()
    try:
        inspect_album(request(), roots, tag_reader=reader, before_finish=mutate)
        check("target replacement refuses", False)
    except InspectionUnavailable as exc:
        check("target replacement refuses", str(exc) == "root_changed_during_scan", exc)
    os.replace(root / "Artist" / "old", album)

    def malformed(_fileobj, _relative):
        raise ValueError("bad tags")
    partial = inspect_album(request(), roots, tag_reader=malformed)
    check("malformed tags are partial", partial.state == "partial" and
          set(partial.errors) == {"requested_identity_mismatch", "stable_identity_missing",
                                  "tags_unavailable", "technical_evidence_invalid"}, partial.errors)

    def homonym(fileobj, relative):
        tags, technical = reader(fileobj, relative)
        tags["musicbrainz_artistid"] = [ARTIST_B]
        return tags, technical
    wrong = inspect_album(request(), roots, tag_reader=homonym)
    check("artist homonym refuses complete", wrong.state == "partial" and
          "requested_identity_mismatch" in wrong.errors, wrong.errors)

    def edition(fileobj, relative):
        tags, technical = reader(fileobj, relative)
        if Path(relative).stem.endswith("2"):
            tags["musicbrainz_albumid"] = [DELUXE]
        return tags, technical
    mixed = inspect_album(request(), roots, tag_reader=edition)
    check("mixed editions are partial", mixed.state == "partial" and
          "album_identity_conflict" in mixed.errors, mixed.errors)

    def compilation(fileobj, relative):
        tags, technical = reader(fileobj, relative)
        tags["albumartist"] = ["Various Artists"]
        tags["musicbrainz_artistid"] = [ARTIST_A if Path(relative).stem.endswith("1") else ARTIST_B]
        return tags, technical
    compilation_profile = inspect_album(
        request(musicbrainz_artist_id=""), roots, tag_reader=compilation)
    check("compilation permits track artists", compilation_profile.state == "complete",
          compilation_profile.errors)

    def invalid_technical(fileobj, relative):
        tags, _ = reader(fileobj, relative)
        return tags, {"duration_seconds": float("nan"), "codec": "flac",
                      "sample_rate": "48000", "channels": -1}
    invalid = inspect_album(request(), roots, tag_reader=invalid_technical)
    check("invalid technical evidence is partial and sanitized",
          invalid.state == "partial" and "technical_evidence_invalid" in invalid.errors
          and invalid.tracks[0].duration_seconds is None, invalid.errors)

    try:
        inspect_album({**request(), "extra": True}, roots, tag_reader=reader)
        check("unknown request field refuses", False)
    except ValueError as exc:
        check("unknown request field refuses", str(exc) == "request_schema_invalid", exc)
    bad_version = request()
    bad_version["schema"] = "music.inspection.request.v2"
    try:
        inspect_album(bad_version, roots, tag_reader=reader)
        check("unknown version refuses", False)
    except ValueError as exc:
        check("unknown version refuses", str(exc) == "request_version_unsupported", exc)
    try:
        inspect_album(request(root_id="missing"), roots, tag_reader=reader)
        check("configured root outage refuses", False)
    except InspectionUnavailable as exc:
        check("configured root outage refuses", str(exc) == "root_unavailable", exc)
    for malformed_roots in (None, [{"id": "music", "path": None}]):
        payload, status = inspect_payload(request(), malformed_roots, tag_reader=reader)
        check("malformed root config is unavailable", status == 503 and
              payload["state"] == "unavailable", (payload, status))

    # A failure after open must not leak one descriptor per retry.
    descriptor_dir = Path("/proc/self/fd")
    if descriptor_dir.exists():
        baseline = len(list(descriptor_dir.iterdir()))
        original_fstat = os.fstat
        def fail_audio_fstat(fd):
            result = original_fstat(fd)
            if stat.S_ISREG(result.st_mode):
                raise OSError("fixture outage")
            return result
        os.fstat = fail_audio_fstat
        try:
            for _ in range(5):
                payload, status = inspect_payload(request(), roots, tag_reader=reader)
                check("post-open outage is unavailable", status == 503 and
                      payload["state"] == "unavailable", (payload, status))
        finally:
            os.fstat = original_fstat
        check("post-open outage leaks no descriptors",
              len(list(descriptor_dir.iterdir())) == baseline,
              (baseline, len(list(descriptor_dir.iterdir()))))

        baseline = len(list(descriptor_dir.iterdir()))
        calls = {"count": 0}
        def fail_root_fstat(fd):
            calls["count"] += 1
            if calls["count"] == 1:
                raise OSError("fixture root outage")
            return original_fstat(fd)
        os.fstat = fail_root_fstat
        try:
            payload, status = inspect_payload(request(), roots, tag_reader=reader)
            check("root post-open outage is unavailable", status == 503 and
                  payload["state"] == "unavailable", (payload, status))
        finally:
            os.fstat = original_fstat
        check("root post-open outage leaks no descriptors",
              len(list(descriptor_dir.iterdir())) == baseline,
              (baseline, len(list(descriptor_dir.iterdir()))))
    unrelated = request(musicbrainz_artist_id="", musicbrainz_album_id="",
                        musicbrainz_release_group_id="",
                        lidarr_foreign_artist_id="", lidarr_foreign_album_id=DELUXE)
    profile = inspect_album(unrelated, roots, tag_reader=reader)
    check("Lidarr album identity is checked against release group",
          profile.state == "partial" and "requested_identity_mismatch" in profile.errors,
          profile.errors)
    try:
        inspect_album(request(), roots, tag_reader=reader, max_files=1)
        check("bounded scan refuses overflow", False)
    except InspectionUnavailable as exc:
        check("bounded scan refuses overflow", str(exc) == "scan_limit_exceeded", exc)

    payload, status = inspect_payload(request(), roots, tag_reader=reader)
    check("versioned API contract complete", status == 200 and
          payload["schema"] == "music.inspection.v1")
    payload, status = inspect_payload({"schema": "wrong"}, roots, tag_reader=reader)
    check("API schema failure deterministic", status == 400 and
          payload["state"] == "unavailable")

    # Capability proof: inspection owns no filesystem mutation verb.
    forbidden = []
    saved_rename, saved_replace, saved_unlink, saved_remove = (
        os.rename, os.replace, os.unlink, os.remove)
    def mutation(*args, **kwargs):
        forbidden.append((args, kwargs))
        raise AssertionError("mutation attempted")
    os.rename = os.replace = os.unlink = os.remove = mutation
    try:
        check("inspection has no mutation side effect",
              inspect_album(request(), roots, tag_reader=reader).state == "complete"
              and not forbidden)
    finally:
        os.rename, os.replace, os.unlink, os.remove = (
            saved_rename, saved_replace, saved_unlink, saved_remove)

if fails:
    print(f"FAILED {len(fails)}")
    for failure in fails:
        print(" -", failure)
    raise SystemExit(1)
print("All music-inspection tests passed.")
