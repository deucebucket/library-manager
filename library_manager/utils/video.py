"""ffprobe-based technical metadata for video files.

Reads the ACTUAL stream properties (resolution, codec, HDR, audio, runtime) so
naming/sorting reflects the file, not just whatever the release name claimed.
Release names lie ("1080p" on a 720p file); ffprobe doesn't.

ffprobe is located once (PATH or common Windows locations). If it's missing the
functions degrade gracefully to None rather than raising, so the pipeline can
still run on filename data alone.
"""
from __future__ import annotations
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Optional

_FFPROBE = None


def ffprobe_path() -> Optional[str]:
    global _FFPROBE
    if _FFPROBE is not None:
        return _FFPROBE or None
    cand = shutil.which("ffprobe")
    if not cand:
        for p in (
            os.path.expanduser(r"~\AppData\Local\Microsoft\WinGet\Links\ffprobe.exe"),
            r"C:\ffmpeg\bin\ffprobe.exe",
            os.path.expanduser(r"~\MusicVideoTools\ffprobe.exe"),
        ):
            if os.path.exists(p):
                cand = p
                break
    # Cache only a discovered executable. A long-running install can gain ffprobe
    # later (package install or remounted tool volume); a negative probe must not
    # make that absence permanent until process restart.
    if cand:
        _FFPROBE = cand
    return cand


def _res_label(h: int, w: int) -> Optional[str]:
    v = max(h, 0)
    # use height, but fall back to width-based heuristics for odd aspect ratios
    if v >= 1700 or w >= 3000:
        return "2160p"
    if v >= 900 or w >= 1800:
        return "1080p"
    if v >= 600 or w >= 1200:
        return "720p"
    if v > 0:
        return "480p"
    return None


@dataclass
class MediaInfo:
    resolution: Optional[str] = None       # 2160p/1080p/720p/480p
    width: Optional[int] = None
    height: Optional[int] = None
    video_codec: Optional[str] = None      # h264/hevc/av1...
    bit_depth: Optional[int] = None        # 8/10
    hdr: bool = False
    audio_codec: Optional[str] = None
    audio_channels: Optional[int] = None
    duration_sec: Optional[float] = None
    probed: bool = False                   # True if ffprobe actually ran

    @property
    def quality_tag(self) -> str:
        """e.g. '1080p HEVC 10bit' — useful in filenames where quality is kept."""
        bits = [self.resolution]
        if self.video_codec:
            bits.append({"hevc": "HEVC", "h264": "H264", "av1": "AV1"}.get(
                self.video_codec, self.video_codec.upper()))
        if self.bit_depth and self.bit_depth >= 10:
            bits.append(f"{self.bit_depth}bit")
        if self.hdr:
            bits.append("HDR")
        return " ".join(b for b in bits if b)


def probe(path: str, timeout: int = 30) -> MediaInfo:
    """Return MediaInfo for a video file. Never raises; returns empty info on failure."""
    info = MediaInfo()
    exe = ffprobe_path()
    if not exe or not os.path.isfile(path):
        return info
    try:
        out = subprocess.run(
            [exe, "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True, timeout=timeout,
        )
        data = json.loads(out.stdout or "{}")
    except Exception:
        return info
    info.probed = True
    fmt = data.get("format", {})
    try:
        info.duration_sec = float(fmt.get("duration")) if fmt.get("duration") else None
    except (TypeError, ValueError):
        pass
    for s in data.get("streams", []):
        if s.get("codec_type") == "video" and info.video_codec is None:
            info.video_codec = (s.get("codec_name") or "").lower() or None
            info.width = s.get("width")
            info.height = s.get("height")
            if info.width and info.height:
                info.resolution = _res_label(info.height, info.width)
            pf = (s.get("pix_fmt") or "").lower()
            info.bit_depth = 10 if ("10le" in pf or "10be" in pf or "p010" in pf) else 8
            ct = (s.get("color_transfer") or "").lower()
            cp = (s.get("color_primaries") or "").lower()
            info.hdr = any(x in ct for x in ("smpte2084", "arib-std-b67")) or "bt2020" in cp
        elif s.get("codec_type") == "audio" and info.audio_codec is None:
            info.audio_codec = (s.get("codec_name") or "").lower() or None
            info.audio_channels = s.get("channels")
    return info
