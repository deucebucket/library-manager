"""Tests for video filename parsing + media-server naming presets.

Run: python test-env/test-video-naming.py   (exits non-zero on failure)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from library_manager.video.naming import parse_filename, format_movie, format_episode

fails = []
def check(label, got, want):
    if got != want:
        fails.append(f"{label}: got {got!r} want {want!r}")

# --- movie parsing ---
p = parse_filename("honey.i.shrunk.the.kids.1989.huggyboyMAX.1080p.mkv")
check("movie.kind", p.kind, "movie")
check("movie.title", p.title, "honey i shrunk the kids")
check("movie.year", p.year, 1989)
check("movie.res", p.resolution, "1080p")
check("movie.plex", format_movie(p, "plex"),
      "honey i shrunk the kids (1989)/honey i shrunk the kids (1989).mkv")
check("movie.jellyfin", format_movie(p, "jellyfin", tmdb_id=603),
      "honey i shrunk the kids (1989)/honey i shrunk the kids (1989) [tmdbid-603].mkv")

# --- edition handling ---
p = parse_filename("Blade.Runner.1982.Directors.Cut.2160p.UHD.BluRay.x265-X.mkv")
check("edition", p.edition, "director's cut")
check("edition.plex", format_movie(p, "plex"),
      "Blade Runner (1982)/Blade Runner (1982) {edition-Director'S Cut}.mkv")

# --- episode parsing ---
p = parse_filename("House (2004) - S07E09 - Larger Than Life (1080p BluRay x265 Panda).mkv")
check("ep.kind", p.kind, "episode")
check("ep.title", p.title, "House")
check("ep.season", p.season, 7)
check("ep.eps", p.episodes, [9])
check("ep.etitle", p.episode_title, "Larger Than Life")   # no trailing "("
check("ep.plex", format_episode(p, "plex"),
      "House (2004)/Season 07/House (2004) - S07E09 - Larger Than Life.mkv")

# --- multi-episode ---
p = parse_filename("SpongeBob SquarePants - S01E18-E19 - Nature Pants + Opposite Day WEBDL-1080p.mkv")
check("multi.eps", p.episodes, [18, 19])
check("multi.plex", format_episode(p, "plex"),
      "SpongeBob SquarePants/Season 01/SpongeBob SquarePants - S01E18E19 - Nature Pants + Opposite Day.mkv")

# --- specials -> Season 00 / Specials ---
p = parse_filename("Doctor Who (2005) - S00E11 - The Christmas Invasion.mkv")
check("special.plex", format_episode(p, "plex"),
      "Doctor Who (2005)/Specials/Doctor Who (2005) - S00E11 - The Christmas Invasion.mkv")
check("special.jellyfin", format_episode(p, "jellyfin"),
      "Doctor Who (2005)/Season 00/Doctor Who (2005) S00E11 - The Christmas Invasion.mkv")

# --- no-year movie -> low confidence (needs AI/TMDb) ---
p = parse_filename("Some.Obscure.Film.1080p.WEBRip.mkv")
check("noyear.conf<0.5", p.confidence < 0.5, True)

if fails:
    print(f"FAILED {len(fails)} checks:")
    for f in fails:
        print("  -", f)
    sys.exit(1)
print("All video-naming tests passed.")
