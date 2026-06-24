"""Tests for video identification orchestration (mocked TMDb + AI).

Run: python test-env/test-video-identify.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from library_manager.video import identify as I
from library_manager.providers import tmdb as T

fails = []
def check(label, got, want):
    if got != want:
        fails.append(f"{label}: got {got!r} want {want!r}")

# --- mock the TMDb provider ---
def fake_movie(title, year=None, api_key="", lang="en-US"):
    db = {
        "the matrix": [{"tmdb_id": 603, "title": "The Matrix", "original_title": "The Matrix",
                        "year": 1999, "popularity": 80, "votes": 24000}],
        "honey i shrunk the kids": [{"tmdb_id": 9354, "title": "Honey, I Shrunk the Kids",
                                     "original_title": "Honey, I Shrunk the Kids", "year": 1989,
                                     "popularity": 30, "votes": 3000}],
    }
    return db.get((title or "").lower(), [])
def fake_tv(title, year=None, api_key="", lang="en-US"):
    db = {"house": [{"tmdb_id": 1408, "title": "House", "original_title": "House",
                     "year": 2004, "popularity": 90, "votes": 9000}]}
    return db.get((title or "").lower(), [])
T.search_movie = fake_movie
T.search_tv = fake_tv
T.external_ids = lambda kind, tid, key: {"imdb_id": "tt0133093", "tvdb_id": 12345}
T.episode_title = lambda tid, s, e, key, lang="en-US": "Larger Than Life"

# 1. clean movie
p = I.identify_file("The.Matrix.1999.1080p.BluRay.x265-X.mkv", tmdb_key="k")
check("matrix.tmdb", p.tmdb_id, 603)
check("matrix.approval", p.needs_approval, False)
check("matrix.rejected", p.rejected, False)
check("matrix.path", p.target_path("plex"), "The Matrix (1999)/The Matrix (1999).mkv")
check("matrix.imdb_jellyfin", p.target_path("jellyfin"),
      "The Matrix (1999)/The Matrix (1999) [tmdbid-603].mkv")

# 2. messy no-clean-title movie recovered via AI hook
def ai(fn): return {"title": "Honey I Shrunk the Kids", "year": 1989, "kind": "movie"}
p = I.identify_file("hncy.shrunk.kidz.WhoCares.xvid.avi", tmdb_key="k", ai_fn=ai)
check("ai.title", p.title, "Honey, I Shrunk the Kids")   # canonicalized by TMDb
check("ai.note", "ai-assisted" in p.note, True)
check("ai.tmdb", p.tmdb_id, 9354)

# 3. garbage match -> rejected
p = I.identify_file("Completely.Unknown.Thing.2021.1080p.mkv", tmdb_key="k")
check("garbage.rejected_or_unmatched", p.tmdb_id, None)

# 4. episode identify + enrichment
p = I.identify_file("House (2004) - S07E09 - whatever.1080p.mkv", tmdb_key="k")
check("ep.kind", p.kind, "episode")
check("ep.tmdb", p.tmdb_id, 1408)
check("ep.etitle_from_tmdb", p.episode_title, "Larger Than Life")
check("ep.path", p.target_path("plex"),
      "House (2004)/Season 07/House (2004) - S07E09 - Larger Than Life.mkv")

# 5. no tmdb key -> filename-only, no crash
p = I.identify_file("The.Matrix.1999.1080p.mkv", tmdb_key="")
check("nokey.title", p.title, "The Matrix")
check("nokey.no_match", p.tmdb_id, None)

if fails:
    print(f"FAILED {len(fails)}:")
    for f in fails: print("  -", f)
    sys.exit(1)
print("All video-identify tests passed.")
