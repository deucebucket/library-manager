# Video Libraries (TV + Movies) — Design & Roadmap

Additive media-type for Library Manager. A user who only wants audiobooks sees no
video tabs; enabling TV/Movies adds them. Reuses the existing engine (scan →
identify → verify → rename/approve → undo → history) and automation API.

## Design decisions (from owner)

- **Identification is independent & self-contained** (the reason to use Library
  Manager, not just *arr): identify from scratch via **TMDb + TVDB**, with
  **user-supplied API keys**, plus **AI title-parsing** for messy releases
  (e.g. `honey.i.shrunk.the.kids.1989.huggyboyMAX.1080p.mkv` → ask the user's
  chosen model "what movie is this?"). Skaldleita stays audiobook-only.
- **Plex DB is verification only** — read Plex's SQLite to reconcile what's on
  disk vs what's actually in the library (find stranded / phantom / split). Not
  the source of truth.
- **Technical metadata via ffprobe** — resolution, codec, audio, HDR — read from
  the file to name/sort correctly (e.g. quality tag, edition).
- **Media-server-aware output** — folder & filename layout is picky and differs
  per server. Ship selectable presets so files are *detected* correctly:
  - Movies: `Movie (Year)/Movie (Year)[ {edition-…}][ tags].ext`
  - TV: `Show (Year)/Season NN/Show (Year) - SxxExx - Title.ext`
  - Server differences handled: Plex `Specials` vs Jellyfin `Season 00`; id tags
    `{tmdb-…}`/`[tmdbid-…]`; quality-in-name. (See `library_manager/video/naming.py`.)
- **Progressive trust per media type** — each manager (audiobooks / TV / movies)
  has its own progression from *approve-every-step* (safest) up to *autopilot*
  (the system "runs wild" on high-confidence). Goal: a flawless system you can
  trust. Drastic-change approval + undo always available.

## Phases

1. **Naming core** ✅ (`video/naming.py` + tests) — deterministic parse of messy
   release names → neutral record → server-specific path. Proven on real library.
2. **Identify** — `providers/tmdb.py`, `providers/tvdb.py`, AI title-parse layer
   (reuse existing gemini/ollama/openrouter providers), `models/video_profile.py`,
   ffprobe reader (`utils/video.py`). Confirms title/year/ids + episode titles.
3. **Reconcile** — `plex_client.py` (read Plex SQLite → file index) + disk index;
   surface stranded/phantom/split/orphan as scan findings with the audit logic.
4. **Organize** — rename/move into the chosen server preset, through the existing
   approval/undo/history + drastic-change gate; per-media-type trust setting. The
   provider-neutral root-move coordinator is implemented in `video/organize.py`: it
   requires authoritative idle evidence and approval, sets `move_files=true`, verifies
   the catalogue destination plus exact media-server identity, records no titles/paths,
   and performs a verified provider rollback on a failed postcondition. Narrow Radarr and
   Jellyfin adapters now implement database-consistent root moves and exact-library
   provider-ID verification without importing the Flask monolith. The separate loopback
   apply service accepts only a canonical HMAC-bound, one-use approval plan using configured
   opaque root/library IDs; see `docs/video-organization-api.md`. Live adapter acceptance
   remains to complete this phase.
5. **Modular UI + API** — the first review-only slice is implemented as a separate Flask
   blueprint: independently toggleable Movie/TV managers, `/api/plex/status`, enabled
   sections, exact-section reconciliation, and a responsive dashboard. Findings are
   bounded and relative to their configured Plex section; the blueprint receives neither
   the organization capability secret nor a move verb. Review/apply handoff and
   per-manager progressive trust remain.

## Status
- Branch: `feature/video-libraries`
- Done: phase 1 (naming) + phase 2 (TMDb identify, ffprobe, identify orchestration) + phase 3 (Plex reconcile, file-level).
- Next: finish phase 4 live adapter acceptance, then bind reviewed plans to the phase-5
  one-use approval handoff and add per-manager progressive trust.
