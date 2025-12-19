# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

- **What this is**: A single-process web app that scans audiobook/ebook libraries, proposes safe renames, and tracks history.
- **Tech**: Python + Flask (Jinja templates in `templates/`, static assets in `static/`), SQLite for persistence.
- **Main entrypoint**: `app.py` (monolithic by design; avoid "framework-izing" unless explicitly asked).

## Repository layout (high-signal)

- `app.py`: main Flask app (~350KB monolith), worker thread, scanning/AI logic, API routes.
- `abs_client.py`: Audiobookshelf API client (typed dataclasses for library sync).
- `audio_tagging.py`: metadata embedding into audio files (mutagen-based, supports MP3/M4B/FLAC/Ogg).
- `templates/`: server-rendered Jinja2 UI.
- `static/`: UI assets.
- `docs/`: user-facing documentation.
- `Dockerfile`, `docker-compose.yml`: container setup.
- `metadata_scraper/`: **separate git repo** - BookDB backend (not part of this project's codebase).

## Core architecture: layered processing pipeline

The app identifies books through a **layered verification pipeline** (defined in `app.py`):

### Layer 1: API Lookups (fast, free)
- `process_layer_1_api()` - BookDB (50M+ books), Audnexus, OpenLibrary, Google Books, Hardcover
- Items that get high-confidence matches are done; failures advance to Layer 2

### Layer 2: AI Verification (slower, has rate limits)
- `process_queue()` / `identify_book_with_ai()` - Gemini, OpenRouter, or Ollama
- Used for ambiguous cases, conflicting metadata, or when APIs return nothing

### Layer 3: Audio Analysis (expensive, optional)
- `process_layer_3_audio()` / `analyze_audio_sample()` - Gemini analyzes 90-second audio snippets
- Extracts metadata directly from narrator intros as final fallback

### Book Profile System
Each book gets a `BookProfile` with per-field confidence scoring:
- Sources are weighted: `audio (85) > id3 (80) > json (75) > bookdb (65) > ai (60) > path (40)`
- Multiple agreeing sources boost confidence; conflicts reduce it
- `profile_confidence_threshold` (default 85%) determines when to skip expensive layers

Key functions: `analyze_path()`, `gather_all_api_candidates()`, `identify_book_with_ai()`, `analyze_audio_sample()`

## Using Context7 for up-to-date library docs (MCP) [optional]

If you have the Context7 MCP server configured, use it when touching external library/API code where version-specific behavior matters:

- **Resolve the library ID**: use Context7 `resolve-library-id` for the package/framework name.
- **Fetch docs**: use Context7 `get-library-docs` with the resolved ID.

Skip for fully internal changes (pure refactor, local logic).

## How to run

### Local (Python)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

- App listens on **`http://localhost:5757`** by default.
- Override port with `PORT=XXXX`.

### Docker

- Persistent data is stored in the container at **`/data`** (see `DATA_DIR` below).
- Your audiobook library must be mounted into the container (commonly `/audiobooks`).

```bash
docker run -d \
  --name library-manager \
  -p 5757:5757 \
  -v /path/to/audiobooks:/audiobooks \
  -v library-manager-data:/data \
  ghcr.io/deucebucket/library-manager:latest
```

Or use `docker-compose.yml` (edit the host audiobook mount).

## Data / config model (do not guess)

`app.py` uses these paths:

- **`DATA_DIR`**: `Path(os.environ.get('DATA_DIR', BASE_DIR))`
  - In Docker, `DATA_DIR` is set to `/data` (see `Dockerfile`).
- **SQLite**: `${DATA_DIR}/library.db`
- **Config**: `${DATA_DIR}/config.json`
- **Secrets**: `${DATA_DIR}/secrets.json` (API keys)

On startup (`__main__`):

- `init_config()` creates default `config.json` and `secrets.json` if missing.
- `init_db()` creates/migrates tables.
- A background worker starts via `start_worker()`.

### Security rules

- `secrets.json` is gitignored; **never commit secrets**.
- Don’t log or echo API keys.
- Avoid introducing any new hardcoded paths or personal machine references.

## Testing

There's no unit test framework wired in; the project relies on an **integration test harness**.

### Integration tests (container-based)

```bash
# Full integration test suite (pulls from ghcr.io)
./test-env/run-integration-tests.sh

# Build from local source instead of pulling image
./test-env/run-integration-tests.sh --local

# Rebuild ~2GB test library first
./test-env/run-integration-tests.sh --rebuild
```

Notes:

- The harness uses **`podman`** by default. If you don't have podman, adapt locally to docker (don't commit that change unless requested).
- Tests validate the container can boot, UI returns 200, and core API endpoints respond.
- Test library generators: `test-env/generate-test-library.sh`, `test-env/generate-chaos-library.py`
- Naming issue tests: `test-env/test-naming-issues.py` (39 tests covering GitHub issues)

### Minimal local sanity checks

When you change Python code, at least ensure it still parses:

```bash
python -m py_compile app.py abs_client.py audio_tagging.py
```

## Coding conventions (match existing patterns)

- Prefer **small, surgical edits**. `app.py` is large; keep changes localized.
- Use **4-space indentation**, straightforward imperative style.
- Type hints are used in some modules (e.g., `abs_client.py`) but not everywhere; follow local style.
- Keep dependencies minimal (current `requirements.txt` is intentionally small).
- For UI, modify Jinja templates in `templates/` and keep changes backwards-compatible.

## Behavior & safety conventions (core project intent)

This project is “safety-first” about renames:

- Avoid auto-applying anything that could be wrong.
- Be very cautious about changing the AI prompts/rules that protect against false author swaps.
- Don’t weaken heuristics that prevent destructive renames.

If you touch any rename logic, ensure:

- We don’t overwrite existing folders/files.
- We can undo operations (history/undo flow stays intact).
- Docker-mounted paths continue to work (container only sees mounted paths).

## Best-practice extensions (match existing patterns)

These are “logical extensions” of the patterns already in `app.py`. Follow them to avoid subtle regressions.

### File/rename safety invariants (never break these)

- **Library boundary**: any filesystem operation must remain inside one of the configured `library_paths`.
  - Use the existing pattern: `Path(...).resolve().relative_to(lib_path)` to prove it’s inside.
- **Path construction**: build destinations through `build_new_path()` and its sanitizers.
  - Don’t hand-roll new paths; it already blocks traversal (`..`), strips invalid chars, enforces minimum depth, and prevents escaping the library root.
- **No merges**: if a destination folder exists and contains files, treat it as a conflict (often a different narrator/variant).
  - Prefer blocking with a clear error message rather than “helpfully” merging.
- **Depth checks**: avoid “too shallow” destinations (e.g., dumping at author level or library root).
- **File vs folder moves**: preserve the existing distinction:
  - Loose files/ebooks move into a folder + keep original filename.
  - Folder fixes move the folder.
- **History-first mindset**: for anything not obviously safe, record `pending_fix` and require manual approval.

### Queue/worker patterns (keep it predictable)

- **Config is live**: the worker reloads config each batch so changes take effect immediately—don’t cache config globally.
- **Rate limits are real**: keep API calls bounded via `max_requests_per_hour` and batch delays; don’t add loops that multiply calls silently.
- **Batching**: prefer processing in small batches (`batch_size`) and keeping DB transactions short.
- **Safety gates before processing**: preserve the existing “series folder / multi-book” detection that blocks dangerous auto-processing.

### SQLite + migrations (do the simple thing consistently)

- Use `get_db()` (WAL + timeout) for all DB access; keep connections short-lived.
- Close connections on all paths (success/early return/error).
- For lightweight migrations, follow the existing pattern:
  - `try: ALTER TABLE ... except: pass`
- Handle duplicates explicitly (`sqlite3.IntegrityError`) rather than letting the app crash.

### Adding/changing settings (end-to-end, not half-done)

When introducing a new setting or changing defaults, update all the “touch points”:

- `DEFAULT_CONFIG` (and `DEFAULT_SECRETS` if it’s a secret)
- Settings UI (`templates/settings.html`) so it’s user-configurable
- `load_config()` / `save_config()` semantics (secrets must stay out of `config.json`)
- Docs framework: update `CHANGELOG.md` and `README.md` when user-facing (see the framework below)

### API endpoints (keep responses boring and safe)

- Prefer returning JSON shaped like:
  - `{ "success": true, ... }` or `{ "success": false, "error": "..." }`
- Never return secrets (API keys) or host-specific paths in API responses.
- If an endpoint can trigger heavy work, keep it asynchronous or bounded (use queue + worker patterns).

### Prompt / AI guardrails (don’t degrade safety)

- Treat prompt edits as “high-risk changes”.
- Keep the existing “trust the input author” and garbage-match filtering philosophy intact.
- If you must adjust prompts/thresholds, add/extend test library cases or integration checks that cover the failure mode you’re addressing.

## Versioning + changelog (required for user-facing fixes)

The project uses a beta version string in `app.py`:

- `APP_VERSION = "0.9.0-beta.N"`

### Documentation + release notes framework (follow this every time)

When you change behavior, **treat docs as part of the feature**. Use this framework:

#### 1) Decide the “impact level”

- **User-facing**: Anything a user can notice (UI/UX, rename behavior, scanners, AI/provider behavior, new endpoints, settings, Docker behavior, installation/config).
- **Developer-facing**: Dev scripts, tests, CI, refactors that change how contributors work.
- **Internal-only**: Pure refactor with no observable behavior change.

#### 2) Update the right files

**Always update `CHANGELOG.md` for:**
- Any **user-facing** change (fix/improvement/feature/breaking change).
- Any **developer-facing** change that impacts running/testing/releasing.

**Update `README.md` when:**
- You changed **how to install/run** (Python/Docker/compose/env vars/ports/volumes).
- You added/changed a **headline feature** or a key workflow users rely on.
- You added/changed **core config knobs** (new settings, renamed settings, defaults that matter).
- You added/changed **API endpoints** documented in the README’s API table.

**Optional docs (`docs/`)**:
- If a change is too detailed for the README, update/add the appropriate file in `docs/` and link it from the README if needed.

#### 3) What to write (don’t be vague)

- **Changelog entries** should answer: what changed, who it affects, and any migration steps.
  - Prefer bullets under `Added / Changed / Improved / Fixed`.
  - Call out **breaking changes** explicitly and how to recover.
- **README updates** should be “front door” accurate:
  - Commands should be copy/pasteable.
  - Examples should use generic paths and never include secrets.
  - If you add a setting, mention what it does and where users configure it (web UI Settings).

#### 4) Order of operations for releases

When you ship a user-facing fix/feature:

- Bump `APP_VERSION` (increment the beta number).
- Add an entry to `CHANGELOG.md` for the new version.
- Ensure `README.md` is accurate for any new/changed user workflow (see rules above).

## GitHub automation (issue-bot)

There’s an automation script in `scripts/auto-fix-issues.sh` that can launch Claude Code with repo-specific guidance (`scripts/issue-bot-prompt.md`).

If you’re running it locally:

- Requires `gh`, `jq`, and `claude` on PATH (optionally `tmux`).
- It is designed to act like the maintainer and may push to `main`.

**Do not run automated issue workflows unless explicitly asked.**

## CI/CD

- GitHub Actions builds and publishes a multi-arch image to GHCR (see `.github/workflows/docker-publish.yml`).

## Key database tables (in `library.db`)

- **`books`**: All discovered books with current status, profile JSON, confidence scores
- **`queue`**: Items awaiting processing (tracks `verification_layer`: 0=not processed, 1=API, 2=AI, 3=Audio, 4=complete)
- **`history`**: All applied/pending fixes with original/new paths, status, metadata

## When in doubt

- Prefer aligning with existing user docs in `docs/` and the behavior implied by `README.md`.
- If you can't prove a change is safe, make it opt-in or require manual approval.
