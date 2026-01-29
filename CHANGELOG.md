# Changelog

All notable changes to Library Manager will be documented in this file.

## [0.9.0-beta.101] - 2026-01-29

### Added

- **Issue #81: Multi-Language Audiobook Naming** - Intelligent naming based on book language
  - **Three naming modes:**
    - `Native` - Books keep their original language titles (Russian book → Russian title)
    - `Preferred` - All books use your preferred language (everything in English)
    - `Tagged` - Use preferred language with language tag ("Metro 2033 (Russian)")
  - **Four tag formats:**
    - `(Polish)` - Full name in parentheses (default)
    - `[pl]` - ISO code in brackets
    - `Polish` - Full name without brackets
    - `_pl` - ISO code suffix
  - **Three tag positions:**
    - After title: `Author/Title (Russian)/`
    - Before title: `Author/(Russian) Title/`
    - Subfolder: `Author/Russian/Title/`
  - **Custom template tags:** `{language}` and `{lang_code}` for full control
  - **28 languages supported:** English, German, French, Spanish, Italian, Portuguese, Dutch, Swedish, Norwegian, Danish, Finnish, Polish, Russian, Japanese, Chinese, Korean, Arabic, Hebrew, Hindi, Turkish, Czech, Hungarian, Greek, Thai, Vietnamese, Ukrainian, Romanian, Indonesian
  - Tags only applied to non-preferred languages (English books won't get "(English)" tag if English is preferred)

- **SL Trust Mode** - Control when to trust Skaldleita audio identification
  - `Full` - Trust all Skaldleita results (fastest, recommended)
  - `Boost` - Trust high-confidence results, verify medium/low with AI
  - `Legacy` - Always run AI verification (original behavior)
  - Reduces unnecessary API calls when Skaldleita provides confident matches

### Fixed

- **Issue #81: Polish Language Matching Failures** - Strict language matching now works correctly
  - Polish and other non-English books now properly identified via Skaldleita
  - Language detection integrated into all processing pipelines
  - Fixed missing language_code parameter in pipeline layer build_new_path calls

### Changed

- **Skaldleita Config Rename** - `use_bookdb_for_audio` renamed to `use_skaldleita_for_audio`
  - Backwards compatible - old config name still works
  - New helper function handles migration automatically
  - UI labels updated throughout Settings

---

## [0.9.0-beta.100] - 2026-01-28

### Added

- **Dashboard Activity Log** - Real-time processing results on dashboard
  - Shows last 15 processed books with full metadata
  - Separate columns: Time, Author, Title, Narrator, Series, Book #, Status
  - Auto-refreshes every 5 seconds without page reload
  - Detects author-narrated audiobooks (shows "📖 Author" in narrator column)
  - Only shows actual processed items (verified, needs_fix, needs_attention)

- **Skaldleita IDs in BookProfile** - New fields for instant audiobook lookup
  - `audio_fingerprint` - Chromaprint fingerprint for audio matching
  - `narrator_id` - Voice cluster ID or known narrator name
  - `book_id` - ISBN, ASIN, or internal ID
  - `version_id` - Unique recording version identifier
  - `voice_cluster_id` - Links unknown voices for future identification

- **Extended Metadata Embedding** - Skaldleita IDs written to audio files
  - MP3: TXXX frames for NARRATORID, AUDIOFINGERPRINT, BOOKID, VERSIONID
  - M4B: Freeform atoms in ----:com.apple.iTunes namespace
  - FLAC/Ogg: Vorbis comments
  - Enables instant identification when files are shared/moved

### Changed

- **Skaldleita Rebranding** - Internal rename from BookDB for audio features
  - `SKALDLEITA_BASE_URL` points to skaldleita.com
  - Fingerprint, narrator, and voice endpoints updated
  - Settings "Request API key" link updated to skaldleita.com

### Credits

- Thanks to **@Merijeek** for the original Skaldleita concept (Issue #72)
- Skaldleita = "Seek the Storyteller" in Old Norse

---

## [0.9.0-beta.99] - 2026-01-27

### Added

- **Live Status Bar** - Real-time processing visibility on every page (Issue #73 feedback)
  - Persistent status bar below navbar shows what's happening at all times
  - Displays current book being processed with author/title
  - Shows current processing layer (Audio Transcription, AI Analysis, API Enrichment, Folder Fallback)
  - Queue count and pending fixes always visible
  - Progress indicator (X/Y) when processing batches
  - Different visual states: processing (animated), idle (dimmed), stopped (gray)
  - Auto-updates every 2-3 seconds (faster during active processing)
  - No more digging through logs to see what the app is doing!

- **Skaldleita Voice ID** - "Shazam for audiobook narrators" (Issue #78)
  - Identifies narrators by their voice fingerprint, not just metadata
  - Every audiobook gets its voice stored in the community narrator library
  - When transcript doesn't mention narrator, voice matching fills the gap
  - Uses 256-dimensional voice embeddings with resemblyzer/pyannote
  - Contributes to community voice library for future identification
  - Toggle in Settings → AI Setup → Voice ID (Skaldleita)
  - Works alongside audio transcription in Layer 1

### Improved

- **Processing Status API** - New `/api/live_status` endpoint returns comprehensive status
  - Worker state, current book, layer name, queue depth, pending fixes
  - Recent activity summary
  - Optimized for frequent polling

- **Issue #80: Series Number Padding** - Custom template support for `{series_num.pad(N)}` (derp90)
  - FileBot-style padding: `{series_num.pad(2)}` turns 1 → 01, 10 → 10
  - Works in custom naming templates for series with 10+ books
  - Supports any width: `.pad(3)` for 001, 002... 100
  - New button in Settings → Custom Template builder
  - Handles decimal series numbers (1.5 → 01.5)

### Fixed

- **Issue #79: Stuck Queue Items** - Fixed books remaining in queue after fix applied (Merijeek)
  - `apply_fix()` was setting book status to 'fixed' but not deleting queue entry
  - Queue items now properly removed when fix is applied
  - Prevents duplicate processing and stuck "pending" counts

- **Title Shortening Regression** - Fixed AI replacing specific titles with shorter/generic ones
  - "Double Cross" was incorrectly changed to "Cross" (a different book in the same series)
  - Added substring protection: if AI returns a shorter title that's contained in the original, keep original
  - Updated AI prompt to explicitly preserve longer, more specific titles
  - Added regression test to prevent this from happening again
  - Example fix: "Triple Cross" stays "Triple Cross" even when API finds "Cross" book

---

## [0.9.0-beta.98] - 2026-01-27

### Improved

- **Issue #73: API Keys Always Visible** - Restructured Settings → AI Setup tab
  - All API keys now in dedicated "API Keys" card that's always visible
  - No longer need to select a provider to see/enter its API key
  - Clear labels: "Recommended", "Fallback / Whisper", "Optional"
  - Added BookDB API Key field (was missing from UI)
  - System automatically falls back through configured providers

### Fixed

- **BookDB API Key Not Saving** - Added missing save/load for `bookdb_api_key` in settings handler

---

## [0.9.0-beta.97] - 2026-01-26

### Added

- **Issue #77: SearXNG Web Search Fallback** - Alternative metadata source when APIs fail
  - Scrapes Amazon, Audible, and Goodreads via self-hosted SearXNG instance
  - Configure in Settings → API Setup → SearXNG URL
  - Parses author, title, narrator, series from search results
  - Falls back automatically when BookDB/Audnexus/OpenLibrary fail

### Fixed

- **Issue #76: Series Mismatch Detection** - Books with series info no longer accept wrong series matches
  - If folder says "Mistborn Book 1" but API returns "Elantris", match is rejected
  - Series name validation with fuzzy matching (>60% similarity required)
  - Prevents wrong books from getting auto-fixed

- **Issue #77: Whisper Model Setting Not Saving** - Speech-to-Text model selection now persists
  - Fixed save handler that was ignoring `whisper_model` field
  - Model choice (tiny/base/small/medium/large) now saved correctly

---

## [0.9.0-beta.96] - 2026-01-25

### Fixed

- **Issue #76: Watch Folder Duplicates** - Atomic directory move prevents "Version B" folders
  - File moves now use shutil.move() for atomic operation
  - Detects partially-moved directories and completes the move
  - Prevents duplicate folders when watch mode processes same book twice

---

## [0.9.0-beta.95] - 2026-01-24

### Changed

- **Major Code Refactoring** - 32% code reduction in main app
  - `app.py` reduced from 15,491 to 10,519 lines
  - New `library_manager/` package with organized modules:
    - `config.py` - Configuration loading/saving
    - `providers/` - AI provider integrations
    - `pipeline/` - Processing layer implementations
    - `utils/` - Shared utilities
  - Cleaner separation of concerns
  - Easier to maintain and extend

---

## [0.9.0-beta.94] - 2026-01-23

### Fixed

- **Issue #74: Queue Hanging** - Circuit breaker now properly advances queue when providers fail
  - Books no longer get stuck when Gemini/OpenRouter hit rate limits
  - Failed items advance to next processing layer instead of blocking queue

- **Issue #71: Community Toggle** - "Contribute to Community" setting now saves correctly
  - Checkbox state was being ignored in settings POST handler
  - Now properly persists enable_community_contribution setting

- **Issue #63: Whisper Install** - Docker permission error fixed
  - Whisper venv creation now runs as correct user
  - Fixed "Permission denied" errors when installing speech-to-text

- **API Key Visibility** - Keys now shown in settings (hidden by default)
  - Eye toggle reveals/hides API keys
  - No more copying blindly from secrets.json

- **Apply All Fix** - History entries now store paths to prevent "Source no longer exists" errors
  - old_path and new_path saved at fix creation time
  - Bulk apply works even when books have been moved

- **Dashboard Counts** - Fixed inflated counts by excluding series folders from totals

- **Title Cleaning** - Strips torrent naming junk
  - Removes bitrates (320kbps), timestamps, editor names, year prefixes
  - Cleaner titles in pending fixes
