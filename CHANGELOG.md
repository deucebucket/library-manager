# Changelog

All notable changes to Library Manager will be documented in this file.

## [0.9.0-beta.44] - 2025-12-18

### Added
- **Unified Library View** - New `/library` page consolidates all views into one (Issue #31 feedback)
  - Filter chips at top: All, Pending, Orphans, Queue, Fixed, Verified, Errors, Attention
  - Single table showing all items with contextual actions
  - Quick action bar: Scan, Process Queue, Apply All Pending, Organize Orphans
  - Real-time activity stream showing operations as they happen
  - Auto-refresh every 10 seconds
  - Orphans now integrated into main view (no more separate dead-end page)

- **Skip Confirmations Toggle** - New setting for faster batch workflows
  - Toggle in Library view quick action bar
  - When enabled, skips "Are you sure?" dialogs for apply/reject/undo/organize
  - Persists to config, survives page refresh
  - Also available in Settings page

- **New API Endpoint** - `/api/library` returns unified data
  - All items (books, orphans, pending fixes, queue, errors) in one response
  - Filter counts for each category
  - Pagination support
  - Powers the new Library view

### Changed
- Navigation now includes "Library" link between Dashboard and Queue
- Removed separate "Pending" nav link (now a filter chip in Library view)

---

## [0.9.0-beta.43] - 2025-12-18

### Fixed
- **Issue #29: Multibook False Positive** - Chapter files no longer flagged as multi-book collections
  - Files named `00 - Chapter.mp3`, `01 - Prologue.mp3`, `02 - Part Two.mp3` were incorrectly skipped
  - Root cause: Regex pattern `^(\d+)\s*[-–—:.]` matched leading numbers as "book numbers"
  - Now uses smart detection: chapter indicators (prologue, epilogue, chapter, disc, track) = NOT multibook
  - Sequential numbering from 0/1 = chapters, not books
  - Only explicit patterns like `Book 1`, `Volume 2` trigger multibook detection

### Added
- **Book Profile System (Foundation)** - Infrastructure for confidence-scored metadata
  - New `BookProfile` and `FieldValue` dataclasses for per-field confidence tracking
  - Source weights: audio (85), id3 (80), json (75), nfo (70), bookdb (65), ai (60), path (40)
  - Field weights: author/title (30% each), narrator (15%), series (10%), etc.
  - Consensus-based confidence calculation with agreement bonuses and conflict penalties
  - Database columns added: `books.profile` (JSON), `books.confidence` (integer)

- **New Settings for Verification Control**
  - `enable_api_lookups`: Toggle API database lookups (default: on)
  - `enable_ai_verification`: Toggle AI verification (default: on)
  - `enable_audio_analysis`: Toggle Gemini audio analysis (default: off)
  - `deep_scan_mode`: Always use all enabled layers regardless of confidence
  - `profile_confidence_threshold`: Skip expensive layers when confidence is high enough (default: 85%)
  - `multibook_ai_fallback`: Use AI for ambiguous chapter/multibook cases (default: on)

---

## [0.9.0-beta.42] - 2025-12-18

### Fixed
- **Corrupt Dest Now Moves Valid Source** - When destination has corrupt files, source still gets moved
  - Previously: If existing copy was corrupt, valid source just sat there with "corrupt_dest" status
  - Now: Valid source moves to `Author/Title [Valid Copy]/` path
  - User still needs to manually remove the corrupt copy

---

## [0.9.0-beta.41] - 2025-12-18

### Fixed
- **Different Versions No Longer Error** - Multiple versions of same book now get unique paths
  - Previously: If `Bernard Cornwell/Excalibur/` existed and another copy tried to move there, it errored
  - Now: System creates unique paths like `Bernard Cornwell/Excalibur {Version B}/` automatically
  - Tries to extract narrator from audio files first for better naming
  - Falls back to "Version B", "Version C" etc. when no metadata available
  - Different narrators/recordings are NOT duplicates - they're valid variants

### Added
- **Narrator Extraction** - New `extract_narrator_from_folder()` function
  - Checks audio file ID3/MP4 tags for narrator metadata
  - Parses NFO files for "Narrated by" / "Read by" patterns
  - Checks metadata.json for narrator fields
  - Used to distinguish between different recordings of same book

---

## [0.9.0-beta.40] - 2025-12-18

### Fixed
- **Dashboard Showing "Fixed" for Errors** - Critical bug where dashboard displayed all entries as "Fixed"
  - Root cause: UI showed "Fixed" badge based on path change, not actual status
  - Entries with status "error", "duplicate", "conflict", "corrupt_dest" were all displayed as "Fixed"
  - Users couldn't tell if renames actually succeeded or failed
  - Dashboard now shows actual status: Fixed (green), Error (red), Duplicate, Conflict, Corrupt, Pending, etc.
  - Hover over Error badge to see the error message

### Improved
- **Status Badges** - More informative status display on dashboard
  - Fixed: green, Error: red with tooltip, Duplicate/Conflict: yellow, Pending: gray
  - Helps identify books that need manual attention vs successful renames

---

## [0.9.0-beta.39] - 2025-12-17

### Fixed
- **Update Channel Now Works** - Beta/Stable selection in Settings actually switches branches
  - Selecting "Beta" now pulls from `develop` branch
  - Selecting "Stable" now pulls from `main` branch
  - Previously, update always pulled from current branch regardless of setting

---

## [0.9.0-beta.38] - 2025-12-17

### Fixed
- **ABS Connection Lost on Restart** (Issue #27) - Audiobookshelf API token not persisting
  - Root cause: Token was filtered from config.json (for security) but never saved to secrets.json
  - ABS connection now survives container restarts and settings page saves
  - Token properly stored in secrets.json alongside other API keys

- **Settings Page Wiping ABS Token** - Saving settings no longer overwrites ABS connection
  - Settings page now preserves existing secrets when saving
  - Previously, saving any setting would wipe the ABS token

### Improved
- **Bug Report Security** - Additional API keys now redacted in bug reports
  - `abs_api_token`, `bookdb_api_key`, and `google_books_api_key` now redacted
  - Prevents accidental exposure when sharing bug reports

---

## [0.9.0-beta.37] - 2025-12-16

### Added
- **"Trust the Process" Mode** - Fully automatic verification chain (EXPERIMENTAL)
  - New toggle in Settings > General > Behavior
  - When enabled: drastic author changes verified via AI + audio snippets
  - If AI is uncertain, uses Gemini audio analysis as tie-breaker
  - Only truly unidentifiable items flagged as "Needs Attention" (no pending queue)
  - Verified drastic changes are auto-applied (everything logged in history for undo)
  - Requires: Gemini API key for audio analysis

- **"Needs Attention" Status** - New category for unidentifiable books
  - Items that couldn't be verified by any method appear in History with red "Needs Attention" badge
  - Filter history by `/history?status=attention`
  - Includes detailed error message explaining why verification failed
  - These items are NOT moved - just flagged for manual review

### Changed
- Auto-fix now allows verified drastic changes in Trust the Process mode
- History page shows "Needs Attention" count and filter button

---

## [0.9.0-beta.36] - 2025-12-16

### Added
- **Preferred Metadata Language** (Issue #17) - Localized metadata support for non-English libraries
  - New "Preferred Metadata Language" dropdown in Settings > General > Language
  - Supports 28 languages: German, French, Spanish, Italian, Portuguese, Dutch, Swedish, Norwegian, Danish, Finnish, Polish, Russian, Japanese, Chinese, Korean, Arabic, Hebrew, Hindi, Turkish, Czech, Hungarian, Greek, Thai, Vietnamese, Ukrainian, Romanian, Indonesian
  - Google Books API now uses `langRestrict` parameter to filter results by language
  - OpenLibrary search now includes `language` parameter
  - Audnexus/Audible searches use regional endpoints (audible.de, audible.fr, etc.)

- **Preserve Original Titles** - Prevents translating foreign titles to English
  - New toggle in Settings > General > Language (enabled by default)
  - Detects title language using `langdetect` library
  - Example: German "Der Bücherdrache" stays German instead of becoming "The Book Dragon"
  - Useful for users with localized libraries who want to keep original language titles

- **AI-Assisted Localization** - Get official translated titles via AI
  - New `get_localized_title_via_ai()` function asks AI for official translated book titles
  - Works with all AI providers (Gemini, OpenRouter, Ollama)
  - Only returns real published translations, not machine translations

- **Audio Language Detection** - Detect spoken language from audiobook samples
  - New "Detect Language from Audio" toggle in Settings (requires Gemini API key)
  - Uses Gemini audio analysis to identify narrator's spoken language
  - Returns ISO 639-1 code with confidence level
  - Extended existing audio analysis to also return language field

### Changed
- `search_google_books()` now accepts optional `lang` parameter
- `search_audnexus()` now accepts optional `region` parameter for regional Audible stores
- `search_openlibrary()` now accepts optional `lang` parameter
- `gather_all_api_candidates()` now uses language preferences for all API calls

### Dependencies
- Added `langdetect>=1.0.9` for title language detection

---

## [0.9.0-beta.35] - 2025-12-15

### Added
- **Audio Fingerprinting for Duplicate Detection** - Smart comparison of audiobook folders
  - Uses Chromaprint/fpcalc to create audio fingerprints (same tech as Shazam)
  - Detects if two folders contain the same recording even in different formats/bitrates
  - 70% fingerprint similarity threshold ensures different narrators are NOT confused as duplicates
  - Different editions (e.g., "Warbreaker" vs "Warbreaker Tenth Anniversary") correctly identified as separate

- **Corrupt File Detection** - Identifies unreadable/broken audio files
  - Scans audio files with fpcalc to verify they're actually playable
  - When destination has corrupt files but source is valid, recommends replacing
  - New `corrupt_dest` status in history with "Replace" button
  - Prevents keeping broken downloads over valid copies

- **Deep Audiobook Comparison** - Intelligent version analysis
  - Compares total duration, file count, and audio content
  - Detects partial copies (one version is subset of another)
  - Identifies which version is more complete
  - Provides clear recommendations: keep_source, keep_dest, or keep_both

- **Duplicate Management UI** - Easy removal of confirmed duplicates
  - New "Duplicate" status in history with "Remove" button
  - Filter history by duplicates with `/history?status=duplicate`
  - "Remove All Duplicates" button for bulk cleanup
  - Shows match percentage and file counts for informed decisions

- **Replace Corrupt Destination** - One-click fix for corrupt files
  - New `/api/replace_corrupt/<id>` endpoint
  - Deletes corrupt destination, moves valid source to correct location
  - Cleans up empty parent folders automatically

### Changed
- **Conflict Detection Improved** - Now distinguishes between:
  - True duplicates (same files or same recording) → can be safely removed
  - Different editions/narrators (different audio) → marked as conflict for review
  - Corrupt destinations (unreadable files) → can be replaced with valid source
- **Error Messages Enhanced** - Conflicts now show:
  - Recording similarity percentage
  - File counts and sizes for both versions
  - Clear reason why it's a conflict vs duplicate

### Technical
- New functions: `get_audio_fingerprint()`, `compare_fingerprints()`, `analyze_audiobook_completeness()`, `compare_audiobooks_deep()`
- `compare_book_folders()` now includes optional deep analysis with audio fingerprinting
- Requires `libchromaprint-tools` package (fpcalc) - auto-installed in Docker

---

## [0.9.0-beta.34] - 2025-12-15

### Fixed
- **Issue #23: Config vanishing on updates** - Added migration for legacy config locations
  - Users updating from versions before beta.23 had config stored in `/app/` (non-persistent)
  - The app now checks for config files in the old location on startup
  - Automatically migrates `config.json`, `secrets.json`, `library.db`, and `user_groups.json` to `/data/`
  - Prevents config loss when updating Docker containers

---

## [0.9.0-beta.33] - 2025-12-15

### Fixed
- **Issue #22: Empty series hyphen regression** - Fixed leading hyphen appearing when series is empty
  - Custom templates with `{series_num} - {title}` now properly clean up to just `{title}` when series_num is empty
  - Improved regex to handle both `/- ` and `/ - ` patterns after path separator

---

## [0.9.0-beta.32] - 2025-12-15

### Fixed
- **Issue #21: Manual match fallback search** - Added Google Books fallback when BookDB is unavailable
  - Manual match search now tries BookDB first, falls back to Google Books on failure
  - Works when BookDB is down for maintenance, times out, or returns no results
  - Google Books results include series extraction from subtitles
  - Response includes `source` field ('bookdb' or 'googlebooks') and `fallback_reason` when applicable

---

## [0.9.0-beta.31] - 2025-12-15

### Added
- **Tag restoration on undo** - Undo now restores original audio file tags
  - Reads original tags from `.library-manager.tags.json` sidecar backup
  - Writes original tags back to audio files before moving
  - Deletes sidecar backup after successful restoration
  - Supports all tagged formats: MP3, M4B/M4A, FLAC, Ogg/Opus, WMA
  - New `restore_tags_from_sidecar()` function in `audio_tagging.py`

### Fixed
- **Undo for single file moves** - Fixed undo creating folder instead of restoring file
  - History now stores the actual file path for single-file moves
  - Undo correctly extracts and restores just the file, not the containing folder
  - Cleans up empty parent folders after file undo

- **Database connection leak** - Fixed connection leak in `/api/manual_match` error handler
  - Exception handler now properly closes database connection
  - Prevents "database is locked" errors under repeated failures

---

## [0.9.0-beta.30] - 2025-12-15

### Fixed
- **Manual match save error** - Fixed JSON parsing error when saving manual book matches
  - Root cause: `/api/manual_match` tried to update non-existent columns (`suggested_author`, etc.)
  - Rewrote to properly create pending fixes in the history table (matching the rest of the codebase)
  - Manual "Save as Pending Fix" now works correctly

- **Single file moves losing extension** - Fixed audiobook files being saved without extension
  - When applying fixes to single M4B files, the file was being renamed to the folder name
  - Now properly creates folder structure and moves file inside with original filename
  - Example: `Book.m4b` → `Author/Title/Book.m4b` (preserves extension)
  - Metadata embedding now finds files correctly (was showing "0 files")

---

## [0.9.0-beta.29] - 2025-12-15

### Added
- **Metadata Embedding (Beta)** - Write verified metadata directly into audio file tags
  - New "Metadata Embedding" toggle in Settings > Behavior
  - Supported formats: MP3 (ID3v2), M4B/M4A/AAC (MP4 atoms), FLAC/Ogg/Opus (Vorbis comments), WMA (ASF)
  - Tags written: title, album (book title), artist/albumartist (author), year
  - Custom tags: SERIES, SERIESNUMBER, NARRATOR, EDITION, VARIANT
  - Optional sidecar backup: `.library-manager.tags.json` stores original tags before modification
  - Runs automatically when fixes are applied (auto-fix or manual Apply Fix)
  - New `audio_tagging.py` module with format-specific tagging functions
  - Test suite: `test-env/test-audio-tagging.py`

### Changed
- **History table expanded** - Now stores series/narrator/year/edition/variant metadata
  - Enables metadata embedding when applying pending fixes
  - Tracks embedding status (ok/error) and error messages

---

## [0.9.0-beta.28] - 2025-12-14

### Fixed
- **Issue #18: Manual match JSON error** - Fixed "unexpected character" crash on save
  - Root cause: `/api/manual_match` called non-existent `get_local_db()` function
  - Also queried wrong table (`processing_queue` instead of `queue`)
  - Rewrote endpoint to use correct database and table structure
  - Manual book matching now works properly

### Improved
- **Scan feedback** - Users now see what was actually scanned
  - Before: "Found 0 new books, 0 added to queue" (confusing)
  - After: "Checked: 2 books, Already correct: 2, Need fixing: 0" (clear)
  - Helps users like Dennis understand the scan DID work on their library
  - Logs now show full scan stats: checked, tracked, queued

---

## [0.9.0-beta.27] - 2025-12-13

### Fixed
- **Issue #16: Custom template cleanup** - Fixed "dangling dash" in naming templates
  - Template `{author}/{series}/{series_num} - {title}` with no series
  - Before: `Barbara Truelove/- Of Monsters and Mainframes` (broken)
  - After: `Barbara Truelove/Of Monsters and Mainframes` (clean)
  - Added cleanup for leading/trailing dashes in path segments

---

## [0.9.0-beta.26] - 2025-12-13

### Added
- **Path Diagnostic Tool** - Debug Docker volume mount issues
  - New "Test Paths" button in Settings shows what container can actually see
  - Checks if paths exist, are readable, and lists contents
  - Provides specific advice for fixing Docker volume mounts
  - Shows available mount points if configured path doesn't exist
  - Helps users like Dennis diagnose "can't see my files" issues

---

## [0.9.0-beta.25] - 2025-12-13

### Fixed
- **Issue #15: Search results showing "Unknown"** - Fixed API field mismatch
  - BookDB API returns `name` field, but frontend expected `title`
  - Search results now display correctly, form fields populate on selection
  - Manual match save no longer fails with JSON parse error
- **Config loss on container updates** - Critical Docker persistence fix
  - `user_groups.json` now stored in `/data/` (was `/app/` - wiped on update!)
  - Backup/restore now uses correct DATA_DIR path
  - All persistent files now properly stored in mounted volume

---

## [0.9.0-beta.24] - 2025-12-13

### Fixed
- **Trust existing authors** - Major fix for bad author suggestions
  - When folder already has a valid author name (e.g., "Matt Ruff"), keep it
  - Only replace author if current one is a placeholder (Unknown, Various, etc.)
  - Prevents wrong suggestions like "Matt Ruff" → "Anghel Dragomir" when both wrote "The Destroyer of Worlds"
  - Added `is_placeholder_author()` helper function
- **System folder filtering** - Added metadata/tmp/streams/cache/logs to placeholder list
  - These system folders are no longer misinterpreted as author names

---

## [0.9.0-beta.23] - 2025-12-13

### Fixed
- **BookDB timeout handling** - Fixed cold start timeouts for external users
  - Increased search timeout from 10s to 60s (embedding model takes 45-60s to load)
  - Added retry logic for timeout failures
  - Server-side: Added warmup cron to keep model loaded (every 5 min)

---

## [0.9.0-beta.22] - 2025-12-13

### Added
- **Audio Analysis (Beta)** - Extract metadata from audiobook intros using Gemini
  - Sends 90-second audio sample to Gemini 2.5 Flash for author/title/narrator extraction
  - Used as verification when folder names and ID3 tags disagree
  - New "Audio Analysis" toggle in Settings > Behavior
  - ~3K tokens per book (separate quota from text analysis)
  - Audio samples compressed to 64kbps mono (700KB per sample)

---

## [0.9.0-beta.21] - 2025-12-13

### Fixed
- **Author mismatch rejection** - BookDB now rejects matches where:
  - Title is similar but NOT exact (e.g., "Lost Realms" vs "The Lost Realm")
  - AND author is completely different (e.g., "Thomas Williams" vs "J. D. Rinehart")
  - This prevents wrong book matches that share similar titles

---

## [0.9.0-beta.20] - 2025-12-13

### Added
- **Health Scan** - Detect corrupt/incomplete audio files
  - New `/api/health_scan` endpoint scans entire library
  - Uses ffprobe to verify each audio file is readable
  - Groups corrupt files by folder for easy review
  - Shows total healthy library duration
- **Delete Corrupt Files** - Clean up broken downloads
  - New `/api/delete_corrupt` endpoint safely removes bad files/folders
  - Security: only allows deletion within configured library paths

---

## [0.9.0-beta.19] - 2025-12-13

### Added
- **Ebook Management (Beta)** - Organize ebooks alongside audiobooks
  - New "Enable Ebook Management" toggle in Settings > Behavior
  - Supported formats: `.epub`, `.pdf`, `.mobi`, `.azw3`
  - Two library modes:
    - **Merge with Audiobooks** - Places ebooks in matching audiobook folders (ABS compatible)
    - **Separate Library** - Creates standalone ebook folder structure
  - Scans loose ebook files and ebook-only folders
  - Uses same identification pipeline (BookDB + AI) as audiobooks
- **Audiobookshelf Integration** - Connect to your ABS instance
  - ABS API token stored securely in `secrets.json`
  - Configurable ABS URL (`abs_url` in config)
  - Foundation for future ABS sync features

### Fixed
- **File handling in apply_fix** - Now properly handles single files, not just folders

---

## [0.9.0-beta.18] - 2025-12-13

### Added
- **Search progress tracking** - Monitor queue position and completion status
  - New `/api/search_progress` endpoint shows queue position, percent complete
  - Thread-safe progress tracker for chaos handler operations
  - Users can see their search position when API is busy

### Improved
- **BookDB API integration** - Now primary identification method for chaos handler
  - Uses public `/search` endpoint (50M books, no auth needed)
  - Faster than AI and no rate limits
  - AI now fallback only when BookDB doesn't find a match
- **Garbage match filtering** - Better rejection of wrong matches
  - Added `is_unsearchable_query()` filter for non-book filenames (chapter1, track05, disc2)
  - BookDB API now uses `clean_search_title()` and similarity checking
  - Prevents "chapter1.mp3" matching random books

### Fixed
- **Title cleaning** - "The Martian audiobook.m4b" now cleaned to "The Martian" before search

---

## [0.9.0-beta.17] - 2025-12-13

### Added
- **Chaos Handler** - Handle completely unorganized libraries with loose files
  - Intelligent file grouping by: ID3 metadata → filename patterns → numbered sequences
  - Multi-level identification: metadata → SearXNG search → Gemini AI → audio transcription
  - New `/api/chaos_scan` endpoint to analyze and identify loose files
  - New `/api/chaos_apply` endpoint to create folders and move files
  - Test suite: `test-env/generate-chaos-library.py`

### Fixed
- **Missing `load_secrets()` function** - AI identification in chaos handler now works
- **ID3v2 tag reading** - Fixed MP3 tag extraction using raw ID3 names (TALB, TPE1, TIT2)

---

## [0.9.0-beta.16] - 2025-12-13

### Added
- **Smart path analysis** - Intelligent folder structure detection
  - Works backwards from audio file to library root
  - Uses 50M book database for author/series lookups
  - Fuzzy matching (handles "Dark Tower" → "The Dark Tower")
  - Position-aware disambiguation (Author/Series/Title detection)
  - AI fallback via Gemini for ambiguous paths
  - New `/api/analyze_path` endpoint for testing
- **Integration test environment** - Automated deployment testing
  - `./test-env/run-integration-tests.sh` - Full test suite
  - `./test-env/generate-test-library.sh` - Creates 2GB test library
  - Tests reversed structures, missing authors, edge cases
  - Verifies Docker deployment works for fresh users
  - Tests WITHOUT local BookDB (pattern-only fallback)
- **Docker CI/CD** - Automatic builds to GitHub Container Registry
  - GitHub Actions workflow builds on push to main
  - Multi-arch support (amd64, arm64)
  - Image at `ghcr.io/deucebucket/library-manager:latest`
  - UnRaid template updated with correct ghcr.io URL

### Fixed
- **Safe database fallback** - Connection failures no longer assume "not found"
  - Returns `(found, lookup_succeeded)` tuples
  - Falls back to pattern-only detection on DB errors
  - Adds `db_lookup_failed` issue flag when lookups fail
  - Prevents misclassification due to network/DB issues
- **Structure reversal detection** - Detects Metro 2033/Dmitry Glukhovsky patterns
  - Identifies when Series/Author is swapped with Author/Series
  - Flags for manual review instead of auto-fixing wrong

### Changed
- Updated PROJECT_BIBLE.md with test documentation and release checklist
- Added test-env/ to .gitignore (keeps scripts, ignores 2GB test data)

---

## [0.9.0-beta.15] - 2025-12-13

### Added
- **In-browser updates** - Update directly from the web UI
  - Click version badge (bottom left) to check for updates
  - "Update Now" button performs `git pull` automatically
  - "Restart App" button restarts the service after update
  - Works with systemd-managed services
- **Loose file detection** - Auto-creates folders for files dumped in library root
  - Detects audio files without proper `Author/Title/` structure
  - Searches metadata based on filename
  - Creates proper folder structure automatically

### Fixed
- **System folder skipping** - Scanner no longer processes system folders as books
  - Skips: `metadata`, `streams`, `tmp`, `cache`, `chapters`, `parts`, etc.
  - Skips: `@eaDir`, `#recycle` (Synology special folders)
  - Skips any folder starting with `.` or `@`
  - Applies at both author AND title levels
- **Variable naming conflict** - Fixed `clean_title` shadowing bug in loose file detection

## [0.9.0-beta.14] - 2025-12-12

### Added
- **Universal search** - Search now covers everything
  - Searches across titles, authors, series names, AND years
  - Type "jordan" to find Robert Jordan's books
  - Type "2023" to find books published in 2023
  - Type "wheel of time" to find the series
- **Metadata completeness scoring** - See how complete your book data is
  - 0-100% score based on weighted fields (author 25%, description 25%, cover 20%, year 15%, ISBN 15%)
  - Color-coded badges in search results (red/yellow/blue/green)
  - Hover to see which fields are missing
- **Dynamic database stats** - Live counts instead of hardcoded numbers
  - Shows actual book/author/series counts from database
  - Updates automatically as data grows
- **Improved filename cleaning** - Better handling of YouTube rips and messy filenames
  - Removes "Audiobook", "Full Audiobook", "Complete", "Unabridged" etc.
  - Strips years, quality markers, and other junk
  - Makes searching from filenames more accurate
- **Reddit reply templates** - Pre-written responses for common questions
  - Access at `/static/reddit-replies.html`
  - One-click copy to clipboard
  - Covers safety concerns, naming patterns, YouTube rips

### Fixed
- **OpenLibrary scraper** - Editions-only mode now properly links authors
  - Previously 18M books imported without author data
  - Scraper now builds author cache even in editions-only mode
  - Backfill script created to fix existing orphaned books
- **Search ranking** - Results now prioritize exact matches

### Backend (metadata_scraper)
- Added `/api/bookdb_stats` endpoint for live database counts
- Added `completeness` and `missing_fields` to Book model
- Added `calculate_completeness()` function with weighted scoring
- Created `backfill_authors.py` script to fix 18M orphaned books
- Fixed `build_author_name_cache()` for editions-only imports

## [0.9.0-beta.13] - 2025-12-11

### Added
- **Custom naming templates** - Build your own folder naming convention
  - Clickable tag builder UI in Settings → General
  - Tags: `{author}`, `{title}`, `{series}`, `{series_num}`, `{narrator}`, `{year}`, `{edition}`, `{variant}`
  - Live preview shows how your template will look
  - Missing data automatically cleaned up (empty brackets removed)
  - Example: `{author}/{series}/{series_num} - {title}` → `Brandon Sanderson/Mistborn/1 - The Final Empire/`
- **Manual book matching** - Search and match books manually when AI can't find them
  - Edit button on queue items
  - Search our 49M+ book database directly
  - Select correct book from results to auto-fill author/title/series
  - Goes to Pending for review before applying
- **Backup & restore** - Protect your configuration
  - Download backup creates .zip with all settings, groups, and database
  - Restore backup uploads previous backup to restore setup
  - Current state backed up before restore for safety
  - Found in Settings → Advanced
- **Version-aware renaming** - Different narrators and editions get their own folders
  - Narrator in curly braces: `{Ray Porter}` vs `{Clive Barker}`
  - Edition in brackets: `[30th Anniversary Edition]`
  - Variant in brackets: `[Graphic Audio]`
  - Smart conflict resolution tries narrator → variant → edition → year

### Changed
- Settings now saves custom naming template
- `build_new_path()` supports custom template parsing

## [0.9.0-beta.11] - 2025-12-10

### Added
- **Automated issue handling** - Scripts to auto-process GitHub issues
  - `scripts/auto-fix-issues.sh` - Monitors and processes issues with Claude
  - `scripts/issue-bot-prompt.md` - Guidelines for how Claude should respond
  - Supports cron scheduling for automatic monitoring
  - Claude will fix issues it understands, ask for clarification if unsure
  - Responses written in casual developer tone, not AI-speak

## [0.9.0-beta.10] - 2025-12-10

### Added
- **Complete Docker documentation** - New `docs/DOCKER.md` guide
  - Platform-specific instructions for UnRaid, Synology, Linux, Windows/Mac
  - Dockge and Portainer setup guides
  - Volume mount explanation (why Settings can't access unmounted paths)
  - Multiple library configuration
  - Troubleshooting section
  - Updated README to link to full Docker guide

## [0.9.0-beta.9] - 2025-12-10

### Added
- **Docker support** - Full Docker and Docker Compose setup
  - `Dockerfile` for building the container
  - `docker-compose.yml` with UnRaid/Dockge/Portainer instructions
  - `DATA_DIR` environment variable for persistent config/database storage
  - Health check endpoint for container monitoring
  - Updated README with Docker installation instructions

### Changed
- Config, secrets, and database now support external data directory via `DATA_DIR` env var

## [0.9.0-beta.8] - 2025-12-10

### Fixed
- **Full portability audit** - Scanned entire codebase for hardcoded paths
  - Changed OpenRouter HTTP-Referer to use GitHub repo URL instead of personal domain
  - Updated `config.example.json` with all current settings for new users
  - Verified no other user-specific paths remain

### Changed
- `config.example.json` now includes all available settings with sensible defaults

## [0.9.0-beta.7] - 2025-12-10

### Fixed
- **Hardcoded log path** - Log file path no longer hardcoded to `/home/deucebucket/`
  - Now uses script directory dynamically via `os.path.dirname(__file__)`
  - Fixes startup error for other users (thanks for the first issue report!)

## [0.9.0-beta.6] - 2025-12-10

### Added
- **Series folder detection** - Folders containing 2+ book-like subfolders are now recognized as series containers
  - Detects patterns like `01 Title`, `Book 1`, `#1 - Title`, `Volume 1`
  - Marked as `series_folder` status and skipped from processing
  - Prevents `Warriors: The New Prophecy/` from being treated as a single book

### Fixed
- Restored Warriors sub-series structure (A Vision of Shadows, Omen of the Stars, The New Prophecy)
- Series folders no longer renamed into parent series

## [0.9.0-beta.5] - 2025-12-10

### Added
- **Multi-book collection detection** - Folders containing "Complete Series", "7-Book Set", etc. are now skipped
  - Marked as `needs_split` instead of being processed as single books
  - Prevents mislabeling "The Expanse Complete Series" as just "Leviathan Wakes"
- **Placeholder author handling** - "Unknown" or "Various" authors changing to real authors no longer flagged as drastic changes

### Fixed
- History display no longer shows "audiobooks/" prefix for non-series books
- Undid bad fixes for multi-book collection folders (Expanse, Narnia)

## [0.9.0-beta.4] - 2025-12-10

### Added
- **Improved series detection** from original folder names
  - Extracts series from patterns like `Title (Book N)` at end
  - Uses "author" folder as series when it contains Series/Saga/Edition/etc.
  - Checks original title before AI's cleaned title
- **System folder filtering** - Skips junk folders like `metadata/`, `tmp/`, `cache/`
- **Database locking fix** - 30 second timeout + WAL mode for concurrent access
- **Resizable columns** in history table
- **Full path display** in history showing series structure

### Fixed
- Series info no longer lost when AI returns clean title
- AI prompt updated to never put "Book N" in title field
- History page now shows actual folder path, not just author/title

### Changed
- Rate limit default increased to 2000/hour (Gemini allows 14,400/day)
- History display shows relative path with series structure

## [0.9.0-beta.3] - 2025-12-10

### Changed
- Merged Tools and Advanced tabs into single Advanced tab
- Cleaner settings UI

## [0.9.0-beta.2] - 2025-12-10

### Added
- **Garbage match filtering** - Rejects API results with <30% title similarity
- **Series grouping toggle** - Audiobookshelf-compatible folder structure
- **Dismiss error button** - Clear stale error entries from history
- **Series extraction** from title patterns (Book N, #N, etc.)
- Unicode colon handling for Windows-safe filenames

### Fixed
- Rate limit increased to 400/hour

## [0.9.0-beta.1] - 2025-12-09

### Added
- Initial beta release
- Multi-source metadata pipeline (Audnexus, OpenLibrary, Google Books, Hardcover)
- AI verification with Gemini/OpenRouter
- Smart narrator preservation
- Drastic change protection
- Web dashboard with dark theme
- Queue management
- Fix history with undo
- Orphan file detection
