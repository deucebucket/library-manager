# Changelog

All notable changes to Library Manager will be documented in this file.

## [Unreleased]

### Fixed

- **Issue #155: API key not sent on /search requests** - All Skaldleita API endpoints now include
  authentication headers. GET /search requests were missing the X-API-Key header, causing 403
  Forbidden errors after Skaldleita added auth requirements to all endpoints.
- **Issue #154: Rate limit handling** - Centralized rate limit handling in `handle_rate_limit_response()`
  with exponential backoff (30s/60s/120s), Retry-After header parsing, and circuit breaker
  integration. Applied to bookdb.py and fingerprint.py providers. Frontend displays rate limit
  warnings with retry countdown on library, queue, and history pages.

---

## [0.9.0-beta.125] - 2026-02-14

### Fixed

- **Issue #150: Badge count regression** - Dashboard initial render and `/api/library` queue count
  now use the same filtered query as `/api/stats` (regression from #131 fix). Excludes
  `needs_attention`, `user_locked`, and container statuses. Badge no longer flickers on page load.
- **Issue #150: Author initial matching** - Precog voting system now handles initial-only author
  names. "C Alanson" matches "Craig Alanson", "JRR Tolkien" matches "J R R Tolkien". Uses
  consonant-only detection for collapsed initials and 0.7 weighted scoring for initial matches.
  Also added initial-aware folder dedup in `path_safety.py` to prevent duplicate author folders.
- **Issue #152: Pipeline fetch consistency** - `base_layer.py` and `layer_audio_id.py` now exclude
  `needs_attention` books from queue fetch, matching all other pipeline layers. Prevents wasting
  processing cycles on items that need human review.

---

## [0.9.0-beta.123] - 2026-02-11

### Added

- **Issue #110 Part 2: Folder triage** - New `library_manager/folder_triage.py` module that
  categorizes folder names as clean/messy/garbage before processing. Clean folders use path hints
  normally. Messy folders (scene release tags, torrent markers, quality indicators) skip path
  parsing and rely on audio/metadata only. Garbage folders (hash names, numbers-only, generic
  placeholders) also skip path hints and get a confidence penalty. Triage results stored in DB
  and logged during scans. Integrated into Whisper transcription hints, AI identification
  prompts, and the processing pipeline queue.
- **Issue #103: In-app hints and tooltips** - New `library_manager/hints.py` module with contextual
  documentation for all features and settings. Hover over the (?) icon next to any setting to see a
  plain-language explanation of what it does. Tooltips added to: all identification layers, AI
  providers, confidence threshold, trust modes, safety toggles, watch folder, ebook management,
  metadata embedding, community features, and more. Library page filter chips and action buttons also
  show helpful tooltips on hover. Users never need to ask "what does this do?" again.

---

## [0.9.0-beta.122] - 2026-02-11

### Added

- **Issue #111: Sortable columns** - Library table columns (Author, Title, Status) can now be
  sorted by clicking column headers. Click once for ascending, again for descending, third click
  clears sort back to default order. Visual arrow indicators show active sort column and direction.
  Sort state preserved during pagination and filter changes. Backend validates sort columns against
  a whitelist to prevent SQL injection.

---

## [0.9.0-beta.121] - 2026-02-10

### Fixed

- **Issue #142: Duplicate author folders from name variants** - New `find_existing_author_folder()`
  deduplicates author folders using 3-tier matching: exact normalized, standardized initials, and
  fuzzy match (SequenceMatcher >= 85%). Prevents separate folders like "James S.A. Corey" vs
  "James S. A. Corey" or "Alistair MacLean" vs "Alistair Maclean". Applied to both standard and
  `author_lf/title` naming formats.
- **Issue #143: Series name used as author folder** - Defensive filter in BookDB provider discards
  results where author equals series name (corrupt Skaldleita data per skaldleita#90, e.g. author
  "Laundry Files" instead of "Charles Stross"). Defense-in-depth check in BookProfile.finalize()
  catches this from any source, with automatic fallback to next-best author candidate.
- `standardize_author_initials` now defaults to `True` to reduce author folder fragmentation.

---

## [0.9.0-beta.120] - 2026-02-09

### Fixed

- **Issue #140: Missing HMAC signing on /match requests** - The security commit (beta.117)
  added HMAC request signing to all Skaldleita endpoints but missed `search_bookdb()`. Now
  `/match` requests include `X-LM-Signature` and `X-LM-Timestamp` headers consistent with
  all other Skaldleita API calls. Note: most of the search issues reported in #140 are
  Skaldleita data gaps tracked in deucebucket/skaldleita#83, #84, #85.

---

## [0.9.0-beta.119] - 2026-02-08

### Added

- **Issue #126: Auto-enqueue after watch folder processing** - Books moved from the watch
  folder to the library are now automatically added to the processing queue. Previously they
  sat idle with status 'pending' until the user manually triggered a scan or processing.
  Now they enter the full pipeline (audio ID, AI verification, etc.) immediately.

---

## [0.9.0-beta.118] - 2026-02-08

### Fixed

- **Issue #137: "Process Queue" button stalls** - The button now uses background processing
  with live status bar updates instead of blocking the browser. Previously the HTTP request
  would block while waiting for Skaldleita audio analysis (30+ seconds per book) with no
  feedback to the user. Now runs the full pipeline (all layers including audio) and the
  status bar shows exactly what's happening: current book, provider, queue position, etc.

---

## [0.9.0-beta.117] - 2026-02-08

### Fixed

- **Issue #131: Queue count mismatch** - Dashboard "ready to process" badge now counts only
  actually processable items, matching the filters used by the processing pipeline. Previously
  counted all queue items including ones blocked by status or layer filters.
- **Issue #131: Orphaned queue items** - Books that exhaust all processing layers are now
  properly marked as "Needs Attention" instead of being stuck in an invisible limbo state.
- **Issue #132: Duplicate book entries** - Library scan now resolves all paths before database
  insertion, preventing duplicate entries when symlinks or mount points cause the same file to
  have different path representations.
- **Issue #133: Series grouping with custom templates** - When `series_grouping` is enabled
  and the custom naming template doesn't include `{series_num}`, the series number is now
  automatically prepended to the title folder (matching built-in format behavior).
- **Issue #135: Output folder routing** - All pipeline layers (audio ID, audio credits, AI
  queue) now route watch folder items to the configured output folder. Previously only Layer 4
  honored the output folder setting.

---

## [0.9.0-beta.110] - 2026-02-03

### Added

- **Enhanced Status Bar** - Know exactly what's happening with your library
  - Shows current API/provider being used (Skaldleita, Gemini, Ollama, etc.)
  - **FREE badge** - See when processing uses free APIs (not your quota)
  - Current step display - "Transcribing audio...", "Querying database...", etc.
  - Provider icons: soundwave for Skaldleita, stars for Gemini, PC for Ollama
  - API latency tracking (for future display)

### Fixed

- **Crash: author_last template with truncated names** - "James S. A" no longer crashes
  - When Skaldleita returns truncated author like "James S. A" (should be "Corey")
  - The `{author_last}` template variable would crash: `replace() argument 2 must be str, not None`
  - Single-letter last names like "A" were being rejected by sanitizer
  - Now falls back to full author name instead of crashing

---

## [0.9.0-beta.109] - 2026-02-03

### Fixed

- **Issue #79: AI Hallucination on Generic Titles** - "Match Game" no longer gets fake authors
  - Generic titles like "Match Game", "The Game", "Home", "Gone" were causing AI to invent authors
  - Added `GENERIC TITLE WARNING` to AI prompts - warns about ambiguous titles
  - Added `REASONING REQUIRED` - AI must explain why it identified the book
  - Added `generic_patterns` detection: game, hunt, prey, gone, home, storm, dark, night, etc.
  - Validation now flags "generic title + no input author" as low confidence
  - Better to return null than hallucinate a fake author like "Doc Raymond"

### Added

- **Tester Log Lookup Script** - `test-env/lookup-tester.sh` for debugging user issues
  - Look up Skaldleita logs by tester name or IP hint
  - Shows recent activity, job history, and request stats

---

## [0.9.0-beta.108] - 2026-02-03

### Added

- **Issue #96: Author Name Format Templates** - New template variables for custom naming
  - `{author_lf}` - "LastName, FirstName" format (e.g., "Sanderson, Brandon")
  - `{author_last}` - Last name only (e.g., "Sanderson")
  - `{author_first}` - First name only (e.g., "Brandon")
  - `{author_fl}` - "FirstName LastName" format (normalizes existing names)
  - New preset: "LastName, FirstName / Title - Library style"
  - Handles edge cases: prefixes (van, de, le), suffixes (Jr., III), initials (J. R. R.)

- **Issue #88: Database Cleanup Button** - Settings → Advanced → Database Maintenance
  - One-click removal of @eaDir, #recycle, .AppleDouble, $RECYCLE.BIN entries
  - Shows count of items removed
  - No more manual SQLite editing needed

### Fixed

- **Issue #79: Duplicate History Entries** - ROOT CAUSE IDENTIFIED AND FIXED
  - Found 16 separate `INSERT INTO history` statements across 5 files with no deduplication
  - Created centralized `insert_history_entry()` helper that deletes existing book_id+status before inserting
  - Converted ALL 16 locations to use the helper function
  - Added `cleanup_duplicate_history_entries()` that runs on startup
  - Added API endpoint `/api/cleanup_duplicate_history` for manual cleanup
  - 7 new regression tests added
  - This finally kills the bug where same book appeared 15+ times in history

---

## [0.9.0-beta.107] - 2026-02-01

### Fixed

- **Issue #94: Series Number Without Series Name** - Custom naming templates no longer create broken paths
  - Template `{author}/{series}/{series_num} - {title}` now falls back to `{author}/{title}` when series name is missing
  - Previously created paths like `Author/01 - Title` (missing series folder)
  - Now correctly omits series_num when series is empty
  - Also fixed `.pad()` modifier to return empty string instead of "00" for empty values
  - Root cause: Skaldleita search doesn't return series data (tracked in deucebucket/skaldleita#7)

---

## [0.9.0-beta.106] - 2026-02-01

### Added

- **Issue #92: Strip "Unabridged" from Titles** - New toggle in Settings → Naming Format
  - Removes `(Unabridged)`, `[Unabridged]`, and similar variants from book titles
  - Off by default - enable if you prefer cleaner titles without edition markers
  - Suggested by Merijeek - most modern audiobooks are unabridged by default

### Fixed

- **Garbage Recommendation Prevention** - Validation now catches bad author/title suggestions
  - Rejects garbage authors like "earth", "Tantor Audio", "Don't Panic", "[SCAN] Vol 13"
  - Rejects polluted titles like "written and read for you", "Tantor Audio presents..."
  - Rejects truncated fragments like "lling overview of Celtic myths..."
  - Validation runs at INSERT point, not just profile level
  - 237 test cases covering real-world garbage patterns

- Added more audio format extensions to search title cleanup (mp3, m4b, m4a, flac, wav, ogg)

---

## [0.9.0-beta.105] - 2026-01-31

### Added

- **Issue #67: ISBN Lookup for Ebooks** - Extract ISBN from ebook metadata for better matching
  - Extracts ISBN from EPUB metadata (dc:identifier elements)
  - Extracts ISBN from PDF metadata and first 5 pages
  - Extracts ISBN from MOBI/AZW header data
  - New BookDB endpoint `/api/isbn/{isbn}` for direct ISBN lookup
  - Normalizes ISBN-10 ↔ ISBN-13 conversion
  - Falls back to existing filename parsing if no ISBN found
  - New setting: "ISBN Lookup" toggle in Ebook Management section

---

## [0.9.0-beta.104] - 2026-01-31

### Fixed

- **Issue #88: Synology @eaDir folders appearing as books** - System folder filtering now works at book level
  - Added `@eaDir`, `#recycle`, `.AppleDouble`, `__MACOSX`, `.Trash` to filtered folders
  - Added checks for any folder starting with `@` or `#` prefix
  - Applied same filter inside series folders
  - Synology users will no longer see garbage entries after rescan

---

## [0.9.0-beta.103] - 2026-01-31

### Fixed

- **Issue #79: Phantom History Counts** - Dashboard counts now match actual items
  - History queries now use JOIN to only count items with existing books
  - Prevents orphaned history entries from inflating pending/fixed/error counts
  - Counts on dashboard now accurately reflect what you'll see when clicking through

- **Issue #79: Duplicate History Entries** - Prevents multiple 'fixed' entries per book
  - Now cleans up existing 'fixed' entries before recording a new fix
  - Stops history from accumulating duplicate entries when re-processing books

### Changed

- **Request Tracking** - Added User-Agent header to Skaldleita API requests
  - Requests now identify as `LibraryManager/{version}`
  - Helps track Library Manager traffic vs other API consumers
  - No functional change for users

---

## [0.9.0-beta.102] - 2026-01-29

### Added

- **UI Theming System** - Switch between visual themes
  - **Default** - Original dark blue theme
  - **Skaldleita** - Norse-inspired theme with warm amber accents and rune decorations
  - Theme selector in Settings → Advanced
  - Persistent across sessions (saved to localStorage)

### Fixed

- **Issue #86: Pyannote Warning at Startup** - No longer tries to load local pyannote when Skaldleita handles audio
  - When `use_skaldleita_for_audio=true` (the default), voice ID is done server-side
  - Local voice fingerprinting now correctly skipped, eliminating the "No module named 'pyannote'" warning
  - Users only need pyannote if they explicitly disable Skaldleita audio

- **Issue #86: Worker Crash on Malformed AI Response** - Added defensive validation
  - AI sometimes returns strings instead of dicts when confused by messy filenames
  - Now logs warning and skips invalid results instead of crashing
  - Prevents `'str' object has no attribute 'get'` error in queue processing

- **Theme Persistence Across Pages** - Theme no longer resets when navigating
  - Clicking dashboard icons (hourglass, etc.) no longer switches back to default theme
  - localStorage theme loading now runs on ALL pages, not just Settings
  - Theme applies instantly before CSS loads to prevent flash

- **Author Validation in Audio ID Layer** - Prevents garbage matches from audio identification
  - Added validation check ensuring detected author exists in BookDB or matches known patterns
  - Rejects results where "author" is clearly garbage (random strings, file artifacts)
  - Audio transcription results now verified before being trusted
  - Fixes cases where Whisper misheard narrator intros created fake authors

---

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

### Fixed

- **Issue #76: Series Mismatch Detection** - Books with series info now correctly reject wrong matches
  - AI verification prompt now extracts series context from folder names
  - When input has explicit series (e.g., "Expeditionary Force Book 14"), mismatched results are rejected
  - Example: "Expeditionary Force Book 14 - Match Game" no longer misidentified as "Doc Raymond - Match Game"

- **Issue #77: Whisper Model Setting Not Saving** - Added missing `whisper_model` to settings handler

---

## [0.9.0-beta.96] - 2026-01-26

### Fixed

- **Issue #76: Watch Folder Creates Duplicate "Version B" Folders** - Fixed interrupted moves causing split audiobooks
  - Watch folder now uses atomic directory move when possible (same filesystem, no hard links)
  - Detects partial/interrupted moves by checking if destination files are a subset of source
  - When partial move detected, completes the move instead of creating "Version B"
  - Files already at destination are skipped during completion

- **Issue #75: Book Edits Not Persisting** - Fixed edit_book losing changes on multiple edits
  - When editing a pending_fix item multiple times, original metadata was being overwritten
  - Now preserves the original old_author/old_title from the first pending_fix entry
  - Ensures "Original → Latest" is shown, not "Previous edit → Latest"

---

## [0.9.0-beta.94] - 2026-01-25

### Fixed

- **Issue #74: Queue Hanging** - Fixed queue getting stuck waiting for providers
  - Circuit breaker now properly trips when providers fail repeatedly
  - Queue advances to next layer instead of stalling indefinitely

- **Issue #71: Community Contributions Toggle** - Setting now persists correctly
  - Added missing `contribute_to_community` to settings save handler

- **Issue #64: Multiple Bug Fixes** (Merijeek's reported issues)
  - **API Key Visibility** - Keys now shown in settings (hidden by default, eye toggle reveals actual value)
  - **"Source no longer exists" Errors** - History entries now store old_path/new_path at creation time
  - **apply_fix() Fallback** - When stored path doesn't exist, tries current book path as fallback
  - **Dashboard Counts** - Fixed inflated counts by excluding series_folder/multi_book_files entries
  - **Title Cleaning** - Expanded JUNK_PATTERNS for torrent naming conventions:
    - Year prefixes (2007 - Title)
    - Series prefixes (DM-08 - Title)
    - Bitrates (62k, 128k), timestamps (23.35.16), file sizes ({1.27gb})
    - Editor/narrator names in brackets/parentheses ([Dozois,Strahan], (Thorne))
    - Version markers ((V), v01)

- **Issue #63: Whisper Install Permission Error** - Fixed in Docker entrypoint
  - Creates required directories with proper ownership before app starts

---

## [0.9.0-beta.93] - 2026-01-24

### Added

- **P2P Book Cache** (Issue #62) - Optional decentralized cache for book lookups
  - When enabled, successful BookDB lookups are cached locally AND shared via Gun.db P2P network
  - Helps when BookDB is temporarily unavailable - results served from other users' caches
  - **Opt-in only** - disabled by default, toggle in Settings → P2P Book Cache
  - Privacy-focused: only book metadata shared (title, author, series), no file paths
  - Data validation prevents malformed or malicious cache entries
  - Local SQLite cache always works, P2P is an optional enhancement
  - Requires `pygundb` package (optional dependency)

- **BookDB Retry Logic** - Smarter handling of temporary BookDB outages
  - Retries 5 times with exponential backoff when no fallback provider configured
  - Retries 2 times when fallback available (then uses fallback)
  - Queue items return to pending state if all retries fail (will retry later)

### Security

- **API Keys No Longer Exposed in HTML** - API keys are never rendered in page source
  - Input fields show "Key configured" placeholder instead of actual value
  - Green checkmark indicates key is set without exposing it
  - New "Clear" button to remove configured keys
  - New `/api/clear_api_key` endpoint with whitelist validation

### Changed

- **"Trust the Process" Danger Visibility** - Moved to its own danger-styled card
  - Red border and header with warning icon
  - Renamed badge from "EXPERIMENTAL" to "YOLO MODE"
  - Added explicit warning: "Can rename entire library automatically. BACKUP FIRST!"

- **Removed Outdated "NEW" Badge** from Watch Folder (it's been in the app for several versions)

### Fixed

- **Placeholder Authors Not Queued During Scan** (Issue #59 complete fix)
  - Books with "Unknown Author" or "Various Authors" in proper folder structure were marked "Already correct"
  - Now correctly detected during scan phase and queued for identification
  - Uses existing `is_placeholder_author()` check in `analyze_author()` function

- **Docker Whisper Install Permission Error** (Issue #63)
  - Creates `/app/.local` and `/app/.cache/pip` directories with proper ownership
  - Fixes "Permission denied: '/app/.local'" when installing Whisper from UI

- **BookDB Stability Improvements** (Issue #62)
  - Added circuit breaker for rate limiting (backs off after repeated 429s)
  - Increased base rate limits to prevent self-banning
  - Added round-robin queue system for multi-user fairness

---

## [0.9.0-beta.91] - 2026-01-17

### Major Feature: Audio-First Book Identification

**Revolutionary Approach** - Library Manager now identifies audiobooks by transcribing narrator introductions first, before falling back to APIs or folder names. This leverages the fact that most audiobooks begin with the narrator announcing the book title and author.

#### How It Works
1. **Layer 1: Audio Transcription** - Extracts 45-second intro, transcribes with faster-whisper
2. **AI Parsing** - Sends transcript to AI to extract author, title, narrator, series
3. **Layer 2: AI Audio Analysis** - For unclear transcripts, sends audio directly to Gemini
4. **Layer 3: API Enrichment** - Adds metadata to already-identified books
5. **Layer 4: Folder Fallback** - Last resort, uses folder structure

#### Test Results
- **52% of books identified from audio alone** in Layer 1
- Combined with Layer 2, audio-based identification resolves majority of books
- Correctly identified books like:
  - Jack London - White Fang
  - James S. A. Corey - The Vital Abyss
  - Brandon Sanderson - The Frugal Wizard's Handbook
  - James Patterson - Cross the Line

### Technical Improvements

- **faster-whisper Integration** - Local, free speech-to-text via venv
- **Increased ffmpeg Timeout** - 120 seconds for large m4b files
- **Known Narrator Detection** - Prevents AI from confusing narrators with authors
- **Null String Validation** - Rejects AI responses like "None", "null", "N/A"
- **Layer Advancement Fix** - Items properly flow between layers

### Bug Fixes

- Fixed crash when `current_author` was None
- Fixed Layer 2 not finding items (wrong verification_layer in query)
- Fixed items stuck as `needs_attention` not being reprocessed
- Fixed Gemini model selection (forced `gemini-2.0-flash` for audio support)

---

## [0.9.0-beta.90] - 2026-01-15

### Major Feature: Layer 4 Content Analysis

**The Final Layer** - When all else fails, Library Manager can now transcribe actual story content to identify books. This catches books that have:
- No intro credits (e.g., Part 2 of multi-part files)
- Music-only intros
- Corrupted/cut credit sections
- Files with zero metadata

#### How It Works
1. Extracts 60-second audio sample from the **middle** of the book (actual story content)
2. **Primary Path**: Sends audio to Gemini Audio API for transcription + identification
3. **Fallback Path**: If Gemini is rate-limited or unavailable:
   - Uses **faster-whisper** for local transcription (no GPU required)
   - Sends transcript to **OpenRouter** free models for book identification

#### New Settings (Settings → Processing Layers)
- **Enable Layer 4** toggle
- **Speech-to-Text Model**: tiny (75MB) / base (150MB) / small (465MB) / medium (1.5GB)
- **Book ID Model**: Choice of free OpenRouter models
- **One-click Whisper Install**: Install faster-whisper directly from the UI

### New Feature: Narrator Detection

Integrates with BookDB to detect known audiobook narrators:
- **Prevents narrator-as-author errors** - Scott Brick, RC Bray, Steven Pacey correctly identified
- **AI prompt enhancement** - Warns AI when a name is a known narrator
- **Auto-save narrators** - Discovered narrators automatically added to BookDB

### New Feature: Deep Verification Mode

New "nuclear option" for library cleanup (Settings → Library Management):
- **Queue ALL books** for API verification regardless of current status
- Catches cases where folder structure looks correct but author is actually wrong
- Shows progress and estimated API usage before starting

### Performance: Faster API Rate Limits

Tuned rate limits to 80-90% of actual API limits:
- BookDB: 0.2s delay (was 0.5s) - Our API, can burst
- Audnexus: 0.8s delay (was 1.5s)
- OpenLibrary: 1.0s delay (was 1.5s)
- Google Books: 0.5s delay (was 2.5s)
- Default batch size: 10 (was 3)
- Default max requests/hour: 200 (was 30)

**Result**: Library scans complete ~3-4x faster

### New API Endpoints

- `GET /api/whisper-status` - Check if faster-whisper is installed and model ready
- `POST /api/install-whisper` - Install faster-whisper via pip from the UI
- `POST /api/deep_verify` - Trigger deep verification of entire library

### Improved: Layer 2→3→4 Advancement

Fixed multiple code paths where books were marked "needs_attention" instead of advancing to the next layer:
- AI returning placeholder authors now advances to Layer 3 (audio credits)
- Layer 3 failures now advance to Layer 4 (content analysis) if enabled
- Queue cleanup properly removes items with terminal statuses

### Technical Details

New functions added:
- `extract_audio_sample_from_middle()` - Gets audio from middle of file (not intro)
- `transcribe_with_whisper()` - Local transcription via faster-whisper
- `identify_book_from_transcript()` - OpenRouter book identification
- `_try_gemini_content_identification()` - Gemini Audio API handler
- `check_if_narrator()` - BookDB narrator lookup
- `auto_save_narrator()` - Auto-contribute discovered narrators

### Tests

- 184 tests passing (was 175)
- New tests for watch folder verification
- New tests for series number handling

---

## [0.9.0-beta.89] - 2026-01-12

### Fixed
- **Track Number Stripping** (Issue #57) - "02 Night Without Stars" now correctly searches as "Night Without Stars"
- **Local BookDB Support** - Uses configured bookdb_url instead of hardcoded cloud URL
- **Confidence Threshold Fix** - 60% confidence now correctly passes threshold check
- **Database Column Fix** - Fixed SQL error when marking books as needs_attention

---

## [0.9.0-beta.88] - 2026-01-11

### Fixed
- **Watch Folder Verification** (Issue #57) - Watch folder now verifies API results before accepting
  - Previously: Watch folder blindly trusted first API result with confidence > 60
  - Now: Calls `verify_drastic_change()` when API returns different author than expected
  - Prevents wrong matches like "Night Without Stars" (Judith Otto) when it should be Peter F. Hamilton

### Improved
- **Parent Folder as Author Hint** (Issue #57) - Uses subfolder name in watch folder as author context
  - `/watch/Peter F. Hamilton/02 Night Without Stars.mp3` → author hint = "Peter F. Hamilton"
  - Provides context that was missing when files had no embedded metadata

- **Same-Title-Different-Author Detection** (Issue #57) - Flags ambiguous matches for review
  - When multiple APIs return same title but different authors, flags for user attention
  - Prevents auto-accepting when there are multiple books with identical titles

- **Author Similarity Check** - Compares API author against folder/filename hint before accepting
  - If similarity < 50%, triggers verification instead of blind acceptance

### Tests
- 5 new regression tests for watch folder verification logic

## [0.9.0-beta.87] - 2026-01-11

### Fixed
- **Concurrent Scan SQLite Errors** (Issue #61) - Prevent race conditions during library scans
  - Added `SCAN_LOCK` mutex to prevent multiple scans running simultaneously
  - Added `scan_in_progress` flag for quick status checks
  - API endpoint now returns HTTP 409 if scan already in progress
  - New `/api/scan/status` endpoint to check if scanning
  - Prevents "UNIQUE constraint failed: books.path" and "database is locked" errors

- **API Key Fields Not Pasteable** (Issue #60) - Added show/hide toggles to all password fields
  - Settings page: Gemini, OpenRouter, Google Books API keys
  - ABS Dashboard: API token field
  - Setup Wizard: Gemini and OpenRouter API keys
  - Toggle button with eye icon for each field

- **"Unknown" Author Still Verified** (Issue #59 follow-up) - Fixed remaining gap
  - When AI returns empty results AND current author is placeholder, now marks "needs_attention"
  - Previously: empty AI result → always "verified" regardless of current author
  - Now: empty AI result + placeholder author → "needs_attention" with helpful message

### Tests
- 9 new regression tests:
  - 3 tests for Issue #61 (scan lock, scan_in_progress variable, blocking parameter)
  - 6 tests for Issue #60 (togglePasswordVisibility function and bi-eye icons in 3 templates)

## [0.9.0-beta.86] - 2026-01-10

### Fixed
- **"Unknown" Author Marked as Fixed/Verified** (Issue #59) - Placeholder authors now flag for attention
  - Fixed: Books with "Unknown" author were marked as "Fixed" or "Verified" instead of "Needs Attention"
  - System now checks for placeholder authors (Unknown, Various, etc.) before setting success statuses
  - Affected books now correctly show "Needs Attention" with message explaining the issue
  - Applies to all status transitions: verified, fixed, pending_fix paths

## [0.9.0-beta.85] - 2026-01-10

### Fixed
- **Watch Folder Files Treated as Orphans** (Issue #57) - Watch folder excluded from orphan scanning
  - Fixed: Files in watch folder (nested inside library) appeared as orphans with author "watch"
  - `find_orphan_audio_files()` now skips the watch folder path
  - Watch folder has its own processing flow and shouldn't mix with orphan organization

- **Watch Folder API Lookup Bug** (Issue #57) - Fixed argument order in API search
  - Fixed: `gather_all_api_candidates()` was called with swapped arguments (author, title instead of title, author)
  - Now also tries full filename search if initial parsing gives poor results
  - Files like "Extremity - Nicholas Binge.mp3" now correctly identified

### Code Quality
- Fixed bare `except:` clause in `find_orphan_audio_files()` - now uses `except Exception:` (PEP 8 compliance)

## [0.9.0-beta.84] - 2026-01-09

### Fixed
- **Output Folder Routing** (Issue #57 follow-up) - Books from watch folder now go to configured output folder
  - Fixed: When `watch_output_folder` was set, books from watch folder were still being sorted in-place
  - Now correctly routes to the configured output folder during queue processing and manual matching
  - Affected areas: `process_queue()`, `api_manual_match()`, watch folder processing

- **Author Initials Not Applied to AI Results** (Issue #57 follow-up) - Standardization now works everywhere
  - Fixed: "Peter F Hamilton" and "Peter F. Hamilton" folders created as separate authors
  - `standardize_author_initials` setting now applies to all author sources:
    - AI/API identification results
    - Audio analysis (Layer 3) results
    - Manual matches and edits
    - Watch folder processing
  - Previously only applied during path extraction, not when authors came from APIs

### Improved
- **Clearer Queue Processing Status** (Issue #57 feedback) - Better feedback on what happened
  - Processing now shows "X renamed, Y already correct" instead of just "Processed X items"
  - Shows remaining queue count when not complete: "3 remaining in queue"
  - Added tooltips to Fixed/Verified filter chips explaining the difference:
    - **Fixed**: "Books that were renamed/moved to new locations"
    - **Verified**: "Books already in correct location - no changes needed"
  - Status badges now have more descriptive tooltips

## [0.9.0-beta.79] - 2026-01-06

### Improved
- **Hide Media Type Filters When Irrelevant** (Issue #57 feedback) - UI cleanup for audio-only users
  - "Audio Only", "Ebook Only", and "Both" filter chips now hidden when Ebook Management is disabled
  - Reduces UI clutter for users who only manage audiobooks
  - URL parameters for these filters are also ignored when ebook management is off

## [0.9.0-beta.78] - 2026-01-04

### Fixed
- **SQLite Database Locking (Proper Fix)** - Complete architectural fix for database locking during processing
  - Previous fix (beta.73) only added `busy_timeout` which was a band-aid
  - Root cause: Worker functions held DB connections open during external API calls (10-30+ seconds)
  - Refactored `process_layer_1_api()`, `process_queue()`, and `process_layer_3_audio()` to use 3-phase approach:
    - Phase 1: Quick fetch, release connection immediately
    - Phase 2: External work (API/AI/audio calls) with NO database lock held
    - Phase 3: Quick write, release connection
  - Eliminates "database is locked" errors when triggering deep rescan during processing
  - Connection now held for milliseconds instead of 20-30+ seconds

## [0.9.0-beta.77] - 2026-01-03

### Improved
- **BookDB Rate Limiting** - Better handling of API rate limits
  - Server now returns proper `Retry-After` headers on all 429 responses
  - Client respects `Retry-After` instead of hardcoded backoff times
  - Prevents hammering the API when rate limited

## [0.9.0-beta.76] - 2026-01-03

### Fixed
- **Standardize Author Initials Setting Not Saving** (Issue #56) - Toggle now saves from web UI
  - Setting was missing from the form submission handler
  - Manually editing config.json was the only workaround

## [0.9.0-beta.75] - 2026-01-03

### Fixed
- **Multi-Edit Queue Accessible** (Issue #37) - Queue page now renders properly with Multi-Edit
  - `/queue` route was incorrectly redirecting to library view
  - Multi-Edit button and functionality now accessible at `/queue`

## [0.9.0-beta.74] - 2026-01-03

### Fixed
- **BookDB Rejecting Standalone Books** - BookDB was only returning results for series books
  - Standalone books (no series) were being discarded even when found
  - Now properly returns author/title for books not in a series
  - Fixes David Baldacci and other prolific authors with many standalone titles

## [0.9.0-beta.73] - 2026-01-02

### Fixed
- **SQLite Database Locking** (Issue #55) - Added `busy_timeout` pragma to handle large libraries
  - Prevents "database is locked" errors during concurrent operations
  - Added timeout to `init_db()` function as well

## [0.9.0-beta.72] - 2026-01-02

### Added
- **Multi-Edit Queue** (Issue #37) - Edit multiple queue items at once in a single view
  - "Multi-Edit" button opens modal showing all queue items in editable table
  - Edit author/title inline for each item
  - Search button per item to auto-fill from BookDB
  - Mark items as OK directly from the modal
  - "Save All Changes" applies all edits as pending fixes
  - Modified items highlighted, count shown in footer

- **Media Type Filter** (Issue #53) - Filter library by format: Audio Only, Ebook Only, or Both
  - New filter chips on Library page to find books missing a format
  - Helps identify audiobooks that don't have ebooks and vice versa
  - `media_type` column tracks format for each book
  - Works with existing filters (combine with Verified, Attention, etc.)

- **Standardize Author Initials** (Issue #54) - Option to normalize author initials to consistent format
  - "James S A Corey" → "James S. A. Corey"
  - "JRR Tolkien" → "J. R. R. Tolkien"
  - "C.S. Lewis" → "C. S. Lewis"
  - Preserves Mc/Mac/O' prefixes (McFadden, MacLeod, O'Brien)
  - Toggle in Settings → Library → "Standardize Author Initials"

- **ABS Integration Explanation** (Issue #47) - Info banner explaining what the Audiobookshelf integration does
  - Explains Progress Grid, Archive Candidates, Untouched, and User Groups features
  - Describes how Library Manager and ABS work together
  - Tips for enabling ABS-compatible folder structure
  - Dismissible banner (remembers preference)

- **BookDB Rate Limiting** - Added rate limiting and backoff for BookDB API calls
  - 0.5 second delay between calls (prevents hammering)
  - Exponential backoff on 429 responses (30s, 60s, 90s)
  - Prevents users with large libraries from getting rate-limited

### Added (Tests)
- 17 new tests for Issue #54 (author initials standardization)
- 7 new tests for Issue #53 (media type filter + detect_media_type function)
- 6 new tests for Issue #37 (multi-edit queue modal)
- New UI feature test suite (`test-env/test-ui-features.py`) - 22 tests total
  - Tests for Issue #47 (ABS explanation banner)
  - Tests for Issue #43 (tooltips on badges)
  - Tests for Issue #42 (edit warning during processing)
  - Tests for Issue #53 (media type filters)
  - Tests for Issue #37 (multi-edit queue)

---

## [0.9.0-beta.71] - 2026-01-01

### Fixed
- **Author Sanity Check** (Issue #50) - Strip junk suffixes from author folder names
  - "Peter F. Hamilton Bibliography" → "Peter F. Hamilton"
  - "Stephen King Collection" → "Stephen King"
  - Handles: Bibliography, Collection, Anthology, Complete Works, Selected Works, Best of, Works of, Omnibus
  - Also strips Calibre-style IDs from author names: "Author Name (123)" → "Author Name"

- **Title Sanity Check** (Issue #50) - Strip Calibre-style IDs from book titles
  - "The Great Gatsby (123)" → "The Great Gatsby" (Calibre internal book ID)
  - "Foundation (4567)" → "Foundation"
  - Preserves valid series info: "(Book 1)", "(Part 2)", "(Volume 3)" are NOT stripped
  - Only strips bare numeric IDs in parentheses at the end

### Added
- 27 new regression tests for Issue #50 (author sanity + title sanity + integration)

---

## [0.9.0-beta.70] - 2026-01-01

### Fixed
- **Author Prefix in Book Folder Names** (Issue #53 - Dennis's Log) - Fixed incorrect title extraction from folders
  - When a book folder is named "David Baldacci - Dream Town" under "David Baldacci/", the title was being stored as "David Baldacci - Dream Town" instead of just "Dream Town"
  - This caused 1% API confidence (comparing "David Baldacci - Dream Town" vs "Dream Town") and unnecessary AI processing
  - Also caused useless pending approvals: "PENDING APPROVAL: David Baldacci -> David Baldacci"
  - Now strips author prefix from book folder names when it matches the parent folder author
  - Added 7 new regression tests for Issue #53

---

## [0.9.0-beta.69] - 2026-01-01

### Removed
- **Reversed Structure Detection** - Completely removed the pattern-based "reversed detection" system
  - Was causing false positives (Issue #52) by guessing author/title based on regex patterns
  - Now trusts API lookups instead: if structure is wrong, APIs won't find matches → item goes to "Needs Attention"
  - Simpler approach: scan → API lookup → done (or manual review if no match)
  - Removed ~100 lines of pattern matching code and 2 API endpoints
  - Philosophy: trust the APIs, don't guess with scripts

---

## [0.9.0-beta.68] - 2026-01-01

### Fixed
- **False Positive Reversed Structure Detection** (Issue #52) - Books incorrectly flagged as title/author reversed
  - "James S A Corey" was wrongly flagged because multiple single initials without periods weren't recognized
  - "Freida McFadden" and similar Mc/Mac/O' names weren't recognized as valid author patterns
  - Titles like "Leviathan Wakes" were falsely matching as person names (any "Word Word" pattern)
  - Added new author name patterns: multiple single initials, Irish/Scottish prefixes (Mc, Mac, O')
  - Made title-as-name detection smarter: now requires first word to be a common first name
  - Added 15 new regression tests for Issue #52

---

## [0.9.0-beta.67] - 2026-01-01

### Fixed
- **Watch Folder Creating Duplicates** - Fixed bug where hard link fallback created duplicates
  - When `watch_use_hard_links` is enabled but source/dest are on different filesystems
  - Hard link would fail, system would copy file, but **never delete the original**
  - Now properly deletes source files after successful copy fallback
  - This was causing duplicate audiobooks when watch folder was on a different drive

---

## [0.9.0-beta.66] - 2026-01-01

### Fixed
- **Edit Book SQLite Error** (Issue #51) - Fixed crash when editing books in Attention tab
  - Error was: `'sqlite3.Row' object has no attribute 'get'`
  - Row objects use bracket access `row['column']`, not `.get()` method
  - Edit & Lock now works correctly for all book types

---

## [0.9.0-beta.65] - 2026-01-01

### Fixed
- **Watch Folder Retry Loop** (Issue #49) - Failed watch folder items no longer retry forever
  - Items that fail to move (e.g., "Too many versions exist") are now tracked in the database
  - Failed items show up in "Needs Attention" with the error message
  - User can edit the author/title and apply the fix to retry with corrected metadata
  - Successfully applied fixes move the item from watch folder to library
  - Prevents infinite retry loops that spam logs with the same error

- **Watch Folder Treated as Author** (Issue #46) - Watch folder inside library no longer parsed as author
  - If watch folder is inside a library path (e.g., `/library/watch`), it's now skipped during library scans
  - Prevents the watch folder name from appearing as an author in the library

### Improved
- **Encoding Info Cleanup** (Issue #48) - More aggressive cleanup of encoding artifacts from titles
  - Now strips standalone bitrates (128k, 64kbps, etc.) even outside brackets
  - Removes file sizes (463mb, 1.2gb) with or without curly braces
  - Strips audio channel info (mono, stereo, multi)
  - Removes codec info (vbr, cbr, aac, lame, opus)

- **Audnexus Logging** (Issue #45) - Better logging to debug API issues
  - Errors now logged at WARNING level instead of DEBUG
  - Each API's results logged at INFO level showing what was matched
  - Garbage match rejections logged at INFO level for visibility

---

## [0.9.0-beta.64] - 2025-12-31

### Fixed
- **Queue Not Auto-Processing** (Issue #44) - Queue now processes automatically regardless of Auto-Fix setting
  - Previously, queue processing was tied to the Auto-Fix toggle - queue would never process unless Auto-Fix was enabled
  - Now queue always processes on schedule (scans library, runs Layer 1/2/3 identification)
  - Auto-Fix toggle now only controls whether fixes are applied automatically or sent to Pending for manual review
  - This is how it always should have worked - Auto-Fix should control renaming, not identification

---

## [0.9.0-beta.63] - 2025-12-31

### Fixed
- **BookDB Integration** (Issue #45) - BookDB metadata lookups now work without configuration
  - Hardcoded public API key so users don't need to configure `bookdb_api_key`
  - Layer 1 API lookups now properly use BookDB as the first source
  - Edit dialog "Search Book Database" now returns results from BookDB instead of falling back to Google Books
  - Fixed slow queries that were causing timeouts (was doing full table scans on 50M books)

### Improved
- **BookDB Search Performance** - Searches now complete in ~2.5 seconds instead of timing out
  - Optimized FTS (Full-Text Search) queries to avoid slow OR LIKE clauses
  - Better author-filtered matching for accurate results

---

## [0.9.0-beta.62] - 2025-12-26

### Improved
- **Settings Page Reorganization** - Cleaner 5-tab layout for better discoverability
  - **Library** - Paths, naming format, watch folder, ebook management
  - **Processing** - Background processing, confidence settings, identification layers
  - **AI Setup** - Provider selection (Gemini/OpenRouter/Ollama), metadata sources
  - **Safety** - Auto-apply, author approval, trust the process, metadata embedding
  - **Advanced** - Language, error reporting, updates, danger zone, backup, debug/logs
  - "How It Works" banner now remembers dismissal via localStorage

### Added
- **UI Tooltips** (Issue #43) - Hover over status badges to see what they mean
  - Library page: Tooltips on all status badges (OK, Needs Fix, Processing, etc.)
  - Dashboard page: Tooltips on status counts
  - History page: Tooltips on Fixed/Pending/Undone badges

- **Edit Warning During Processing** (Issue #42) - Warning when editing during queue processing
  - If you click Edit while processing is active, you get a warning that the item may change
  - "Don't show again" option suppresses warning for the rest of the session
  - Prevents confusion when items change mid-edit

---

## [0.9.0-beta.61] - 2025-12-24

### Improved
- **Watch Folder Unknown Author Handling** (Issue #40) - Unknown authors now flagged for review
  - When watch folder can't determine author (shows as "Unknown"), item is flagged for user attention
  - File is still moved to library (so watch folder doesn't fill up)
  - But status is set to `needs_attention` instead of `pending`
  - Shows up in the "Attention" tab with message explaining the issue
  - User can edit to correct author/title before processing continues
  - Uses existing `is_placeholder_author()` to catch all placeholder names (Unknown, Various, N/A, etc.)

---

## [0.9.0-beta.60] - 2025-12-24

### Fixed
- **Series Folders Showing in Queue** (Issue #36) - Series folders no longer appear as items needing fixes
  - When a folder is detected as a series folder, it's now removed from the processing queue
  - Same fix applied for multi-book collection folders
  - Queue view now filters out series_folder and multi_book_files status items
  - Queue count now accurately excludes these non-processable items

---

## [0.9.0-beta.59] - 2025-12-24

### Fixed
- **Ollama Model Dropdown** (Issue #41) - Model names now display correctly instead of "undefined"
  - Fixed JavaScript that was treating model name strings as objects
  - Dropdown now properly shows available models from Ollama server

---

## [0.9.0-beta.58] - 2025-12-24

### Fixed
- **PUID/PGID Improvements** (Issue #39) - Fixed startup errors with common GIDs
  - Handles existing GIDs (e.g., GID 100 = "users" group in Debian)
  - Uses `-o` flag for useradd to allow duplicate UIDs
  - Log file moved to `/data/app.log` (persistent, accessible to non-root user)
  - Properly sets ownership of data directories before starting app

---

## [0.9.0-beta.57] - 2025-12-23

### Added
- **PUID/PGID Support** (Issue #39) - Docker container now respects user permissions
  - Set `PUID` and `PGID` environment variables to control file ownership
  - UnRaid users: use `PUID=99` and `PGID=100` for "nobody" user
  - Defaults to root (0/0) for backwards compatibility
  - Files created by the container will have correct ownership

---

## [0.9.0-beta.56] - 2025-12-23

### Fixed
- **Watch Folder Settings Not Saving** (Issue #32) - Toggle and settings now save properly from UI
  - Previously required manual editing of config.json
  - All watch folder settings (mode, paths, intervals, hard links) now save correctly

- **Watch Folder `analyze_path` Error** (Issue #32) - Fixed `name 'analyze_path' is not defined` error
  - Watch folder processing now uses `extract_author_title` for path analysis
  - Books are properly identified before moving to output folder

- **False Positive Series Folder Detection** (Issue #36) - Series folders with 1 book no longer flagged as needing fixes
  - Previously required 2+ books in series folder to be detected as series
  - Now detects series folders even with just 1 numbered book subfolder
  - Also detects series structure when folder has no direct audio but subfolders do
  - Properly scans book folders inside series folders (3-level structure: Author/Series/Book)

- **Book Numbers Polluting Search** (Issue #38) - Leading book numbers no longer break BookDB searches
  - Searches like "5 - The Rhesus Chart" now find the correct book
  - Extracts series number from query before cleaning (preserves book position)
  - Cleans query to remove leading numbers before sending to BookDB
  - Titles like "1984" are preserved (only strips numbers followed by separators)

---

## [0.9.0-beta.55] - 2025-12-22

### Added
- **Watch Folder Mode** (Issue #32) - Monitor a folder for new audiobooks and organize automatically
  - Enable in Settings → Behavior → Watch Folder Mode
  - Set Watch Folder (input path to monitor for new downloads)
  - Set Output Folder (where to move organized books - defaults to library)
  - Configurable check interval (default 60 seconds)
  - Min file age setting (wait for downloads to complete)
  - **Hard link support** - save disk space by hard linking instead of moving (same filesystem only)
  - Delete empty folders option after moving
  - Runs as separate worker thread for fast response
  - Uses API lookups to identify books before moving

- **Library Search** - Find books in your library by author or title
  - New search box on Library page
  - Real-time search across your entire collection
  - Quick way to find specific books to edit or lock

- **Locked Books Filter** - View all user-locked books
  - New "Locked" filter chip on Library page
  - See which books have been manually edited and locked
  - Quick access to unlock if needed

- **Edit from Library** - Edit any book directly from Library view
  - Edit button on all library items (not just History)
  - Same BookDB search and manual entry as History page
  - Locks book after editing to protect your changes

### Changed
- **Library API** - Now returns `locked` count and `user_locked` field for each item
- **Library filters** - Added `locked` and `search` filter options

---

## [0.9.0-beta.54] - 2025-12-22

### Added
- **User Edit & Lock System** - Manually edit any book's metadata and lock it from future changes
  - Edit button on all History items (pending, fixed, verified)
  - Search BookDB to find the correct match
  - Manually set author, title, series name, and series number
  - **Changes are "cemented"** - system will never overwrite user-set metadata
  - Lock icon shows which books have user-locked settings
  - Unlock button available to allow re-processing if desired

### Changed
- **Locked Books Skip Processing** - User-locked books are completely skipped during:
  - Library scanning (won't be re-queued)
  - Layer 1 API processing
  - Layer 2 AI processing
  - Layer 3 Audio analysis
  - Protects your manual corrections from being overwritten

### Fixed
- **Issue #36 Continuation** - Users can now correct wrong AI identifications before applying fixes
  - Previously could only Apply or Reject - no way to fix wrong matches
  - Now has Edit button to search and select the correct book

---

## [0.9.0-beta.53] - 2025-12-20

### Fixed
- **Critical: Process Button Skipped Layer 1** - Clicking "Process" went straight to Layer 2 (AI)
  - Items queued at Layer 1 were never picked up because Layer 2 only looks for `verification_layer=2`
  - Now properly runs Layer 1 (API) -> Layer 2 (AI) -> Layer 3 (Audio) even for single-click processing
  - This was why users saw "Fetched 0 items from queue" despite having queued items

---

## [0.9.0-beta.52] - 2025-12-20

### Fixed
- **Verification Now Saves Profile Data** - When Layer 1 (API) verifies a book, it now saves the verification source
  - Shows which API confirmed the book (BookDB, OpenLibrary, Google Books, etc.)
  - Displays confidence percentage in the Library view

### Added
- **Legacy Badge for Old Verifications** - Books verified before profile system show "Legacy" badge
  - Hovering shows "Verified before profile system - run Deep Scan to re-verify"
  - Clear indication of which books need re-verification

- **Deep Scan Re-verifies Legacy Books** - Running a Deep Scan now re-queues legacy verified books
  - Books with no profile data get re-processed through proper verification
  - Populates profile with source and confidence data

---

## [0.9.0-beta.51] - 2025-12-20

### Fixed
- **Critical: Placeholder Authors Incorrectly Verified** - Books with "Unknown" author were auto-verified
  - Layer 1 API verification gave placeholder authors (Unknown, Various, etc.) 100% match score
  - Now properly advances placeholder authors to Layer 2 (AI) for actual identification
  - "Unknown / Trailer Park Elves" no longer shows as verified without knowing the real author

- **Clear Queue Marked Books as Verified** - Clearing the queue falsely marked unverified books as verified
  - Now resets books to `pending` status with `verification_layer=0`
  - Books can be properly re-scanned and processed

- **Reject All Pending Marked Books as Verified** - Rejecting proposed fixes falsely verified books
  - Now resets books to `pending` status instead
  - Rejecting a fix ≠ verifying the book is correct

---

## [0.9.0-beta.50] - 2025-12-20

### Added
- **Anonymous Error Reporting** - Opt-in system to help improve Library Manager
  - Toggle in Settings → Debug Menu → "Anonymous Error Reporting"
  - "Send to Developer" button sends error reports with optional message
  - Reports include error context and traceback (no personal data)
  - Helps identify bugs users encounter in real usage

- **API Connection Tests** - Debug menu now has "Test Connections" button
  - Tests BookDB, Gemini, OpenRouter, Ollama, Google Books, Hardcover
  - Shows connection status and response times
  - Helps troubleshoot configuration issues

- **Clear All Buttons** - Debug menu improvements
  - "Clear All" buttons for error reports, activity log, and queue log
  - Easier cleanup during troubleshooting

### Improved
- **Activity Panel** - Better tracking of background operations
  - Fixed issues with activity display
  - Cleaner formatting

- **Settings UI** - More polished debug tools section
  - Grouped related functions together
  - Better button layouts

---

## [0.9.0-beta.49] - 2025-12-20

### Fixed
- **Critical: Queue Processing Not Working** - Items stuck in queue, "processed 0" returned
  - Layer 1 (API) had incomplete code that marked items as layer=4 but never removed them from queue
  - Layer 3 (Audio) had same issue - extracted metadata but never created fixes
  - Items got stuck at verification_layer=4 with no handler processing them
  - Now properly: verifies items and removes from queue, or advances to next layer

### Improved
- **Layered Processing Reliability** - All three processing layers now complete their work properly
  - Layer 1: Verifies correct items (90%+ match), advances others to Layer 2
  - Layer 3: Creates pending fixes from audio analysis, or marks verified

### Added
- **Real User Workflow Tests** - New integration tests that catch processing bugs
  - `test_process_empties_queue` - Catches "processed 0 but queue full" bugs
  - `test_queue_items_not_stuck` - Catches items stuck at invalid layers
  - `test-env/test-user-workflow.py` - Full end-to-end workflow testing

---

## [0.9.0-beta.48] - 2025-12-19

### Added
- **Series Number Extraction from Search** (Issue #34) - Manual match search now extracts series info from query
  - "Horus Heresy Book 36" → auto-fills series="Horus Heresy", position=36
  - "Mistborn #3" → extracts position=3
  - "No. 5" format also supported
  - Results without series data get enriched with extracted info

- **Manual Series Override UI** - New editable fields when selecting a search result
  - Series Name and Book # fields appear after selecting a result
  - Pre-populated with data from database or extracted from query
  - Shows hint "(from database)" or "(extracted from your search)"
  - User can edit/correct before saving

### Fixed
- **Bug Report Privacy** (Issue #35) - Sensitive info no longer exposed in bug reports
  - API keys replaced with connection status: `Gemini: connected`, `Google Books: not configured`
  - Library paths hidden - shows `library_paths_count: 2` instead of actual paths
  - Error log paths sanitized: `/home/user/books/file.mp3` → `[path]/file.mp3`
  - Only safe config settings included (no secrets, no personal info)

---

## [0.9.0-beta.47] - 2025-12-19

### Fixed
- **Verification Layer Settings Not Saving** - `enable_audio_analysis`, `deep_scan_mode`, `enable_api_lookups`, `enable_ai_verification`, and `profile_confidence_threshold` now persist
  - Form field names were mismatched between template and save handler
  - All toggles and the confidence slider now properly save and load

- **Search Strips Leading Track Numbers** (Issue #33) - Manual match search now strips leading numbers
  - `06 - Dragon Teeth` → searches for `Dragon Teeth`
  - `01. The Martian` → searches for `The Martian`
  - `Track 05 - Something` → searches for `Something`
  - Safe patterns preserved: `1984`, `11/22/63` stay unchanged

- **Orphan Organize Moves Companion Files** (Issue #31) - Covers, NFO, and metadata files now move with audio
  - Covers: `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`
  - Metadata: `.nfo`, `.txt`, `.json`, `.xml`, `.cue`
  - Companion ebooks: `.pdf`, `.epub`, `.mobi`
  - Empty source folders cleaned up automatically

- **Missing Default Config** - `series_grouping` was used but missing from DEFAULT_CONFIG

- **Duplicate Setting Removed** - Removed `audio_analysis` toggle from Behavior section (was duplicate of `enable_audio_analysis` in Identification Sources)

### Changed
- **Unified Navigation** - Removed separate Queue and Orphans pages from navbar
  - Both now redirect to Library page with appropriate filter pre-selected
  - `/queue` → `/library?filter=queue`
  - `/orphans` → `/library?filter=orphan`
  - Cleaner navigation: Dashboard → Library → History → ABS → Settings

- **Settings Descriptions Improved** - All settings now have clear, plain-language descriptions
  - "Smart Verification" renamed to "Require Approval for Author Changes"
  - "Enable Background Processing" now explains what it does
  - "Verification Layers" section renamed to "Identification Sources" with Layer 1/2/3 labels
  - Confidence threshold slider explanation improved

### Added
- **Skip Confirmations in Settings** - New toggle in Settings > Behavior
  - Previously only available in Library view quick action bar
  - Now accessible from main Settings page
  - Disables "Are you sure?" popups for apply/reject/undo actions

---

## [0.9.0-beta.46] - 2025-12-19

### Fixed
- **UnRaid Config Persistence** - App now auto-detects `/config` mount point (UnRaid default)
  - Previously hardcoded `DATA_DIR=/data` in Dockerfile, ignoring UnRaid's `/config`
  - Now uses `os.path.ismount()` to detect which directory is actually mounted
  - UnRaid users no longer need to manually add `/data` path
  - Existing configs are NEVER lost - always checks for existing files first

- **Search Title Cleanup Improvements**
  - Underscores now converted to spaces (`audiobook_Title` → `audiobook Title`)
  - Curly brace junk removed (`{465mb}`, `{narrator}`)
  - Titles like "1984" and "11/22/63" no longer incorrectly stripped
  - Added "Unknown Author" to placeholder detection

### Changed
- Dockerfile no longer sets `DATA_DIR` env var - app auto-detects
- Both `/data` and `/config` directories created in container for compatibility
- Migration checks both locations for legacy config files
- `clean_search_title()` is now minimal - doesn't strip dates/timestamps
  - Layered verification (API + AI + Audio) determines the real title
  - Multiple agreeing sources = high confidence (Book Profile system)

### Technical
- `_detect_data_dir()` priority:
  1. Explicit `DATA_DIR` env var (user override)
  2. Directory with existing config files (never lose settings)
  3. Actually mounted volume via `os.path.ismount()` (fresh install detection)
  4. `/data` fallback (our documented default)
  5. `/config` fallback (UnRaid)
  6. App directory (local development)
- Added comprehensive naming issue test suite (39 tests covering all GitHub issues)

---

## [0.9.0-beta.45] - 2025-12-18

### Added
- **Layered Processing Architecture** - Queue processing now uses independent verification layers
  - **Layer 1 (API)**: Fast database lookups via BookDB, Audnexus, OpenLibrary, etc.
  - **Layer 2 (AI)**: AI verification for items that failed API lookup
  - **Layer 3 (Audio)**: Gemini audio analysis as final fallback
  - Each layer processes independently and hands off failures to the next
  - Respects existing settings: `enable_api_lookups`, `enable_ai_verification`, `enable_audio_analysis`

- **New Database Column** - `verification_layer` tracks which layer each book is at
  - 0 = Not processed, 1 = Awaiting API, 2 = Awaiting AI, 3 = Awaiting Audio, 4 = Complete

- **Layer Functions** - New processing functions for cleaner code separation
  - `process_layer_1_api()`: Handles API database lookups
  - `process_layer_3_audio()`: Handles Gemini audio analysis
  - `process_queue()` now only handles Layer 2 (AI verification)

### Changed
- `process_all_queue()` now processes layers in sequence: API → AI → Audio
- Processing status now shows which layer is active
- Queue items only advance through enabled layers
- When API lookups are disabled, items go directly to AI layer

### Technical
- All existing features preserved: multibook detection, series sorting, naming templates, etc.
- All bug fixes remain intact: config persistence, template cleanup, version handling
- Integration tests pass: 9/9

---

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
