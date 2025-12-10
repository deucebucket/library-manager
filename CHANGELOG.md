# Changelog

All notable changes to Library Manager will be documented in this file.

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
