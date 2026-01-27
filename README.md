# Library Manager

<div align="center">

**Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification**

[![Version](https://img.shields.io/badge/version-0.9.0--beta.98-blue.svg)](CHANGELOG.md)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue.svg)](https://ghcr.io/deucebucket/library-manager)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

*Automatically fix messy audiobook folders using real book databases + AI intelligence*

</div>

---

## Recent Changes (stable)

> **beta.97** - 🔍 **Series Mismatch Detection & SearXNG Fallback** (Issues #76, #77)
> - **Series Mismatch Fix** - Books with series info now correctly reject wrong matches
> - **SearXNG Fallback** - New web search provider when APIs fail (Amazon, Audible, Goodreads parsing)
> - **Whisper Setting Fix** - Speech-to-Text model selection now saves correctly
> - **External API Updates** - Audnexus adapter updated for Jan 2026 API changes (now ASIN-only)

> **beta.96** - 🐛 **Watch Folder Duplicates Fix** (Issue #76)
> - **Atomic Directory Move** - Prevents partial moves creating "Version B" folders
> - **Partial Move Detection** - Completes interrupted moves instead of duplicating
> - **Edit Persistence Fix** - Multiple edits to pending_fix items now preserve original metadata

> **beta.95** - 🔧 **Major Code Refactoring**
> - **32% Code Reduction** - `app.py` reduced from 15,491 to 10,519 lines
> - **Modular Architecture** - New `library_manager/` package with organized modules
> - **No User-Facing Changes** - Same functionality, cleaner codebase for future development

> **beta.94** - 🐛 **Bug Fixes** (Issues #64, #71, #74)
> - **Queue Hanging Fix** - Circuit breaker now properly advances queue when providers fail (#74)
> - **Community Toggle** - "Contribute to Community" setting now saves correctly (#71)
> - **Whisper Install** - Docker permission error fixed (#63)
> - **API Key Visibility** - Keys now shown in settings (hidden by default, eye toggle reveals)
> - **Apply All Fix** - History entries now store paths to prevent "Source no longer exists" errors
> - **Dashboard Counts** - Fixed inflated counts by excluding series folders from totals
> - **Title Cleaning** - Strips torrent naming junk (bitrates, timestamps, editor names, year prefixes)

> **beta.93** - 🌐 **P2P Cache & Resilience** (Issue #62)
> - **P2P Book Cache** - Optional decentralized cache shares BookDB results with other users
> - **Helps During Outages** - Get results from P2P network when BookDB is temporarily down
> - **Opt-in & Private** - Disabled by default, only metadata shared (no file paths)
> - **BookDB Retry Logic** - 5 retries with backoff when no fallback configured
> - **Data Validation** - Rejects malformed/malicious P2P cache entries

> **beta.92** - 🔒 **Security & Stability**
> - **Confidence Threshold** - Books only marked "verified" when confidence ≥40%, prevents false positives
> - **API Keys Hidden** - Keys no longer exposed in HTML source, shows "Key configured" instead
> - **Issue #59 Complete Fix** - Placeholder authors ("Unknown Author") now detected during scan
> - **Issue #63 Fix** - Docker Whisper install permission error resolved
> - **BookDB Stability** - Circuit breaker for rate limiting, improved multi-user fairness
> - **Layer 2 Recovery** - Stuck items now properly advanced when Layer 2 disabled

> **beta.92** - 🎧 **Audio-First Identification** (Major Feature)
> - **Revolutionary Approach** - Now identifies books from narrator introductions FIRST
> - **52% Identification Rate** - Half of books identified from audio alone in Layer 1
> - **4-Layer Pipeline** - Audio transcription → AI parsing → API enrichment → Folder fallback
> - **faster-whisper Integration** - Local, free speech-to-text via Python venv
> - **Known Narrator Detection** - Prevents AI from confusing narrators with authors

> **beta.90** - 🎯 **Layer 4 Content Analysis** (Major Feature)
> - **The Final Layer** - Transcribes actual story content to identify books when all else fails
> - **Whisper + OpenRouter Fallback** - Local transcription + free AI when Gemini unavailable
> - **No GPU Required** - faster-whisper runs on CPU, model downloads automatically

> **beta.89** - Watch Folder Reliability (Issue #57)
> - Track number stripping, local BookDB support, confidence threshold fix

> **beta.87-88** - Watch Folder Verification & Scan Locking (Issues #57, #59-61)
> - API result verification, parent folder hints, concurrent scan fix, password toggles

> **beta.84-86** - Status & Output Fixes (Issues #57, #59)
> - Placeholder author detection, output folder routing, author initials standardization

> **beta.78-83** - SQLite Locking, Setup Wizard, Orphan Organization
> - 3-phase processing, first-run wizard, duplicate detection fix

> **beta.72-77** - Multi-Edit, Media Filters, Author Initials
> - Edit all queue items, media type filter, "J R R Tolkien" → "J. R. R. Tolkien"

[Full Changelog](CHANGELOG.md)

---

## The Problem

Audiobook libraries get messy. Downloads leave you with:

```
Your Library (Before):
├── Shards of Earth/Adrian Tchaikovsky/        # Author/Title swapped!
├── Boyett/The Hollow Man/                     # Missing first name
├── Metro 2033/Dmitry Glukhovsky/              # Reversed structure
├── [bitsearch.to] Dean Koontz - Watchers/     # Junk in filename
├── The Great Gatsby Full Audiobook.m4b        # Loose file, no folder
└── Unknown/Mistborn Book 1/                   # No author at all
```

---

## The Solution

Library Manager combines **real book databases** (50M+ books) with **AI verification** to fix your library:

```
Your Library (After):
├── Adrian Tchaikovsky/Shards of Earth/
├── Steven Boyett/The Hollow Man/
├── Dmitry Glukhovsky/Metro 2033/
├── Dean Koontz/Watchers/
├── F. Scott Fitzgerald/The Great Gatsby/
└── Brandon Sanderson/Mistborn/1 - The Final Empire/
```

---

## Features

### Smart Path Analysis
- Works backwards from audio files to understand folder structure
- Database-backed author/series detection (50M+ books)
- Fuzzy matching ("Dark Tower" finds "The Dark Tower")
- AI fallback for ambiguous cases
- **Safe fallback** - connection failures don't cause misclassification

### 4-Layer Identification Pipeline (Audio-First)
```
Layer 1: Audio Transcription + AI Parsing (Most Reliable)
         Transcribes 45-second intro → AI extracts author/title/narrator
         ✓ 52% of books identified from audio alone

Layer 2: AI Audio Analysis (Deeper Analysis)
         Sends audio directly to Gemini for unclear transcripts

Layer 3: API Enrichment (Add Metadata)
         BookDB → Audnexus → OpenLibrary → Google Books → Hardcover

Layer 4: Folder Name Fallback (Last Resort)
         Uses folder structure when audio identification fails
         Works even when file has zero metadata or intro credits
```
Each layer only runs if the previous layer couldn't confidently identify the book.

### Safety First
- **Drastic changes require approval** - author swaps need manual review
- **Garbage match filtering** - rejects unrelated results (<30% similarity)
- **Undo any fix** - every rename can be reverted
- **Structure reversal detection** - catches Metro 2033/Author patterns
- **System folders ignored** - skips `metadata`, `cache`, `@eaDir`, etc.

### Series Grouping (Audiobookshelf-Compatible)
```
Brandon Sanderson/Mistborn/1 - The Final Empire/
Brandon Sanderson/Mistborn/2 - The Well of Ascension/
James S.A. Corey/The Expanse/1 - Leviathan Wakes/
```

### Custom Naming Templates
Build your own folder structure:
```
{author}/{title}                          → Brandon Sanderson/The Final Empire/
{author}/{series}/{series_num} - {title}  → Brandon Sanderson/Mistborn/1 - The Final Empire/
{author} - {title} ({narrator})           → Brandon Sanderson - The Final Empire (Kramer)/
```

### Language Support
- **28 languages** - German, French, Spanish, Italian, Portuguese, Dutch, Swedish, Norwegian, Danish, Finnish, Polish, Russian, Japanese, Chinese, Korean, Arabic, Hebrew, Hindi, Turkish, Czech, Hungarian, Greek, Thai, Vietnamese, Ukrainian, Romanian, Indonesian
- **Preserve original titles** - keeps "Der Bücherdrache" instead of translating to English
- **Regional Audible search** - queries audible.de, audible.fr, etc. for localized results
- **Audio language detection** - use Gemini to detect spoken language in audiobooks

### Additional Features
- **Web dashboard** with dark theme
- **Watch folder mode** - monitor downloads folder, auto-organize new audiobooks
- **Manual book matching** - search 50M+ database directly
- **Edit & lock metadata** - correct wrong matches, lock to prevent overwriting
- **Library search** - find any book by author or title
- **Loose file detection** - auto-creates folders for dumped files
- **Ebook management (Beta)** - organize ebooks alongside audiobooks
- **Health scan** - detect corrupt/incomplete audio files
- **Audio analysis (Beta)** - extract metadata from audiobook intros via Gemini
- **In-browser updates** - update from the web UI
- **Backup & restore** - protect your configuration
- **Version-aware renaming** - different narrators get separate folders

---

## Quick Start

### Option 1: Docker (Recommended)

```bash
# Pull from GitHub Container Registry
docker run -d \
  --name library-manager \
  -p 5757:5757 \
  -v /path/to/audiobooks:/audiobooks \
  -v library-manager-data:/data \
  ghcr.io/deucebucket/library-manager:latest
```

Or with Docker Compose:

```yaml
version: '3.8'
services:
  library-manager:
    image: ghcr.io/deucebucket/library-manager:latest
    container_name: library-manager
    ports:
      - "5757:5757"
    volumes:
      - /your/audiobooks:/audiobooks
      - library-manager-data:/data
    restart: unless-stopped

volumes:
  library-manager-data:
```

### Option 2: Direct Install

```bash
git clone https://github.com/deucebucket/library-manager.git
cd library-manager
pip install -r requirements.txt
python app.py
```

### Configure

1. Open **http://localhost:5757**
2. Go to **Settings**
3. Add library path (`/audiobooks` for Docker, or your actual path)
4. Add AI API key (Gemini recommended - 14,400 free calls/day)
5. **Save** and **Scan Library**

---

## Docker Installation

### Volume Mounts

Docker containers are isolated. Mount your audiobook folder:

```yaml
volumes:
  - /your/audiobooks:/audiobooks  # LEFT = host, RIGHT = container
  - library-manager-data:/data    # Persistent config/database
```

Use `/audiobooks` (container path) in Settings.

### Platform Examples

| Platform | Volume Mount |
|----------|-------------|
| **UnRaid** | `/mnt/user/media/audiobooks:/audiobooks` |
| **Synology** | `/volume1/media/audiobooks:/audiobooks` |
| **Linux** | `/home/user/audiobooks:/audiobooks` |
| **Windows** | `C:/Users/Name/Audiobooks:/audiobooks` |

See [docs/DOCKER.md](docs/DOCKER.md) for detailed setup guides.

---

## Configuration

### Key Settings

| Option | Default | Description |
|--------|---------|-------------|
| `library_paths` | `[]` | Folders to scan |
| `naming_format` | `author/title` | Folder structure |
| `series_grouping` | `false` | Audiobookshelf-style series folders |
| `auto_fix` | `false` | Auto-apply vs manual approval |
| `protect_author_changes` | `true` | Require approval for author swaps |
| `scan_interval_hours` | `6` | Auto-scan frequency |

### AI Providers

**Google Gemini** (Recommended)
- 14,400 free API calls/day
- Get key at [aistudio.google.com](https://aistudio.google.com)

**OpenRouter**
- Multiple model options
- Free tier available

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/scan` | POST | Trigger library scan |
| `/api/deep_rescan` | POST | Re-verify all books |
| `/api/process` | POST | Process queue items |
| `/api/queue` | GET | Get queue |
| `/api/library` | GET | Get library with filters |
| `/api/stats` | GET | Dashboard stats |
| `/api/apply_fix/{id}` | POST | Apply pending fix |
| `/api/reject_fix/{id}` | POST | Reject suggestion |
| `/api/undo/{id}` | POST | Revert applied fix |
| `/api/edit_book` | POST | Edit & lock book metadata |
| `/api/unlock_book/{id}` | POST | Unlock book for reprocessing |
| `/api/analyze_path` | POST | Test path analysis |

---

## Troubleshooting

**Wrong author detected?**
→ Go to Pending → Click Reject (✗)

**Want to undo a fix?**
→ Go to History → Click Undo (↩)

**Series not detected?**
→ Enable Series Grouping in Settings → General

**Docker can't see files?**
→ Check volume mounts in docker-compose.yml

---

## Development

### Run Tests

```bash
# Full integration test suite (pulls from ghcr.io)
./test-env/run-integration-tests.sh

# Build from local source instead
./test-env/run-integration-tests.sh --local

# Rebuild 2GB test library first
./test-env/run-integration-tests.sh --rebuild
```

### Local Development

```bash
python app.py  # Runs on http://localhost:5757
```

---

## Testing

We take testing seriously. Every release is validated against real-world chaos scenarios.

### Chaos Library Testing

Before every release, we test against a **500-book "chaos library"** - a nightmare collection designed to break the app:

| Chaos Type | Example | What We're Testing |
|------------|---------|-------------------|
| **Wrong Author** | `Stephen King - The Martian` | Can we detect misattribution? |
| **Narrator as Author** | `Ray Porter - Project Hail Mary` | Common audiobook mistake |
| **Swapped Fields** | `The Final Empire - Brandon Sanderson` | Structure reversal detection |
| **Foreign Characters** | `Nick Offerman - 罪と罰` | Unicode handling |
| **Heavy Typos** | `Nil Gaiman - Annsi Boys` | Fuzzy matching resilience |
| **Torrent Prefixes** | `[MAM] Dean Koontz - Watchers (2021)` | Junk stripping |
| **Missing Info** | `Audiobook_574` | Identification from nothing |
| **Wrong Series Number** | `Mistborn Book 15 - The Final Empire` | Series validation |
| **Mixed Languages** | `Харуки Мураками - Dune` | Cross-language chaos |

The chaos library uses **symlinks to real audiobook files** (166GB represented, ~0 disk usage), so we're testing with actual audio content - not just filename patterns.

### Regression Testing

**Every GitHub issue becomes a test case.** When users report bugs, we:

1. **Reproduce** the exact scenario
2. **Fix** the underlying issue
3. **Add a test** that catches this specific case
4. **Run tests before every commit** to ensure we never revert fixes

Our test suite (`test-env/test-naming-issues.py`) currently validates **184+ edge cases** derived from real user issues:

```bash
# Run naming/path edge case tests
python test-env/test-naming-issues.py

# Example output:
# --- Issue #57: Watch folder verification ---
# [PASS] Watch folder verifies drastic author changes
# [PASS] Watch folder detects same-title-different-author
# --- Issue #60: Password visibility toggles ---
# [PASS] templates/settings.html has togglePasswordVisibility
# ...
# RESULTS: 184 passed, 0 failed
```

### Pre-Push Verification

Before pushing any changes, we run:

1. **Syntax check** - `python -m py_compile app.py`
2. **Regression tests** - All 184+ edge cases
3. **Code review** - Adversarial review of changes
4. **Security audit** - Check for common vulnerabilities
5. **Chaos library scan** - Full 500-book identification test

---

## Contributing

Pull requests welcome! Ideas:
- [ ] Ollama/local LLM support
- [ ] Cover art fetching
- [x] Metadata embedding (added in v0.9.0-beta.20)
- [ ] Movie/music library support

---

## Support & Contact

- **Issues/Bugs:** [GitHub Issues](https://github.com/deucebucket/library-manager/issues)
- **Email:** hello@deucebucket.com

---

## License

AGPL-3.0 License - See [LICENSE](LICENSE) for details.

**What this means:**
- Free to use, modify, and distribute
- If you modify and run this as a service, you must release your source code
- Commercial use requires either open-sourcing your changes OR obtaining a commercial license
