# Library Manager

<div align="center">

**Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification**

[![Version](https://img.shields.io/badge/version-0.9.0--beta.75-blue.svg)](CHANGELOG.md)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue.svg)](https://ghcr.io/deucebucket/library-manager)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

*Automatically fix messy audiobook folders using real book databases + AI intelligence*

</div>

---

## Recent Changes (develop)

> **beta.75** - Multi-Edit Queue Fix
> - **Queue Page Restored** - `/queue` now renders properly with Multi-Edit button accessible

> **beta.74** - BookDB Standalone Books Fix
> - **BookDB Fix** - Now returns results for standalone books (not just series)

> **beta.73** - SQLite Locking Fix (Issue #55)
> - **Database Locking Fix** - Added `busy_timeout` pragma for large libraries

> **beta.72** - Multi-Edit, Media Filters & Author Initials
> - **Multi-Edit Queue** (Issue #37) - Edit all queue items at once in a single modal view
> - **Media Type Filter** (Issue #53) - Filter library by format (Audio Only / Ebook Only / Both)
> - **Standardize Author Initials** (Issue #54) - Normalize "J R R Tolkien" → "J. R. R. Tolkien"
> - **ABS Integration Docs** (Issue #47) - Info banner explaining Audiobookshelf features

> **beta.71** - Author & Title Sanity Checks (Issue #50)
> - **Author Cleanup** - Strips "Bibliography", "Collection", Calibre IDs from author names
> - **Title Cleanup** - Strips Calibre-style numeric IDs from book titles

> **beta.69-70** - Reversed Structure & Path Fixes
> - **Removed False Positives** - Simplified reversed structure detection
> - **Author Prefix Fix** - "Author - Title" in book folder no longer duplicates author

> **beta.65-68** - Watch Folder & Bug Fixes
> - Watch folder retry loop fixed, encoding cleanup improved, SQLite errors fixed

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

### Multi-Source Metadata
```
1. Audnexus     - Audible's audiobook data
2. OpenLibrary  - 50M+ book database
3. Google Books - Wide coverage
4. Hardcover    - Modern/indie books
5. AI Fallback  - Gemini/OpenRouter when APIs fail
```

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
