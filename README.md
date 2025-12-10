# Library Manager

<div align="center">

**Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification**

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://python.org)
[![Flask](https://img.shields.io/badge/flask-2.0+-green.svg)](https://flask.palletsprojects.com)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

*Automatically fix messy audiobook folders using real book databases + AI intelligence*

</div>

---

## The Problem

Audiobook libraries get messy. Downloads from various sources leave you with:

```
Your Library (Before):
├── Shards of Earth/Adrian Tchaikovsky/        # Author/Title swapped!
├── Boyett/The Hollow Man/                     # Missing first name
├── Christopher Golden, Amber Benson/Slayers/  # Wrong author entirely
├── The Expanse 2019/Leviathan Wakes/          # Year in wrong place
├── Tchaikovsky, Adrian/Service Model/         # LastName, FirstName format
├── [bitsearch.to] Dean Koontz - Watchers/     # Junk in filename
└── Unknown/Mistborn Book 1/                   # No author at all
```

Manually researching and fixing hundreds of these? *No thanks.*

---

## The Solution

Library Manager combines **4 real book databases** with **AI verification** to intelligently fix your library:

```
Your Library (After):
├── Adrian Tchaikovsky/Shards of Earth/        # ✓ Correct!
├── Steven Boyett/The Hollow Man/              # ✓ Full name found
├── Christopher Golden/Slayers/                # ✓ Fixed author
├── James S.A. Corey/Leviathan Wakes/          # ✓ Proper author
├── Adrian Tchaikovsky/Service Model/          # ✓ Name normalized
├── Dean Koontz/Watchers/                      # ✓ Cleaned up
└── Brandon Sanderson/The Final Empire/        # ✓ Found the real book!
```

With **Series Grouping** enabled (Audiobookshelf-compatible):
```
├── James S.A. Corey/The Expanse/1 - Leviathan Wakes/
├── James S.A. Corey/The Expanse/2 - Caliban's War/
└── Brandon Sanderson/Mistborn/1 - The Final Empire {Kramer}/
```

---

## How It Works

### Multi-Source Metadata Pipeline

Library Manager doesn't just guess—it **verifies against real book databases**:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        METADATA PIPELINE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Your Messy Folder ──► Search APIs ──► Verify ──► Rename          │
│                              │                                      │
│                              ▼                                      │
│                    ┌─────────────────┐                             │
│                    │  1. Audnexus    │  Audible's audiobook data   │
│                    │  2. OpenLibrary │  Massive book database      │
│                    │  3. Google Books│  Wide coverage              │
│                    │  4. Hardcover   │  Modern/indie books         │
│                    │  5. AI Fallback │  When APIs can't find it    │
│                    └─────────────────┘                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Smart Verification System

When metadata is found, the system verifies it makes sense:

| Scenario | What Happens |
|----------|--------------|
| **Minor change** (e.g., "Boyett" → "Steven Boyett") | Auto-applied (same person, fuller name) |
| **Drastic change** (e.g., "Golden" → "Sussman") | **Requires manual approval** to prevent mistakes |
| **Author/Title swap** (e.g., "Title/Author") | Detected and fixed automatically |
| **Uncertain match** | Held for review, never auto-applied |
| **Garbage match** (e.g., "Chapter 19" → "College Accounting") | **Automatically rejected** - won't even suggest it |

### Garbage Match Filtering

APIs sometimes return completely unrelated books. Library Manager uses **Jaccard similarity** to detect and reject these:

```
✗ "Chapter 19" → "College Accounting, Chapters 1-9"  (only "chapter" matches)
✗ "Death Genesis" → "The Darkborn AfterLife Genesis" (only "genesis" matches)
✗ "Mr. Murder" → "Frankenstein"                       (no word overlap at all)
```

Matches with less than 30% word overlap are automatically rejected.

### Safety First

- **Drastic author changes ALWAYS require approval** - even in auto-fix mode
- **Garbage matches filtered** - Won't suggest completely unrelated books
- **Undo any fix** - Every rename can be reverted with one click
- **History tracking** - See every change that was made
- **Reject bad suggestions** - Delete wrong AI guesses without applying them
- **Dismiss errors** - Clear stale error entries when source files no longer exist

---

## Features

### Web Dashboard
Beautiful dark-themed UI showing:
- Library statistics (total books, queue size, fixes)
- Quick action buttons (Scan, Process, Apply Pending)
- Worker status indicator (running/stopped)
- Recent activity feed
- 7-day stats graph

### Processing Queue
- View all books flagged for review
- Live processing log
- Progress tracking for bulk operations
- Remove items from queue (mark as OK)

### Fix History
- Complete log of all changes
- Before/After comparison
- **Undo button** - Revert any fix instantly
- **Dismiss errors** - Remove stale entries when files don't exist
- Status badges (Applied, Pending, Undone, Error)

### Pending Approvals
- Dedicated page for fixes needing manual review
- **"Review" badge** highlights drastic author changes
- **Apply** (✓) or **Reject** (✗) each suggestion
- Bulk apply all pending fixes

### Orphan File Detection
Find and organize loose audio files sitting directly in author folders:
- **Automatic detection** - Reads ID3 metadata (album tag) to identify books
- **Visual management** - See all orphans with detected titles
- **One-click organize** - Creates proper book folders and moves files
- **Edit before organizing** - Change detected title if needed
- **Batch organize** - Process all orphans at once

### Smart Narrator Preservation
Keeps different audiobook versions separate:
- Detects narrator names from folder patterns like `(Kafer)`, `(Vance)`
- Creates separate folders: `The Hellbound Heart (Kafer)`, `The Hellbound Heart (Barker)`
- **Won't merge different versions** - Protects your narrator-specific copies
- Distinguishes narrators from junk: `(Horror)` = genre (stripped), `(Kafer)` = narrator (kept)

### Series Grouping (Audiobookshelf-Compatible)

Enable series grouping to organize books in a format compatible with Audiobookshelf:

| Setting | Structure | Example |
|---------|-----------|---------|
| **Off** | `Author/Title` | `Brandon Sanderson/The Final Empire/` |
| **On** | `Author/Series/# - Title` | `Brandon Sanderson/Mistborn/1 - The Final Empire/` |
| **On + Narrator** | `Author/Series/# - Title {Narrator}` | `Brandon Sanderson/Mistborn/1 - The Final Empire {Kramer}/` |

Series detection works from:
- **API metadata** - Google Books, Audnexus, etc. provide series info
- **Title patterns** - Automatically parses titles like:
  - `The Firefly Series, Book 8: Coup de Grâce` → Series: Firefly, #8
  - `Mistborn Book 1: The Final Empire` → Series: Mistborn, #1
  - `The Expanse #3 - Abaddon's Gate` → Series: The Expanse, #3

Standalone books (not part of a series) remain in `Author/Title` format.

### Customizable Naming Formats

Choose the folder structure that matches your player:

| Format | Example | Compatible With |
|--------|---------|-----------------|
| `Author/Title` | `Brandon Sanderson/Mistborn/` | Audiobookshelf, Plex, Jellyfin |
| `Author - Title` | `Brandon Sanderson - Mistborn/` | Booksonic, basic players |

### AI Providers

**Google Gemini** (Recommended)
- 14,400 free API calls/day
- Fast and accurate
- Get key at [aistudio.google.com](https://aistudio.google.com)

**OpenRouter**
- Access to multiple models
- Free tier available
- Supports Claude, GPT-4, Llama, etc.

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/deucebucket/library-manager.git
cd library-manager
pip install -r requirements.txt
```

### 2. Run

```bash
python app.py
```

Open **http://localhost:5060** in your browser.

### 3. Configure (via Web UI)

1. Go to **Settings**
2. Add your **library path** (e.g., `/mnt/audiobooks`)
3. Choose **AI Provider** and add API key
4. Enable **Series Grouping** if you use Audiobookshelf
5. **Save Settings**
6. Go to **Dashboard** → **Scan Library**

That's it! Watch as your messy library gets organized.

---

## Configuration

### Settings Tabs

| Tab | Contents |
|-----|----------|
| **General** | Library paths, naming format, series grouping, auto-fix toggle, scan interval |
| **AI Setup** | Provider selection, API keys, model choice |
| **Advanced** | Danger zone, bug reports, live logs |

### Key Options

| Option | Default | Description |
|--------|---------|-------------|
| `library_paths` | `[]` | Folders to scan (one per line) |
| `naming_format` | `author/title` | How to structure renamed folders |
| `series_grouping` | `false` | Enable Audiobookshelf-style series folders |
| `auto_fix` | `false` | Apply fixes automatically vs manual approval |
| `protect_author_changes` | `true` | Extra verification for drastic changes |
| `scan_interval_hours` | `6` | Auto-scan frequency |
| `batch_size` | `3` | Books per API batch |
| `max_requests_per_hour` | `400` | Rate limiting |

---

## How Detection Works

Books get flagged for review when they have:

| Issue | Example | Detection |
|-------|---------|-----------|
| Year in author | `The Expanse 2019/Leviathan` | Regex: 1950-2030 |
| Swapped fields | `Mistborn/Brandon Sanderson` | Title looks like name |
| LastName, First | `Tchaikovsky, Adrian/Book` | Comma detection |
| Missing author | `Unknown/The Book Title` | Common placeholder |
| Junk in name | `[site.to] Author - Book` | Brackets, URLs |
| Title words in author | `The Brandon Sanderson/Book` | Articles in wrong place |

---

## API Pipeline Details

### Search Order

1. **Audnexus** - Audible's audiobook database (best for audiobooks)
2. **OpenLibrary** - Internet Archive's massive book DB
3. **Google Books** - Wide coverage, good metadata + series info
4. **Hardcover** - Modern and indie titles
5. **AI Fallback** - When no API finds a match

### Verification Flow

```
API finds "Paul Sussman / The Lost Army of Cambyses"
for your folder "Christopher Golden / The Lost Army"

                    ↓

Is title similar? NO (< 30% word overlap) → GARBAGE MATCH REJECTED

                    ↓ (if title is similar)

Is author completely different? YES → DRASTIC CHANGE DETECTED

                    ↓

Run verification: Search ALL APIs, have AI vote on best match

                    ↓

AI uncertain or says WRONG? → Goes to PENDING (requires your approval)
AI confident it's correct? → Still goes to PENDING (drastic = always review)
```

---

## Production Deployment

### Systemd Service

```bash
sudo tee /etc/systemd/system/library-manager.service << 'EOF'
[Unit]
Description=Library Manager - Audiobook Organizer
After=network.target

[Service]
Type=simple
User=yourusername
WorkingDirectory=/path/to/library-manager
ExecStart=/usr/bin/python3 app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now library-manager
```

### Nginx Reverse Proxy

```nginx
server {
    listen 443 ssl http2;
    server_name library.yourdomain.com;

    ssl_certificate /etc/letsencrypt/live/library.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/library.yourdomain.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:5060;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/scan` | POST | Trigger library scan |
| `/api/deep_rescan` | POST | Re-verify ALL books with fresh metadata |
| `/api/process` | POST | Process queue (`{all: true}` or `{limit: N}`) |
| `/api/queue` | GET | Get current queue items |
| `/api/stats` | GET | Dashboard statistics |
| `/api/apply_fix/{id}` | POST | Apply a pending fix |
| `/api/reject_fix/{id}` | POST | Reject/delete a bad suggestion |
| `/api/undo/{id}` | POST | Revert an applied fix |
| `/api/dismiss_error/{id}` | POST | Remove an error entry from history |
| `/api/apply_all_pending` | POST | Apply all pending fixes at once |
| `/api/worker/start` | POST | Start background worker |
| `/api/worker/stop` | POST | Stop background worker |

---

## Troubleshooting

### "Wrong author detected"
1. Go to **Pending** page
2. Find the incorrect suggestion
3. Click **✗ Reject** to delete it
4. The book will be marked as "verified OK"

### "Want to undo a fix"
1. Go to **History**
2. Find the change you want to revert
3. Click the **↩ Undo** button
4. Folder will be renamed back to original

### "Error entry for file that doesn't exist"
1. Go to **History**
2. Find the error entry
3. Click the **🗑 Dismiss** button
4. Entry will be removed from history

### "Deep rescan everything"
1. Go to **Dashboard**
2. Click **Deep Re-scan (Verify All)**
3. This queues ALL books for fresh metadata verification
4. Useful after updating API keys or fixing bugs

### "Series not detected"
- Make sure **Series Grouping** is enabled in Settings → General → Behavior
- The title pattern must be recognizable (e.g., "Series Name, Book N: Title")
- If API doesn't have series info, try a deep rescan

---

## Contributing

Ideas for future development:
- [ ] Ollama/local LLM support
- [ ] Audiobookshelf API integration
- [ ] Cover art fetching
- [ ] Metadata embedding in files
- [ ] Multi-user support
- [ ] Docker container

Pull requests welcome!

---

## License

MIT License - do whatever you want with it.

---

<div align="center">

**Built with [Claude Code](https://claude.ai/code)**

*Making messy audiobook libraries beautiful since 2024*

</div>
