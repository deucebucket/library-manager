# Configuration

All settings are configured through the web UI at **Settings**.

## Settings Tabs

### General Tab

| Setting | Default | Description |
|---------|---------|-------------|
| Library Paths | `[]` | Folders to scan (one per line) |
| Naming Format | `author/title` | Folder structure for renames |
| Series Grouping | `false` | Enable `Author/Series/# - Title` format |
| Auto-Fix | `false` | Automatically apply safe fixes |
| Protect Author Changes | `true` | Require approval for drastic author changes |
| Ebook Management | `false` | Organize ebooks alongside audiobooks |
| Metadata Embedding | `false` | Write tags into audio files on fix |
| Scan Interval | `6 hours` | How often to auto-scan |

### AI Setup Tab

| Setting | Description |
|---------|-------------|
| AI Provider | Gemini (recommended) or OpenRouter |
| API Key | Your API key from the provider |
| Model | Which AI model to use |

### Advanced Tab

- Danger Zone (reset database, clear history)
- Bug report generator
- Live logs

## Naming Formats

| Format | Example |
|--------|---------|
| `author/title` | `Brandon Sanderson/Mistborn/` |
| `author - title` | `Brandon Sanderson - Mistborn/` |

## Series Grouping

When enabled, books in a series get organized as:

```
Author/Series Name/1 - Book Title/
Author/Series Name/2 - Book Title/
```

Standalone books stay as `Author/Title/`.

## Metadata Embedding (Beta)

When enabled, the app will write verified metadata directly into audio file tags when fixes are applied.

| Setting | Default | Description |
|---------|---------|-------------|
| Enable Embedding | `false` | Write tags when fixes are applied |
| Overwrite Tags | `true` | Overwrite existing managed fields |
| Backup Tags | `true` | Save original tags before modifying |

### Supported Formats

- **MP3** - ID3v2 tags (TIT2, TALB, TPE1, etc.)
- **M4B/M4A/AAC** - MP4 atoms with iTunes freeform tags
- **FLAC/Ogg/Opus** - Vorbis comments
- **WMA** - ASF tags

### Tags Written

| Field | Standard Tag | Custom Tag |
|-------|-------------|------------|
| Title | ALBUM (book title) | - |
| Author | ARTIST, ALBUMARTIST | - |
| Year | DATE/TDRC | - |
| Series | - | SERIES |
| Series # | - | SERIESNUMBER |
| Narrator | - | NARRATOR |
| Edition | - | EDITION |
| Variant | - | VARIANT |

### Backup Files

When "Backup Tags" is enabled, original tags are saved to `.library-manager.tags.json` in each book folder before any modifications. This allows manual recovery if needed.

## Rate Limits

| Provider | Free Tier |
|----------|-----------|
| Gemini | 14,400 calls/day |
| OpenRouter | Varies by model |

The app defaults to 2000 calls/hour to stay well under limits.

## Config Files

Settings are stored in:
- `config.json` - General settings
- `secrets.json` - API keys (gitignored)
- `library.db` - Database

For Docker, these are stored in the `/data` volume.
