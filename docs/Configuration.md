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

## Multi-Language Naming

For libraries with books in more than one language. All of this is opt-in - by default LM uses "native" naming (a German book gets its German title, no tags, no extra folders).

| Setting | Default | Description |
|---------|---------|-------------|
| Naming mode | `native` | `native` = book's own language, `preferred` = always your library language, `tagged` = preferred + language tag |
| Add Language Tags | `false` | Master switch for language tags/folders |
| Tag Format | `bracket_full` | `(Polish)`, `[pl]`, `Polish`, `_pl`, or `🇵🇱` (emoji flag) |
| Tag Position | `after_title` | `after_title`, `before_title`, `subfolder` (`Author/Language/Title`), or `top_folder` (`Language/Author/Title`) |
| Code Format | `iso639-1` | Two-letter (`pl`) or three-letter ISO 639-2 bibliographic codes (`pol`, `ger`) for code-based tags and `{lang_code}` |

Notes:

- `top_folder` partitions the **whole** library by language, including books in your preferred language: `German/Brandon Sanderson/Mistborn/01 - The Final Empire`. It wraps every naming format, including custom templates.
- Tags are only added to books whose language differs from your preferred language (except `top_folder`, which always applies).
- Custom templates also support `{language}` (full name), `{lang_code}`, and `{lang_flag}` (flag emoji).
- Three-letter codes from Skaldleita (`eng`, `ger`) are normalized to two-letter internally; the Code Format setting only controls output.

### Series Language Overrides

Settings > Library > **Series Language Overrides**: lock a series to a language (e.g. the German edition of Mistborn → `de`). Books in a locked series skip audio/title language detection entirely. Set to `auto` (or remove the override) to detect per book again. Only applies when the book's series is known. API: `GET/POST/DELETE /api/series-language-overrides`.

### Multi-Language Books

For books containing multiple languages (language courses, dual narration): edit the book in the Library page and pick multiple languages (first = primary). Folder tags show all of them: `Title (German, English)`, `[de,en]`, `🇩🇪 🇬🇧`. The primary language drives the Audible region hint; the full list is kept on the profile for downstream tools. API: `GET/POST /api/books/<id>/languages`.

### Multi-Language Onboarding Prompt

When a scan finds books in multiple languages (or in a language other than your preferred one), a dismissible dashboard banner offers one-click setup: keep native naming, tag non-preferred titles, or sort into language folders. The choice applies to future processing only - existing books are not moved until you reprocess them through the queue.

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
| Audible Region URL | - | WWWAUDIOFILE |
| Edition | - | EDITION |
| Variant | - | VARIANT |

`WWWAUDIOFILE` holds the per-book Audible product URL (e.g. `https://www.audible.com/pd/B001180MB2`), built from the audio-detected language so downstream tools (beets-audible, etc.) pick the correct marketplace per book instead of relying on one global region setting.

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
