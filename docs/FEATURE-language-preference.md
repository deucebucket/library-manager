# Multilingual Metadata and Naming

This feature is implemented. The current behavior is configured in Settings and the defaults are defined in `library_manager/config.py`.

## Metadata language

- `preferred_language` controls metadata lookup and Audible region hints.
- `preserve_original_titles` keeps localized titles when enabled.
- `detect_language_from_audio` optionally uses configured audio analysis to detect spoken language.
- `strict_language_matching` helps avoid cross-language matches.
- Series language overrides can lock a known series to a language; `auto` restores per-book detection.

## Folder naming

`multilang_naming_mode` supports:

- `native` — use the book's detected language.
- `preferred` — use the configured preferred language.
- `tagged` — use preferred naming with a language tag.

Language tags can use full names, ISO 639-1/639-2 codes, bracketed forms, or flags. Positions include before/after title, language subfolder, and top-level language folder. Custom templates support `{language}`, `{lang_code}`, and `{lang_flag}`.

For multi-language books, the Library edit flow stores an ordered language list (primary first). Tags can display all known languages.

## Onboarding

When a scan detects a multilingual library, the Dashboard can offer native naming, tags for non-preferred titles, or top-level language folders. The choice affects future processing; existing books are not moved until reprocessed.

## Verification sources

The user-facing settings are in `templates/settings.html`; language normalization and path construction are in `app.py` and `library_manager/utils/path_safety.py`. Provider-specific language behavior is subject to the provider's available metadata. This page intentionally omits speculative translation logic and unverified provider quality claims.
