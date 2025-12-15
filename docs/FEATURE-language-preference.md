# Feature: Preferred Metadata Language

**Issue:** #17
**Status:** Planned
**Requested by:** grapefruit89

## Problem

German audiobook "Der Bücherdrache" by Walter Moers gets renamed to English title "The Book Dragon" because metadata providers default to English.

## Solution

Comprehensive language handling with multiple options:

### 1. Library Language Setting
- **Purpose:** "I want my entire library in German" (or French, Spanish, etc.)
- Dropdown with ALL ISO 639-1 languages (not just common ones)
- Metadata providers filter to preferred language first
- Fallback to any language if no match

### 2. Preserve Original Titles (Checkbox)
- **Purpose:** "Don't translate localized titles to English"
- When enabled: If audiobook folder is already in a foreign language, keep it
- Detects source language from folder name/filename
- Prevents "Der Bücherdrache" → "The Book Dragon" translation

### 3. Audio Analysis for Language Detection
- Use existing audio snippet feature to detect spoken language
- If book has duplicates (different language editions), verify via audio
- AI prompt: "What language is this audiobook being narrated in?"
- Helps match correct language edition automatically

## Implementation Plan

### Config Changes
```python
'preferred_language': 'en',           # ISO 639-1 code (any language)
'preserve_original_titles': True,     # Don't translate foreign titles
'detect_language_from_audio': False,  # Use audio analysis
```

### Settings UI

**Settings > General > Language:**
```
┌─────────────────────────────────────────────────────────┐
│ Library Language                                        │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ German (de)                                       ▼ │ │
│ └─────────────────────────────────────────────────────┘ │
│                                                         │
│ ☑ Preserve original titles                              │
│   Keep foreign language titles instead of translating   │
│                                                         │
│ ☐ Detect language from audio                            │
│   Use audio analysis to verify book language            │
└─────────────────────────────────────────────────────────┘
```

### Language Dropdown Options

Support ALL ISO 639-1 languages (not a limited list):
- German (de)
- French (fr)
- Spanish (es)
- Italian (it)
- Portuguese (pt)
- Dutch (nl)
- Swedish (sv)
- Norwegian (no)
- Danish (da)
- Finnish (fi)
- Polish (pl)
- Russian (ru)
- Japanese (ja)
- Chinese (zh)
- Korean (ko)
- Arabic (ar)
- Hebrew (he)
- Hindi (hi)
- ... (full ISO 639-1 list - 184 languages)

### Metadata Provider Updates

**Google Books API:**
```python
params['langRestrict'] = config.get('preferred_language', 'en')
```

**Audnexus/Audible:**
- Map language codes to Audible marketplace endpoints:
  - `de` → audible.de
  - `fr` → audible.fr
  - `es` → audible.es
  - etc.

**BookDB:**
- Add language filter: `/search?q=...&lang=de`
- Already has `language` field in books table

**OpenLibrary:**
- Supports `language` parameter

### Preserve Original Titles Logic

```python
def should_preserve_title(original_title, suggested_title, config):
    """Check if we should keep the original foreign title."""
    if not config.get('preserve_original_titles', True):
        return False

    # Detect language of original title
    original_lang = detect_language(original_title)  # Use langdetect library

    # If original is in a different language than English suggestion, preserve it
    if original_lang != 'en' and suggested_title != original_title:
        return True

    return False
```

### Audio Language Detection

Extend existing audio analysis feature:

```python
def detect_audio_language(audio_file):
    """Use Gemini to detect spoken language from audio sample."""
    audio_sample = extract_audio_sample(audio_file, duration=30)

    prompt = """Listen to this audiobook sample and identify:
    1. What language is the narrator speaking?
    2. Confidence level (high/medium/low)

    Return JSON: {"language": "de", "language_name": "German", "confidence": "high"}
    """

    return call_gemini_with_audio(audio_sample, prompt)
```

Use cases:
- Verify book is actually in expected language
- Handle duplicates: same book, different language editions
- Flag mismatches: "Folder says German but audio is English"

## Files to Modify

- `app.py`: Add config, modify search functions, add language detection
- `templates/settings.html`: Add language settings section
- `static/settings.js`: Handle new settings
- `requirements.txt`: Add `langdetect` for title language detection
- `CHANGELOG.md`: Document feature
- `README.md`: Document new settings

## Testing

- [ ] Test with German library (Walter Moers example)
- [ ] Test with mixed-language library
- [ ] Test "preserve original titles" toggle
- [ ] Test audio language detection
- [ ] Test fallback when no localized results
- [ ] Test all metadata providers with language filter

## Notes

- The `langdetect` library is lightweight and doesn't require API calls
- Audio detection uses existing Gemini integration (no new dependencies)
- Language codes follow ISO 639-1 standard
- Default behavior unchanged for existing users (`en`, preserve=true)
