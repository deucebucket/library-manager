# Feature: Preferred Metadata Language

**Issue:** #17
**Status:** Planned
**Requested by:** grapefruit89

## Problem

German audiobook "Der Bücherdrache" by Walter Moers gets renamed to English title "The Book Dragon" because metadata providers default to English.

## Solution

Add a "Preferred Metadata Language" setting (ISO 639-1 codes: `de`, `fr`, `es`, `en`, etc.)

## Implementation Plan

### 1. Config Changes
- Add `preferred_language` to DEFAULT_CONFIG (default: `en`)
- Add dropdown in Settings > General with common languages

### 2. Metadata Provider Updates

**Google Books API:**
- Add `langRestrict` parameter to API calls
- Example: `https://www.googleapis.com/books/v1/volumes?q=...&langRestrict=de`

**Audnexus/Audible:**
- Audible has regional endpoints (audible.de, audible.fr, etc.)
- May need to map language codes to Audible marketplace codes

**BookDB:**
- Already stores `language` field in books table
- Could add language filter to search: `/search?q=...&lang=de`

**OpenLibrary:**
- Supports `language` parameter in search

### 3. Matching Logic
- When language preference is set, prioritize results in that language
- Fall back to any language if no matches found
- Consider: fuzzy matching should compare against localized title

### 4. UI Changes
- Add language dropdown to Settings > General
- Show detected language in queue items
- Manual match should filter by language preference

## Files to Modify

- `app.py`: Add config, modify search functions
- `templates/settings.html`: Add language dropdown
- `static/settings.js`: Handle new setting
- `CHANGELOG.md`: Document feature
- `README.md`: Document new setting

## Testing

- Test with German library (Walter Moers example)
- Test fallback when no localized results
- Test manual match with language filter
