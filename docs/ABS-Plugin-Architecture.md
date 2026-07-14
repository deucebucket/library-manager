# Audiobookshelf Plugin Architecture for Library Manager

## Overview

Library Manager as a metadata provider plugin for Audiobookshelf (ABS). When ABS needs to identify or enrich audiobook metadata, it queries Library Manager, which leverages the full Skaldleita pipeline: GPU Whisper transcription, 50M+ book database matching, multi-source API lookups, and AI-powered consensus verification.

**Current state:** LM has an `abs_client.py` that pulls data FROM ABS (listening progress, library items, user management). This document covers the reverse direction — making LM available TO ABS as a metadata agent.

---

## Data Flow

```
Audiobookshelf                Library Manager              Skaldleita (BookDB)
     │                              │                              │
     │  1. "Identify this book"     │                              │
     │  ─────────────────────────>  │                              │
     │  (title, author, audio clip) │                              │
     │                              │  2. Audio → Whisper queue    │
     │                              │  ─────────────────────────>  │
     │                              │                              │
     │                              │  3. Metadata match (50M DB)  │
     │                              │  <─────────────────────────  │
     │                              │                              │
     │                              │  4. API enrichment           │
     │                              │  (Audnexus, Google Books,    │
     │                              │   OpenLibrary, Hardcover)    │
     │                              │                              │
     │                              │  5. AI consensus verify      │
     │                              │  (Gemini / OpenRouter)       │
     │                              │                              │
     │  6. Enriched metadata        │                              │
     │  <─────────────────────────  │                              │
     │  (author, title, narrator,   │                              │
     │   series, year, confidence)  │                              │
```

---

## Architecture Options

### Option A: LM as ABS Metadata Provider (Recommended)

ABS has a metadata provider plugin system. LM registers as a provider that ABS queries during its "Match" and "Quick Match" flows.

**How it works:**
- ABS sends search queries to LM's API
- LM runs its full pipeline (BookDB + multi-source + AI verification)
- LM returns structured metadata in ABS's expected format
- ABS users see LM results alongside other providers (Google Books, Audible, etc.)

**LM endpoints needed:**
```
GET  /api/abs/search?query=<title>&author=<author>
     → Returns ABS-formatted search results

GET  /api/abs/match?title=<title>&author=<author>&narrator=<narrator>
     → Returns single best match with confidence

POST /api/abs/identify-audio
     → Accepts audio clip, returns identification via Skaldleita Whisper

GET  /api/abs/cover?title=<title>&author=<author>
     → Returns cover image URL if available
```

**Pros:** Native ABS integration, users see LM in their provider list
**Cons:** ABS metadata provider API has a specific contract we must match exactly

### Option B: LM as Standalone Enrichment Service

LM runs alongside ABS and periodically scans the ABS library for books with missing/low-quality metadata, then pushes corrections back via ABS API.

**How it works:**
- LM uses existing `abs_client.py` to read ABS library
- Identifies books with missing narrators, series info, etc.
- Runs its pipeline to find correct metadata
- Pushes updates back to ABS via API

**Pros:** Works without ABS plugin system, LM controls the schedule
**Cons:** Delayed updates, LM needs write access to ABS

### Option C: Hybrid (Both A + B)

Register as metadata provider AND run background enrichment. Provider handles new additions; background scan catches existing gaps.

---

## Recommended Approach: Option A (Metadata Provider)

### ABS Metadata Provider Contract

ABS expects providers to implement these operations:

```json
// Search request
GET /search?query=Brandon+Sanderson+Mistborn

// Search response
{
  "matches": [
    {
      "title": "The Final Empire",
      "subtitle": "Mistborn Book 1",
      "author": "Brandon Sanderson",
      "narrator": "Michael Kramer",
      "publisher": "Macmillan Audio",
      "publishedYear": "2006",
      "description": "...",
      "cover": "https://...",
      "isbn": "9780765350381",
      "asin": "B002V1O7UE",
      "series": [
        { "series": "Mistborn", "sequence": "1" }
      ],
      "language": "en",
      "duration": 95940
    }
  ]
}
```

### LM → ABS Field Mapping

| ABS Field | LM Source | Notes |
|-----------|-----------|-------|
| `title` | BookProfile.title | Highest confidence value |
| `author` | BookProfile.author | Consensus from all sources |
| `narrator` | BookProfile.narrator | Often from audio credits (L2) or Audnexus |
| `series[].series` | BookProfile.series | From BookDB or API lookups |
| `series[].sequence` | BookProfile.series_num | Position in series |
| `publishedYear` | BookProfile.year | From API lookups |
| `cover` | External API | Audnexus or Google Books cover URL |
| `isbn` | API lookups | Google Books or OpenLibrary |
| `asin` | Audnexus | Audible ASIN if available |
| `duration` | Audio file analysis | From ffprobe or ABS library item |
| `language` | Gemini detection | Audio language detection (L2) |
| `description` | API enrichment | Google Books or OpenLibrary |

### Confidence Translation

LM uses 0-100 confidence scores. ABS doesn't have a native confidence field, but we can:
1. Only return results above a configurable threshold (default: 60)
2. Sort results by confidence (best first)
3. Include confidence in a custom field for display

### New LM Files

```
library_manager/abs_provider.py    # ABS metadata provider Blueprint
  - /api/abs/search               # Search endpoint
  - /api/abs/match                # Best-match endpoint
  - /api/abs/identify             # Audio identification endpoint
  - /api/abs/covers/<isbn>        # Cover proxy
```

### Authentication

Options:
1. **Shared secret** — LM generates an API key, user enters it in ABS custom provider config
2. **Token exchange** — ABS sends its API token, LM validates via ABS `/api/me` endpoint
3. **None (local only)** — If both run on same machine, trust localhost

Recommend: Shared secret (simple, works across networks).

### Configuration (LM Settings Page)

Add to the Integrations tab:

```
ABS Metadata Provider
├── Enable as ABS metadata source: [toggle]
├── Provider API Key: [auto-generated, copyable]
├── Minimum confidence threshold: [slider 0-100, default 60]
├── Include audio identification: [toggle, default on]
└── Provider URL for ABS config: http://<lm-host>:5757/api/abs
```

---

## Security Considerations

### What This Opens
- A new API surface (`/api/abs/*`) accessible from the network
- Audio clip uploads from ABS to LM
- LM's metadata forwarded to ABS (already LM's core function)

### Mitigations
- API key authentication on all `/api/abs/*` endpoints
- Rate limiting (reuse existing rate limiter infrastructure)
- Audio clip size limits (same as existing Skaldleita submission limits)
- Input validation on all query parameters (sanitize before pipeline)
- SSRF protection already in place for plugin endpoints (issue #236)
- No new filesystem access — ABS provider only reads/returns metadata

### What NOT to Expose
- Direct Skaldleita API passthrough (LM should be the intermediary, not a proxy)
- BookDB API keys or signing credentials
- Internal BookProfile objects (serialize to ABS format only)
- Direct database access

---

## Implementation Phases

### Phase 1: Search Provider
- New Blueprint: `abs_provider.py`
- `/api/abs/search` endpoint using existing `search_bookdb()` + API lookups
- API key auth
- Settings UI toggle
- Basic test with ABS custom provider

### Phase 2: Audio Identification
- `/api/abs/identify` endpoint
- Accept audio clips from ABS
- Route through Skaldleita Whisper pipeline
- Return structured results

### Phase 3: Background Enrichment (Option B hybrid)
- Scan ABS library via existing `abs_client.py`
- Queue items with missing metadata
- Push enrichments back to ABS
- Configurable schedule (daily/weekly)

### Phase 4: Cover Art + Descriptions
- `/api/abs/covers/<isbn>` proxy endpoint
- Cache covers locally
- Pull descriptions from Google Books / OpenLibrary enrichment

---

## ABS Custom Provider Setup (User-Facing)

In ABS Settings → Providers → Custom:
```
Name: Library Manager
Base URL: http://<lm-ip>:5757/api/abs
API Key: <paste from LM settings>
```

Then in ABS, when matching a book:
1. Click "Match" on a library item
2. Select "Library Manager" as provider
3. See results from LM's multi-source pipeline
4. One-click apply metadata

---

## Related Issues
- #257 — This feature
- #252-#256 — Integration fixes that improve the pipeline feeding this provider
- abs_client.py — Existing read-only ABS integration (complements this)
