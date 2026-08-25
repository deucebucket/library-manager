# Video organization API

`video_organization_service.py` is the phase-4 mutation adapter. It is separate from the
general Flask application and from `read_only_music_service.py`; enabling video moves does
not add organization authority to the Music inspection surface.

The server always binds `127.0.0.1` (default port `5759`) and exposes only:

- `GET /health`
- `POST /api/v1/video/move`

`PUT`, `PATCH`, and `DELETE` return 405. Request bodies are capped at 64 KiB and logs are
suppressed because even opaque catalogue coordinates do not belong in access logs.

## Configuration

All credentials remain in environment/secrets, never request bodies or receipts:

| Variable | Meaning |
| --- | --- |
| `LM_VIDEO_DB` | Absolute path for the mode-0600 receipt database |
| `LM_VIDEO_CAPABILITY_SECRET` | At least 32 bytes, shared only with the approving orchestrator |
| `LM_VIDEO_ROOTS_JSON` | Object mapping opaque root IDs to configured Radarr root paths |
| `LM_VIDEO_LIBRARIES_JSON` | Object mapping opaque library IDs to Jellyfin library item IDs |
| `LM_RADARR_URL` / `LM_RADARR_API_KEY` | Radarr adapter connection |
| `LM_JELLYFIN_URL` / `LM_JELLYFIN_API_KEY` | Jellyfin visibility and idle-observation connection |
| `PORT` | Optional loopback port; default `5759` |

At least two absolute roots, one library, and a 32-byte capability secret are required.

## Signed request

The caller sends `Content-Type: application/json` plus
`X-LM-Video-Capability: sha256=<hex>`. The signature is HMAC-SHA256 over the exact
canonical JSON object (UTF-8, keys sorted, compact separators). The body has no title or
path:

```json
{
  "schema": "video.move.request.v1",
  "approval_ref": "opaque-one-use-approval",
  "subject_id": "7",
  "provider": "tmdb",
  "provider_id": "123",
  "media_kind": "movie",
  "runtime_minutes": 90,
  "source_root_id": "movies",
  "destination_root_id": "documentaries",
  "destination_library_id": "documentaries"
}
```

The configured IDs are resolved in memory. A valid signature does not bypass policy:
movie-length/movie scope, exact configured roots, an authoritative empty Jellyfin session
read, and the one-use approval receipt must all pass before Radarr is called.

Radarr receives `moveFiles=true` in both query and body. Commit requires exact root/path
readback and one exact TMDb/IMDb identity inside the configured Jellyfin library. A failed
postcondition requests the source root through Radarr and verifies that readback. An
unverified rollback returns `manual_recovery` and is never called committed.

Receipts retain only SHA-256 digests, booleans, bounded state/error codes, and timestamps.
Titles, paths, provider IDs, library IDs, credentials, and exception prose are absent.
