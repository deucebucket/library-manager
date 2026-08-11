# Music inspection API

`POST /api/v1/music/inspect` is a read-only identity and filesystem evidence
surface. It never searches a provider, changes tags, renames a file, or organizes a
library. Callers supply stable MusicBrainz and Lidarr foreign identifiers; titles
alone are not identity.

The configured `music_library_roots` entries have an opaque `id` and an absolute
local `path`. Requests name the opaque root and a normalized relative album path.
The scanner rejects traversal, symlinks, mount/device changes, file replacement,
unsupported schemas, and scans over 500 supported audio files. Responses expose
root-relative paths and opaque subjects, never the configured host root.

The result state is `complete` only when every supported file has readable tags,
stable artist/album-or-release-group/recording identity, consistent album identity,
truthful duration/codec/sample-rate/channel evidence, and matches supplied identity.
Unknown codecs are not inferred from extensions; missing or conflicting evidence is `partial`.
Invalid requests and inaccessible or unsafe filesystem state are `unavailable`.
These states are observations only and grant no authority to move or edit files.

Request schema:

```json
{
  "schema": "music.inspection.request.v1",
  "root_id": "music-primary",
  "relative_path": "Artist/Album",
  "identity": {
    "musicbrainz_artist_id": "...",
    "musicbrainz_album_id": "...",
    "musicbrainz_release_group_id": "...",
    "lidarr_foreign_artist_id": "...",
    "lidarr_foreign_album_id": "..."
  }
}
```

All five identity keys are required by the schema; unknown values are represented
by an empty string. At least one stable identifier must be present.
