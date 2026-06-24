"""Video (TV / movie) library management — additive media-type for library-manager.

Mirrors the audiobook pipeline (scan -> identify -> verify -> rename/approve ->
undo) for video, but identifies via TMDb/TVDB + AI title-parsing and formats to
the picky folder/file conventions Plex/Jellyfin/Emby/Kodi expect.
"""
