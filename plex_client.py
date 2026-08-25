"""Plex client — read Plex's SQLite catalog and reconcile it against disk.

Per the design, Plex is a VERIFICATION source, not the truth: we read the exact
file path of every item Plex has indexed and diff it against what's actually on
disk, so we can surface:
  - STRANDED : file on disk that Plex never indexed (bad name/nesting/mis-match)
  - PHANTOM  : Plex has a file entry whose file is gone

This mirrors abs_client.py (the Audiobookshelf client) for the video side.

Reads the live database in SQLite read-only/query-only mode.  The reader stays aware of
the live WAL so it sees one consistent current snapshot without copying or writing it.
"""
from __future__ import annotations
import os
import sqlite3
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional

VIDEO = (".mkv", ".mp4", ".avi", ".m4v", ".ts", ".wmv", ".mpg", ".mpeg", ".flv")

_DEFAULT_DB = os.path.expanduser(
    r"~\AppData\Local\Plex Media Server\Plug-in Support\Databases"
    r"\com.plexapp.plugins.library.db"
)

# Plex section_type: 1=movie, 2=show, 8=artist/music, 13=photo
SECTION_TYPES = {1: "movie", 2: "show", 8: "music", 13: "photo"}


def _norm(p: str) -> str:
    return os.path.normcase(os.path.normpath(p))


@dataclass
class PlexSection:
    id: int
    name: str
    type: str
    locations: list = field(default_factory=list)


@dataclass
class Reconciliation:
    section: PlexSection
    indexed: int = 0
    on_disk: int = 0
    stranded: list = field(default_factory=list)   # on disk, not in Plex
    phantom: list = field(default_factory=list)     # in Plex, file gone


class PlexDB:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or _DEFAULT_DB

    def _connect(self):
        uri = "file:" + urllib.parse.quote(self.db_path.replace("\\", "/")) + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.execute("PRAGMA query_only=ON")
        return connection

    def available(self) -> bool:
        if not os.path.exists(self.db_path):
            return False
        try:
            with self._connect() as c:
                c.execute("SELECT 1 FROM library_sections LIMIT 1")
            return True
        except Exception:
            return False

    def sections(self) -> list[PlexSection]:
        out = []
        with self._connect() as c:
            rows = c.execute("SELECT id,name,section_type FROM library_sections "
                             "WHERE section_type IN (1,2,8,13)").fetchall()
            for sid, name, stype in rows:
                locs = [r[0] for r in c.execute(
                    "SELECT root_path FROM section_locations WHERE library_section_id=?", (sid,))]
                out.append(PlexSection(sid, name or "", SECTION_TYPES.get(stype, str(stype)), locs))
        return out

    def indexed_files(self, section_id: int) -> set:
        """Normalized paths of every file Plex indexed in this section."""
        q = """
            SELECT mp.file
            FROM media_parts mp
            JOIN media_items mi   ON mp.media_item_id = mi.id
            JOIN metadata_items m ON mi.metadata_item_id = m.id
            WHERE m.library_section_id = ? AND mp.file IS NOT NULL
        """
        files = set()
        with self._connect() as c:
            for (f,) in c.execute(q, (section_id,)):
                if f and f.lower().endswith(VIDEO):
                    files.add(_norm(f))
        return files

    @staticmethod
    def disk_files(roots: list) -> set:
        files = set()
        for root in roots:
            if not os.path.isdir(root):
                continue
            for dp, _, fs in os.walk(root):
                for f in fs:
                    if f.lower().endswith(VIDEO):
                        files.add(_norm(os.path.join(dp, f)))
        return files

    def reconcile(self, section: PlexSection, extra_roots: Optional[list] = None) -> Reconciliation:
        roots = list(section.locations) + list(extra_roots or [])
        plex = self.indexed_files(section.id)
        disk = self.disk_files(roots)
        r = Reconciliation(section=section, indexed=len(plex), on_disk=len(disk))
        r.stranded = sorted(disk - plex)
        r.phantom = sorted(plex - disk)
        return r
