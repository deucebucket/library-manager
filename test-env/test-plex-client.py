"""Test plex_client reconciliation against a synthetic Plex-schema DB + temp disk.

Builds a minimal library.db with the tables/columns PlexDB reads, lays down real
temp files, and verifies STRANDED (disk-not-Plex) / PHANTOM (Plex-file-gone).

Run: python test-env/test-plex-client.py
"""
import sys, os, tempfile, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plex_client import PlexDB

fails = []
def check(label, got, want):
    if got != want:
        fails.append(f"{label}: got {got!r} want {want!r}")

tmp = tempfile.mkdtemp(prefix="lm-plex-test-")
root = os.path.join(tmp, "TV Shows")
os.makedirs(os.path.join(root, "My Show"), exist_ok=True)
# disk files: e01 indexed, e02 indexed, e03 STRANDED (not in Plex)
paths = {n: os.path.join(root, "My Show", f"My Show - S01E0{n}.mkv") for n in (1, 2, 3)}
for p in paths.values():
    open(p, "w").close()
phantom_path = os.path.join(root, "My Show", "My Show - S01E09.mkv")  # in Plex, not on disk

dbp = os.path.join(tmp, "library.db")
con = sqlite3.connect(dbp)
con.executescript("""
CREATE TABLE library_sections (id INTEGER PRIMARY KEY, name TEXT, section_type INTEGER);
CREATE TABLE section_locations (id INTEGER PRIMARY KEY, library_section_id INTEGER, root_path TEXT);
CREATE TABLE metadata_items (id INTEGER PRIMARY KEY, library_section_id INTEGER);
CREATE TABLE media_items (id INTEGER PRIMARY KEY, metadata_item_id INTEGER);
CREATE TABLE media_parts (id INTEGER PRIMARY KEY, media_item_id INTEGER, file TEXT);
""")
con.execute("INSERT INTO library_sections VALUES (2,'TV Shows',2)")
con.execute("INSERT INTO section_locations VALUES (1,2,?)", (root,))
# index e01, e02, and the phantom — but NOT e03
indexed = [paths[1], paths[2], phantom_path]
for i, f in enumerate(indexed, start=1):
    con.execute("INSERT INTO metadata_items VALUES (?,2)", (i,))
    con.execute("INSERT INTO media_items VALUES (?,?)", (i, i))
    con.execute("INSERT INTO media_parts VALUES (?,?,?)", (i, i, f))
con.commit(); con.close()

db = PlexDB(dbp)
check("available", db.available(), True)
secs = db.sections()
check("sections", len(secs), 1)
check("section.type", secs[0].type, "show")
r = db.reconcile(secs[0])
check("indexed", r.indexed, 3)
check("on_disk", r.on_disk, 3)
check("stranded.count", len(r.stranded), 1)
check("stranded.is_e03", os.path.normcase(paths[3]) in r.stranded, True)
check("phantom.count", len(r.phantom), 1)

# cleanup
import shutil
shutil.rmtree(tmp, ignore_errors=True)

if fails:
    print(f"FAILED {len(fails)}:")
    for f in fails: print("  -", f)
    sys.exit(1)
print("All plex-client tests passed.")
