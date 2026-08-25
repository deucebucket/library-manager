"""The modular video UI can reconcile enabled Plex sections without host paths."""
import json
import hashlib
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask  # noqa: E402
from library_manager.video import api as video_api  # noqa: E402


def fixture(root: Path) -> Path:
    movies = root / "Movies"
    television = root / "TV"
    movies.mkdir()
    television.mkdir()
    indexed = movies / "Indexed (2026).mkv"
    stranded = movies / "Stranded (2026).mkv"
    indexed.touch()
    stranded.touch()
    phantom = movies / "Phantom (2026).mkv"
    db_path = root / "plex.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
CREATE TABLE library_sections (id INTEGER PRIMARY KEY, name TEXT, section_type INTEGER);
CREATE TABLE section_locations (id INTEGER PRIMARY KEY, library_section_id INTEGER, root_path TEXT);
CREATE TABLE metadata_items (id INTEGER PRIMARY KEY, library_section_id INTEGER);
CREATE TABLE media_items (id INTEGER PRIMARY KEY, metadata_item_id INTEGER);
CREATE TABLE media_parts (id INTEGER PRIMARY KEY, media_item_id INTEGER, file TEXT);
""")
    conn.execute("INSERT INTO library_sections VALUES (1, 'Movies', 1)")
    conn.execute("INSERT INTO library_sections VALUES (2, 'Television', 2)")
    conn.execute("INSERT INTO section_locations VALUES (1, 1, ?)", (str(movies),))
    conn.execute("INSERT INTO section_locations VALUES (2, 2, ?)", (str(television),))
    for item_id, path in enumerate((indexed, phantom), 1):
        conn.execute("INSERT INTO metadata_items VALUES (?, 1)", (item_id,))
        conn.execute("INSERT INTO media_items VALUES (?, ?)", (item_id, item_id))
        conn.execute("INSERT INTO media_parts VALUES (?, ?, ?)", (item_id, item_id, str(path)))
    conn.commit()
    conn.close()
    return db_path


with tempfile.TemporaryDirectory(prefix="lm-video-api-") as directory:
    root = Path(directory)
    db_path = fixture(root)
    database_digest = hashlib.sha256(db_path.read_bytes()).hexdigest()
    config = {"movie_manager_enabled": True, "tv_manager_enabled": False,
              "plex_db_path": str(db_path), "ui_theme": "default"}
    video_api.load_config = lambda: dict(config)
    app = Flask(__name__, template_folder=str(Path(__file__).parents[1] / "templates"))
    app.jinja_env.globals["_"] = lambda value: value
    app.register_blueprint(video_api.video_api_bp)
    client = app.test_client()

    status = client.get("/api/plex/status")
    assert status.status_code == 200
    assert status.get_json()["database_available"] is True

    sections = client.get("/api/plex/sections")
    assert sections.status_code == 200
    assert sections.get_json()["sections"] == [
        {"id": 1, "name": "Movies", "type": "movie", "root_count": 1}]

    result = client.get("/api/plex/reconcile/1")
    assert result.status_code == 200
    body = result.get_json()
    assert body["counts"] == {"indexed": 2, "on_disk": 2, "stranded": 1, "phantom": 1}
    assert body["findings"] == {
        "stranded": ["Stranded (2026).mkv"],
        "phantom": ["Phantom (2026).mkv"],
    }
    encoded = json.dumps(body)
    assert directory not in encoded

    assert client.get("/api/plex/reconcile/2").status_code == 409
    assert client.post("/api/plex/reconcile/1").status_code == 405
    page = client.get("/video")
    assert page.status_code == 200
    assert b"Movies" in page.data and b"Review only" in page.data
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == database_digest

    config["movie_manager_enabled"] = False
    assert client.get("/api/plex/sections").status_code == 409

print("All video API tests passed.")
