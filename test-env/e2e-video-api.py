"""Drive the modular Video dashboard in Chromium and refresh documentation shots."""
import os
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
from werkzeug.serving import make_server  # noqa: E402
from library_manager.video import api as video_api  # noqa: E402


ROOT = Path(__file__).parents[1]
SHOTS = ROOT / "docs" / "images"


def build_fixture(root: Path) -> Path:
    movies = root / "Movies"
    television = root / "TV Shows"
    movies.mkdir()
    television.mkdir()
    indexed = movies / "Sample Film (2026).mkv"
    stranded = movies / "New Arrival (2026).mkv"
    indexed.touch()
    stranded.touch()
    phantom = movies / "Removed File (2025).mkv"
    db_path = root / "plex.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
CREATE TABLE library_sections (id INTEGER PRIMARY KEY, name TEXT, section_type INTEGER);
CREATE TABLE section_locations (id INTEGER PRIMARY KEY, library_section_id INTEGER, root_path TEXT);
CREATE TABLE metadata_items (id INTEGER PRIMARY KEY, library_section_id INTEGER);
CREATE TABLE media_items (id INTEGER PRIMARY KEY, metadata_item_id INTEGER);
CREATE TABLE media_parts (id INTEGER PRIMARY KEY, media_item_id INTEGER, file TEXT);
""")
    conn.execute("INSERT INTO library_sections VALUES (1, 'Test Movies', 1)")
    conn.execute("INSERT INTO library_sections VALUES (2, 'Test Television', 2)")
    conn.execute("INSERT INTO section_locations VALUES (1, 1, ?)", (str(movies),))
    conn.execute("INSERT INTO section_locations VALUES (2, 2, ?)", (str(television),))
    for item_id, path in enumerate((indexed, phantom), 1):
        conn.execute("INSERT INTO metadata_items VALUES (?, 1)", (item_id,))
        conn.execute("INSERT INTO media_items VALUES (?, ?)", (item_id, item_id))
        conn.execute("INSERT INTO media_parts VALUES (?, ?, ?)", (item_id, item_id, str(path)))
    conn.commit()
    conn.close()
    return db_path


with tempfile.TemporaryDirectory(prefix="lm-video-e2e-") as directory:
    fixture_root = Path(directory)
    db_path = build_fixture(fixture_root)
    config = {"movie_manager_enabled": True, "tv_manager_enabled": True,
              "plex_db_path": str(db_path), "ui_theme": "default"}
    video_api.load_config = lambda: dict(config)
    app = Flask(__name__, template_folder=str(ROOT / "templates"),
                static_folder=str(ROOT / "static"))
    app.jinja_env.globals["_"] = lambda value: value
    app.register_blueprint(video_api.video_api_bp)

    @app.get("/api/live_status")
    def live_status():
        return jsonify({"state": "idle", "queue": {"count": 0, "pending_fixes": 0},
                        "last_activity": "Synthetic browser fixture",
                        "last_activity_time_ago": "now"})

    @app.get("/api/version")
    def version():
        return jsonify({"version": "0.9.0-beta.165"})

    @app.get("/api/check_update")
    def check_update():
        return jsonify({"update_available": False})
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    SHOTS.mkdir(parents=True, exist_ok=True)
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.goto(base + "/video", wait_until="networkidle")
            page.get_by_role("button", name="Test Movies").click()
            page.get_by_text("Plex indexed 2 files; disk contains 2.").wait_for()
            assert page.get_by_text("New Arrival (2026).mkv").is_visible()
            assert page.get_by_text("Removed File (2025).mkv").is_visible()
            assert directory not in page.locator("body").inner_text()
            page.screenshot(path=str(SHOTS / "video-reconciliation-web.png"), full_page=True)

            mobile = browser.new_page(viewport={"width": 390, "height": 844}, is_mobile=True)
            mobile.goto(base + "/video", wait_until="networkidle")
            mobile.get_by_role("button", name="Test Movies").click()
            mobile.get_by_text("Plex indexed 2 files; disk contains 2.").wait_for()
            assert mobile.get_by_text("New Arrival (2026).mkv").is_visible()
            assert mobile.get_by_text("Removed File (2025).mkv").is_visible()
            mobile.screenshot(path=str(SHOTS / "video-reconciliation-mobile.png"), full_page=True)
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)

print("[PASS] desktop/mobile Video reconciliation browser flow and screenshots")
