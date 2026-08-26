#!/usr/bin/env python3
"""Browser/backend E2E for personal-key removal and shared Skaldleita access."""

import hashlib
import hmac
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parent.parent
SCREENSHOT_PATH = Path(os.environ.get(
    "TEST_SCREENSHOT_PATH",
    ROOT / "docs" / "images" / "skaldleita-shared-access-settings.png",
))
CHROMIUM_EXECUTABLE = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
PERSONAL_KEY = "bdb_personal_e2e_key"
PUBLIC_KEY = "lm-public-2024_85TbJ2lbrXGm38tBgliPAcAexLA_AeWxyqvHPbwRIrA"
VERSION = "0.9.0-beta.168"


def reserve_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for_app(base_url):
    for _ in range(60):
        try:
            if requests.get(f"{base_url}/api/stats", timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Library Manager did not become ready at {base_url}")


def assert_signed(headers, expected_key):
    assert headers.get("User-Agent") == f"LibraryManager/{VERSION}"
    assert headers.get("X-API-Key") == expected_key
    timestamp = headers.get("X-LM-Timestamp")
    nonce = headers.get("X-LM-Nonce")
    signature = headers.get("X-LM-Signature")
    assert timestamp and nonce and signature
    secret = hashlib.sha256(f"skaldleita-lm-2024:{VERSION}".encode()).hexdigest()[:32]
    message = f"{timestamp}:{VERSION}:{nonce}"
    expected = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()[:32]
    assert hmac.compare_digest(signature, expected)


class StubState:
    def __init__(self):
        self.validate_headers = []
        self.register_headers = []
        self.register_bodies = []
        self.search_headers = []
        self.search_modes = ["success", "rate", "success", "deny"]


def make_stub_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            return

        def send_json(self, status, payload, headers=None):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            headers = dict(self.headers.items())
            if path == "/api/validate-key":
                state.validate_headers.append(headers)
                self.send_json(200, {"valid": True, "rate_limit": 300})
                return
            if path == "/search":
                state.search_headers.append(headers)
                mode = state.search_modes.pop(0) if state.search_modes else "success"
                if mode == "rate":
                    self.send_json(429, {"detail": "slow down"}, {"Retry-After": "37"})
                elif mode == "deny":
                    self.send_json(403, {"detail": {
                        "code": "client_blocked",
                        "message": "Access denied.",
                    }})
                else:
                    self.send_json(200, [{
                        "id": 101,
                        "name": "Project Hail Mary",
                        "title": "Project Hail Mary",
                        "author_name": "Andy Weir",
                        "year_published": 2021,
                        "completeness": 100,
                        "missing_fields": [],
                    }])
                return
            self.send_json(404, {"detail": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length) if length else b"{}"
            if path == "/api/register-instance":
                state.register_headers.append(dict(self.headers.items()))
                state.register_bodies.append(json.loads(raw_body))
                self.send_json(400, {"detail": "isolated E2E registration stub"})
                return
            self.send_json(404, {"detail": "not found"})

    return Handler


def seed_queue(db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO books (path, current_author, current_title, status) "
            "VALUES (?, ?, ?, 'pending')",
            ("/isolated/E2E Unknown Book", "Unknown", "E2E Unknown Book"),
        )
        conn.execute(
            "INSERT INTO queue (book_id, priority, reason) VALUES (?, 5, ?)",
            (cursor.lastrowid, "E2E manual search"),
        )


def run_browser(base_url, state):
    SCREENSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    launch_options = {"headless": True}
    if CHROMIUM_EXECUTABLE:
        launch_options["executable_path"] = CHROMIUM_EXECUTABLE

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**launch_options)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})

        page.goto(f"{base_url}/settings", wait_until="networkidle")
        page.locator('button[data-bs-target="#tab-engine"]').click()
        validate_button = page.get_by_title("Validate saved personal key")
        assert validate_button.is_visible()
        validate_button.click()
        page.locator("#skaldleita-key-status", has_text="Valid (300/hr limit)").wait_for()
        assert len(state.validate_headers) == 1
        assert_signed(state.validate_headers[0], PERSONAL_KEY)

        dialogs = []

        def accept_dialog(dialog):
            dialogs.append(dialog.message)
            dialog.accept()

        page.on("dialog", accept_dialog)
        with page.expect_navigation(wait_until="networkidle"):
            page.locator("button[onclick=\"clearApiKey('bookdb_api_key')\"]").click()
        assert dialogs == [
            "Remove this personal Skaldleita key? No-signup shared access will remain available."
        ]

        page.locator('button[data-bs-target="#tab-engine"]').click()
        assert page.get_by_title("Validate saved personal key").count() == 0
        assert page.locator("#skaldleita-register-card").is_visible()
        assert page.get_by_text(
            "The shared key works without signup at 300 requests/hour per source IP.",
            exact=False,
        ).is_visible()
        page.locator("#instance-id").evaluate(
            "element => { element.textContent = 'SKALD-TEST00'; }"
        )
        key_section = page.locator("label", has_text="Skaldleita API Key").locator("xpath=..")
        key_section.scroll_into_view_if_needed()
        page.wait_for_timeout(600)
        key_section.screenshot(path=str(SCREENSHOT_PATH))

        email = page.locator("#skaldleita-email")
        email.fill("invalid")
        page.get_by_role("button", name="Request API Key").click()
        page.locator("#skaldleita-register-result", has_text="Please enter a valid email").wait_for()
        assert state.register_headers == []

        email.fill("e2e@example.invalid")
        page.get_by_role("button", name="Request API Key").click()
        page.locator(
            "#skaldleita-register-result",
            has_text="Registration failed (HTTP 400)",
        ).wait_for()
        assert len(state.register_headers) == 1
        assert_signed(state.register_headers[0], None)
        assert state.register_bodies[0]["app_version"] == VERSION

        audio_toggle = page.locator("#use_skaldleita_for_audio")
        initial_audio = audio_toggle.is_checked()
        audio_toggle.click()
        with page.expect_navigation(wait_until="networkidle"):
            page.get_by_role("button", name="Save Settings").click()
        page.locator('button[data-bs-target="#tab-engine"]').click()
        assert page.locator("#use_skaldleita_for_audio").is_checked() is (not initial_audio)

        page.goto(f"{base_url}/queue", wait_until="networkidle")
        page.get_by_title("Edit", exact=True).click()
        modal = page.locator("#editModal")
        modal.wait_for(state="visible")
        modal.locator("#search-query").fill("Project Hail Mary")
        search_button = modal.locator('button[onclick="searchBookDB()"]')

        search_button.click()
        modal.get_by_text("Project Hail Mary", exact=True).wait_for()
        modal.locator(".search-result-item").click()
        modal.locator("#selected-match", has_text="Project Hail Mary").wait_for()
        assert_signed(state.search_headers[0], PUBLIC_KEY)

        search_button.click()
        modal.get_by_text("Rate limited. Try again in 37s.", exact=True).wait_for()
        assert len(state.search_headers) == 2

        search_button.click()
        modal.get_by_text("Project Hail Mary", exact=True).wait_for()
        assert len(state.search_headers) == 3

        search_button.click()
        modal.get_by_text("Skaldleita denied this client", exact=True).wait_for()
        assert len(state.search_headers) == 4
        search_button.click()
        page.wait_for_timeout(500)
        assert len(state.search_headers) == 4

        browser.close()


def main():
    with tempfile.TemporaryDirectory(prefix="library-manager-skald-e2e-") as temp:
        root = Path(temp)
        data_dir = root / "data"
        library_dir = root / "library"
        data_dir.mkdir()
        library_dir.mkdir()
        (data_dir / "library.db").touch()

        state = StubState()
        stub = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            make_stub_handler(state),
        )
        stub_thread = threading.Thread(target=stub.serve_forever, daemon=True)
        stub_thread.start()
        stub_url = f"http://127.0.0.1:{stub.server_address[1]}"

        config = {
            "library_paths": [str(library_dir)],
            "bookdb_url": stub_url,
            "enabled": False,
            "setup_completed": True,
            "use_skaldleita_for_audio": True,
        }
        (data_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
        (data_dir / "secrets.json").write_text(
            json.dumps({"bookdb_api_key": PERSONAL_KEY}),
            encoding="utf-8",
        )
        (data_dir / "user_groups.json").write_text("{}", encoding="utf-8")

        port = reserve_local_port()
        base_url = f"http://127.0.0.1:{port}"
        environment = os.environ.copy()
        environment.update({
            "DATA_DIR": str(data_dir),
            "PORT": str(port),
            "PYTHONUNBUFFERED": "1",
        })
        log_path = root / "app-process.log"
        with log_path.open("w+", encoding="utf-8") as app_log:
            process = subprocess.Popen(
                [sys.executable, "app.py"],
                cwd=ROOT,
                env=environment,
                stdout=app_log,
                stderr=subprocess.STDOUT,
            )
            try:
                wait_for_app(base_url)
                seed_queue(data_dir / "library.db")
                run_browser(base_url, state)
            except Exception:
                app_log.flush()
                app_log.seek(0)
                print("\nIsolated app output:\n" + app_log.read()[-12000:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                stub.shutdown()
                stub.server_close()
                stub_thread.join(timeout=5)

        saved_secrets = json.loads((data_dir / "secrets.json").read_text(encoding="utf-8"))
        assert "bookdb_api_key" not in saved_secrets
        assert len(state.search_headers) == 4
        print("[PASS] clicked Settings validation/removal/registration/audio controls")
        print("[PASS] clicked manual search success, selection, 429 recovery, and terminal 403")
        print(f"[PASS] screenshot written to {SCREENSHOT_PATH}")


if __name__ == "__main__":
    main()
