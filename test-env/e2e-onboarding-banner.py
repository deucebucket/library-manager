"""Browser E2E for issue #284 onboarding banner (runs inside lm-e2e-ct).

Precondition: the DB has a book whose detected language differs from the
preferred language (set up by the caller). Verifies the banner renders,
then clicks "Sort into language folders" and verifies config was applied.
"""
import json
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:5757"
SHOTS = Path("/opt/library-manager/test-screenshots")
SHOTS.mkdir(exist_ok=True)
failures = []


def check(ok, name):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    if not ok:
        failures.append(name)


def get_config_key(key):
    with open("/opt/library-manager/config.json") as f:
        return json.load(f).get(key)


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_context(viewport={"width": 1280, "height": 900}).new_page()

    page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=30000)
    time.sleep(1.5)  # let the summary fetch + banner render

    banner = page.locator("#multilang-banner-row")
    check(banner.count() == 1, "banner element exists in DOM")
    visible = banner.is_visible()
    check(visible, "banner visible when multi && !dismissed")
    if visible:
        text = banner.inner_text()
        print(f"[INFO] banner text: {text!r}")
        check("German" in text, "banner names the detected language")
        page.screenshot(path=str(SHOTS / "e2e-onboarding-banner.png"), full_page=True)

        btn = page.locator("button[data-multilang-choice='top_folder']")
        check(btn.count() == 1, "'Sort into language folders' button present")
        if btn.count():
            btn.click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            check(get_config_key("language_tag_position") == "top_folder",
                  "config language_tag_position=top_folder after apply")
            check(get_config_key("language_tag_enabled") is True,
                  "config language_tag_enabled=true after apply")
            check(get_config_key("multilang_onboarding_dismissed") is True,
                  "onboarding marked dismissed after apply")
            time.sleep(1.5)
            check(not banner.is_visible(), "banner gone after apply + reload")
            page.screenshot(path=str(SHOTS / "e2e-onboarding-applied.png"), full_page=True)

    browser.close()

print("\nOnboarding E2E", "FAILED" if failures else "passed")
raise SystemExit(1 if failures else 0)
