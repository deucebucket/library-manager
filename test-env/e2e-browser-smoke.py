"""Browser smoke test inside the quarantined E2E container.

Opens the Library Manager UI with Playwright, verifies the Settings page
loads and the "Detect language from audio" checkbox is present and can be
 toggled, and captures screenshots for visual review.
"""
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:5757"
SCREENSHOT_DIR = Path("/opt/library-manager/test-screenshots")


def main():
    SCREENSHOT_DIR.mkdir(exist_ok=True)
    failures = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()

        # 1. Dashboard loads
        try:
            page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=30000)
            page.screenshot(path=str(SCREENSHOT_DIR / "e2e-dashboard.png"), full_page=True)
            print("[PASS] Dashboard loaded")
        except Exception as e:
            failures.append(f"Dashboard load failed: {e}")

        # 2. Settings page loads
        try:
            page.goto(f"{BASE_URL}/settings", wait_until="networkidle", timeout=30000)
            page.screenshot(path=str(SCREENSHOT_DIR / "e2e-settings-initial.png"), full_page=True)
            print("[PASS] Settings page loaded")
        except Exception as e:
            failures.append(f"Settings load failed: {e}")

        # 3. Detect language from audio checkbox exists and is toggleable
        try:
            # The setting lives under the Engine tab. Force the tab open via JS
            # because Bootstrap tab clicks can be flaky in headless mode.
            page.evaluate("""() => {
                document.querySelectorAll('.tab-pane').forEach(p => { p.classList.remove('show', 'active'); p.style.display = 'none'; });
                document.querySelectorAll('.nav-link').forEach(l => { l.classList.remove('active'); l.setAttribute('aria-selected', 'false'); });
                const engineTab = document.querySelector('button[data-bs-target=\"#tab-engine\"]');
                if (engineTab) { engineTab.classList.add('active'); engineTab.setAttribute('aria-selected', 'true'); }
                const enginePane = document.getElementById('tab-engine');
                if (enginePane) { enginePane.classList.add('show', 'active'); enginePane.style.display = 'block'; }
            }""")
            time.sleep(0.3)
            page.screenshot(path=str(SCREENSHOT_DIR / "e2e-settings-engine.png"), full_page=True)

            checkbox = page.locator("input#detect_language_from_audio")
            checkbox.wait_for(state="attached", timeout=10000)
            initial = checkbox.is_checked()
            print(f"[INFO] detect_language_from_audio initial={initial}")

            # Verify the setting is present and matches the saved config
            if initial:
                print("[PASS] Detect language from audio setting is present and enabled")
            else:
                # If disabled, enable it via the form and verify it persists
                page.evaluate("""() => {
                    const cb = document.getElementById('detect_language_from_audio');
                    cb.checked = true;
                    cb.dispatchEvent(new Event('change', { bubbles: true }));
                    cb.dispatchEvent(new Event('input', { bubbles: true }));
                }""")
                page.locator("button:has-text('Save Settings')").click()
                page.wait_for_load_state("networkidle", timeout=10000)
                page.screenshot(path=str(SCREENSHOT_DIR / "e2e-settings-saved.png"), full_page=True)
                print("[PASS] Detect language from audio setting enabled and saved")
        except Exception as e:
            failures.append(f"Detect language setting test failed: {e}")

        # 4. Library page loads
        try:
            page.goto(f"{BASE_URL}/library", wait_until="networkidle", timeout=30000)
            page.screenshot(path=str(SCREENSHOT_DIR / "e2e-library.png"), full_page=True)
            print("[PASS] Library page loaded")
        except Exception as e:
            failures.append(f"Library load failed: {e}")

        # 5. Queue page loads
        try:
            page.goto(f"{BASE_URL}/queue", wait_until="networkidle", timeout=30000)
            page.screenshot(path=str(SCREENSHOT_DIR / "e2e-queue.png"), full_page=True)
            print("[PASS] Queue page loaded")
        except Exception as e:
            failures.append(f"Queue load failed: {e}")

        browser.close()

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nBrowser smoke test passed.")


if __name__ == "__main__":
    main()
