"""End-to-end test for the language naming pack, inside the quarantined container.

Covers:
- #278 language_tag_position=top_folder (Language/Author/Title)
- #282 language_tag_format=emoji_flag + preview
- #279 language_code_format select presence
- #281 series language overrides (settings UI + API + pipeline hard override)
- #280 per-book languages list (API + library edit modal)

Flow: verify settings UI, change language tag settings, save via the real UI,
add a "Dune" -> German series override, process a messy Dune copy end-to-end,
verify the fix lands under a German/ top folder, exercise the book languages
API + edit modal, then clean up everything it touched.
"""
import json
import shutil
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "http://127.0.0.1:5757"
APP_DIR = Path("/opt/library-manager")
LIBRARY_PATH = Path("/opt/test-library")
SOURCE_BOOK = LIBRARY_PATH / "Frank Herbert" / "Dune (Scott Brick) (ENG)" / "audiobook.mp3"
TEST_FOLDER = LIBRARY_PATH / "[ messy ] Herbert, Frank - Doon2 (audio)"
TEST_AUDIO = TEST_FOLDER / "audiobook.mp3"
NESTED_PARENT = LIBRARY_PATH / "Frank, Herbert" / "Dune Series 01"
NESTED_FOLDER = NESTED_PARENT / "[ messy ] Herbert, Frank - Doon2 (audio)"
CANONICAL_FOLDER = LIBRARY_PATH / "Frank Herbert" / "Dune (Scott Brick) (ENG)"
DB_PATH = APP_DIR / "library.db"
CONFIG_FILES = [APP_DIR / "config.json", APP_DIR / "data" / "config.json"]
SCREENSHOT_DIR = APP_DIR / "test-screenshots"
MAX_WAIT_SECONDS = 300

FAILURES = []


def check(ok, msg):
    """Record a PASS/FAIL line."""
    if ok:
        print(f"[PASS] {msg}")
    else:
        print(f"[FAIL] {msg}")
        FAILURES.append(msg)


def info(msg):
    print(f"[INFO] {msg}")


def api_request(method, path, payload=None, timeout=30):
    """Small JSON API helper. Returns (status, parsed_json_or_None)."""
    data = json.dumps(payload).encode() if payload is not None else (b"" if method in ("POST", "DELETE") else None)
    req = urllib.request.Request(f"{BASE_URL}{path}", method=method, data=data)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except (ValueError, json.JSONDecodeError):
            return e.code, None
    except Exception as e:
        info(f"API {method} {path} failed: {e}")
        return None, None


def force_open_library_tab(page):
    """Force the Settings Library tab pane visible (Bootstrap tabs are flaky headless)."""
    page.evaluate("""() => {
        document.querySelectorAll('.tab-pane').forEach(p => { p.classList.remove('show', 'active'); p.style.display = 'none'; });
        document.querySelectorAll('.nav-link').forEach(l => { l.classList.remove('active'); l.setAttribute('aria-selected', 'false'); });
        const tab = document.querySelector('button[data-bs-target="#tab-library"]');
        if (tab) { tab.classList.add('active'); tab.setAttribute('aria-selected', 'true'); }
        const pane = document.getElementById('tab-library');
        if (pane) { pane.classList.add('show', 'active'); pane.style.display = 'block'; }
    }""")
    time.sleep(0.3)


def db_query(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def db_execute(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def delete_book_records(path_like):
    """Remove books/history/queue rows referencing a path prefix."""
    rows = db_query("SELECT id FROM books WHERE path LIKE ?", (path_like,))
    for row in rows:
        bid = row['id']
        db_execute("DELETE FROM history WHERE book_id = ?", (bid,))
        db_execute("DELETE FROM queue WHERE book_id = ?", (bid,))
        db_execute("DELETE FROM books WHERE id = ?", (bid,))
    db_execute("DELETE FROM history WHERE old_path LIKE ? OR new_path LIKE ?", (path_like, path_like))
    return [r['id'] for r in rows]


def wait_for_book_state(test_folder, status_targets, timeout=MAX_WAIT_SECONDS):
    """Poll the DB until the messy test book reaches one of the target statuses."""
    started = time.time()
    while time.time() - started < timeout:
        rows = db_query(
            "SELECT id, current_author, current_title, status, verification_layer, profile "
            "FROM books WHERE path LIKE ?", (str(test_folder) + '%',))
        if rows and rows[0]['status'] in status_targets:
            return dict(rows[0])
        time.sleep(5)
    return None


# ---------------------------------------------------------------------------
# Phase 1-4: browser-based settings UI tests
# ---------------------------------------------------------------------------

def phase_settings_ui():
    info("Phase 1: Settings UI - language naming controls")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 1000})
        page = context.new_page()
        page.on("dialog", lambda d: d.accept())

        page.goto(f"{BASE_URL}/settings", wait_until="networkidle", timeout=30000)
        force_open_library_tab(page)
        page.screenshot(path=str(SCREENSHOT_DIR / "e2e-lang-01-settings-library-tab.png"), full_page=True)

        # New options/selects exist
        fmt_options = page.eval_on_selector_all(
            "#language_tag_format option", "els => els.map(e => e.value)")
        check('emoji_flag' in fmt_options, f"#language_tag_format has emoji_flag option (got {fmt_options})")
        pos_options = page.eval_on_selector_all(
            "#language_tag_position option", "els => els.map(e => e.value)")
        check('top_folder' in pos_options, f"#language_tag_position has top_folder option (got {pos_options})")
        check(page.locator("#language_code_format").count() == 1, "#language_code_format select exists")
        check(page.locator("#series-language-overrides-list").count() == 1
              and page.locator("#series-override-name").count() == 1,
              "Series Language Overrides card present")

        # Phase 2: enable tags, set emoji_flag + top_folder, check preview
        info("Phase 2: enable Add Language Tags, set emoji_flag + top_folder")
        page.evaluate("""() => {
            const cb = document.getElementById('language_tag_enabled');
            if (!cb.checked) { cb.checked = true; }
            cb.dispatchEvent(new Event('change', { bubbles: true }));
        }""")
        time.sleep(0.3)

        # First verify the flag renders in the preview with after_title position
        page.evaluate("""() => {
            const fmt = document.getElementById('language_tag_format');
            fmt.value = 'emoji_flag';
            fmt.dispatchEvent(new Event('change', { bubbles: true }));
        }""")
        time.sleep(0.3)
        preview_flag = page.locator("#language-tag-preview").text_content()
        check('🇷🇺' in preview_flag, f"emoji_flag preview contains flag: {preview_flag!r}")

        # Now switch to top_folder and check the Language/Author/Title preview
        page.evaluate("""() => {
            const pos = document.getElementById('language_tag_position');
            pos.value = 'top_folder';
            pos.dispatchEvent(new Event('change', { bubbles: true }));
        }""")
        time.sleep(0.3)
        preview_top = page.locator("#language-tag-preview").text_content()
        check('Russian/Author/Title/' in preview_top,
              f"top_folder preview is Language/Author/Title: {preview_top!r}")
        if '🇷🇺' not in preview_top:
            info("Note: top_folder preview does not render the flag emoji "
                 "(top folder uses full language name regardless of tag format)")
        page.screenshot(path=str(SCREENSHOT_DIR / "e2e-lang-02-tag-preview.png"), full_page=True)

        # Phase 3: save via the real UI save button, verify persistence
        info("Phase 3: save settings via UI")
        # Keep background processing enabled - saving the form with it unchecked
        # would disable the pipeline needed for phase 5.
        page.evaluate("""() => {
            const en = document.getElementById('enabled');
            if (en && !en.checked) { en.checked = true; }
        }""")
        mtimes_before = {str(f): f.stat().st_mtime for f in CONFIG_FILES if f.exists()}

        with page.expect_navigation(wait_until="networkidle", timeout=30000):
            page.locator("button:has-text('Save Settings')").click()

        mtimes_after = {str(f): f.stat().st_mtime for f in CONFIG_FILES if f.exists()}
        changed = [p for p in mtimes_after if mtimes_after[p] != mtimes_before.get(p)]
        info(f"Config files written by UI save: {changed or 'NONE'}")
        check(len(changed) > 0, "UI save wrote at least one config file")
        for f in CONFIG_FILES:
            info(f"  {f}: mtime {mtimes_before.get(str(f))} -> {mtimes_after.get(str(f))}")

        # Reload and verify the selects kept the new values
        page.goto(f"{BASE_URL}/settings", wait_until="networkidle", timeout=30000)
        force_open_library_tab(page)
        saved_fmt = page.locator("#language_tag_format").input_value()
        saved_pos = page.locator("#language_tag_position").input_value()
        saved_enabled = page.locator("#language_tag_enabled").is_checked()
        check(saved_fmt == 'emoji_flag', f"language_tag_format persisted as emoji_flag (got {saved_fmt})")
        check(saved_pos == 'top_folder', f"language_tag_position persisted as top_folder (got {saved_pos})")
        check(saved_enabled, "language_tag_enabled persisted as checked")
        page.screenshot(path=str(SCREENSHOT_DIR / "e2e-lang-03-settings-saved.png"), full_page=True)

        # Phase 4: add series override "Dune" -> German via the UI
        info("Phase 4: add series override Dune -> de via UI")
        page.locator("#series-override-name").fill("Dune")
        page.locator("#series-override-language").select_option("de")
        page.locator("button:has-text('Add')").first.click()
        # Wait for the overrides list to re-render with the new entry
        try:
            page.wait_for_function(
                """() => document.getElementById('series-language-overrides-list')
                        .textContent.includes('Dune')""",
                timeout=10000)
            check(True, "Series override 'Dune' appears in the UI list")
        except Exception:
            check(False, "Series override 'Dune' did not appear in the UI list")
        page.screenshot(path=str(SCREENSHOT_DIR / "e2e-lang-04-series-override.png"), full_page=True)

        browser.close()

    # API verification of the override
    status, data = api_request("GET", "/api/series-language-overrides")
    overrides = (data or {}).get('overrides', [])
    match = [o for o in overrides
             if o.get('series_name', '').lower() == 'dune' and o.get('language_code') == 'de']
    check(bool(match), f"API lists Dune -> de override (got {overrides})")


# ---------------------------------------------------------------------------
# Phase 5a: deterministic check that the override mechanism reaches language
# resolution and path building (uses the app's own code with the live DB
# override set in phase 4, independent of fixture metadata containing a series)
# ---------------------------------------------------------------------------

def phase_override_mechanism():
    info("Phase 5a: verify override mechanism reaches language resolution + path building")
    from library_manager import database as app_db
    from library_manager.pipeline.layer_audio_id import _resolve_metadata_language
    from library_manager.utils.path_safety import build_new_path
    from library_manager.config import load_config

    # database module keeps its own global db path (normally set by the app at
    # startup); without this, get_series_language_override() silently misses.
    app_db.set_db_path(str(DB_PATH))

    config = load_config()
    check(config.get('language_tag_position') == 'top_folder',
          f"config language_tag_position is top_folder (got {config.get('language_tag_position')!r})")

    # Hard override: with Dune -> de set, resolution must return 'de' without detection
    resolved = _resolve_metadata_language(None, 'Dune', config, series_name='Dune')
    check(resolved == 'de',
          f"series override wins in _resolve_metadata_language (got {resolved!r}, expected 'de')")

    # The resolved language must produce a German/ top folder
    path_de = build_new_path(LIBRARY_PATH, 'Frank Herbert', 'Dune',
                             series='Dune', language_code='de', config=config)
    top_de = Path(path_de).relative_to(LIBRARY_PATH).parts[0] if path_de else None
    check(top_de == 'German',
          f"override language reaches path building: de -> German/ top folder (got {top_de!r})")

    # ISO 639-2 codes (what Skaldleita emits, e.g. 'eng') must normalize to 'en'
    path_eng = build_new_path(LIBRARY_PATH, 'Frank Herbert', 'Dune',
                              language_code='eng', config=config)
    top_eng = Path(path_eng).relative_to(LIBRARY_PATH).parts[0] if path_eng else None
    check(top_eng == 'English',
          f"PRODUCT BUG if failed: ISO 639-2 'eng' should map to 'English' top folder (got {top_eng!r})")


# ---------------------------------------------------------------------------
# Phase 5b: processing E2E with top_folder + series override
# ---------------------------------------------------------------------------

def clean_library_artifacts():
    """Remove leftovers from previous runs (messy folders + language trees)."""
    for folder in (TEST_FOLDER, NESTED_FOLDER):
        if folder.exists():
            shutil.rmtree(folder)
        delete_book_records(str(folder) + '%')
    if NESTED_PARENT.exists():
        delete_book_records(str(NESTED_PARENT) + '%')
        shutil.rmtree(NESTED_PARENT)
    for lang_dir in ("German", "ENG", "English"):
        lang_root = LIBRARY_PATH / lang_dir
        if lang_root.exists():
            delete_book_records(str(lang_root) + '%')
            shutil.rmtree(lang_root)
            info(f"Removed leftover {lang_dir}/ tree from a previous run")
    # Nested parents from previous runs
    for parent in (NESTED_PARENT, NESTED_PARENT.parent):
        delete_book_records(str(parent) + '%')
        if parent.exists() and parent != LIBRARY_PATH:
            shutil.rmtree(parent)


def run_processing_scenario(test_folder, label, expect_detected, expect_top,
                            override_needs_series=False):
    """Copy the Dune audio into test_folder, process it, apply the fix, and
    verify the resolved language and the language top folder.

    override_needs_series: when True, the expect_* checks are tied to a series
    override firing; if the identification metadata carries no series (fixture
    limitation), those checks are skipped with an INFO instead of failing.

    Returns (book_row_dict, final_path) or (book_row_dict, None).
    """
    info(f"Processing scenario [{label}]: {test_folder}")
    test_audio = test_folder / "audiobook.mp3"

    # Prepare messy copy with stripped tags
    if test_folder.exists():
        shutil.rmtree(test_folder)
    delete_book_records(str(test_folder) + '%')
    test_folder.mkdir(parents=True)
    shutil.copy2(SOURCE_BOOK, test_audio)
    from mutagen.mp3 import MP3
    audio = MP3(str(test_audio))
    if audio.tags:
        audio.tags.clear()
        audio.save()
    info(f"Copied and stripped test audio to {test_audio}")

    # Trigger scan + background processing
    info("Triggering library scan...")
    api_request("POST", "/api/scan")
    info("Starting background processing...")
    api_request("POST", "/api/process_background")

    info("Waiting for book to reach pending_fix or verified...")
    book = wait_for_book_state(test_folder, {"pending_fix", "verified"})
    if not book:
        check(False, f"[{label}] messy book did not reach pending_fix or verified within timeout")
        return None, None

    info(f"Book state: {book['status']} (layer {book['verification_layer']}, "
         f"author={book['current_author']!r}, title={book['current_title']!r})")
    profile = json.loads(book['profile'] or '{}')

    detected = profile.get('detected_language') or (profile.get('language') or {}).get('value')
    series_profile = (profile.get('series') or {}).get('value') if isinstance(
        profile.get('series'), dict) else profile.get('series')
    info(f"Profile: detected_language={profile.get('detected_language')!r}, "
         f"language={profile.get('language')!r}, series={series_profile!r}")
    override_missed = override_needs_series and not series_profile
    if override_missed:
        info(f"[{label}] identification metadata carries no series - the "
             "series override cannot fire (fixture/server data limitation); "
             "skipping override-dependent checks")
    if expect_detected and not override_missed:
        check(detected == expect_detected,
              f"[{label}] language resolution yields {expect_detected!r} "
              f"(detected={detected!r}, series={series_profile!r})")

    # Apply the fix
    hist_rows = db_query(
        "SELECT id FROM history WHERE book_id = ? AND status = 'pending_fix' "
        "ORDER BY id DESC LIMIT 1", (book['id'],))
    if not hist_rows:
        info("No pending_fix history entry; book may already be verified in place")
        return book, None

    history_id = hist_rows[0]['id']
    info(f"Applying fix (history id {history_id})...")
    status, result = api_request("POST", f"/api/apply_fix/{history_id}", timeout=60)
    if not (result and result.get('success')):
        check(False, f"[{label}] apply_fix failed: status={status} result={result}")
        return book, None
    info(f"apply_fix result: {result}")

    rows = db_query("SELECT path FROM books WHERE id = ?", (book['id'],))
    if not rows or not rows[0]['path']:
        check(False, f"[{label}] book has no path after apply_fix")
        return book, None
    final_path = Path(rows[0]['path'])
    info(f"Final path: {final_path}")

    try:
        rel = final_path.relative_to(LIBRARY_PATH)
        top_component = rel.parts[0]
    except ValueError:
        top_component = None
        check(False, f"[{label}] final path {final_path} is outside the library")
    if top_component:
        if not override_missed:
            check(top_component == expect_top,
                  f"[{label}] final path starts with language folder {expect_top!r} "
                  f"(got {top_component!r})")
        check(final_path.exists(), f"[{label}] final path exists on disk: {final_path}")
        check(any(final_path.glob("*.mp3")) or any(final_path.glob("*.m4b")),
              f"[{label}] final folder contains an audio file")

    return book, final_path


def phase_processing():
    """Two live processing scenarios with top_folder active.

    A) Flat messy folder (as prescribed): the Skaldleita fixture record for
       this Dune audio carries series=null, so the Dune=de override has no
       series to match and cannot fire; language falls back to the Skaldleita
       hint 'eng' (ISO 639-2). Expectation if the product were correct:
       override wins (German) - failing that, the folder must at least be the
       proper language name 'English', not the raw code 'ENG'.
    B) Messy folder nested under 'Frank, Herbert/Dune Series 01/': path
       completion (#127) supplies series='Dune Series', the matching override
       'Dune Series'=de must fire live, and the book must land under German/.
    """
    info("Phase 5b: live processing E2E with top_folder")

    # Scenario A: flat messy folder, series-less fixture
    book_a, path_a = run_processing_scenario(
        TEST_FOLDER, "flat/series-less", expect_detected='de', expect_top='German',
        override_needs_series=True)
    if book_a:
        profile = json.loads(book_a['profile'] or '{}')
        detected = profile.get('detected_language')
        series_profile = profile.get('series')
        if detected != 'de' and not series_profile:
            info("Override did not fire in scenario A: identification metadata carries "
                 "no series for this fixture (Skaldleita identify_audio returns "
                 "series=null for the Dune record), so the Dune=de override had "
                 "nothing to match. Override wiring is verified in phase 5a and "
                 "scenario B; this is a fixture/server data limitation.")
            if detected and detected not in ('de',):
                # The folder must still be a proper language NAME for whatever
                # language was detected (ISO 639-2 codes must be normalized).
                from library_manager.utils.path_safety import ISO_639_2_TO_1, LANGUAGE_NAMES
                normalized = ISO_639_2_TO_1.get(detected, detected)
                expected_name = LANGUAGE_NAMES.get(normalized)
                if path_a and expected_name:
                    actual_top = Path(path_a).relative_to(LIBRARY_PATH).parts[0]
                    check(actual_top == expected_name,
                          f"[flat/series-less] PRODUCT BUG if failed: detected {detected!r} "
                          f"should produce top folder {expected_name!r} (got {actual_top!r})")

    # Scenario B: nested folder supplies the series via path completion
    api_request("POST", "/api/series-language-overrides",
                {"series_name": "Dune Series", "language_code": "de"})
    book_b, path_b = run_processing_scenario(
        NESTED_FOLDER, "nested/series-from-path", expect_detected='de', expect_top='German')

    return (book_b or book_a), (path_b or path_a)


# ---------------------------------------------------------------------------
# Phase 6: per-book languages API + library edit modal
# ---------------------------------------------------------------------------

def phase_book_languages(book):
    info("Phase 6: per-book languages API + edit modal")
    if not book:
        check(False, "No processed book available for languages test")
        return
    book_id = book['id']

    status, data = api_request("POST", f"/api/books/{book_id}/languages",
                               {"languages": ["de", "en"]})
    check(status == 200 and data and data.get('success'),
          f"POST languages ['de','en'] succeeded (status={status}, body={data})")

    status, data = api_request("GET", f"/api/books/{book_id}/languages")
    langs = (data or {}).get('languages')
    check(langs == ['de', 'en'], f"GET languages returns ordered ['de','en'] (got {langs})")

    # Library edit modal must show both selected
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 1000})
        page = context.new_page()
        page.on("dialog", lambda d: d.accept())
        page.goto(f"{BASE_URL}/library", wait_until="networkidle", timeout=30000)

        opened = page.evaluate(
            """(bid) => {
                if (typeof editBook !== 'function') return 'editBook not defined';
                editBook(bid, 'Frank Herbert', 'Dune');
                return null;
            }""", book_id)
        if opened:
            check(False, f"Could not open edit modal: {opened}")
            browser.close()
            return

        try:
            # Wait until the async language fetch marks both options selected
            page.wait_for_function(
                """() => {
                    const sel = document.getElementById('edit-languages');
                    if (!sel || sel.options.length === 0) return false;
                    const selVals = Array.from(sel.selectedOptions).map(o => o.value);
                    return selVals.includes('de') && selVals.includes('en');
                }""",
                timeout=10000)
            check(True, "Edit modal Languages multi-select shows de + en selected")
        except Exception:
            selected = page.evaluate(
                """() => Array.from(document.getElementById('edit-languages').selectedOptions)
                        .map(o => o.value)""")
            check(False, f"Edit modal Languages multi-select did not show de+en (selected={selected})")

        # Make sure the modal is visible for the screenshot
        try:
            page.wait_for_selector("#editBookModal.show", timeout=5000)
        except Exception:
            pass
        page.screenshot(path=str(SCREENSHOT_DIR / "e2e-lang-05-edit-book-languages.png"), full_page=True)
        browser.close()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(config_backups, final_path, book_ids):
    info("Cleanup: restoring configs, removing overrides and test artifacts")

    # Remove the series overrides via the API
    for name in ("Dune", "Dune Series"):
        status, data = api_request("DELETE", "/api/series-language-overrides",
                                   {"series_name": name})
        check(status == 200 and data and data.get('success'),
              f"Series override {name!r} removed (status={status}, body={data})")

    # Restore both config backups
    for original, backup in config_backups.items():
        if backup.exists():
            shutil.copy2(backup, original)
            backup.unlink()
            info(f"Restored {original}")

    # Remove the messy test folders and their DB records
    for folder in (TEST_FOLDER, NESTED_FOLDER):
        if folder.exists():
            shutil.rmtree(folder)
        delete_book_records(str(folder) + '%')
    # Also remove the nested parents (the scanner registers the series-pattern
    # dir as a 'series_folder' record, and may do so again mid-run)
    for parent in (NESTED_PARENT, NESTED_PARENT.parent):
        delete_book_records(str(parent) + '%')
        if parent.exists() and parent != LIBRARY_PATH:
            shutil.rmtree(parent)
            info(f"Removed nested test parent {parent}")

    # Remove the language-folder destination trees produced by apply_fix
    for bid in book_ids:
        db_execute("DELETE FROM history WHERE book_id = ?", (bid,))
        db_execute("DELETE FROM queue WHERE book_id = ?", (bid,))
        db_execute("DELETE FROM books WHERE id = ?", (bid,))
    for lang_dir in ("German", "ENG", "English"):
        lang_root = LIBRARY_PATH / lang_dir
        if lang_root.exists():
            delete_book_records(str(lang_root) + '%')
            shutil.rmtree(lang_root)
            info(f"Removed test destination tree {lang_root}")

    # Sanity: canonical known-good folder must still be there
    check(SOURCE_BOOK.exists(), f"canonical source book intact: {SOURCE_BOOK}")
    if final_path and str(final_path).startswith(str(CANONICAL_FOLDER)):
        info("WARN: final path overlapped the canonical folder - manual check advised")


def main():
    SCREENSHOT_DIR.mkdir(exist_ok=True)

    # Back up BOTH config files before touching anything
    config_backups = {}
    for f in CONFIG_FILES:
        if f.exists():
            backup = f.with_name(f.name + ".e2e-bak")
            shutil.copy2(f, backup)
            config_backups[f] = backup
            info(f"Backed up {f} -> {backup}")

    book = None
    final_path = None
    book_ids = []
    try:
        clean_library_artifacts()
        phase_settings_ui()
        phase_override_mechanism()
        book, final_path = phase_processing()
        if book:
            book_ids.append(book['id'])
        phase_book_languages(book)
    except Exception as e:
        import traceback
        traceback.print_exc()
        check(False, f"Unexpected exception: {e}")
    finally:
        try:
            cleanup(config_backups, final_path, book_ids)
        except Exception as e:
            import traceback
            traceback.print_exc()
            check(False, f"Cleanup failed: {e}")

    if FAILURES:
        print("\nFAILURES:")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("\nLanguage features E2E test passed.")


if __name__ == "__main__":
    main()
