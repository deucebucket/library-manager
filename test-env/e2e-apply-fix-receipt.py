#!/usr/bin/env python3
"""Browser E2E for verified transfer receipts and rollback safety."""

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parent.parent
BASE_URL = os.environ.get('TEST_APP_URL')
DB_PATH = Path(os.environ['TEST_DB_PATH']) if os.environ.get('TEST_DB_PATH') else None
LIBRARY_ROOT = (
    Path(os.environ['TEST_LIBRARY_ROOT'])
    if os.environ.get('TEST_LIBRARY_ROOT') else None
)
SCREENSHOT_DIR = Path(os.environ.get('TEST_SCREENSHOT_DIR', ROOT / 'docs' / 'images'))
CHROMIUM_EXECUTABLE = os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE')


def wait_for_app():
    import requests
    for _ in range(60):
        try:
            if requests.get(f'{BASE_URL}/api/stats', timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError(f'Library Manager did not become ready at {BASE_URL}')


def safe_remove(path):
    resolved_root = LIBRARY_ROOT.resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise RuntimeError(f'Refusing to remove path outside test library: {path}') from exc
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def reserve_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def run_isolated():
    """Boot a disposable app/database/library and run the real browser flow."""
    global BASE_URL, DB_PATH, LIBRARY_ROOT

    with tempfile.TemporaryDirectory(prefix='library-manager-receipt-e2e-') as temp_dir:
        test_root = Path(temp_dir)
        data_dir = test_root / 'data'
        data_dir.mkdir()
        LIBRARY_ROOT = test_root / 'library'
        LIBRARY_ROOT.mkdir()
        DB_PATH = data_dir / 'library.db'
        DB_PATH.touch()

        config = {
            'library_paths': [str(LIBRARY_ROOT)],
            'watch_folder': '',
            'watch_output_folder': '',
            'enabled': False,
            'setup_completed': True,
            'metadata_embedding_enabled': False,
            'auto_fix': False,
        }
        (data_dir / 'config.json').write_text(json.dumps(config), encoding='utf-8')
        (data_dir / 'secrets.json').write_text('{}', encoding='utf-8')
        (data_dir / 'user_groups.json').write_text('{}', encoding='utf-8')

        port = reserve_local_port()
        BASE_URL = f'http://127.0.0.1:{port}'
        environment = os.environ.copy()
        environment.update({
            'DATA_DIR': str(data_dir),
            'PORT': str(port),
            'PYTHONUNBUFFERED': '1',
        })
        log_path = test_root / 'app-process.log'
        with log_path.open('w+', encoding='utf-8') as app_log:
            process = subprocess.Popen(
                [sys.executable, 'app.py'],
                cwd=ROOT,
                env=environment,
                stdout=app_log,
                stderr=subprocess.STDOUT,
            )
            try:
                main()
            except Exception:
                app_log.flush()
                app_log.seek(0)
                print('\nIsolated app output:\n' + app_log.read()[-8000:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


def create_book_fixture(cursor, author, old_title, new_title):
    source = LIBRARY_ROOT / author / old_title
    destination = LIBRARY_ROOT / author / new_title
    safe_remove(source)
    safe_remove(destination)
    source.mkdir(parents=True)
    (source / 'Part 01.mp3').write_bytes(b'verified browser handoff - part one\x00\x01')
    extras = source / 'Artwork'
    extras.mkdir()
    (extras / 'cover.jpg').write_bytes(b'verified browser cover\x02\x03')

    cursor.execute('''INSERT INTO books
                      (path, current_author, current_title, status, source_type)
                      VALUES (?, ?, ?, 'pending_fix', 'library')''',
                   (str(source), author, old_title))
    book_id = cursor.lastrowid
    cursor.execute("INSERT INTO queue (book_id, reason) VALUES (?, 'browser receipt test')",
                   (book_id,))
    cursor.execute('''INSERT INTO history
                      (book_id, old_author, old_title, new_author, new_title,
                       old_path, new_path, status)
                      VALUES (?, ?, ?, ?, ?, ?, ?, 'pending_fix')''',
                   (book_id, author, old_title, author, new_title,
                    str(source), str(destination)))
    return {
        'book_id': book_id,
        'history_id': cursor.lastrowid,
        'source': source,
        'destination': destination,
        'old_title': old_title,
        'new_title': new_title,
    }


def seed_fixtures():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('DROP TRIGGER IF EXISTS e2e_force_apply_failure')
    failed = create_book_fixture(
        cursor,
        'Receipt Showcase',
        'Rollback Source',
        'Rollback Destination',
    )
    committed = create_book_fixture(
        cursor,
        'Receipt Showcase',
        'Verified Source',
        'Verified Destination',
    )
    cursor.execute(f'''CREATE TRIGGER e2e_force_apply_failure
                       BEFORE UPDATE OF path ON books
                       WHEN OLD.id = {failed['book_id']}
                       BEGIN
                         SELECT RAISE(ABORT, 'browser-forced database failure');
                       END''')
    conn.commit()
    conn.close()
    return failed, committed


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def row_for(page, title):
    return page.locator('tbody tr', has_text=title)


def assert_database_outcome(failed, committed):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    failed_book = conn.execute('SELECT * FROM books WHERE id = ?', (failed['book_id'],)).fetchone()
    failed_history = conn.execute('SELECT * FROM history WHERE id = ?', (failed['history_id'],)).fetchone()
    failed_receipt = conn.execute(
        'SELECT * FROM file_operations WHERE history_id = ? ORDER BY id DESC LIMIT 1',
        (failed['history_id'],),
    ).fetchone()
    committed_book = conn.execute('SELECT * FROM books WHERE id = ?',
                                  (committed['book_id'],)).fetchone()
    committed_history = conn.execute('SELECT * FROM history WHERE id = ?',
                                     (committed['history_id'],)).fetchone()
    committed_receipt = conn.execute(
        'SELECT * FROM file_operations WHERE history_id = ? ORDER BY id DESC LIMIT 1',
        (committed['history_id'],),
    ).fetchone()
    failed_inventory_phases = {
        row[0] for row in conn.execute(
            'SELECT DISTINCT phase FROM file_operation_inventory WHERE operation_id = ?',
            (failed_receipt['id'],),
        ).fetchall()
    }
    committed_unverified = conn.execute(
        'SELECT COUNT(*) FROM file_operation_inventory WHERE operation_id = ? AND verified = 0',
        (committed_receipt['id'],),
    ).fetchone()[0]
    conn.close()

    assert failed_book['path'] == str(failed['source'])
    assert failed_history['status'] == 'error'
    assert failed_receipt['status'] == 'rolled_back'
    assert failed_receipt['source_digest'] == failed_receipt['rollback_digest']
    assert failed_inventory_phases == {'source', 'destination', 'rollback'}

    assert committed_book['path'] == str(committed['source'])
    assert committed_book['status'] == 'protected'
    assert committed_history['status'] == 'undone'
    assert committed_receipt['status'] == 'committed'
    assert committed_receipt['source_digest'] == committed_receipt['destination_digest']
    assert committed_unverified == 0


def main():
    wait_for_app()
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    failed, committed = seed_fixtures()
    failed_hashes = {
        path.relative_to(failed['source']).as_posix(): sha256(path)
        for path in failed['source'].rglob('*') if path.is_file()
    }
    dialogs = []

    with sync_playwright() as playwright:
        launch_args = {'headless': True}
        if CHROMIUM_EXECUTABLE:
            launch_args['executable_path'] = CHROMIUM_EXECUTABLE
        browser = playwright.chromium.launch(**launch_args)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})

        def accept_dialog(dialog):
            dialogs.append(dialog.message)
            dialog.accept()

        page.on('dialog', accept_dialog)
        page.goto(f'{BASE_URL}/history?status=pending', wait_until='networkidle')
        failed_row = row_for(page, failed['old_title'])
        failed_row.locator('button[title="Apply fix"]').wait_for()
        with page.expect_response(f'**/api/apply_fix/{failed["history_id"]}') as response_info:
            failed_row.locator('button[title="Apply fix"]').click()
        failed_payload = response_info.value.json()
        assert failed_payload['success'] is False
        assert 'original inventory verified' in failed_payload['message']
        assert any('browser-forced database failure' in message for message in dialogs)

        page.goto(f'{BASE_URL}/history?status=error', wait_until='networkidle')
        failed_row = row_for(page, failed['old_title'])
        failed_row.locator('button[title="View transfer receipt"]').click()
        page.locator('#transferReceiptModal.show').wait_for()
        page.locator('#transferReceiptSummary', has_text='rolled back').wait_for()
        page.locator('#transferReceiptSummary', has_text='Rollback inventory digest').wait_for()
        page.wait_for_timeout(500)
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'transfer-receipt-rollback.png'),
            full_page=False,
        )
        assert page.locator('#transferReceiptInventory tr').count() >= 6
        page.locator('#transferReceiptModal button[data-bs-dismiss="modal"]').last.click()

        page.goto(f'{BASE_URL}/history?status=pending', wait_until='networkidle')
        committed_row = row_for(page, committed['old_title'])
        with page.expect_response(f'**/api/apply_fix/{committed["history_id"]}') as response_info:
            committed_row.locator('button[title="Apply fix"]').click()
        committed_payload = response_info.value.json()
        assert committed_payload['success'] is True
        page.wait_for_timeout(750)
        page.wait_for_load_state('networkidle')

        page.goto(f'{BASE_URL}/history', wait_until='networkidle')
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'transfer-receipts-history.png'),
            full_page=True,
        )
        committed_row = row_for(page, committed['old_title'])
        committed_row.locator('button[title="View transfer receipt"]').click()
        page.locator('#transferReceiptModal.show').wait_for()
        page.locator('#transferReceiptSummary', has_text='committed').wait_for()
        page.locator('#transferReceiptSummary', has_text='Destination inventory digest').wait_for()
        page.wait_for_timeout(500)
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'transfer-receipt-committed.png'),
            full_page=False,
        )
        assert page.locator('#transferReceiptInventory .text-danger').count() == 0
        page.locator('#transferReceiptModal button[data-bs-dismiss="modal"]').last.click()
        page.locator('#transferReceiptModal').wait_for(state='hidden')

        committed_row = row_for(page, committed['old_title'])
        with page.expect_response(f'**/api/undo/{committed["history_id"]}') as response_info:
            committed_row.locator('button[title="Undo - revert to original"]').click()
        undo_payload = response_info.value.json()
        assert undo_payload['success'] is True
        page.wait_for_timeout(750)
        page.wait_for_load_state('networkidle')
        browser.close()

    assert failed['source'].is_dir()
    assert not failed['destination'].exists()
    restored_hashes = {
        path.relative_to(failed['source']).as_posix(): sha256(path)
        for path in failed['source'].rglob('*') if path.is_file()
    }
    assert restored_hashes == failed_hashes
    assert committed['source'].is_dir()
    assert not committed['destination'].exists()
    assert_database_outcome(failed, committed)
    print('[PASS] browser applied a verified transfer and inspected its committed receipt')
    print('[PASS] browser undo restored the exact source from History')
    print('[PASS] browser forced a database failure and inspected its verified rollback receipt')
    print(f'[PASS] screenshots written to {SCREENSHOT_DIR}')


if __name__ == '__main__':
    try:
        supplied = (BASE_URL is not None, DB_PATH is not None, LIBRARY_ROOT is not None)
        if any(supplied) and not all(supplied):
            raise RuntimeError(
                'Set TEST_APP_URL, TEST_DB_PATH, and TEST_LIBRARY_ROOT together, '
                'or omit all three to use the isolated runner.'
            )
        if all(supplied):
            main()
        else:
            run_isolated()
    except Exception as exc:
        print(f'[FAIL] {exc}')
        sys.exit(1)
