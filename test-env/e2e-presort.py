#!/usr/bin/env python3
"""Actual-browser E2E for pre-sort planning, receipts, rollback, and Undo."""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parent.parent
SCREENSHOT_DIR = Path(os.environ.get('TEST_SCREENSHOT_DIR', ROOT / 'docs' / 'images'))
CHROMIUM_EXECUTABLE = os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE')


def reserve_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def wait_for_app(base_url):
    import requests
    for _ in range(60):
        try:
            if requests.get(f'{base_url}/api/stats', timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError(f'Library Manager did not become ready at {base_url}')


def write_old(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    old = time.time() - 900
    os.utime(path, (old, old))
    current = path.parent
    while current != current.parent:
        os.utime(current, (old, old))
        if current.name == 'watch':
            break
        current = current.parent


def plan_row(page, name):
    return page.locator('.presort-plan', has_text=name)


def run_browser(base_url, db_path, watch_root):
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    dialogs = []
    with sync_playwright() as playwright:
        options = {'headless': True}
        if CHROMIUM_EXECUTABLE:
            options['executable_path'] = CHROMIUM_EXECUTABLE
        browser = playwright.chromium.launch(**options)
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})

        def accept_dialog(dialog):
            dialogs.append(dialog.message)
            dialog.accept()

        page.on('dialog', accept_dialog)
        page.goto(f'{base_url}/presort', wait_until='networkidle')
        assert page.locator('h2', has_text='Verified Pre-sort').is_visible()
        with page.expect_navigation(wait_until='networkidle'):
            page.locator('#scan-presort-btn').click()

        success_row = plan_row(page, 'Browser Split')
        failure_row = plan_row(page, 'Rollback Split')
        success_row.wait_for()
        failure_row.wait_for()
        assert success_row.get_by_text('High confidence').is_visible()
        assert success_row.get_by_text('2 inventory items').is_visible()
        success_row.get_by_role('button', name='2 inventory items').click()
        success_row.locator('tbody tr').first.wait_for()
        page.wait_for_timeout(500)
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'presort-review.png'),
            full_page=True,
        )
        page.goto(f'{base_url}/', wait_until='networkidle')
        dashboard_notice = page.get_by_text('2 verified pre-sort plans waiting for review', exact=False)
        assert dashboard_notice.is_visible()
        page.get_by_role('link', name='Review handoffs').click()
        page.wait_for_load_state('networkidle')

        with page.expect_navigation(wait_until='networkidle'):
            success_row.locator('button[title="Apply verified pre-sort plan"]').click()
        assert any('Apply this exact file plan' in message for message in dialogs)
        success_row = plan_row(page, 'Browser Split')
        assert success_row.get_by_text('Applied', exact=True).is_visible()
        success_row.locator('button[title="View pre-sort receipt"]').click()
        receipt_modal = page.locator('#presortReceiptModal')
        receipt_modal.get_by_text('committed', exact=True).wait_for()
        assert receipt_modal.get_by_text('split', exact=True).is_visible()
        assert receipt_modal.locator('tbody tr').count() == 2
        page.wait_for_timeout(400)
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'presort-committed-receipt.png'),
            full_page=True,
        )
        receipt_modal.get_by_role('button', name='Close').click()
        receipt_modal.wait_for(state='hidden')
        with page.expect_navigation(wait_until='networkidle'):
            success_row.locator('button[title="Undo pre-sort operation"]').click()
        plan_row(page, 'Browser Split').get_by_text('Pending', exact=True).wait_for()

        conn = sqlite3.connect(db_path)
        failed_plan_id = conn.execute(
            "SELECT id FROM presort_plans WHERE display_name = 'Rollback Split'"
        ).fetchone()[0]
        conn.execute(f'''CREATE TRIGGER e2e_force_presort_verify_failure
                         BEFORE UPDATE OF destination_verified ON presort_operation_items
                         WHEN NEW.operation_id IN (
                             SELECT id FROM presort_operations WHERE plan_id = {failed_plan_id}
                         )
                         BEGIN
                             SELECT RAISE(ABORT, 'simulated destination verification failure');
                         END''')
        conn.commit()
        conn.close()

        failure_row = plan_row(page, 'Rollback Split')
        dialog_count = len(dialogs)
        failure_row.locator('button[title="Apply verified pre-sort plan"]').click()
        for _ in range(50):
            if len(dialogs) >= dialog_count + 2:
                break
            page.wait_for_timeout(100)
        assert any('simulated destination verification failure' in message for message in dialogs)
        page.reload(wait_until='networkidle')
        failure_row = plan_row(page, 'Rollback Split')
        assert failure_row.get_by_text('Pending', exact=True).is_visible()
        page.evaluate('window.scrollTo(0, 0)')
        failure_row.locator('button[title="View pre-sort receipt"]').click()
        receipt_modal = page.locator('#presortReceiptModal')
        receipt_modal.get_by_text('rolled_back', exact=True).wait_for()
        assert receipt_modal.get_by_text('filesystem rollback verified', exact=False).is_visible()
        page.wait_for_timeout(400)
        page.screenshot(
            path=str(SCREENSHOT_DIR / 'presort-rollback-receipt.png'),
            full_page=True,
        )
        receipt_modal.get_by_role('button', name='Close').click()
        receipt_modal.wait_for(state='hidden')
        page.goto(f'{base_url}/settings', wait_until='networkidle')
        assert page.locator('#presort_enabled').is_checked()
        assert page.locator('#presort_use_fingerprints').is_checked()
        page.locator('#watch_mode').check()
        assert page.locator('#presort_settle_seconds').is_visible()
        page.locator('#presort_settle_seconds').fill('120')
        page.locator('#watch_mode').uncheck()
        with page.expect_navigation(wait_until='networkidle'):
            page.get_by_role('button', name='Save Settings').click()
        assert page.locator('#presort_settle_seconds').input_value() == '120'
        browser.close()

    assert (watch_root / 'Browser Split' / 'Book 1 - Alpha.mp3').read_bytes() == b'alpha browser'
    assert (watch_root / 'Browser Split' / 'Book 2 - Beta.mp3').read_bytes() == b'beta browser'
    assert (watch_root / 'Rollback Split' / 'Book 1 - Gamma.mp3').read_bytes() == b'gamma rollback'
    assert (watch_root / 'Rollback Split' / 'Book 2 - Delta.mp3').read_bytes() == b'delta rollback'
    assert not (watch_root / 'Book 1 - Alpha').exists()
    assert not (watch_root / 'Book 1 - Gamma').exists()
    saved_config = json.loads((db_path.parent / 'config.json').read_text(encoding='utf-8'))
    assert saved_config['presort_enabled'] is True
    assert saved_config['presort_use_fingerprints'] is True
    assert saved_config['presort_settle_seconds'] == 120

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    success_operation = conn.execute('''SELECT operation.* FROM presort_operations operation
                                        JOIN presort_plans plan ON plan.id = operation.plan_id
                                        WHERE plan.display_name = 'Browser Split'
                                        ORDER BY operation.id DESC LIMIT 1''').fetchone()
    failure_operation = conn.execute('''SELECT operation.* FROM presort_operations operation
                                        JOIN presort_plans plan ON plan.id = operation.plan_id
                                        WHERE plan.display_name = 'Rollback Split'
                                        ORDER BY operation.id DESC LIMIT 1''').fetchone()
    failure_unverified = conn.execute('''SELECT COUNT(*) FROM presort_operation_items
                                         WHERE operation_id = ? AND rollback_verified = 0''',
                                      (failure_operation['id'],)).fetchone()[0]
    conn.close()
    assert success_operation['status'] == 'undone'
    assert success_operation['source_digest'] == success_operation['rollback_digest']
    assert failure_operation['status'] == 'rolled_back'
    assert failure_operation['source_digest'] == failure_operation['rollback_digest']
    assert failure_unverified == 0
    print('[PASS] browser scan, plan review, committed receipt, actual Undo, and forced rollback receipt')


def main():
    with tempfile.TemporaryDirectory(prefix='library-manager-presort-e2e-') as temp:
        root = Path(temp)
        data_dir = root / 'data'
        watch_root = root / 'watch'
        library_root = root / 'library'
        data_dir.mkdir()
        watch_root.mkdir()
        library_root.mkdir()
        db_path = data_dir / 'library.db'
        db_path.touch()

        write_old(watch_root / 'Browser Split' / 'Book 1 - Alpha.mp3', b'alpha browser')
        write_old(watch_root / 'Browser Split' / 'Book 2 - Beta.mp3', b'beta browser')
        write_old(watch_root / 'Rollback Split' / 'Book 1 - Gamma.mp3', b'gamma rollback')
        write_old(watch_root / 'Rollback Split' / 'Book 2 - Delta.mp3', b'delta rollback')

        config = {
            'library_paths': [str(library_root)],
            'watch_folder': str(watch_root),
            'watch_output_folder': str(library_root),
            'watch_mode': False,
            'watch_min_file_age_seconds': 30,
            'presort_enabled': True,
            'presort_auto_apply': False,
            'presort_settle_seconds': 30,
            'enabled': False,
            'setup_completed': True,
        }
        (data_dir / 'config.json').write_text(json.dumps(config), encoding='utf-8')
        (data_dir / 'secrets.json').write_text('{}', encoding='utf-8')
        (data_dir / 'user_groups.json').write_text('{}', encoding='utf-8')

        port = reserve_local_port()
        base_url = f'http://127.0.0.1:{port}'
        environment = os.environ.copy()
        environment.update({
            'DATA_DIR': str(data_dir),
            'PORT': str(port),
            'PYTHONUNBUFFERED': '1',
        })
        log_path = root / 'app-process.log'
        with log_path.open('w+', encoding='utf-8') as app_log:
            process = subprocess.Popen(
                [sys.executable, 'app.py'],
                cwd=ROOT,
                env=environment,
                stdout=app_log,
                stderr=subprocess.STDOUT,
            )
            try:
                wait_for_app(base_url)
                run_browser(base_url, db_path, watch_root)
            except Exception:
                app_log.flush()
                app_log.seek(0)
                print('\nIsolated app output:\n' + app_log.read()[-10000:])
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


if __name__ == '__main__':
    main()
