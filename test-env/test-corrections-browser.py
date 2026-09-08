#!/usr/bin/env python3
"""Disposable actual-browser acceptance for the corrections review workflow.

Run with Playwright installed and PLAYWRIGHT_CHROMIUM_EXECUTABLE configured.
All application data and the fixture HTTP server are local to this test.
"""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from unittest.mock import patch

from flask import Flask, jsonify
from playwright.sync_api import sync_playwright
from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    artifacts = Path(os.environ.get('CORRECTIONS_ARTIFACTS', tempfile.mkdtemp(prefix='lm205-browser-')))
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='lm205-data-') as temporary:
        os.environ['DATA_DIR'] = temporary
        import app
        from library_manager.config import DEFAULT_CONFIG
        from library_manager.corrections import store
        from library_manager.corrections.feed import FeedFailure

        fixture = Flask('corrections-fixture')
        payload = json.loads((ROOT / 'test-env/fixtures/corrections-page.json').read_text())
        original_payload = copy.deepcopy(payload)
        requests_seen = []
        response_status = [200]

        @fixture.get('/api/corrections')
        def feed():
            requests_seen.append(True)
            return jsonify(payload), response_status[0], {'Retry-After': '60'}

        upstream = make_server('127.0.0.1', 0, fixture, threaded=True)
        source = f'http://127.0.0.1:{upstream.server_port}'
        upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        upstream_thread.start()
        settings = copy.deepcopy(DEFAULT_CONFIG)
        settings.update(bookdb_url=source, setup_completed=True, enabled=False,
                        receive_corrections=False, auto_apply_corrections=False,
                        library_paths=[], watch_folder='', auto_fix=False)
        app.save_config(settings)
        app.init_db()
        profile = {'book_id': 'B012345678', 'skaldleita_book_id': '42',
                   'skaldleita_source_url': source, 'author': {'value': 'Example Author'},
                   'title': {'value': 'Example Book'}, 'narrator': {'value': 'Old Narrator'},
                   'language': {'value': 'en'}, 'languages': ['en']}
        conn = app.get_db()
        conn.execute('INSERT INTO books(path,current_author,current_title,status,profile) VALUES(?,?,?,?,?)',
                     ('/example-library/Example Book', 'Example Author', 'Example Book', 'verified', json.dumps(profile)))
        conn.commit()
        conn.close()

        with patch.object(app, 'load_secrets', return_value={}), \
                patch.object(app, 'save_secrets', return_value=None):
            service, application = app.get_corrections_components()
            service.start()
            server = make_server('127.0.0.1', 0, app.app, threaded=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            url = f'http://127.0.0.1:{server.server_port}'
            checks = []
            try:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(executable_path=os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE'))
                    page = browser.new_page(viewport={'width': 1280, 'height': 800})
                    page.goto(url + '/corrections', wait_until='networkidle')
                    assert page.locator('#check-corrections').is_disabled()
                    assert not requests_seen
                    checks.append('disabled startup and browser review make zero feed requests')
                    page.goto(url + '/settings', wait_until='networkidle')
                    page.locator('[data-bs-target="#tab-integrations"]').click()
                    page.locator('#receive_corrections').check()
                    assert not page.locator('#auto_apply_corrections').is_checked()
                    page.locator('#correction-settings').scroll_into_view_if_needed()
                    page.evaluate('window.scrollBy(0, 180)')
                    page.wait_for_timeout(400)
                    page.screenshot(path=str(artifacts / 'corrections-settings.png'))
                    page.locator('.settings-save-bar button[type="submit"]').click()
                    page.wait_for_load_state('networkidle')
                    assert app.load_config()['receive_corrections'] is True
                    assert app.load_config()['auto_apply_corrections'] is False
                    checks.append('actual Settings form preserves separate receive and auto-apply opt-ins')
                    page.goto(url + '/corrections', wait_until='networkidle')
                    page.locator('#check-corrections').click()
                    page.locator('.correction-review').first.wait_for(timeout=15000)
                    assert requests_seen
                    decision = application.list_decisions()[0]
                    assert decision['status'] == 'pending'
                    checks.append('Check now ingests pinned upstream fixture through HTTP without auto-applying identity change')
                    page.goto(url + '/', wait_until='networkidle')
                    assert page.locator('#corrections-dashboard').is_visible()
                    page.goto(url + '/corrections', wait_until='networkidle')
                    page.screenshot(path=str(artifacts / 'corrections-review.png'))
                    page.locator('.correction-review').first.click()
                    page.locator('#correction-modal.show').wait_for()
                    assert page.locator('#correction-conflicts').inner_text().find('identity') >= 0
                    assert 'Corrected Example Book' in page.locator('#correction-fields').inner_text()
                    assert page.locator('#apply-correction').is_enabled()
                    page.wait_for_timeout(300)
                    page.screenshot(path=str(artifacts / 'corrections-identity-review.png'))
                    with page.expect_navigation(wait_until='networkidle'):
                        page.locator('#apply-correction').click()
                    assert application.detail(decision['id'])['status'] == 'applied'
                    conn = app.get_db()
                    row = conn.execute('SELECT path,profile FROM books').fetchone()
                    conn.close()
                    assert row['path'] == '/example-library/Example Book'
                    assert json.loads(row['profile'])['skaldleita_book_id'] == '84'
                    checks.append('explicit identity review updates metadata and receipt while retaining audiobook path')
                    page.locator('.correction-review').first.click()
                    page.locator('#correction-modal.show').wait_for()
                    page.locator('details summary').click()
                    assert 'review' in page.locator('#correction-receipts').inner_text()
                    page.wait_for_timeout(300)
                    page.screenshot(path=str(artifacts / 'corrections-receipt.png'))
                    page.locator('#correction-modal .modal-footer [data-bs-dismiss]').click()

                    # A second valid fixture variant conflicts with current metadata.
                    event = copy.deepcopy(payload['corrections'][0])
                    event.update(id=2, affected_skaldleita_book_ids=[84], reason='narrator_mismatch',
                                 original_result={'narrator': 'Different stored narrator'},
                                 corrected_result={'narrator': 'Another Narrator'})
                    payload['corrections'] = [event]
                    payload['next_cursor'] = 'fixture-second-checkpoint'
                    assert service.poll_once(force=True)['status'] == 'updated'
                    page.goto(url + '/corrections', wait_until='networkidle')
                    page.locator('.correction-review').first.click()
                    page.locator('#correction-modal.show').wait_for()
                    assert 'original_mismatch' in page.locator('#correction-conflicts').inner_text()
                    assert page.locator('#apply-correction').is_disabled()
                    page.wait_for_timeout(300)
                    page.screenshot(path=str(artifacts / 'corrections-conflict.png'))
                    with page.expect_navigation(wait_until='networkidle'):
                        page.locator('#dismiss-correction').click()
                    assert application.list_decisions()[0]['status'] == 'dismissed'
                    checks.append('conflicting original blocks Apply and can be explicitly dismissed through browser')

                    page.goto(url + '/settings', wait_until='networkidle')
                    page.locator('[data-bs-target="#tab-integrations"]').click()
                    page.locator('#auto_apply_corrections').check()
                    with page.expect_navigation(wait_until='networkidle'):
                        page.locator('.settings-save-bar button[type="submit"]').click()
                    assert app.load_config()['auto_apply_corrections'] is True
                    event.update(id=3, original_result={'narrator': 'Correct Narrator'},
                                 corrected_result={'narrator': 'New Verified Narrator'})
                    payload['next_cursor'] = 'fixture-third-checkpoint'
                    assert service.poll_once(force=True)['status'] == 'updated'
                    assert application.list_decisions()[0]['status'] == 'applied'
                    conn = app.get_db()
                    conn.execute('UPDATE books SET user_locked=1')
                    conn.commit()
                    conn.close()
                    event.update(id=4, original_result={'narrator': 'New Verified Narrator'},
                                 corrected_result={'narrator': 'Locked Change'})
                    payload['next_cursor'] = 'fixture-fourth-checkpoint'
                    assert service.poll_once(force=True)['status'] == 'updated'
                    page.goto(url + '/corrections', wait_until='networkidle')
                    page.locator('.correction-review').first.click()
                    page.locator('#correction-modal.show').wait_for()
                    assert 'user_locked' in page.locator('#correction-conflicts').inner_text()
                    assert page.locator('#apply-correction').is_disabled()
                    checks.append('browser auto-apply opt-in applies safe metadata but user lock still blocks subsequent correction')
                    response_status[0] = 429
                    assert service.poll_once(force=True)['error'] == 'rate_limited'
                    page.goto(url + '/corrections', wait_until='networkidle')
                    assert 'rate_limited' in page.locator('#feed-status').inner_text()
                    before_requests = len(requests_seen)
                    page.locator('#check-corrections').click()
                    page.wait_for_timeout(2500)
                    assert len(requests_seen) == before_requests
                    checks.append('HTTP quota failure is visible and browser Check now respects retry cooldown')

                    conn = app.get_db()
                    state = store.checkpoint(conn, source)
                    store.record_failure(conn, state, FeedFailure('bootstrap_required'), next_poll_at=0)
                    conn.close()
                    page.goto(url + '/corrections', wait_until='networkidle')
                    page.on('dialog', lambda dialog: dialog.accept())
                    with page.expect_navigation(wait_until='networkidle'):
                        page.locator('#reset-corrections').click()
                    assert service.summary()['generation'] == state.generation + 1
                    assert len(application.list_decisions()) == 4
                    checks.append('explicit browser feed reset advances generation and retains all decisions')
                    response_status[0] = 200
                    payload.clear()
                    payload.update(copy.deepcopy(original_payload))
                    assert service.poll_once(force=True)['status'] == 'updated'
                    replay = application.list_decisions()[0]
                    assert replay['status'] == 'applied'
                    assert replay['prior_decision_id'] == decision['id']
                    assert replay['receipts'] == application.detail(decision['id'])['receipts']
                    checks.append('identical identity-changing replay after reset preserves the prior applied decision and original receipt')
                    # A normal retained backlog must not hide an older pending review.
                    conn = app.get_db()
                    conn.execute("UPDATE correction_decisions SET status='pending' WHERE id=?", (decision['id'],))
                    for number in range(101):
                        conn.execute('''INSERT INTO correction_decisions
                            (source,generation,event_id,reconciliation_key,book_id,event_json,snapshot_json,status,conflicts_json)
                            SELECT source,generation,?,reconciliation_key,book_id,event_json,snapshot_json,'dismissed',conflicts_json
                            FROM correction_decisions WHERE id=?''', ('backlog-' + str(number), decision['id']))
                    conn.commit()
                    conn.close()
                    page.goto(url + '/corrections', wait_until='networkidle')
                    assert page.locator('.correction-review').count() == 100
                    assert page.locator(f'.correction-review[data-id="{decision["id"]}"]').count() == 0
                    page.locator('#corrections-older').click()
                    page.wait_for_load_state('networkidle')
                    page.locator(f'.correction-review[data-id="{decision["id"]}"]').click()
                    page.locator('#correction-modal.show').wait_for()
                    assert 'pending' in page.locator('#correction-reason').inner_text()
                    page.locator('#correction-modal .modal-footer [data-bs-dismiss]').click()
                    page.wait_for_timeout(300)
                    page.screenshot(path=str(artifacts / 'corrections-pagination.png'))
                    page.locator('#corrections-newest').click()
                    page.wait_for_load_state('networkidle')
                    page.locator('#correction-status').select_option('pending')
                    with page.expect_navigation(wait_until='networkidle'):
                        page.get_by_role('button', name='Filter', exact=True).click()
                    assert page.locator('.correction-review').count() == 1
                    assert page.locator('#correction-status option:checked').inner_text() == 'Pending (1)'
                    assert page.locator(f'.correction-review[data-id="{decision["id"]}"]').count() == 1
                    page.screenshot(path=str(artifacts / 'corrections-filtered-review.png'))
                    checks.append('older pending decision stays reachable beyond100 newer rows; newest navigation and status filter preserve accurate total')
                    page.set_viewport_size({'width': 390, 'height': 844})
                    page.goto(url + '/corrections?status=pending', wait_until='networkidle')
                    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
                    page.screenshot(path=str(artifacts / 'corrections-phone.png'), full_page=True)
                    checks.append('390px review page has no horizontal page overflow')
                    browser.close()
                (artifacts / 'browser-results.json').write_text(json.dumps({'checks': checks, 'passed': len(checks)}, indent=2))
                for check in checks:
                    print('[PASS]', check)
            finally:
                service.stop()
                server.shutdown()
                thread.join(timeout=5)
                upstream.shutdown()
                upstream_thread.join(timeout=5)


if __name__ == '__main__':
    main()
