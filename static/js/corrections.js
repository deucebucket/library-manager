/* Corrections are rendered as text, never upstream HTML. */
(() => {
    let selected = null;
    const modal = new bootstrap.Modal(document.getElementById('correction-modal'));
    const notice = document.getElementById('corrections-message');
    function message(text, good = false) {
        notice.textContent = text;
        notice.className = `alert ${good ? 'alert-success' : 'alert-warning'}`;
    }
    async function post(path, body = {}) {
        const response = await fetch(path, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
        const result = await response.json();
        if (!response.ok || result.success === false) throw new Error(result.error || 'Request could not be completed. Refresh and review again.');
        return result;
    }
    function value(item) {
        if (item === null) return 'Clear';
        if (item === undefined) return 'Not stored';
        if (typeof item === 'object') return JSON.stringify(item);
        return String(item);
    }
    document.querySelectorAll('.correction-review').forEach(button => button.addEventListener('click', async () => {
        try {
            const response = await fetch(`/api/corrections/${button.dataset.id}`);
            const result = await response.json();
            if (!response.ok || !result.success) throw new Error(result.error || 'Could not load correction.');
            selected = result.decision;
            document.getElementById('correction-title').textContent = selected.title || 'Review correction';
            document.getElementById('correction-reason').textContent = `${(selected.reason || selected.event?.reason || '').replaceAll('_', ' ')} — ${selected.status}`;
            const conflicts = document.getElementById('correction-conflicts');
            conflicts.textContent = (selected.conflicts || []).map(value).join('; ');
            conflicts.classList.toggle('d-none', !conflicts.textContent);
            const fields = document.getElementById('correction-fields');
            fields.replaceChildren();
            const patch = selected.event?.corrected_result || selected.after || {};
            for (const [name, proposed] of Object.entries(patch)) {
                const row = document.createElement('tr');
                for (const item of [name.replaceAll('_', ' '), selected.before?.[name] == null ? 'Not stored' : value(selected.before[name]), value(proposed)]) {
                    const cell = document.createElement('td'); cell.textContent = item; row.appendChild(cell);
                }
                fields.appendChild(row);
            }
            document.getElementById('correction-receipts').textContent = JSON.stringify(selected.receipts || [], null, 2);
            document.getElementById('apply-correction').disabled = selected.can_apply !== true;
            document.getElementById('dismiss-correction').disabled = selected.can_dismiss !== true;
            modal.show();
        } catch (error) { message(error.message); }
    }));
    for (const action of ['apply', 'dismiss']) {
        document.getElementById(`${action}-correction`).addEventListener('click', async event => {
            if (!selected) return;
            event.target.disabled = true;
            try {
                await post(`/api/corrections/${selected.id}/${action}`, {expected_revision: selected.revision});
                window.location.reload();
            } catch (error) { modal.hide(); message(error.message); }
        });
    }
    document.getElementById('check-corrections').addEventListener('click', async event => {
        event.target.disabled = true; message('Checking for corrections…', true);
        try {
            const result = await post('/api/corrections/check');
            if (result.result?.last_error || result.result?.error) message(result.result.last_error || result.result.error);
            else if (result.result?.status === 'scheduled') {
                message('Checking in the background. This page will refresh when the check finishes.', true);
                let checks = 0;
                const timer = window.setInterval(async () => {
                    checks += 1;
                    try {
                        const response = await fetch('/api/corrections/summary');
                        const summary = await response.json();
                        if (!summary.feed.polling || checks >= 30) {
                            window.clearInterval(timer); window.location.reload();
                        }
                    } catch (_) { window.clearInterval(timer); message('Could not refresh status. Reload this page.'); }
                }, 2000);
            } else window.location.reload();
        } catch (error) { message(error.message); }
        finally { event.target.disabled = false; }
    });
    document.getElementById('reset-corrections')?.addEventListener('click', async () => {
        if (!window.confirm('Restart from all retained feed events? Existing decisions and receipts stay available. Conflicting or reused event identities require review.')) return;
        try { await post('/api/corrections/reset', {confirm: true}); window.location.reload(); }
        catch (error) { message(error.message); }
    });
})();
