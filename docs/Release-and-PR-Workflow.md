# Release and PR Workflow

Documentation is part of every user-visible change. A pull request is not ready until code, UI behavior, tests, release notes, screenshots, and wiki agree.

## Required flow

1. Read `AGENTS.md`, the issue, current docs, and affected code/templates.
2. Implement focused tests for the risky path. File operations require apply, undo, path-safety, failure, and interrupted-operation coverage as applicable. Watch pre-sort changes also require split, multipart merge, flatten, settle-time, collision, unassigned-file, and hash-mutation coverage.
3. Run focused tests, `ruff check .`, the naming check, and the local integration suite for workflow changes. Exercise UI behavior in a real browser against the running app.
4. Verify the Docker build and packaging files when touched.
5. Update `CHANGELOG.md`, `README.md`, `config.example.json`, and relevant docs when behavior, configuration, or version changes.
6. Audit the GitHub wiki in an isolated wiki clone against the final branch. Remove obsolete claims, correct paths/ports/defaults/provider names, add verified behavior, and preserve useful navigation/history.
7. Verify README/docs/wiki claims against source, templates, packaging, tests, or release notes. Do not publish provider quotas, performance percentages, database sizes, or future integrations without a current source.
8. For UI changes, capture fresh screenshots from the final branch and include them in the PR. Receipt-related changes must include the History receipt list, committed receipt modal, and rollback/manual-recovery receipt views. Current references are:

   - `docs/images/transfer-receipts-history.png`
   - `docs/images/transfer-receipt-committed.png`
   - `docs/images/transfer-receipt-rollback.png`
   - `docs/images/presort-review.png`
   - `docs/images/presort-committed-receipt.png`
   - `docs/images/presort-rollback-receipt.png`
   - `docs/images/skaldleita-shared-access-settings.png`

9. In the PR description, list documentation files reviewed, tests/browser flows run, screenshots, migration/rollback notes, and intentionally omitted uncertain claims.

## Documentation checklist

- Installation matches `Dockerfile`, `docker-compose.yml`, `entrypoint.sh`, and Python support.
- Configuration matches `DEFAULT_CONFIG`, the Library/Engine/Pipeline/Integrations tabs, and secrets handling.
- Naming fields include `{asin}`, `{ripper}`, `{author_fl}`, and `{series_num.pad(N)}` when supported.
- Pipeline/provider descriptions match current configurable chains.
- File-operation claims distinguish history/undo from verified receipt behavior and never promise an absolute no-loss guarantee.
- Pre-sort claims distinguish folder merge from track concatenation/M4B conversion and document the direct-watch-child, settle-time, and downstream-Undo limits.
- Docker guidance uses port 5757, container-side paths, targeted permissions, and `docker compose`.
- ABS provider material is clearly marked proposal-only unless the provider contract is implemented and tested.
- Screenshots show the current UI/version and do not retain stale tabs, ports, provider lists, or quotas.
- The final verification date and branch/release scope are clear.

## Release handoff

Before merge, verify deployed version and published links/images. Keep the issue open until production behavior, docs, wiki, and user-facing verification are complete.
