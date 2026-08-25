# Repository Guidelines

## Project Structure & Module Organization

`app.py` is the main Flask application entry point. Core reusable code lives in `library_manager/`, with providers in `library_manager/providers/`, pipeline stages in `library_manager/pipeline/`, models in `library_manager/models/`, and shared utilities in `library_manager/utils/`. Jinja templates are in `templates/`; browser assets are in `static/css/` and `static/js/`. Documentation lives under `docs/`, Docker and Unraid packaging are in `Dockerfile`, `docker-compose.yml`, and `unraid/`, and plugin examples are under `examples/plugins/`. Integration and workflow tests are concentrated in `test-env/`; generated test libraries, local databases, logs, and secrets should not be treated as source. Checked-in documentation images under `docs/images/` are maintained user-facing assets and must be refreshed when UI behavior changes.

## Build, Test, and Development Commands

```bash
pip install -r requirements.txt
python app.py
```

Install dependencies and run the app locally at `http://localhost:5757`.

```bash
ruff check .
python test-env/test-naming-issues.py
./test-env/run-integration-tests.sh --local
docker build -t library-manager .
```

Use Ruff for Python lint checks, run the naming test used by CI, run the local integration suite when behavior changes affect workflows, and verify Docker builds before PRs that touch runtime or packaging.

For every user-visible change, also verify README/docs/wiki claims against the final branch. UI changes require fresh browser screenshots from the final branch; transfer-receipt changes require History, committed-receipt, and rollback/manual-recovery views. See [`docs/Release-and-PR-Workflow.md`](docs/Release-and-PR-Workflow.md).

## Coding Style & Naming Conventions

Target Python 3.9+ and follow existing module patterns. Use 4-space indentation, descriptive snake_case for functions and variables, PascalCase for classes, and uppercase constants. Keep Flask routes, database access, and pipeline/provider logic separated when practical. Ruff is configured in `ruff.toml` for `E`, `F`, and `W` rules while tolerating some legacy patterns such as long lines and Flask import ordering.

## Testing Guidelines

Name Python tests as `test-*.py` in `test-env/`. Prefer focused tests for parsing, naming, validation, and provider behavior; use `run-integration-tests.sh` for UI/API workflow coverage. For file-operation changes, test apply, undo, and path-safety behavior against realistic audiobook folder layouts.

## Commit & Pull Request Guidelines

Recent commits use concise prefixes such as `fix:`, `feat:`, `docs:`, or `Fix #211:` with issue references when available. Branch from `develop`, keep each PR focused, and target `develop` unless maintainers say otherwise. PRs should include a summary, change list, testing notes, linked issues, documentation/wiki verification, and fresh screenshots for UI changes. Update `CHANGELOG.md`, `config.example.json`, README/docs, and `APP_VERSION` in `app.py` when code changes require them. Use the actual browser UI for workflow verification; API-only checks do not replace it.

## Issue Lifecycle

Do **not** close an issue just because the related PR has merged. Issues are closed only when the maintainer confirms the fix/feature is working in production and all wrap-up work (docs, follow-up tests, user verification) is complete. Use linking keywords (`Closes #NNN`) in PR descriptions for traceability, but keep the issue open until wrap-up is confirmed.

## Security & Configuration Tips

Never commit `config.json`, `secrets.json`, local databases, logs, or generated test libraries. Checked-in documentation images belong under `docs/images/`; do not add screenshots containing secrets or private library paths. Validate paths before file writes, keep operations inside configured library paths, preserve rollback data and receipts, and avoid shell execution or network calls unless they are explicit, reviewed behavior.
