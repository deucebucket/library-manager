# Contributing to Library Manager

Thanks for your interest in contributing! We're building this together.

## Looking for Something to Work On?

Check the current [open issues](https://github.com/deucebucket/library-manager/issues) and [open pull requests](https://github.com/deucebucket/library-manager/pulls). Older feature-branch references and issue-specific design plans may no longer describe active work; verify the current branch and source before implementing one.

---

## Starting Fresh? Here's how to get started.

## Branch Strategy

```
main     = stable releases (Docker builds from here)
develop  = active development (PRs merge here)
feature/ = your feature branches
```

**All PRs should target `develop`**, not main. We merge develop → main for releases.

## Getting Started

1. Fork the repo
2. Clone and set upstream:
   ```bash
   git clone https://github.com/YOUR_USERNAME/library-manager.git
   git remote add upstream https://github.com/deucebucket/library-manager.git
   ```
3. Create feature branch from develop:
   ```bash
   git checkout develop
   git pull upstream develop
   git checkout -b feature/my-feature
   ```
4. Make your changes
5. Test thoroughly (see below)
6. Update README/docs/wiki and release notes when behavior or user-facing UI changes
7. Push and create PR to `develop`

## Staying Updated

Before submitting your PR:
```bash
git fetch upstream
git rebase upstream/develop
```

## Development Setup

```bash
git clone https://github.com/YOUR_USERNAME/library-manager.git
cd library-manager
python -m pip install -r requirements.txt
python app.py
```

## Testing

Before submitting:
```bash
# Run integration tests
./test-env/run-integration-tests.sh --local

# Browser/UI verification against the running app
# 1. Web UI loads at http://localhost:5757
# 2. Setup/settings page saves correctly
# 3. Library scan finds audiobooks
# 4. Queue review and apply fix move files correctly
# 5. History shows the transfer receipt and inventory modal
# 6. Undo restores the original location
# 7. Failure/interruption paths leave a verified rollback or explicit manual-recovery state

# Static checks
ruff check .
python test-env/test-naming-issues.py
docker build -t library-manager .
```

Use the actual browser UI for workflow verification; API-only checks do not replace the user-facing flow. For file-operation changes, test source/destination hashes, non-empty destination refusal, database failure rollback, startup recovery, and receipt display.

## PR Guidelines

- Keep PRs focused - one feature/fix per PR
- Update CHANGELOG.md with your changes
- If adding new config options, update config.example.json
- Update README.md and relevant docs when behavior, configuration, provider support, or UI changes
- Audit the GitHub wiki in an isolated clone and include the verification result in the PR
- Add fresh screenshots for UI changes; receipt changes require History, committed receipt, and rollback/manual-recovery views
- Test with Docker: `docker build -t library-manager .`

## Code Style

- Python 3.9+ compatible
- Use existing patterns in the codebase
- Comments for non-obvious logic
- Meaningful variable names

---

## Security Review

All PRs are reviewed for security before merge.

### We Check For:

**Malware/Exploits:**
- Path traversal (file ops outside library paths)
- Unauthorized network calls
- Command injection
- Credential exposure
- Destructive operations without confirmation
- Obfuscated code (base64, eval, exec)

**Sabotage:**
- Logic that fails in edge cases
- Data scrambling (swapping author/title)
- Bad default values
- Fake error messages
- Subtle data corruption

### File Operation Rules:
- All file writes MUST stay within configured `library_paths`
- Metadata changes MUST preserve originals in backup
- Rollback MUST actually restore original state
- Apply-fix moves MUST preserve and verify the source/destination/rollback inventory receipt; ambiguous or unverifiable states require manual recovery
- Never trust paths without validation

This isn't about distrust - it's about protecting users' libraries. Open source means anyone can contribute, which is great, but it also means we verify everything.

---

## Questions?

Open an issue or start a discussion on GitHub.

---

## License

By contributing, you agree that your contributions will be licensed under the **MIT License**.

This means:
- Your contributions may be used, modified, distributed, sublicensed, and sold
- Private and commercial modifications are permitted
- The copyright and license notice must be retained
