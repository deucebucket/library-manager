# Library Manager Wiki

**Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification**

Welcome! This wiki contains detailed documentation for Library Manager.

## Quick Links

- [[Installation]] - Get up and running
- [[Docker Setup]] - Docker/UnRaid/Synology guide
- [[Configuration]] - All settings explained
- [[How It Works]] - Understanding the metadata pipeline
- [[Troubleshooting]] - Common issues and fixes
- [[FAQ]] - Frequently asked questions
- [[Release and PR Workflow]] - Documentation, screenshot, and verification requirements

## What Is This?

Library Manager automatically fixes messy audiobook folder names using real book databases + AI verification.

**Before:**
```
├── Shards of Earth/Adrian Tchaikovsky/     # Swapped!
├── Boyett/The Hollow Man/                   # Missing first name
├── The Expanse 2019/Leviathan Wakes/       # Year in wrong place
└── Unknown/Mistborn Book 1/                 # No author
```

**After:**
```
├── Adrian Tchaikovsky/Shards of Earth/
├── Steven Boyett/The Hollow Man/
├── James S.A. Corey/Leviathan Wakes/
└── Brandon Sanderson/Mistborn/1 - The Final Empire/
```

## Features

- Multi-source metadata (Skaldleita/BookDB, Audnexus, OpenLibrary, Google Books, Hardcover)
- AI verification with Gemini, OpenRouter, Ollama, llama.cpp, or another OpenAI-compatible server
- Series grouping (Audiobookshelf-compatible)
- Smart narrator preservation
- Undo any fix and inspect transfer receipts in History
- Verified watch-folder pre-sort for multi-book splits, multipart folder merges, and redundant nesting
- Web dashboard

The develop branch currently reports beta.170. The loopback video organizer now creates or tightens its receipt ledger as one mode-0600 regular file before schema initialization or request handling and refuses symlink, non-regular, or hard-linked paths. Exact Jellyfin 10.11 video-move verification scans only the configured destination library with a bounded exact-identity check instead of relying on a removed server-side provider filter. Skaldleita works without signup through a heavily per-IP-limited shared key, while signed client/version policy keeps blocked or outdated instances denied before either shared or personal key authorization. The #292/#298 pre-sort and beta.161 #300 apply-fix flows retain exact, hash-verified transfer and rollback receipts. These controls improve auditability but are not a substitute for filesystem or hardware backups.

![Verified pre-sort review](images/presort-review.png)

## Getting Help

- [GitHub Issues](https://github.com/deucebucket/library-manager/issues) - Bug reports
- [GitHub Discussions](https://github.com/deucebucket/library-manager/discussions) - Questions & ideas
