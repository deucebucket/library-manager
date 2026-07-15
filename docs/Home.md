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

- Multi-source metadata (Audnexus, OpenLibrary, Google Books, Hardcover)
- AI verification with Gemini, OpenRouter, Ollama, llama.cpp, or another OpenAI-compatible server
- Series grouping (Audiobookshelf-compatible)
- Smart narrator preservation
- Undo any fix
- Web dashboard

## Getting Help

- [GitHub Issues](https://github.com/deucebucket/library-manager/issues) - Bug reports
- [GitHub Discussions](https://github.com/deucebucket/library-manager/discussions) - Questions & ideas
