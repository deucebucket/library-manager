"""Configuration management for Library Manager."""
import os
import json
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Base directory points to project root (parent of this package)
BASE_DIR = Path(__file__).parent.parent


def _detect_data_dir():
    """Auto-detect the data directory for Docker persistence.

    Priority:
    1. Explicit DATA_DIR env var (user override)
    2. Directory with existing config files (preserves user settings)
    3. Mounted volume (detects actual Docker mounts vs container dirs)
    4. /data for fresh installs (our documented default)
    5. /config if /data doesn't exist (UnRaid fallback)
    6. App directory (local development)
    """
    # If explicitly set via env var, use that
    if 'DATA_DIR' in os.environ:
        return Path(os.environ['DATA_DIR'])

    # Check for existing config files - NEVER lose user settings
    # Check both locations, prefer whichever has config
    for mount_point in ['/data', '/config']:
        mount_path = Path(mount_point)
        if mount_path.exists() and mount_path.is_dir():
            if (mount_path / 'config.json').exists() or (mount_path / 'library.db').exists():
                return mount_path

    # Fresh install: detect which is actually mounted (persistent storage)
    # os.path.ismount() returns True for Docker volume mounts
    if os.path.ismount('/data'):
        return Path('/data')
    if os.path.ismount('/config'):
        return Path('/config')

    # Fallback: prefer /data (our documented default), then /config
    if Path('/data').exists():
        return Path('/data')
    if Path('/config').exists():
        return Path('/config')

    # Fall back to app directory (local development)
    return BASE_DIR


# Initialize data directory and paths
DATA_DIR = _detect_data_dir()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / 'library.db'
CONFIG_PATH = DATA_DIR / 'config.json'
SECRETS_PATH = DATA_DIR / 'secrets.json'

DEFAULT_CONFIG = {
    "library_paths": [],  # Empty by default - user configures via Settings
    "ai_provider": "gemini",  # "gemini", "openrouter", or "ollama"
    "openrouter_model": "xiaomi/mimo-v2-flash:free",  # Best free model: 262K context
    "gemini_model": "gemma-3-27b-it",  # Gemma 3 - unlimited free tier
    "ollama_url": "http://localhost:11434",  # Ollama server URL
    "ollama_model": "llama3.2:3b",  # Default model - good for 8-12GB VRAM
    "scan_interval_hours": 6,
    "batch_size": 10,
    "max_requests_per_hour": 200,
    "auto_fix": False,
    "protect_author_changes": True,  # Require manual approval when author changes completely
    "enabled": True,
    "series_grouping": False,  # Group series: Author/Series/1 - Title (Audiobookshelf compatible)
    "ebook_management": False,  # Enable ebook organization (Beta)
    "ebook_library_mode": "merge",  # "merge" = same folder as audiobooks, "separate" = own library
    "enable_isbn_lookup": True,  # Issue #67: Extract ISBN from EPUB/PDF files for metadata lookup
    "update_channel": "beta",  # "stable", "beta", or "nightly"
    "naming_format": "author/title",  # "author/title", "author - title", "custom"
    "custom_naming_template": "{author}/{title}",  # Custom template with {author}, {title}, {series}, etc.
    "standardize_author_initials": False,  # Normalize initials: "James S A Corey" -> "James S. A. Corey" (Issue #54)
    # Metadata embedding settings
    "metadata_embedding_enabled": False,  # Embed tags into audio files when fixes are applied
    "metadata_embedding_overwrite_managed": True,  # Overwrite managed fields (title/author/series/etc)
    "metadata_embedding_backup_sidecar": True,  # Create .library-manager.tags.json backup before modifying
    # Language preference settings (Issue #17)
    "preferred_language": "en",  # ISO 639-1 code for metadata lookups
    "preserve_original_titles": True,  # Don't replace foreign language titles with English translations
    "detect_language_from_audio": False,  # Use Gemini audio analysis to detect spoken language
    "strict_language_matching": True,  # Only match books in preferred language (prevents cross-language mismatches, Issue #81)
    # UI Language (i18n) - translates UI elements only, not book metadata
    "ui_language": "en",  # ISO 639-1 code for UI translation (en, es, de, fr, pl, ru, etc.)
    # UI Theme - visual appearance
    "ui_theme": "default",  # "default" (blue/pink), "skaldleita" (gold/Norse)
    # Multi-language naming - how to name books based on their detected language
    "multilang_naming_mode": "native",      # "native" = book's language, "preferred" = user's language, "tagged" = preferred + tag
    "language_tag_enabled": False,          # Add language tag to folder names (e.g., "Title (Russian)")
    "language_tag_format": "bracket_full",  # "code" (_pl), "full" (Polish), "bracket_code" ([pl]), "bracket_full" ((Polish))
    "language_tag_position": "after_title", # "after_title", "before_title", "subfolder"
    # Trust the Process mode - fully automatic verification chain
    "trust_the_process": False,  # Auto-verify drastic changes, use audio analysis as tie-breaker, only flag truly unidentifiable
    # Book Profile System settings - progressive verification with confidence scoring
    "enable_api_lookups": True,           # Layer 2: API database lookups (Skaldleita, Audnexus, etc.)
    "enable_ai_verification": True,       # Layer 3: AI verification (uses configured provider)
    "enable_audio_analysis": False,       # Layer 4: Audio analysis (requires Gemini API key)
    "enable_content_analysis": False,      # Layer 4 sub-option: Content analysis (deeper audio analysis)
    "use_skaldleita_for_audio": True,      # Use Skaldleita GPU Whisper for audio identification (faster, no rate limits)
    # DEPRECATED: use_bookdb_for_audio - kept for backwards compatibility, use use_skaldleita_for_audio instead
    # Skaldleita Trust Mode - controls how much LM trusts SL audio identification
    "sl_trust_mode": "full",               # "full" = trust 80%+ audio ID, "boost" = verify with APIs, "legacy" = use AI fallback
    "sl_confidence_threshold": 80,         # Minimum confidence to trust SL audio ID without AI verification
    # Provider Chains - ordered lists of providers to try (first = primary, rest = fallbacks)
    # Audio providers: "bookdb" (Skaldleita), "gemini", "openrouter", "ollama"
    # Text providers: "gemini", "openrouter", "ollama"
    "audio_provider_chain": ["bookdb", "gemini"],  # Order to try audio identification (bookdb = Skaldleita)
    "text_provider_chain": ["gemini", "openrouter"],  # Order to try text-based AI
    "deep_scan_mode": False,              # Always use all enabled layers regardless of confidence
    "profile_confidence_threshold": 85,   # Minimum confidence to skip remaining layers (0-100)
    "multibook_ai_fallback": True,         # Use AI for ambiguous chapter/multibook detection
    "skip_confirmations": False,           # Skip confirmation dialogs in Library view for faster workflow
    # Anonymous error reporting - helps improve the app
    "anonymous_error_reporting": False,    # Opt-in: send anonymous error reports to help fix bugs
    "error_reporting_include_titles": True, # Include book title/author ONLY when they caused the error
    # Community contribution - crowdsourced audiobook metadata
    "contribute_to_community": False,      # Opt-in: share audio-extracted metadata (author/title/narrator) to help others
    # P2P Cache - decentralized book lookup cache via Gun.db
    "enable_p2p_cache": False,             # Opt-in: share book lookup cache with other Library Manager users (helps when Skaldleita is down)
    # Watch Folder settings - monitor a folder for new audiobooks and organize them
    "watch_mode": False,                   # Enable folder watching
    "watch_folder": "",                    # Path to monitor for new audiobooks
    "watch_output_folder": "",             # Where to move organized books (empty = same as library_paths[0])
    "watch_use_hard_links": False,         # Hard link instead of move (saves space, same filesystem only)
    "watch_interval_seconds": 60,          # How often to check for new files
    "watch_delete_empty_folders": True,    # Remove empty source folders after moving
    "watch_min_file_age_seconds": 30       # Minimum file age before processing (wait for downloads to complete)
}

DEFAULT_SECRETS = {
    "openrouter_api_key": "",
    "gemini_api_key": "",
    "bookdb_api_key": "",  # Optional API key for Skaldleita (not required for public endpoints)
    "abs_api_token": ""
}


def use_skaldleita_for_audio(config: dict) -> bool:
    """Check if Skaldleita should handle audio identification.

    Handles backwards compatibility: checks both new name (use_skaldleita_for_audio)
    and deprecated name (use_bookdb_for_audio).

    Args:
        config: Configuration dictionary

    Returns:
        True if Skaldleita should handle audio, False for local processing
    """
    # Check new name first, fall back to old name for backwards compatibility
    if 'use_skaldleita_for_audio' in config:
        return config['use_skaldleita_for_audio']
    if 'use_bookdb_for_audio' in config:
        return config['use_bookdb_for_audio']
    return True  # Default: use Skaldleita


def migrate_legacy_config():
    """Migrate config files from old locations to DATA_DIR.

    Issue #23: In beta.23 we fixed config paths to use DATA_DIR instead of BASE_DIR,
    but didn't migrate existing configs. Users updating from older versions would
    lose their config because the app looked in the new location.

    Also checks /config and /data for UnRaid vs standard Docker setups.
    """
    if DATA_DIR == BASE_DIR:
        return  # Not running with separate data dir, nothing to migrate

    migrate_files = ['config.json', 'secrets.json', 'library.db', 'user_groups.json']

    # Check multiple possible legacy locations
    # - BASE_DIR: old versions stored in app directory
    # - /data: our default Docker mount (user might have switched to /config)
    # - /config: UnRaid default (user might have switched to /data)
    legacy_locations = [BASE_DIR]
    if DATA_DIR != Path('/data') and Path('/data').exists():
        legacy_locations.append(Path('/data'))
    if DATA_DIR != Path('/config') and Path('/config').exists():
        legacy_locations.append(Path('/config'))

    for filename in migrate_files:
        new_path = DATA_DIR / filename

        # Skip if already exists in target location
        if new_path.exists():
            continue

        # Try to find file in legacy locations
        for legacy_dir in legacy_locations:
            old_path = legacy_dir / filename
            if old_path.exists():
                try:
                    shutil.copy2(old_path, new_path)
                    logger.info(f"Migrated {filename} from {old_path} to {new_path}")
                    break  # Found and migrated, stop looking
                except Exception as e:
                    logger.warning(f"Failed to migrate {filename} from {old_path}: {e}")


def init_config():
    """Create default config files if they don't exist."""
    if not CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        logger.info(f"Created default config at {CONFIG_PATH}")

    if not SECRETS_PATH.exists():
        with open(SECRETS_PATH, 'w') as f:
            json.dump(DEFAULT_SECRETS, f, indent=2)
        logger.info(f"Created default secrets at {SECRETS_PATH}")


def needs_setup(config):
    """Check if app needs initial setup wizard.

    Returns True only for fresh installs that haven't been configured.
    Existing users who update will NOT see the wizard.

    Logic:
    - If user already has library_paths configured -> skip wizard
    - If setup_completed flag is set -> skip wizard
    - Otherwise (fresh install with no paths) -> show wizard
    """
    # Existing users already have library_paths - don't show wizard
    paths = config.get('library_paths', [])
    if paths and len(paths) > 0:
        return False  # Already configured = skip wizard

    # New flag for explicit wizard completion
    if config.get('setup_completed', False):
        return False

    # Fresh install with no paths = show wizard
    return True


def load_config():
    """Load configuration and secrets from files."""
    config = DEFAULT_CONFIG.copy()

    # Load main config
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            logger.warning(f"Error loading config: {e}")

    # Load secrets (API keys)
    if SECRETS_PATH.exists():
        try:
            with open(SECRETS_PATH) as f:
                secrets = json.load(f)
                config.update(secrets)
        except Exception as e:
            logger.warning(f"Error loading secrets: {e}")

    return config


def save_config(config):
    """Save configuration to file (excludes secrets)."""
    # Separate secrets from config
    secrets_keys = ['openrouter_api_key', 'gemini_api_key', 'google_books_api_key', 'abs_api_token']
    config_only = {k: v for k, v in config.items() if k not in secrets_keys}

    with open(CONFIG_PATH, 'w') as f:
        json.dump(config_only, f, indent=2)


def save_secrets(secrets):
    """Save API keys to secrets file."""
    with open(SECRETS_PATH, 'w') as f:
        json.dump(secrets, f, indent=2)


def load_secrets():
    """Load API keys from secrets file."""
    if SECRETS_PATH.exists():
        try:
            with open(SECRETS_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading secrets: {e}")
    return {}


# Export what app.py needs
__all__ = [
    'BASE_DIR', 'DATA_DIR', 'DB_PATH', 'CONFIG_PATH', 'SECRETS_PATH',
    'DEFAULT_CONFIG', 'DEFAULT_SECRETS',
    'migrate_legacy_config', 'init_config', 'needs_setup',
    'load_config', 'save_config', 'save_secrets', 'load_secrets'
]
