#!/bin/bash
# Docker entrypoint script with PUID/PGID support
# Similar to linuxserver.io pattern for UnRaid compatibility

# Default to root if not specified
PUID=${PUID:-0}
PGID=${PGID:-0}

# If running as non-root (PUID/PGID specified and not 0)
if [ "$PUID" != "0" ] && [ "$PGID" != "0" ]; then
    echo "Setting up user with PUID=$PUID and PGID=$PGID"

    # Create group if it doesn't exist
    if ! getent group appgroup > /dev/null 2>&1; then
        groupadd -g "$PGID" appgroup
    fi

    # Create user if it doesn't exist
    if ! getent passwd appuser > /dev/null 2>&1; then
        useradd -u "$PUID" -g "$PGID" -d /app -s /bin/bash appuser
    fi

    # Fix ownership of data directories
    chown -R appuser:appgroup /data /config 2>/dev/null || true
    chown -R appuser:appgroup /app 2>/dev/null || true

    # Run as the specified user
    exec gosu appuser python app.py
else
    # Run as root (default behavior, backwards compatible)
    exec python app.py
fi
