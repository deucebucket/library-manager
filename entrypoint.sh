#!/bin/bash
# Docker entrypoint script with PUID/PGID support
# Similar to linuxserver.io pattern for UnRaid compatibility

# Default to root if not specified
PUID=${PUID:-0}
PGID=${PGID:-0}

# If running as non-root (PUID/PGID specified and not 0)
if [ "$PUID" != "0" ] && [ "$PGID" != "0" ]; then
    echo "Setting up user with PUID=$PUID and PGID=$PGID"

    # Get or create group with specified GID
    GROUP_NAME=$(getent group "$PGID" | cut -d: -f1)
    if [ -z "$GROUP_NAME" ]; then
        # GID doesn't exist, create it
        groupadd -g "$PGID" appgroup
        GROUP_NAME="appgroup"
    fi
    echo "Using group: $GROUP_NAME (GID $PGID)"

    # Create user if it doesn't exist with specified UID
    if ! getent passwd appuser > /dev/null 2>&1; then
        useradd -o -u "$PUID" -g "$PGID" -d /app -s /bin/bash appuser 2>/dev/null || true
    fi

    # Fix ownership of data directories (where logs and config live)
    chown -R "$PUID:$PGID" /data /config 2>/dev/null || true

    # Only change ownership of writable areas in /app, not the whole app
    # This prevents issues with read-only mounted volumes
    touch /app/app.log 2>/dev/null || true
    chown "$PUID:$PGID" /app/app.log 2>/dev/null || true

    # Run as the specified user
    exec gosu appuser python app.py
else
    # Run as root (default behavior, backwards compatible)
    exec python app.py
fi
