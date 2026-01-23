FROM python:3.11-slim

LABEL maintainer="deucebucket"
LABEL description="Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification"

# Set working directory
WORKDIR /app

# Install system dependencies
# - curl: healthcheck
# - ffmpeg: audio processing
# - libchromaprint-tools: audio fingerprinting
# - gosu: drop privileges for PUID/PGID support (UnRaid compatibility)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ffmpeg libchromaprint-tools gosu && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create common persistent storage directories
# /data = our default, /config = UnRaid default
# App auto-detects which one is mounted/has config
RUN mkdir -p /data /config

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Environment variables
# NOTE: DATA_DIR intentionally NOT set - app auto-detects /config or /data
# PUID/PGID default to 0 (root) for backwards compatibility
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production
ENV PUID=0
ENV PGID=0

# Expose port
EXPOSE 5757

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5757/ || exit 1

# Use entrypoint for PUID/PGID handling
ENTRYPOINT ["/app/entrypoint.sh"]
