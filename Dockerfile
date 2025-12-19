FROM python:3.11-slim

LABEL maintainer="deucebucket"
LABEL description="Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification"

# Set working directory
WORKDIR /app

# Install system dependencies (curl for healthcheck, ffmpeg for audio processing, chromaprint for audio fingerprinting)
RUN apt-get update && apt-get install -y --no-install-recommends curl ffmpeg libchromaprint-tools && \
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

# Environment variables
# NOTE: DATA_DIR intentionally NOT set - app auto-detects /config or /data
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production

# Expose port
EXPOSE 5757

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5757/ || exit 1

# Run the application
CMD ["python", "app.py"]
