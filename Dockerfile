FROM python:3.11-slim

LABEL maintainer="deucebucket"
LABEL description="Smart Audiobook Library Organizer with Multi-Source Metadata & AI Verification"

# Set working directory
WORKDIR /app

# Install system dependencies (curl for healthcheck, ffmpeg for audio processing)
RUN apt-get update && apt-get install -y --no-install-recommends curl ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create data directory for persistence
RUN mkdir -p /data

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production
ENV DATA_DIR=/data

# Expose port
EXPOSE 5757

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5757/ || exit 1

# Run the application
CMD ["python", "app.py"]
