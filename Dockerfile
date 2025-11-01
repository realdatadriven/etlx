# ============================================
# 🛠️ Stage 1: Build etlx from Source
# ============================================
FROM golang:1.24 AS builder

WORKDIR /app

# Install build deps if needed
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CGO_ENABLED=1

# Clone etlx repository
RUN git clone --depth=1 https://github.com/realdatadriven/etlx.git .

# Build etlx binary
RUN go build -o etlx ./cmd

# ============================================
# 🚀 Stage 2: Runtime Image
# ============================================
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    unixodbc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binary
COPY --from=builder /app/etlx /usr/local/bin/etlx

# Ensure binary is executable
RUN chmod +x /usr/local/bin/etlx

# Volume mounts (db/config/env handled externally)
VOLUME ["/app/database"]

# Entry script for env/config handling
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Load env if mounted\n\
if [ -f "/app/.env" ]; then\n\
    echo "Loading environment variables from /app/.env"\n\
    set -a\n\
    source /app/.env\n\
    set +a\n\
fi\n\
\n\
# If first arg is empty, show help\n\
if [ $# -eq 0 ]; then\n\
    echo "Usage: docker run etlx [command] [args]"\n\
    echo "Run \\"docker run etlx help\\" for full CLI usage."\n\
    exit 0\n\
fi\n\
\n\
echo "Executing: etlx $@"\n\
exec /usr/local/bin/etlx "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD []

# ============================================
# 📝 Usage Instructions
#docker build --no-cache -t  etlx:latest .
#docker run -v ./.env:/app/.env:ro -v ./config.md:/app/config.md:ro -v ./database:/app/database etlx:latest --config /app/config.md
#podman tag etlx:latest docker.io/realdatadriven/etlx:latest
#podman tag etlx:latest docker.io/realdatadriven/etlx:v1.4.7
#podman login docker.io
#podman push docker.io/realdatadriven/etlx:latest
#podman push docker.io/realdatadriven/etlx:v1.4.7