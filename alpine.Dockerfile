# Use a minimal Alpine-based image
FROM alpine:latest

# Set the ETLX version and architecture
ARG ETLX_VERSION=v0.2.1
ARG ETLX_ARCH=amd64  # Change to arm64 if needed for ARM-based systems

# Define the download URL for the zipped release
ENV ETLX_URL="https://github.com/realdatadriven/etlx/releases/download/${ETLX_VERSION}/etlx-linux-${ETLX_ARCH}.zip"

# Install dependencies (curl for downloading, unzip for extracting, libc6 is replaced by musl)
RUN apk update && apk add --no-cache \
    curl \
    unzip \
    ca-certificates \
    unixodbc \
    libc6-compat \
    bash \
    && rm -rf /var/cache/apk/*

# Set working directory
WORKDIR /app

# Download and extract the ETLX binary
RUN curl -L $ETLX_URL -o etlx.zip && \
    unzip etlx.zip && \
    rm etlx.zip && \
    mv etlx-linux-${ETLX_ARCH} /usr/local/bin/etlx && \
    chmod +x /usr/local/bin/etlx

# Allow users to mount a config file
VOLUME ["/app/config"]

# Set the entrypoint to pass CLI arguments
ENTRYPOINT ["/usr/local/bin/etlx"]
