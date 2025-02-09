# Use a minimal Ubuntu-based image
FROM ubuntu:latest

# Set the ETLX version and architecture
ARG ETLX_VERSION=v0.2.1
ARG ETLX_ARCH=amd64  # Change to arm64 if needed for ARM-based systems

# Define the download URL for the zipped release
ENV ETLX_URL="https://github.com/realdatadriven/etlx/releases/download/${ETLX_VERSION}/etlx-linux-${ETLX_ARCH}.zip"

# Install dependencies (curl for downloading, unzip for extracting, and necessary libraries)
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    ca-certificates \
    unixodbc \
    build-essential \
    libc6 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install a newer version of glibc (careful with this in production)
RUN wget http://ftp.gnu.org/gnu/libc/glibc-2.38.tar.gz && \
    tar -xvzf glibc-2.38.tar.gz && \
    cd glibc-2.38 && \
    mkdir build && \
    cd build && \
    ../configure && \
    make -j$(nproc) && \
    make install

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
