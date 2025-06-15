# ============================================
# 🛠️ Stage 1: Build ETLX from Source
# ============================================
FROM golang:1.24 as builder

# Set working directory inside the container
WORKDIR /app

# Install system dependencies required for building
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Enable CGO for ODBC support
ENV CGO_ENABLED=1

# Clone the ETLX repository
RUN git clone --depth=1 https://github.com/realdatadriven/etlx.git .

# Build the ETLX binary
#RUN CGO_ENABLED=1 go build -o etlx ./cmd/main.go
RUN CGO_ENABLED=1 \
    CGO_LDFLAGS="-lstdc++ -lm -ldl" \
    go build -o etlx ./cmd/main.go

# ============================================
# 🚀 Stage 2: Create Minimal Runtime Image
# ============================================
FROM debian:bookworm-slim

# Install runtime dependencies (unixODBC)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    unixodbc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the compiled ETLX binary from the builder stage
COPY --from=builder /app/etlx /usr/local/bin/etlx

# Ensure the binary is executable
RUN chmod +x /usr/local/bin/etlx

# Allow users to mount a config file
VOLUME ["/app/config", "/app/data"]

# Set the entrypoint to pass CLI arguments
ENTRYPOINT ["/usr/local/bin/etlx"]

#docker build -t etlx:latest .
#docker run --rm etlx --help
#docker run --rm --env-file $(pwd)/.env -v $(pwd)/examples/s3.md:/app/config.md etlx --config /app/config.md --date 2024-01-01
#docker run --rm --env-file $(pwd)/.env -v $(pwd)/examples:/app/examples etlx --config /app/s3.md --date 2024-01-01

