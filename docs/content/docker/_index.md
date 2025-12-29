---
title: "Docker"
weight: 70
---

## Docker

You can run ETLX inside Docker without Go installed.

Build the image:

```bash
docker build -t etlx:latest .
```

Run commands:

```bash
docker run --rm etlx:latest help
docker run --rm etlx:latest version
docker run --rm etlx:latest run --config /app/config.md
```

Mounting examples (config, .env, database):

```bash
docker run --rm \
  -v $(pwd)/.env:/app/.env:ro \
  -v $(pwd)/config.md:/app/config.md:ro \
  -v $(pwd)/database:/app/database \
  etlx:latest run --config /app/config.md
```
