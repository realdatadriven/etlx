+++
title = 'Running ETLX'
weight = 22
draft = false
+++

### Running ETLX

The binary supports the following flags:

- `--config`: Path to the Markdown configuration file. *(Default: `config.md`)*
- `--date`: Reference date for the ETL process in `YYYY-MM-DD` format. *(Default: yesterday's date)*
- `--only`: Comma-separated list of keys to run.
- `--skip`: Comma-separated list of keys to skip.
- `--steps`: Steps to run within the ETL process (`extract`, `transform`, `load`).
- `--file`: Path to a specific file to extract data from. Typically used with the `--only` flag.
- `--clean`: Execute `clean_sql` on items (conditional based on `--only` and `--skip`).
- `--drop`: Execute `drop_sql` on items (conditional based on `--only` and `--skip`).
- `--rows`: Retrieve the number of rows in the target table(s).

```bash
etlx --config etl_config.md --date 2023-10-31 --only sales --steps extract,load
```

---

### 🐳 Running ETLX with Docker

You can run **etlx** directly from Docker without installing Go or building locally.

#### Build the Image

Clone the repo and build:

```bash
docker build -t etlx:latest .
```

Or pull the prebuilt image (when published):

```bash
docker pull docker.io/realdatadriven/etlx:latest
```

#### Running Commands

The image behaves exactly like the CLI binary. For example:

```bash
docker run --rm etlx:latest help
docker run --rm etlx:latest version
docker run --rm etlx:latest run --config /app/config.md
```

#### Using a `.env` File

If you have a `.env` file with environment variables, mount it into `/app/.env`:

```bash
docker run --rm \
  -v $(pwd)/.env:/app/.env:ro \
  etlx:latest run --config /app/config.md
```

#### Mounting Config Files

Mount your config file into the container and reference it by path:

```bash
docker run --rm \
  -v $(pwd)/config.md:/app/config.md:ro \
  etlx:latest run --config /app/config.md
```

#### Database Directory

`etlx` can attach a database directory. Mount your local `./database` directory into `/app/database`:

```bash
docker run --rm \
  -v $(pwd)/database:/app/database \
  etlx:latest run --config /app/config.md
```

#### Combine All Together

Mount `.env`, config, and database directory:

```bash
docker run --rm \
  -v $(pwd)/.env:/app/.env:ro \
  -v $(pwd)/config.md:/app/config.md:ro \
  -v $(pwd)/database:/app/database \
  etlx:latest run --config /app/config.md
```

#### Interactive Mode

For interactive subcommands (like `repl`):

```bash
docker run -it --rm etlx:latest repl
```

#### 💡 Pro Tip: Local Alias

You can add an alias so Docker feels like the native binary:

```bash
alias etlx="docker run --rm -v $(pwd):/app etlx:latest"
```

Now you can just run:

```bash
etlx help
etlx run --config /app/config.md
```

---

### How It Works

Create a Markdown file with the ETL process configuration. For example, see the example use case in the examples section.
