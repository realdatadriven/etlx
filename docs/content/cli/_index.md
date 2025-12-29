---
title: "CLI"
weight: 60
---

## CLI Flags

The `etlx` binary supports these flags:

- `--config`: Path to the Markdown configuration file. (Default: `config.md`)
- `--date`: Reference date in `YYYY-MM-DD` format. (Default: yesterday)
- `--only`: Comma-separated list of keys to run.
- `--skip`: Comma-separated list of keys to skip.
- `--steps`: Steps to run (`extract`, `transform`, `load`).
- `--file`: Path to a specific file to extract from.
- `--clean`: Execute `clean_sql` on selected items.
- `--drop`: Execute `drop_sql` on items.
- `--rows`: Retrieve the number of rows in the target table(s).

Usage:

```bash
etlx --config etl_config.md --date 2024-01-01 --only sales --steps extract,load
```
