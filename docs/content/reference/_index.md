---
title: "Reference"
weight: 20
---

## Reference

This reference collects the core concepts and configuration keys used by ETLX. It is derived from the repository README and broken out for easier navigation.

### Core concepts

- ETL top-level section: defines `connection`, `database`, and global defaults.
- Each second-level heading under `# ETL` is an item (for example `## sales_data`) containing a `metadata` block and SQL blocks.
- Steps: `extract`, `transform`, `load` — each supports `_before_sql`, `_sql`, and `_after_sql`.
- Connections: `_conn` falls back to the main connection if null.

### Execution flow

1. Parse Markdown file and extract metadata and SQL code blocks.
2. For each ETL item, run pre-steps, main queries, validations, and post-steps.
3. Use DuckDB for execution, with optional attachments and extension loading.

Refer to other pages for detailed examples and common usage patterns.
