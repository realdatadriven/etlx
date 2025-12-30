+++
title = 'Future Plans'
weight = 80
draft = false
+++

## Future Plans

ETLX is a powerful tool for defining and executing ETL processes using Markdown configuration files. It supports complex SQL queries, exports to multiple formats, and dynamic configuration loading. ETLX can be used as a library, CLI tool, or integrated into other systems for advanced data workflows.

### To-Do List

#### ✅ Completed

- **Config Parsing**:
  - Parses and validates Markdown configurations with nested sections and metadata.
  - Supports YAML, TOML, and JSON for metadata.

- **ETL Execution**:
  - Modular handling of extract, transform, and load processes.
  - Flexible step configuration with before and after SQL.

- **Query Documentation**:
  - Handles complex SQL queries by breaking them into logical components.
  - Dynamically combines query parts to create executable SQL.

- **Exports**:
  - Supports exporting data to files in formats like CSV and Excel.
  - Includes options for templates and data mapping.

- **Requires**:
  - Loads additional configurations dynamically from files or database queries.
  - Integrates loaded configurations into the main process.

- **CLI Interface**:
  - Provides a command-line interface for running configurations.
  - Accepts flags for custom execution parameters.

#### 🕒 To-Do

- **Web API**:

  - Create a RESTful web API for executing ETL configurations.
  - Expose endpoints for:
    - Uploading and managing configurations.
    - Triggering ETL workflows.
    - Monitoring job status and logs.
  - Add support for multi-user environments with authentication and authorization.
