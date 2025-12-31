+++
title = 'ETLX'
linkTitle = 'A modern, composable ETL framework built for data engineers'
description = 'ETLX is an open-source, developer-first ETL framework designed to make data pipelines **simpler, faster, and more maintainable**. It focuses on **clarity, portability, and performance**, allowing you to build reliable data workflows without heavy orchestration platforms or vendor lock-in.'
weight = 0
draft = false
+++

---

# ETLX

**A modern, composable ETL framework built for data engineers**

ETLX is an open-source, developer-first ETL framework designed to make data pipelines **simpler, faster, and more maintainable**.
It focuses on **clarity, portability, and performance**, allowing you to build reliable data workflows without heavy orchestration platforms or vendor lock-in.

---

## 🚀 Why ETLX?

Modern data stacks are powerful — but often overcomplicated.

ETLX takes a different approach:

* **Code-first, configuration-driven**
* **Database-centric**, powered by DuckDB
* **Composable pipelines**, not monolithic workflows
* **Local-first**, but production-ready

Whether you're building a one-off data pipeline or a repeatable data product, ETLX gives you full control with minimal overhead.

---

## 🧱 Core Concepts

### 🔹 Declarative Pipelines

Define what should happen, not how.
ETLX handles execution order, dependencies, and validation.

### 🔹 DuckDB at the Core

Leverage DuckDB for:

* In-process analytics
* SQL-first transformations
* Efficient joins across files, APIs, and databases

### 🔹 Multiple Data Sources

Work seamlessly with:

* Files (CSV, Parquet, JSON, Excel, ...)
* Databases (Postgres, SQLite, DuckDB, ODBC, ... any DBMS with a DuckDB Extention)
* APIs & external systems

### 🔹 Reproducible & Portable

ETLX pipelines are:

* Version-controlled
* Environment-agnostic
* Easy to run locally, in CI, or in containers

---

## 🧩 Example Workflow
````md
# INPUTS
```yaml
name: INPUTS
description: Extracts data from source and load on target
runs_as: ETL
active: true
```

## INPUT_1
```yaml
name: INPUT_1
description: Input 1 from an ODBC Source
table: INPUT_1 # Destination Table
load_conn: "duckdb:"
load_before_sql:
  - "ATTACH 'ducklake:@DL_DSN_URL' AS DL (DATA_PATH 's3://dl-bucket...')"
  - "ATTACH '@OLTP_DSN_URL' AS PG (TYPE POSTGRES)"
load_sql: load_input_in_dl
load_on_err_match_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
load_on_err_match_sql: create_input_in_dl
load_after_sql:
  - DETACH DL
  - DETACH pg
active: true
```

```sql
-- load_input_in_dl
INSERT INTO DL.INPUT_1 BY NAME
SELECT * FROM PG.INPUT_1
```

```sql
-- create_input_in_dl
CREATE TABLE DL.INPUT_1 AS
SELECT * FROM PG.INPUT_1
```
...
````
> `@DL_DSN_URL` (e.g. `mysql:db=ducklake_catalog host=your_mysql_host`) and
`@OLTP_DSN_URL` (e.g. `postgres:dbname=erpdb host=your_postgres_host user=postgres password=your_pass`) are **environment variables** used to define database connection strings.

>They can be provided through a `.env` file located at the root of the project and are automatically loaded at runtime.

>These variables allow ETLX to connect to different data sources without hardcoding credentials, making configurations portable, secure, and environment-agnostic.


Run it with:

```bash
etlx run --config pipeline.md
```

Simple. Transparent. Repeatable.

---

## 🛠️ Built for Engineers

ETLX is designed for teams who want:

* 🧠 Full control over transformations
* 🔍 Debuggable, inspectable execution
* ⚡ Fast local iteration
* 📦 Clean separation between logic and infrastructure

No black boxes. No hidden state.

---

## 🌍 Use Cases

* Data ingestion & normalization
* Analytics pipelines
* Data quality validation
* Lightweight ELT for analytics teams
* Prototyping before production pipelines


---

## 🔌 Multi-Engine by Design

While **DuckDB is the default and recommended execution engine**, ETLX is designed to be **engine-agnostic**.

Depending on your use case, pipelines can run on:

* **DuckDB** (recommended for analytics, local-first, and embedded workloads)
* **PostgreSQL**
* **SQLite**
* **MySQL / MariaDB**
* **SQL Server**
* Any engine supported through **ODBC or DuckDB extensions**

This allows ETLX to adapt to:

* Local development and experimentation
* On-prem or cloud-hosted databases
* Hybrid architectures mixing analytical and operational systems

DuckDB remains the **best fit for analytical workloads**, but ETLX does not lock you into a single execution engine.

---

## 🧠 Metadata-Driven by Design

ETLX pipelines are more than just execution instructions —
they are **structured metadata documents**.

Every pipeline definition can describe:

* Inputs and outputs
* Field-level transformations
* Business meaning and context
* Data ownership and responsibility
* Validation rules and expectations

This means your pipeline definition can also serve as:

* 📘 **Living documentation**
* 🧭 **Data lineage source**
* 📊 **Data dictionary**
* 🛡️ **Governance metadata**

All from the same configuration.

---

## 🧾 Self-Documenting Pipelines

Because ETLX pipelines are defined as structured text, they can be **parsed, analyzed, and rendered** into documentation automatically.

From a single pipeline definition, you can generate:

* Table-level and column-level lineage
* Field descriptions and transformation logic
* Source → target mappings
* Ownership and domain information
* SQL logic and derived field explanations

This makes ETLX suitable for:

* Technical documentation portals
* Data catalogs
* Governance and compliance reporting
* Automated lineage visualization

---

## 📊 Metadata → Documentation → Governance

ETLX treats metadata as a **first-class citizen**.

When properly defined, the same configuration can power:

| Capability                 | Generated From                  |
| -------------------------- | ------------------------------- |
| Data dictionary            | Field metadata                  |
| Lineage graphs             | Source & transformation mapping |
| Transformation logic docs  | SQL + metadata                  |
| Ownership & domain mapping | Dataset attributes              |
| Audit & governance views   | Execution metadata              |

In other words:

> **Your pipeline configuration becomes your documentation.**

No duplicated effort. No drift.

---

## 🧩 Designed for Automation & AI Assistance

Because ETLX configurations are structured, readable, and machine-friendly:

* They can be validated automatically
* Used to generate documentation sites
* Fed into LLMs for explanation, validation, or review
* Used to auto-generate data contracts or schema docs

This makes ETLX an ideal foundation for **metadata-driven data platforms**.

## 🧾 Full Observability & Execution Traceability

Every ETLX execution is **fully observable by design**.

Each pipeline, step, and sub-step automatically captures detailed runtime metadata, making executions transparent, auditable, and debuggable — without requiring external tooling.

For every run, ETLX records:

* ⏱ **Start time and end time**
* ⌛ **Execution duration**
* 💾 **Memory usage and resource footprint**
* ✅ **Validation results**
* ⚠️ **Warnings and failed conditions**
* 🔁 **Retries and conditional branches**
* 📍 **Execution status per step and sub-step**

This information is available at **pipeline**, **task**, and **sub-task** levels.

---

## 🔍 Fine-Grained Execution Details

Each process and sub-process exposes:

* Input and output metadata
* Validation rules applied and their results
* Conditional logic evaluation (why a step ran or was skipped)
* Error context and failure reason (when applicable)

This allows you to fully reconstruct **what happened, when, and why** — even long after execution.

---

## 📊 Built-In Operational Metadata

All execution metadata can be:

* Stored alongside pipeline results
* Queried using SQL
* Exported for observability platforms
* Used to generate execution reports

This enables:

* Performance analysis over time
* SLA and reliability tracking
* Root-cause analysis
* Auditable operational history

---

## 🧩 Configuration as the Source of Truth

ETLX treats configuration as **executable documentation**.

The same configuration that defines:

* Sources and targets
* Transformations
* Validation rules
* Conditions and dependencies

…also defines:

* What is logged
* How it is validated
* What metadata is captured
* How execution should be interpreted

This makes every pipeline **self-describing**, reproducible, and reviewable.

---

## 🧠 Designed for Observability, Governance & Scale

By combining:

* Structured configuration
* Deterministic execution
* Rich metadata capture

ETLX enables:

* End-to-end lineage generation
* Data quality reporting
* Governance and compliance workflows
* Auditable execution trails

All without introducing external orchestration complexity.

---

---

## 📄 Beyond ETL: Reporting, Document & Template Generation

ETLX is not limited to traditional extract–transform–load workflows.

Because it operates on structured data and metadata, ETLX can also be used as a **general-purpose data-driven document generator**.

This makes it suitable for producing:

* 📊 Analytical reports
* 📑 Regulatory and compliance documents
* 📈 Periodic exports and structured files
* 🧾 Human-readable and machine-readable outputs

All from the same pipeline definitions.

---

## 🧩 Structured Outputs from Structured Data

ETLX can generate and populate:

* **Spreadsheets** (Excel, CSV)
* **Formatted reports** (HTML, Markdown, PDF)
* **Machine-readable formats** (JSON, XML, YAML)
* **Templated documents** (e.g. regulatory submissions, internal reports)

Templates can be defined once and reused across executions, while the data, metadata, and transformations remain fully traceable.

---

## 📊 Reporting & Regulatory Use Cases

ETLX is well suited for:

* Regulatory submissions
* Financial and operational reporting
* Periodic disclosures
* Standardized data exchanges
* Audit and compliance documentation

By combining structured metadata with deterministic execution, ETLX ensures that generated outputs are:

* Consistent
* Reproducible
* Auditable
* Aligned with defined business rules

---

## 🧠 Metadata-Driven Templates

Templates can reference:

* Dataset fields and transformations
* Derived metrics and calculations
* Validation results and rule outcomes
* Execution metadata (timestamps, versions, sources)

This allows the same pipeline to produce both:

* The **data**
* And the **documentation explaining that data**

All from a single source of truth.

---

## 🧾 One Pipeline, Many Outputs

A single ETLX pipeline can simultaneously:

* Load and transform data
* Generate analytical tables
* Produce formatted reports
* Export structured files
* Emit metadata for governance systems

This reduces duplication, manual work, and inconsistencies across reporting layers.

---

## 🔚 Summary

ETLX is not just an ETL tool.

It is a **declarative, metadata-first execution framework** that:

* Runs across multiple SQL engines and data platforms
* Produces fully **auditable, inspectable, and reproducible** pipelines
* Captures execution metadata, validations, timings, and lineage at every step
* Bridges engineering, analytics, and governance in a single workflow
* Turns configuration into **executable documentation**
* Enables generation of **reports, datasets, and structured outputs** (tables, files, templates)
* Supports **data lineage, data dictionaries, and governance artifacts** by design

From data ingestion to reporting and documentation, ETLX provides a unified, transparent, and extensible foundation for building trustworthy data systems.

---

## 📦 Open Source & Extensible

ETLX is open source and designed to grow with your needs.

You can:

* Extend it with custom operators
* Integrate it into CI/CD
* Embed it inside larger data platforms

👉 Source code:
[https://github.com/realdatadriven/etlx](https://github.com/realdatadriven/etlx)

---

## 🚀 Getting Started

```bash
git clone https://github.com/realdatadriven/etlx
cd etlx
```

Documentation, examples, and guides are available throughout this site.

