+++
title = 'ETLX'
linkTitle = 'A modern, composable ETL framework built for data engineers'
description = 'ETLX is an open-source, developer-first ETL framework designed to make data pipelines **simpler, faster, and more maintainable**.
It focuses on **clarity, portability, and performance**, allowing you to build reliable data workflows without heavy orchestration platforms or vendor lock-in.'
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

* Files (CSV, Parquet, JSON, Excel)
* Databases (Postgres, SQLite, DuckDB)
* APIs & external systems

### 🔹 Reproducible & Portable

ETLX pipelines are:

* Version-controlled
* Environment-agnostic
* Easy to run locally, in CI, or in containers

---

## 🧩 Example Workflow

```yaml
pipeline:
  - extract:
      source: postgres
      table: customers

  - transform:
      sql: |
        SELECT *
        FROM customers
        WHERE active = true

  - load:
      target: parquet
      path: data/customers.parquet
```

Run it with:

```bash
etlx run pipeline.yaml
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

---

