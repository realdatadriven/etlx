# ETLX

**ETLX** is an **open-source, SQL-first data workflow engine** and an **evolving specification** for building **self-documenting data pipelines**.

Pipelines are defined using **structured Markdown**, which serves simultaneously as:

* executable configuration
* human-readable documentation
* governance and audit artifacts source

ETLX pipelines can be **executed**, **versioned**, and **rendered as documentation** — making the workflow itself the source of truth.

It combines:

* **Declarative pipelines**
* **Executable documentation**
* **Multi-engine SQL execution**
* **Built-in observability**

Powered by [**DuckDB**](https://duckdb.org), but **not locked to it**.

---

## ✨ What Makes ETLX Different?

- ✔ Pipelines are written in **Markdown + YAML + SQL**
- ✔ The pipeline **is the documentation**
- ✔ Runs on **DuckDB, PostgreSQL, SQLite, MySQL, SQL Server, ODBC**
- ✔ One specification for **ETL / ELT, data quality, report generation and automation, scripts execution, ...**
- ✔ Fully **auditable & reproducible** by design
- ✔ Available as a **CLI and embeddable Go library**

> ETLX is not just a runtime — it is also a **specification for declarative data workflows**, where **all logic is explicit, inspectable, and versionable**.

---

## 🚀 Quick Example - pipeline.md

````md
# INPUTS
```yaml
name: INPUTS
description: this defines a ETL / ELT block where every level two block with proper metadata (yaml) is treated as a step in the workflow
runs_as: ETL # the runs_as defines how the block shoud be treated
active: true # active if missing the is consider active, if false this block and all its child are ignored
```

## SALES
```yaml
name: SALES
table: sales
load_conn: "duckdb:" # Opens a DuckDB in-memory instance
load_before:
    - ATTACH 'postgres:@PG_CON' AS SRC (TYPE POSTGRES) #  Ataches data source as SRC in this case postgres OLTP DB, but could be any DBMS with a connecter / scanner
    - ATTACH 'ducklake:@DL_CON' AS TGT (DATA_PATH 's3://my-lakehouse_bucket...', ENCRYPTED) # Attaches target DB, TGT in this case a ducklake, prefirable, but again could be any DMBMS
load_validation: # Basic validation, normally used to check updates, avoid data duplication and unnessessary extractions (for more advanced conditional check use <step>_condition)
  - type: throw_if_empty # The processes will fail and be logged as such if the query returns empty
    sql: FROM SRC.<table> WHERE date_field = '{YYYY-MM-DD}' LIMIT 10 # The query that is executed
    msg: "The given date ({YYYY-MM-DD}) is not avaliable in the source!" # The message to be logged
    active: true
  - type: throw_if_not_empty # Fails if query return any row
    sql: FROM TGT.<table> WHERE date_field = '{YYYY-MM-DD}' LIMIT 10
    msg: "The date {YYYY-MM-DD} is already imported in the target, check to avoid duplications, or clean this period first!"
    active: true
load_sql: load_sales_data # Extracts from source and load on target in a sigle query thanks to duckdb capability of attaching different DBMS
load_on_err_match_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist' # In case the load data query throws an error because the table is not created yet, in runs the sql in load_on_err_match_sql
load_on_err_match_sql: create_sales_table_instead # this sql only runs in case the load data fails and the error matchs the pattern in load_on_err_match_patt
load_after:
    - DETACH SRC # detaches the source DB
    - DETACH TGT # detaches the target DB
```

<!-- INSERT -->
```sql load_sales_data
INSERT INTO TGT.<table> BY NAME
SELECT *
FROM SRC.<table>
WHERE date_field = '{YYYY-MM-DD}'
```

<!-- CREATE -->
```sql create_sales_table_instead
CREATE TABLE TGT.<table> AS
SELECT *
FROM SRC.<table>
```
...
````
> @PG_CON, @DL_CON are connection strings defined in the environment or in the `.env` file.

Run it:

```bash
etlx --config pipeline.md
```

---

## 📘 Documentation

👉 **Full documentation, concepts, and examples**
[https://realdatadriven.github.io/etlxdocs](https://realdatadriven.github.io/etlxdocs)

Includes:

* Quickstart
* Core concepts
* Specification reference
* Advanced examples
* Go API usage
* Logging & observability
* Multi-engine execution

---

## 🧠 Philosophy

ETLX embraces:

* **SQL as the transformation language**
* **Markdown as the contract**
* **Metadata as a first-class citizen**
* **Transparency over magic**

No hidden state.
No proprietary DSL.
No opaque execution model.

---

## 🤝 Contributing

ETLX is community-driven.

👉 Contribution guide:
[https://realdatadriven.github.io/etlxdocs/docs/contributing/](https://realdatadriven.github.io/etlxdocs/docs/contributing/)

---

## 📜 License

Apache License 2.0
