+++
title = 'Logs'
weight = 48
draft = false
+++

# Logs Handling (`# LOGS`)

ETLX provides a **logging mechanism** that allows **saving logs** into a database. This is useful for tracking **executions, debugging, and auditing ETL processes**.

---

# **🔹 How It Works**

- The `LOGS` section defines where and how logs should be saved.
- The process consists of **three main steps**:
  1. **Prepare the environment** using `before_sql` (e.g., loading extensions, attaching databases).
  2. **Execute `save_log_sql`** to store logs in the database.
  3. **Run `after_sql`** for cleanup (e.g., detaching the database).

# **🛠 Example LOGS Configuration**

Below is an example that **saves logs** into a **database**:

## **📄 LOGS Markdown Configuration**

````md
# LOGS
```yaml metadata
name: LOGS
description: "Example saving logs"
table: logs
connection: "duckdb:"
before_sql:
  - load_extentions
  - attach_db
save_log_sql: load_logs
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: create_logs
after_sql: detach_db
tmp_dir: tmp
active: true
```

```sql
-- load_extentions
INSTALL Sqlite;
LOAD Sqlite;
INSTALL json;
LOAD json;
```

```sql
-- attach_db
ATTACH 'examples/S3_EXTRACT.db' AS "DB" (TYPE SQLITE)
```

```sql
-- detach_db
DETACH "DB";
```

```sql
-- load_logs
INSERT INTO "DB"."<table>" BY NAME
SELECT * 
FROM READ_JSON('<fname>');
```

```sql
-- create_logs
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT * 
FROM READ_JSON('<fname>');
```
````

---

# **🔹 How to Use**

- This example saves logs into a **SQLite database attached to DuckDB**.
- The **log table (`logs`) is created or replaced** on each run.
- The `<table>` and `<fname>` placeholders are dynamically replaced.

# **🎯 Summary**

✔ **Keeps a persistent log of ETL executions**  
✔ **Uses DuckDB for efficient log storage**  
✔ **Supports preprocessing (`before_sql`) and cleanup (`after_sql`)**  
✔ **Highly customizable to different logging needs**

# Default logs

By default is generated a sqlite db `etlx_logs.db` in temp folder, that'll depende on the OS, it adds to your config this peace os md:

````markdown
# AUTO_LOGS

```yaml metadata
name: LOGS
description: "Logging"
table: logs
connection: "duckdb:"
before_sql:
  - "LOAD Sqlite"
  - "ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE)"
  - "USE etlx_logs"
  - "LOAD json"
  - "get_dyn_queries[create_missing_columns](ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE),DETACH etlx_logs)"
save_log_sql: |
  INSERT INTO "etlx_logs"."<table>" BY NAME
  SELECT *
  FROM READ_JSON('<fname>');
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: |
  CREATE TABLE "etlx_logs"."<table>" AS
  SELECT *
  FROM READ_JSON('<fname>');
after_sql:
  - 'USE memory'
  - 'DETACH "etlx_logs"'
active: true
```

```sql
-- create_missing_columns
WITH source_columns AS (
    SELECT "column_name", "column_type"
    FROM (DESCRIBE SELECT * FROM READ_JSON('<fname>'))
),
destination_columns AS (
    SELECT "column_name", "data_type" as "column_type"
    FROM "duckdb_columns"
    WHERE "table_name" = '<table>'
),
missing_columns AS (
    SELECT "s"."column_name", "s"."column_type"
    FROM source_columns "s"
    LEFT JOIN destination_columns "d" ON "s"."column_name" = "d"."column_name"
    WHERE "d"."column_name" IS NULL
)
SELECT 'ALTER TABLE "etlx_logs"."<table>" ADD COLUMN "' || "column_name" || '" ' || "column_type" || ';' AS "query"
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```
````
---

But it can be overiden to be saved on your own database of choice by changing `ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE)`