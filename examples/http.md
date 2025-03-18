# ETL

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

```yaml metadata
name: HTTP_EXTRACT
description: "Example extrating from web to a local sqlite3 file"
connection: "duckdb:"
active: true
```

## VERSION

```yaml metadata
name: VERSION
description: "DDB Version"
table: VERSION
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## NYC_TAXI

```yaml metadata
name: NYC_TAXI
description: "Example extrating from web to a local sqlite3 file"
table: NYC_TAXI
load_conn: "duckdb:"
load_before_sql:
  - load_extentions
  - attach_db
load_sql: load_query
load_after_sql: detach_db
drop_sql: drop_sql
clean_sql: clean_sql
rows_sql: nrows
active: true
```

```sql
-- load_extentions
INSTALL sqlite;
LOAD sqlite;
```

```sql
-- attach_db
ATTACH 'database/HTTP_EXTRACT.db' AS "DB" (TYPE SQLITE)
```

```sql
-- detach_db
DETACH "DB";
```

```sql
-- load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT * 
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';
```

```sql
-- drop_sql
DROP TABLE IF EXISTS "DB"."<table>";
```

```sql
-- clean_sql
DELETE FROM "DB"."<table>";
```

```sql
-- nrows
SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
```

# EXPORTS

Exports data to files.

```yaml metadata
name: DailyReports
description: "Daily reports"
connection: "duckdb:"
path: "~/Downloads/YYYYMMDD"
active: true
```

## CSV_EXPORT

```yaml metadata
name: CSV_EXPORT
description: "Export data to CSV."
connection: "duckdb:"
export_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "INSTALL excel"
  - "LOAD excel"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
  - export
  - "DETACH DB"
active: true
```

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."NYC_TAXI"
    WHERE "tpep_pickup_datetime"::DATETIME <= '{YYYY-MM-DD}'
    LIMIT 100
) TO '~/Downloads/YYYYMMDD/nyc_taxy_YYYYMMDD.csv' (FORMAT 'csv', HEADER TRUE);
```

## XLSX_EXPORT

```yaml metadata
name: XLSX_EXPORT
description: "Export data to Excel file."
connection: "duckdb:"
export_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "INSTALL excel"
  - "LOAD excel"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
  - export
  - "DETACH DB"
active: true
```

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."NYC_TAXI"
    WHERE "tpep_pickup_datetime"::DATETIME <= '{YYYY-MM-DD}'
    LIMIT 100
) TO '~/Downloads/YYYYMMDD/nyc_taxy_YYYYMMDD.xlsx' (FORMAT XLSX, HEADER TRUE, SHEET 'Sheet1');
```

# LOGS

```yaml metadata
name: LOGS
description: "Example saving logs"
table: etlx_logs
connection: "duckdb:"
before_sql:
  - load_extentions
  - attach_db
  - 'USE DB;'
save_log_sql: load_logs
save_on_err_patt: '(?i)table.+does.+not.+exist|does.+not.+have.+column.+with.+name'
save_on_err_sql:
  - create_logs
  - get_dyn_queries[create_columns_missing]
  - load_logs
after_sql:
  - 'USE memory;'
  - detach_db
tmp_dir: /tmp
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
ATTACH 'database/HTTP_EXTRACT.db' AS "DB" (TYPE SQLITE)
```

```sql
-- detach_db
DETACH "DB";
```

```sql
-- load_logs
INSERT INTO "DB"."<table>" BY NAME
SELECT * 
FROM read_json('<fname>');
```

```sql
-- create_logs
CREATE TABLE IF NOT EXISTS "DB"."<table>" AS
SELECT * 
FROM read_json('<fname>');
```

```sql
-- create_columns_missing
WITH source_columns AS (
    SELECT column_name, column_type 
    FROM (DESCRIBE SELECT * FROM read_json('<fname>'))
),
destination_columns AS (
    SELECT column_name, data_type as column_type
    FROM duckdb_columns 
    WHERE table_name = '<table>'
),
missing_columns AS (
    SELECT s.column_name, s.column_type
    FROM source_columns s
    LEFT JOIN destination_columns d ON s.column_name = d.column_name
    WHERE d.column_name IS NULL
)
SELECT 'ALTER TABLE "DB"."<table>" ADD COLUMN "' || column_name || '" ' || column_type || ';' AS query
FROM missing_columns;
```
