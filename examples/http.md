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
active: false
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
path: "static/uploads/tmp"
active: false
```

## CSV_EXPORT

```yaml metadata
name: CSV_EXPORT
description: "Export data to CSV"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
export_sql: export
after_sql: "DETACH DB"
path: 'nyc_taxy_YYYYMMDD.csv'
tmp_prefix: 'tmp'
active: true
```

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."NYC_TAXI"
    WHERE "tpep_pickup_datetime"::DATETIME <= '{YYYY-MM-DD}'
    LIMIT 100
) TO '<fname>' (FORMAT 'csv', HEADER TRUE);
```

## XLSX_EXPORT

```yaml metadata
name: XLSX_EXPORT
description: "Export data to Excel file"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "INSTALL excel"
  - "LOAD excel"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
export_sql: xl_export
after_sql: "DETACH DB"
path: 'nyc_taxy_YYYYMMDD.xlsx'
tmp_prefix: 'tmp'
active: true
```

```sql
-- xl_export
COPY (
    SELECT *
    FROM "DB"."NYC_TAXI"
    WHERE "tpep_pickup_datetime"::DATETIME <= '{YYYY-MM-DD}'
    LIMIT 100
) TO '<fname>' (FORMAT XLSX, HEADER TRUE, SHEET 'NYC');
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
active: false
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

# NOTIFY

```yaml metadata
name: Notefication
description: "Notefication"
connection: "duckdb:"
path: "static/uploads/tmp"
active: true
```

## ETL_STATUS

```yaml metadata
name: ETL_STATUS
description: "ETL Satus"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
data_sql:
  - logs
after_sql: "DETACH DB"
type: mail #sms
to:
  - real.datadriven@gmail.com
cc: null
bcc: null
subject: 'ETLX YYYYMMDD'
body: body_tml
_body: |
  <b>Good Morning</b><br />
attachments:
  - 'nyc_taxy_YYYYMMDD.csv'
active: true
```

```html body_tml
<b>Good Morning</b><br /><br />
This email is gebnerated by ETLX automatically!<br />
<strong>LOGS:</strong>
```

```sql
-- logs
SELECT *
FROM "DB"."etlx_logs"
WHERE "ref" = '{YYYY-MM-DD}'
```
