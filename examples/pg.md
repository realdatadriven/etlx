# ETL

<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

```yaml metadata
name: HTTP_EXTRACT
description: "Example extrating from web to a local postgres file"
connection: "postgres:user=postgres password=1234 dbname=ETLX_DATA host=localhost port=5432 sslmode=disable"
database: ETLX_DATA
active: true
```

## VERSION

```yaml metadata
name: VERSION
description: "DDB Version"
table: VERSION
load_conn: "duckdb:"
load_before_sql: "ATTACH 'user=postgres password=1234 dbname=ETLX_DATA host=localhost port=5432 sslmode=disable' AS DB (TYPE POSTGRES)"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## NYC_TAXI

```yaml metadata
name: NYC_TAXI
description: "Example extrating from web to a local postgres file"
table: NYC_TAXI
load_conn: "duckdb:"
load_before_sql: "ATTACH 'user=postgres password=1234 dbname=ETLX_DATA host=localhost port=5432 sslmode=disable' AS DB (TYPE POSTGRES)"
load_sql: load_query
load_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: false
```

```sql
-- load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT * 
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';
```

## PeadkHours

```yaml metadata
name: PeadkHours
description: Peask Hours Analysis
table: PeadkHours
transform_conn: "duckdb:"
transform_before_sql: "ATTACH 'user=postgres password=1234 dbname=ETLX_DATA host=localhost port=5432 sslmode=disable' AS DB (TYPE POSTGRES)"
transform_sql: preadk_hours_load_query
transform_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: true
```

```sql
-- preadk_hours_load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
[[PeakHoursAnalysis]]
```

<!-- markdownlint-disable MD025 -->

# LOGS

```yaml metadata
name: LOGS
description: "Example saving logs"
table: etlx_logs
connection: "duckdb:"
before_sql:
  - "ATTACH 'user=postgres password=1234 dbname=ETLX_DATA host=localhost port=5432 sslmode=disable' AS DB (TYPE POSTGRES)"
  - 'USE DB;'
  - LOAD json
  - 'get_dyn_queries[create_columns_missing]'
save_log_sql: load_logs
save_on_err_patt: '(?i)table.+does.+not.+exist'
save_on_err_sql: create_logs
after_sql:
  - 'USE memory;'
  - DETACH "DB"
active: true
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
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```

# REQUIRES

```yaml metadata
name: REQUIRES
description: "Example requires"
active: true
```

## PeakHoursAnalysis

```yaml metadata
name: PeakHoursAnalysis
description: "Analyze peak hours for NYC Yellow Taxi rides"
path: examples/PeakHoursAnalysis.sql          
```
