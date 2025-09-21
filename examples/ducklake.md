# GENERATE_SAMPLE_DATA

ex source [https://www.youtube.com/watch?v=NbnEVFAtx9o&ab_channel=JoeReis](DuckLake w/ Hannes Mühleisen - Practical Data Lunch and Learn)

```yaml metadata
name: GENERATE_SAMPLE_DATA
runs_as: SCRIPTS
description: Here we are just going to generate a sample databese for the exercise mimicing a real traditional database
connection: "duckdb:database/sample.duckdb"
active: false
```

## SAMPLE_DB

```yaml metadata
name: SAMPLE_DB
description: Generate sample data
connection: "duckdb:database/sample.duckdb"
script_sql: CALL dbgen(sf = 1)
active: true
```

# DUCKLAKE

```yaml metadata
name: GENERATE_SAMPLE_DATA
runs_as: ETL
description: Data lake exemple
connection: "'ducklake:sqlite:database/dl_metadata.sqlite' AS dl (DATA_PATH 'database/dl/')"
active: true
```

## lineitem

```yaml metadata
name: lineitem
description: lineitem
table: lineitem
database: "ATTACH 'ducklake:sqlite:database/dl_metadata.sqlite' AS dl (DATA_PATH 'database/dl/')"
load_conn: "duckdb:"
load_before_sql:
  - INSTALL ducklake -- OR FORCE INSTALL ducklake FROM core_nightly
  - INSTALL sqlite
  - "ATTACH 'ducklake:sqlite:database/dl_metadata.sqlite' AS dl (DATA_PATH 'database/dl/')"
  - ATTACH 'database/sample.duckdb' AS S
load_sql: INSERT INTO dl."<table>" BY NAME SELECT * FROM S."<table>"
load_on_err_match_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
load_on_err_match_sql: CREATE TABLE dl."<table>" AS SELECT * FROM S."<table>"
load_after_sql:
  - DETACH S 
  - DETACH dl
drop_sql: DROP TABLE dl."<table>"
clean_sql: DELETE FROM dl."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM dl."<table>"
active: true
```

# ETLX_LOGS

```yaml metadata
name: ETLX_LOGS
runs_as: LOGS
description: Logging
table: logs
database: 'sqlite3:database/dl_etlx_logs.db'
connection: "duckdb:"
before_sql:
  - "LOAD Sqlite"
  - "ATTACH 'database/dl_etlx_logs.db' AS l (TYPE SQLITE)"
  - "USE l"
  - "LOAD json"
  - "get_dyn_queries[create_missing_columns](ATTACH 'database/dl_etlx_logs.db' AS l (TYPE SQLITE),DETACH l)"
save_log_sql: |
  INSERT INTO "l"."<table>" BY NAME
  SELECT *
  FROM READ_JSON('<fname>');
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: |
  CREATE TABLE "l"."<table>" AS
  SELECT *
  FROM READ_JSON('<fname>');
after_sql:
  - 'USE memory'
  - 'DETACH "l"'
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
SELECT 'ALTER TABLE "l"."<table>" ADD COLUMN "' || "column_name" || '" ' || "column_type" || ';' AS "query"
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```