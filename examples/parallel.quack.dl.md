<!-- markdownlint-disable MD025 -->
<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->

<!-- 
# SERVER
curl https://install.duckdb.org | sh
cd database
duckdb quack_lake_catalog.duckdb
CALL quack_serve('quack:localhost', token => 'super_secret_test_token');

# CLIENTE
INSTALL ducklake;

-- The token matches server.sql.
CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');

-- The DuckLake catalog is the DuckDB database exposed by Quack.
-- Data files are stored locally under ./data.
ATTACH 'ducklake:quack:localhost' AS lake (DATA_PATH 'database/lake');
USE lake;

-->

# PARALLEL
```yaml metadata
name: PARALLEL
runs_as: ETL
description: This paralel section runs every item in parallel, good for extracting inputs 
connection: "sqlite3:database/parallel.db"
database: "sqlite3:database/parallel.db"
parallel: true
active: true
```

## DDBPTEST
```yaml metadata
name: DDBPTEST
description: Test query
table: DDBPTEST
load_conn: "duckdb:"
load_before_sql: "CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');ATTACH 'ducklake:quack:localhost' AS DB (DATA_PATH 'database/lake');"
load_sql: |
  CREATE OR REPLACE TABLE DB."<table>" AS SELECT  v1.x % 1000 AS category,  COUNT(*) AS Total, APPROX_COUNT_DISTINCT(v1.y) AS TotalDups, AVG(v1.z) AS _AVG
  FROM  (SELECT range AS x, random() AS y, random() * 100 AS z FROM range(100_000_000)) v1
  JOIN  (SELECT range AS id FROM range(5000)) v2 ON (v1.x % 5000) = v2.id
  GROUP BY category;
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## DDBPTEST2
```yaml metadata
name: DDBPTEST2
description: Test query
table: DDBPTEST2
load_conn: "duckdb:"
load_before_sql: "CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');ATTACH 'ducklake:quack:localhost' AS DB (DATA_PATH 'database/lake');"
load_sql: |
  CREATE OR REPLACE TABLE DB."<table>" AS SELECT  v1.x % 1000 AS category,  COUNT(*) AS Total, APPROX_COUNT_DISTINCT(v1.y) AS TotalDups, AVG(v1.z) AS _AVG
  FROM  (SELECT range AS x, random() AS y, random() * 100 AS z FROM range(10_000_000)) v1
  JOIN  (SELECT range AS id FROM range(5000)) v2 ON (v1.x % 5000) = v2.id
  GROUP BY category;
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## DDBPTEST3
```yaml metadata
name: DDBPTEST3
description: Test query
table: DDBPTEST3
load_conn: "duckdb:"
load_before_sql: "CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');ATTACH 'ducklake:quack:localhost' AS DB (DATA_PATH 'database/lake');"
load_sql: |
  CREATE OR REPLACE TABLE DB."<table>" AS SELECT  v1.x % 1000 AS category,  COUNT(*) AS Total, APPROX_COUNT_DISTINCT(v1.y) AS TotalDups, AVG(v1.z) AS _AVG
  FROM  (SELECT range AS x, random() AS y, random() * 100 AS z FROM range(20_000_000)) v1
  JOIN  (SELECT range AS id FROM range(5000)) v2 ON (v1.x % 5000) = v2.id
  GROUP BY category;
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## DDBPTEST4
```yaml metadata
name: DDBPTEST4
description: Test query
table: DDBPTEST4
load_conn: "duckdb:"
load_before_sql: "CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');ATTACH 'ducklake:quack:localhost' AS DB (DATA_PATH 'database/lake');"
load_sql: |
  CREATE OR REPLACE TABLE DB."<table>" AS SELECT  v1.x % 1000 AS category,  COUNT(*) AS Total, APPROX_COUNT_DISTINCT(v1.y) AS TotalDups, AVG(v1.z) AS _AVG
  FROM  (SELECT range AS x, random() AS y, random() * 100 AS z FROM range(50_000_000)) v1
  JOIN  (SELECT range AS id FROM range(5000)) v2 ON (v1.x % 5000) = v2.id
  GROUP BY category;
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## DDBPTEST5
```yaml metadata
name: DDBPTEST5
description: Test query
table: DDBPTEST5
load_conn: "duckdb:"
load_before_sql: "CREATE SECRET (TYPE quack, TOKEN 'super_secret_test_token');ATTACH 'ducklake:quack:localhost' AS DB (DATA_PATH 'database/lake');"
load_sql: |
  CREATE OR REPLACE TABLE DB."<table>" AS SELECT  v1.x % 1000 AS category,  COUNT(*) AS Total, APPROX_COUNT_DISTINCT(v1.y) AS TotalDups, AVG(v1.z) AS _AVG
  FROM  (SELECT range AS x, random() AS y, random() * 100 AS z FROM range(80_000_000)) v1
  JOIN  (SELECT range AS id FROM range(5000)) v2 ON (v1.x % 5000) = v2.id
  GROUP BY category;
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

# SAVE_LOGS
```yaml metadata
name: SAVE_LOGS
runs_as: LOGS
description: Saving the logs in the same DB instead of the deafult temp style
table: etlx_logs
connection: "duckdb:"
before_sql:
  - INSTALL sqlite
  - INSTALL json
  - "ATTACH 'database/parallel.db' AS DB (TYPE SQLITE)"
  - "USE DB;"
  - LOAD json
  - "get_dyn_queries[create_columns_missing](ATTACH 'database/parallel.db' AS DB (TYPE SQLITE),DETACH DB)"
save_log_sql: INSERT INTO "DB"."<table>" BY NAME FROM read_json('<fname>')
save_on_err_patt: "(?i)table.+does.+not.+exist"
save_on_err_sql: CREATE TABLE IF NOT EXISTS "DB"."<table>" AS FROM read_json('<fname>');
after_sql:
  - "USE memory;"
  - DETACH "DB"
active: true
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
