# ETL

```yaml metadata
name: DB
description: "Example extrating from S3 to a local sqlite3 file"
connection: "duckdb:"
active: true
```

## VERSION

```yaml metadata
name: VERSION
description: "DDB Version"
table: VERSION
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/DB.db' AS DB (TYPE SQLITE)"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

# REQUIRES

```yaml metadata
name: REQUIRES               
description: load dependencies
active: true
```

## RAW_SQL
```yaml
name: RAW_SQL
description: load raw sql from file
path: examples/raw_sql_file.sql
active: true
```