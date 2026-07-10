<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->

# REMOTE_EXEC
```yaml
name: RemoteExec
runs_as: REMOTE_EXEC
```

## 127.0.0.1
```yaml
name: local.test
host: 127.0.0.1
port: 22
user: ubuntu
key: $HOME/.ssh/id...
host_key: ~/.ssh/known_hosts
working_dir: $HOME/etlx
run:
  - EXTRACTX
  - TRFX
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: ETL.db , dest: ./database/ETL.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
  - etlx --config pipeline.md --only EXTRACTX,TRFX
```

## 127.0.0.2
```yaml
name: local.test2
host: 127.0.0.2
port: 22
user: ubuntu
key: $HOME/.ssh/id...
host_key: ~/.ssh/known_hosts
working_dir: $HOME/etlx
run:
  - EXTRACTY
  - TRFY
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: ETL.db , dest: ./database/ETL.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
  - etlx --config pipeline.md --only EXTRACTY,TRFY
```

```env .env
CONN=ETL.db
ETLX_DEBUG_QUERY=true
ETLX_DEBUG_LOG_LEVEL=ERROR
```

# RUNETL
```yaml
name: ETL
runs_as: ETL
```

## EXTRACTX
```yaml
name: EXTRACTX
load_conn: "duckdb:"
load_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
```

## EXTRACTY
```yaml
name: EXTRACTY
load_conn: "duckdb:"
load_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
```

## TRFX
```yaml
name: TRFX
load_conn: "duckdb:"
load_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
load_sql: "CREATE OR REPLACE TABLE DB.<table> AS SELECT version() || '<table>' AS VERSION FROM DB.EXTRACTX;"
load_after_sql: "DETACH DB;"
```

## TRFY
```yaml
name: TRFY
load_conn: "duckdb:"
load_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
load_sql: "CREATE OR REPLACE TABLE DB.<table> AS SELECT version() || '<table>' AS VERSION FROM DB.EXTRACTY;"
load_after_sql: "DETACH DB;"
```

# RUN_ETL_LOCAL
```yaml
name: RUN_ETL_LOCAL
runs_as: ETL
```

## LOCAL
```yaml
name: LOCAL
table: LOCAL_COMPIL
extract_conn: "duckdb:"
extract_before_sql:
  - INSTALL SQLITE
  - ATTACH 'database/ETL_REMOTE_RESULTS.db' AS DB (TYPE SQLITE)
  - ATTACH 'database/ETL1.db' AS DB1 (TYPE SQLITE)
  - ATTACH 'database/ETL2.db' AS DB2 (TYPE SQLITE)
extract_sql: CREATE OR REPLACE TABLE DB."<table>" AS SELECT * FROM DB1.TRFX UNION SELECT * FROM DB2.TRFY
extract_after_sql: 
  - DETACH DB
  - DETACH DB1
  - DETACH DB2
```