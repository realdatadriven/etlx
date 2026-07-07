<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->

# REMOTE_EXEC
```yaml
name: ...
runs_as: REMOTE_EXEC
timeout: 10_000
```

## 127.0.0.1
```yaml
name: local.test
host: 127.0.0.1
port: 22
user: ubuntu
key: $HOME/.ssh/id...
working_dir: $HOME/etlx
run:
  - EXTRACTX
  - TRFX
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: temp.db , dest: ./database/temp.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
```

## 127.0.0.2
```yaml
name: local.test2
host: 127.0.0.2
port: 22
user: ubuntu
key: $HOME/.ssh/id...
working_dir: $HOME/etlx
run:
  - EXTRACTY
  - TRFY
upload_files: 
  - {source: .env, dest: .env }
download_files: 
  - {source: temp.db , dest: ./database/temp.db  }
commands:
  - curl https://realdatadriven.github.io/etlxdocs/install.sh | sh 
```

```env .env
CONN=temp.db
```

# RUNETL
```yaml
name: ETL
runs_as: ETL
```

## EXTRACTX
```yaml
name: EXTRACTX
extract_conn: "duckdb:"
extract_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
extract_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
extract_after_sql: "DETACH DB;"
```

## EXTRACTY
```yaml
name: EXTRACTY
extract_conn: "duckdb:"
extract_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
extract_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
extract_after_sql: "DETACH DB;"
```

## TRFX
```yaml
name: TRFX
extract_conn: "duckdb:"
extract_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
extract_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() || '<table>' AS "VERSION" FROM "EXTRACTX";'
extract_after_sql: "DETACH DB;"
```

## TRFY
```yaml
name: TRFY
extract_conn: "duckdb:"
extract_before_sql: "INSTALL SQLITE;ATTACH 'ETL.db' AS DB (TYPE SQLITE);"
extract_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() || '<table>' AS "VERSION" FROM "EXTRACTY";'
extract_after_sql: "DETACH DB;"
```