# QUACK
```yaml
name: QUACK
runs_as: SCRIPTS
description: Quack test
connection: "duckdb:"
active: true
```

## SAMPLE_DB
```yaml
name: SAMPLE_DB
description: Generate sample data
connection: "duckdb:database/quack.test.ddb"
script_sql: |
    CALL quack_serve('quack:localhost', token = 'my_token');
    ATTACH 'quack:localhost' AS remote_db (TOKEN 'my_token');
    CREATE OR REPLACE TABLE remote_db.t AS FROM range(10) r(i);
    INSERT INTO remote_db.t VALUES (42);
    CONNECT remote_db;
    SELECT * FROM t;
    DISCONNECT;
    DETACH remote_db;
    CALL quack_stop('quack:localhost');
active: true
```