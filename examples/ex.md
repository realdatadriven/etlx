# GENERATE_DATA_SETS

```yaml
name: GenerateDSs
description: Exports custom ds
runs_as: EXPORTS
connection: "duckdb:"
#path: static/uploads/
active: true
```

## SALES

```yaml
name: GenerateSALESData
description: Exports custom SALES data
connection: "duckdb:"
before_sql:
  - INSTALL erpl_web FROM community
  - LOAD erpl_web
  - create_api_auth_secrete
  - attach_odata_endpoint_with_users_copes
  - attach_sales_datalake
  - attach_logs_db
export_sql:
  - generate_my_sales_data
  - create_logs_table_if_not_exists
  - insert_generated_file_into_logs
after_sql:
  - DETACH scopes
  - DETACH dl
  - DETACH logs
path: sales_by_dep.1.{YYYYMMDD}.{TSTAMP}.parquet
tmp_prefix: tmp
active: true
```

```sql
-- create_api_auth_secrete
CREATE SECRET api_auth (
  TYPE http_bearer,
  TOKEN 'eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjQ0NDQiLCJzdWIiOiJ7XCJhY3RpdmVcIjp0cnVlLFwiYWx0ZXJfcGFzc19ueHRfbG9naW5cIjpmYWxzZSxcImNvZGVfMmZfZXhwaXJlc19hdFwiOm51bGwsXCJlbmFibGVfMmZfYXV0aFwiOmZhbHNlLFwiZXhjbHVkZWRcIjpmYWxzZSxcImZpcnN0X25hbWVcIjpcIlN1cGVyXCIsXCJsYW5nX2lkXCI6MSxcImxhc3RfbmFtZVwiOlwiQWRtaW5cIixcIm54dF9jb2RlXzJmX2F1dGhcIjpudWxsLFwicm9sZV9pZFwiOjEsXCJ1c2VyX2lkXCI6MSxcInVzZXJuYW1lXCI6XCJyb290XCJ9IiwiYXVkIjpbImh0dHA6Ly9sb2NhbGhvc3Q6NDQ0NCJdLCJleHAiOjE3NzcxNDYyNTAuNTA2NTE4NiwibmJmIjoxNzcxOTYyMjUwLjUwNjUxOTYsImlhdCI6MTc3MTk2MjI1MC41MDY1MTg4fQ.94jPZDlzCcqL_b3MmpEj3wB28HMVxjGEh08ByL1dB5c',
  SCOPE 'http://localhost:4444/'
);
```

```sql
-- attach_odata_endpoint_with_users_copes
ATTACH IF NOT EXISTS 'http://localhost:4444/odata/ETLX' AS scopes (TYPE ODATA);
```

```sql
-- attach_ex_sales_datalake
ATTACH 'ducklake:sqlite:database/dl_metadata.sqlite' AS dl (DATA_PATH 'database/dl/');
```

```sql
-- attach_logs_db
ATTACH 'database/logs_for_dyn_gen_ds.db' AS logs (TYPE SQLITE);
```

```sql
-- generate_my_sales_data
COPY (
  SELECT *
  FROM dl.lineitem
) TO '<fname>';
```

```sql
-- create_logs_table_if_not_exists
CREATE TABLE IF NOT EXISTS logs.dynamic_ds_logs (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    table_name VARCHAR NOT NULL,
    fname      VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```sql
-- insert_generated_file_into_logs
INSERT INTO logs.dynamic_ds_logs (user_id, table_name, fname) VALUES (1, 'SALES', '<fname>');
```

```sql x
with _logs as (
  select *
  from dynamic_ds_logs
  where fname is not null
    and user_id = 1
    and (user_id, table_name, created_at) in (
      select user_id, table_name, max(created_at)
      from dynamic_ds_logs
      group by user_id, table_name
    )
)
select user_id, table_name as name, replace(fname, 'tmp/', '') as file
FROM _logs
```
