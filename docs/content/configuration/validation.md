+++
title = 'Validation Rules'
weight = 42
draft = false
+++

### Validation Rules

- Validate data quality during the ETL process by using the key `[step]_validation` in the metadata section.
- Example:

```yaml
...
load_validation:
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
    active: false
  - type: throw_if_not_empty
    sql: validate_data_duplicates
    msg: 'Duplicate data detected!'
...
```

For every object in the `[step]_validation` the `sql` is executed in the `[step]_con` connection, and it can either throw error message defined in `msg` or not if the condition (`type:throw_if_empty | type:throw_if_not_empty` or) is met or not.

#### **Extracting Data from Unsupported Databases**

If the database you are using does not have a direct DuckDB scanner, but it is supported by **sqlx** or it has `odbc` support, you must set the `to_csv` option to `true` in the `extract` configuration. This ensures that data is first exported to a CSV file and then on step `load` the file can be processed by DuckDB.

##### **Example Configuration:**

````markdown
...
## table_from_odbc_source

```yaml metadata
name: table_from_odbc_source
description: 'This is an example o how to extract from databases that does not have a DuckDB scanner'
to_csv: true
extract_conn: 'odbc:DRIVER={ODBC Driver 17 for SQL Server};SERVER=@MSSQL_HOST;UID=@MSSQL_USER;PWD=@MSSQL_PASS;DATABASE=DB'
extract_sql: |
  SELECT [fields]
  FROM [table]
  WHERE [condition]
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_sql: load_exported_csv
load_after_sql: detaches
```
...

Once extracted, the CSV file can be loaded by DuckDB using:

```sql load_exported_csv
CREATE OR REPLACE TABLE DB.target_table AS  
SELECT * FROM READ_CSV('<fname>', HEADER TRUE);  
```

##### **Why Use `to_csv: true`?**

- **Workaround for unsupported databases**: If DuckDB does not have a direct scanner, exporting data to CSV allows and then the CSV can be loaded by the DuckDB.
- **Ensures compatibility**: ETLX will handle the conversion, making the data accessible for further transformation and loading.
- **Required for smooth ETL workflows**: Without this option, DuckDB may fail to recognize or query the database directly.
