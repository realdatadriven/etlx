+++
title = 'ETL'
weight = 41
draft = false
+++

### ETL

- Defines the overall ETL process.
- Example:

````markdown
# ETL

```yaml metadata
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
```

## sales_data

```yaml metadata
name: SalesData
description: 'Daily Sales Data'
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_validation: # Validation is performed during the load phase using YAML
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
  - type: throw_if_not_empty
    sql: validate_data_duplicates
    msg: 'Duplicate data detected!'
load_sql: load_sales
load_after_sql: detaches
```
````

#### 1. **ETL Process Starts**

- Begin with the "ETL" key;
- Extract metadata, specifically:
  - "connection": Main connection to the destination database.
  - "description": For logging the start and end time of the ETL process.

#### 2. **Loop through Level 2 key in under "ETL" key**

- Iterate over each key (e.g., "sales_data")
- For each key, access its "metadata" to process the ETL steps.

#### 3. **ETL Steps**

- Each ETL step (`extract`, `transform`, `load`) has:
  - `_before_sql`: Queries to run first (setup).
  - `_sql`: The main query or queries to run.
  - `_after_sql`: Cleanup queries to run afterward.
- Queries can be:
  - `null`: Do nothing.
  - `string`: Reference a single query key in the same map or the query itself.
  - `array|list`: Execute all queries in sequence.
  - In case is not null it can be the query itself or just the name of a sql code block under the same key, where `sql [query_name]` or first line `-- [query_name]`
- Use `_conn` for connection settings. If `null`, fall back to the main connection.
- Additionally, error handling can be defined using `[step]_on_err_match_patt` and `[step]_on_err_match_sql` to handle specific database errors dynamically, where `[step]_on_err_match_patt` is the `regexp` patthern to match error,and if maches the `[step]_on_err_match_sql` is executed, the same can be applied for `[step]_before_on_err_match_patt` and `[step]_before_on_err_match_sql`.
   You can define patterns to match specific errors and provide SQL statements to resolve those errors. This feature is useful when working with dynamically created databases, tables, or schemas.

#### 4. **Output Logs**n
- Log progress (e.g., connection usage, start/end times, descriptions).
- Gracefully handle missing or `null` keys.
