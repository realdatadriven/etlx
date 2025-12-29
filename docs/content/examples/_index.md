---
title: "Examples"
weight: 30
---

## Examples

### Example ETL configuration (etl_config.md)

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
load_validation:
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
load_sql: load_sales
load_after_sql: detaches
```

```sql
-- load_extentions
load mysql;
load postgres;
```

```sql
-- conn
ATTACH 'user=@MYSQL_USER password=A@MYSQL_PASSWORD port=3306 database=sales' AS "ORG" (TYPE MYSQL);
ATTACH 'ser=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable' AS "DST" (TYPE POSTGRES);
```

```sql load_sales
CREATE OR REPLACE TABLE "DST"."analytics_db" AS 
SELECT * 
FROM "ORG"."sales";
```
````

### Query documentation example

Break large queries into sections: define fields as level-2 headings and provide SQL components (`cte`, `select`, `from`, `join`, `where`). The parser composes them into a final query in the order: `cte -> select -> from -> join -> where -> group_by -> having -> order_by`.
