+++
title = 'Multi-Queries'
weight = 47
draft = false
+++

### Multi-Queries

The `MULTI_QUERIES` section allows you to define multiple queries with similar structures and aggregate their results using a SQL `UNION`. This is particularly useful when generating summaries or reports that combine data from multiple queries into a single result set.

---

#### **Multi-Queries Structure**

1. **Metadata**:
   - The `MULTI_QUERIES` section includes metadata for connection details, pre/post-SQL commands, and activation status.
   - The `union_key` defines how the queries are combined (e.g., `UNION`, `UNION ALL`).

2. **Query Definitions**:
   - Each query is defined as a Level 2 heading under the `MULTI_QUERIES` block.
   - Queries inherit metadata from the parent block unless overridden.

3. **Execution**:
   - All queries are combined using the specified `union_key` (default is `UNION`).
   - The combined query is executed as a single statement.
   - The combined query can be save by specifying `save_sql` normally an insert, create [or replace] table or even a copy to file statement, and insert statement should be used in combination with the `save_on_err_patt` and `save_on_err_sql` in case of an error matching the `table does ... not exist` to create the table instead.

---

#### **Multi-Queries Markdown Example**

````markdown
# MULTI_QUERIES
```yaml
description: "Define multiple structured queries combined with UNION."
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
save_sql: save_mult_query_res
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: create_mult_query_res
after_sql: "DETACH DB"
union_key: "UNION ALL\n" # Defaults to UNION.
active: true
```

```sql
-- save_mult_query_res
INSERT INTO "DB"."MULTI_QUERY" BY NAME
[[final_query]]
```
```sql
-- create_mult_query_res
CREATE OR REPLACE TABLE "DB"."MULTI_QUERY" AS
[[final_query]]
```

## Row1
```yaml
name: Row1
description: "Row 1"
query: row_query
active: true
```

```sql
-- row_query
SELECT '# number of rows' AS "variable", COUNT(*) AS "value"
FROM "sales"
```

## Row2
```yaml
name: Row2
description: "Row 2"
query: row_query
active: true
```

```sql
-- row_query
SELECT 'total revenue' AS "variable", SUM("total") AS "value"
FROM "sales"
```

## Row3
```yaml
name: Row3
description: "Row 3"
query: row_query
active: true
```

```sql
-- row_query
SELECT "region" AS "variable", SUM("total") AS "value"
FROM "sales"
GROUP BY "region"
```
````

---

#### **Multi-Queries How It Works**

1. **Defining Queries**:
   - Queries are defined as Level 2 headings.
   - Each query can include its own metadata and SQL.

2. **Combining Queries**:
   - All queries are combined using the `union_key` (e.g., `UNION` or `UNION ALL`).
   - The combined query is executed as a single statement.

3. **Execution Flow**:
   - Executes the `before_sql` commands at the start.
   - Combines all active queries with the `union_key`.
   - Executes the combined query.
   - Executes the `after_sql` commands after the query execution.

---

#### **Multi-Queries Example Use Case**

For the example above:

1. **Row1**:
   - Counts the number of rows in the `sales` table.
2. **Row2**:
   - Calculates the total revenue from the `sales` table.
3. **Row3**:
   - Sums the revenue for each region in the `sales` table.

The resulting combined query:

```sql
LOAD sqlite;
ATTACH 'reporting.db' AS DB (TYPE SQLITE);

SELECT '# number of rows' AS "variable", COUNT(*) AS "value"
FROM "sales"
UNION ALL
SELECT 'total revenue' AS "variable", SUM("total") AS "value"
FROM "sales"
UNION ALL
SELECT "region" AS "variable", SUM("total") AS "value"
FROM "sales"
GROUP BY "region";

DETACH DB;
```

---

#### **Multi-Queries Benefits**
