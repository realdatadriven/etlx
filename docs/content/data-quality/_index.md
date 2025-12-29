---
title: "Data Quality"
weight: 50
---

## Data Quality

The `DATA_QUALITY` block runs validation rules defined as sub-sections. Each rule includes:

- `query`: SQL to detect violations
- `fix_quality_err`: optional SQL to fix violations
- `before_sql`, `after_sql`: setup/teardown commands

Example rule snippet:

````markdown
# DATA_QUALITY
```yaml
description: "Runs some queries to check quality / validate."
active: true
```

## Rule0001
```yaml
name: Rule0001
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
query: quality_check_query
fix_quality_err: fix_quality_err_query
active: true
```

```sql
-- quality_check_query
SELECT COUNT(*) AS "total_reg_with_err"
FROM "sales"
WHERE "option" NOT IN ('y', 'z');
```
````

Execution: run the validation query; if violations > 0 and a fix is provided, execute the fix.
