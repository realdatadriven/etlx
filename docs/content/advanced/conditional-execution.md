+++
title = 'Conditional Execution'
weight = 62
draft = false
+++

### Conditional Execution

ETLX allows conditional execution of SQL blocks based on the results of a query. This is useful to skip operations dynamically depending on data context (e.g., skip a step if no new data is available, or if a condition in the target is not met.

You can define condition blocks using the following keys:

- For **ETL step-specific conditions**:
  - `extract_condition`
  - `transform_condition`
  - `load_condition`
  - etc.

- For **generic sections** (e.g., DATA_QUALITY, EXPORTS, NOTIFY, etc.):
  - `condition`

You can also specify an optional `*condition_msg` to log a custom message when a condition is not met.

#### **Condition Evaluation Logic**

- The SQL query defined in `*_condition` or `condition` is executed.
- The result must mast be boolean.
- If not met, the corresponding main logig will be skipped.
- If `*_condition_msg` is provided, it will be included in the log entry instead of the default skip message.

#### **Example – Conditional Load Step**

```yaml
load_conn: "duckdb:"
load_condition: check_load_required
load_condition_msg: "No new records to load today"
load_sql: perform_load
```

```sql
-- check_load_required
SELECT COUNT(*) > 0 as _check FROM staging_table WHERE processed = false;
```

```sql
-- perform_load
INSERT INTO target_table
SELECT * FROM staging_table WHERE processed = false;
```

#### **Example – Global Conditional Notification**

```yaml
type: notify
name: notify_if_failures
description: "Send email only if failures occurred"
connection: "duckdb:"
condition: check_failures
condition_msg: "No failures detected, no email sent"
```

```sql
-- check_failures
SELECT COUNT(*) > 0 as chk FROM logs WHERE success = false;
```

> 📝 **Note:** If no `*_condition_msg` is defined and the condition fails, ETLX will simply log the skipped step with a standard message like:  
> `"Condition 'load_condition' was not met. Skipping step 'load'."`