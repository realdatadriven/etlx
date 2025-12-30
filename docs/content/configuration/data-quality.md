+++
title = 'Data Quality'
weight = 44
draft = false
+++

# Data Quality

The `DATA_QUALITY` section allows you to define and execute validation rules to ensure the quality of your data. Each rule performs a check using a SQL query to identify records that violate a specific condition. Optionally, you can define a query to fix any identified issues automatically if applicable.

---

## **Data Quality Structure**

1. **Metadata**:
   - The `DATA_QUALITY` section contains metadata describing its purpose and activation status.

2. **Validation Rules**:
   - Each validation rule is defined as a Level 2 heading under the `DATA_QUALITY` block.
   - Rules include a query to check for violations and, optionally, a query to fix issues.

3. **Execution**:
   - The system loops through all rules in the `DATA_QUALITY` block.
   - For each rule:
     - Runs the validation query.
     - If violations are found and a fix query is defined, executes the fix query.

---

## **Data Quality Markdown Example**

````markdown
# DATA_QUALITY
```yaml
description: "Runs some queries to check quality / validate."
active: true
```

## Rule0001
```yaml
name: Rule0001
description: "Check if the field x has the option y and z."
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
query: quality_check_query
fix_quality_err: fix_quality_err_query
column: total_reg_with_err # Defaults to 'total'.
check_only: false # runs only quality check if true
fix_only: false # runs only quality fix if true and available and possible
after_sql: "DETACH DB"
active: true
```

```sql
-- quality_check_query
SELECT COUNT(*) AS "total_reg_with_err"
FROM "sales"
WHERE "option" NOT IN ('y', 'z');
```

```sql
-- fix_quality_err_query
UPDATE "sales"
SET "option" = 'default value'
WHERE "option" NOT IN ('y', 'z');
```

## Rule0002
```yaml
name: Rule0002
description: "Check if the field y has the option x and z."
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
query: quality_check_query
fix_quality_err: null # no automated fixing for this
column: total_reg_with_err # Defaults to 'total'.
after_sql: "DETACH DB"
active: true
```

```sql
-- quality_check_query
SELECT COUNT(*) AS "total_reg_with_err"
FROM "sales"
WHERE "option2" NOT IN ('x', 'z');
```

````

---

## **How Data Quality Works**

1. **Defining Rules**:
   - Each rule specifies:
     - A SQL query (`query`) to validate data.
     - An optional fix query (`fix_quality_err`) to resolve issues.
     - Metadata for connection, pre/post-SQL commands, and status.

2. **Execution Flow**:
   - The validation query is executed first.
   - If the number of violations is greater than 0:
     - Logs the count of invalid records.
     - Executes the fix query if `fix_quality_err` is defined.

3. **Output**:
   - Provides detailed logs about rule violations and fixes applied.

---

## **Data Quality Example Use Case**

For the example above:

1. **Rule0001**:
   - Validates that the `option` field contains only the values `y` and `z`.
   - Updates invalid records to a default value using the fix query.

2. **Rule0002**:
   - Validates that the `option2` field contains only the values `x` and `z`.
   - Updates invalid records to a default value using the fix query.

---

## **Data Quality Benefits**

- **Automated Quality Assurance**:
  - Identify and fix data issues programmatically.

- **Customizable Rules**:
  - Define rules tailored to your specific data quality requirements.

- **Flexibility**:
  - Supports pre- and post-SQL commands for advanced workflows.

By integrating the `DATA_QUALITY` block, you can ensure the integrity of your data and automate validation processes as part of your ETL pipeline.
