+++
title = 'Query Documentation'
weight = 43
draft = false
+++

### Query Documentation

In some ETL processes, particularly during the **Transform** step, queries may become too complex to manage as a single string. To address this, the configuration supports a structured approach where you can break down a query into individual fields and their respective SQL components. This approach improves modularity, readability, and maintainability.

---

#### **Structure**

A complex query is defined as a top-level heading (e.g., `# My Complex Query`) in the configuration. Each field included in the query is represented as a Level 2 heading (e.g., `## Field Name`).

For each field:

- Metadata can describe the field (e.g., `name`, `description`) if a yaml metadata is not provided the field key is used as field in this example `Field Name`.
- SQL components like `select`, `from`, `join`, `where`, `group_by`, `order_by`, `having`, and `cte` are specified in separate sql blocks.

---

#### **Markdown Example**

````markdown
# My Complex Query
This query processes sales and regions data.

```yaml metadata
name: sales_and_regions_query
description: "Combines sales data with region metadata."
```

## Sales Field
```yaml metadata
name: sales_field
description: "Field representing sales data."
```

```sql
-- select
SELECT S.total_sales AS sales_field
```

```sql
-- from
FROM sales_data AS S
```

## Regions Field
```yaml metadata
name: regions_field
description: "Field representing region metadata."
```

```sql
-- cte
WITH region_cte AS (
    SELECT region_id, region_name
    FROM region_data
    WHERE active = TRUE
)
```

```sql
-- select
    , R.region_name AS regions_field
```

```sql
-- join
LEFT JOIN region_cte AS R ON S.region_id = R.region_id
```

```sql
-- where
WHERE S.total_sales > 1000
```

````

---

(See README for full explanation of how query doc parsing and combining works.)
