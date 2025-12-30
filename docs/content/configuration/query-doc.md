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

#### **How Query Doc. Works**

1. **Parsing the Configuration**:
   - Each query is parsed as a separate section with its metadata stored under the `metadata` key.
   - Fields within the query are parsed as child sections, each containing its own metadata and SQL components.

2. **Combining the Query**:
   - The query is built by iterating over the fields (in the order they appear) and combining their SQL components in the following order:
     - `cte` → `select` → `from` → `join` → `where` → `group_by` → `having` → `order_by`
   - All the resulting parts are concatenated to form the final query.

---

#### **Why This Approach Matters**

Handling complex queries with hundreds of columns and numerous joins can quickly become overwhelming. By breaking down the query into smaller, manageable sections, you gain the ability to focus on individual components independently.

This approach is especially beneficial in a notebook-like environment, where each section is represented as a separate heading and can be linked via a table of contents. With this structure, you can:

- **Enhance Documentation**: Add context to each field, join condition, or transformation directly in the query configuration.
- **Incorporate Formulas**: Include relevant calculations or business logic alongside the query components.
- **Promote Collaboration**: Enable multiple contributors to work on different parts of the query without conflicts.

This method simplifies the process of building and maintaining large queries while promoting organization, clarity, and reusability in your ETL workflows.

---

#### **Example Use Case for this type o query documentation**

Consider a scenario where you need to create a large report combining data from multiple sources. Instead of writing a single, monolithic SQL query, you can use this modular approach to:

- Define each column or join independently.
- Add detailed documentation for each transformation step.
- Generate a table of contents for easier navigation and review.

The result is a well-organized, maintainable, and self-documented query configuration that streamlines the development and review process.

---

#### **Resulting Query**

For the example above, the generated query will look like this:

```sql
WITH region_cte AS (
    SELECT region_id, region_name
    FROM region_data
    WHERE active = TRUE
)
SELECT S.total_sales AS sales_field
    , R.region_name AS regions_field
FROM sales_data AS S
LEFT JOIN region_cte AS R ON S.region_id = R.region_id
WHERE S.total_sales > 1000
```

---

#### **Metadata Options**

- **Query Metadata**:
  - `name`: A unique identifier for the query.
  - `description`: A brief description of the query's purpose.

- **Field Metadata**:
  - `name`: A unique identifier for the field.
  - `description`: A brief description of the field's purpose.
But if you only using the parser for you to document your queries you may want to pass extra information in your metadata to use to generate documentation like data lineage / dictionary, ...

---

#### **Benefits**

- **Modularity**: Each field is defined separately, making the query easier to understand and modify.
- **Reusability**: SQL components like `cte` or `join` can be reused across different queries.
- **Readability**: Breaking down complex queries improves comprehension and reduces debugging time.

---

By leveraging this structure, you can handle even the most complex SQL queries in your ETL process with ease and flexibility. Each query becomes manageable, and you gain the ability to compose intricate SQL logic dynamically.
