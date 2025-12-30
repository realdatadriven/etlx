+++
title = 'Dynamic Queries'
weight = 61
draft = false
+++

# Dynamic Query Generation (`get_dyn_queries[...]`)

In some advanced ETL workflows, you may need to dynamically generate SQL queries based on metadata or schema differences between the source and destination databases.

---

## **🔹 Why Use Dynamic Queries?**

✅ **Schema Flexibility** – Automatically adapt to schema changes in the source system.  
✅ **Self-Evolving Workflows** – ETL jobs can generate and execute additional SQL queries as needed.  
✅ **Automation** – Reduces the need for manual intervention when new columns appear.  

## **🔹 How `get_dyn_queries[query_name](runs_before,runs_after)` Works**

- Dynamic queries are executed using the **`get_dyn_queries[query_name](runs_before,runs_after)`** pattern.
- During execution, **ETLX runs the query** `query_name` and **retrieves dynamically generated queries**.
- The **resulting queries are then executed automatically**.

## **🛠 Example: Auto-Adding Missing Columns**

This example **checks for new columns in a JSON file** and **adds them to the destination table**.

### **📄 Markdown Configuration for `get_dyn_queries[query_name](runs_before,runs_after)`**

>If the `query_name` depends on attaching and detaching the main db where it will run, those should be passed as dependencies, because the dynamic queries are generate before any other query and put in the list for the list where it is to be executed, to be a simpler flow, but they are optional otherwise.

````markdown
....

```yaml metadata
...
connection: "duckdb:"
before_sql:
  - ...
  - get_dyn_queries[create_missing_columns]  # Generates queries defined in `create_missing_columns` and  Executes them
..
```

**📜 SQL Query (Generating Missing Columns)**

```sql
-- create_missing_columns
WITH source_columns AS (
    SELECT column_name, column_type 
    FROM (DESCRIBE SELECT * FROM read_json('<fname>'))
),
destination_columns AS (
    SELECT column_name, data_type as column_type
    FROM duckdb_columns 
    WHERE table_name = '<table>'
),
missing_columns AS (
    SELECT s.column_name, s.column_type
    FROM source_columns s
    LEFT JOIN destination_columns d ON s.column_name = d.column_name
    WHERE d.column_name IS NULL
)
SELECT 'ALTER TABLE "<table>" ADD COLUMN "' || column_name || '" ' || column_type || ';' AS query
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```
````

---

## **🛠 Execution Flow**

1️⃣ **Extract column metadata from the input (in this case a json file, but it could be a table or any other valid query).**  
2️⃣ **Check which columns are missing in the destination table (`<table>`).**  
3️⃣ **Generate `ALTER TABLE` statements for adding missing columns, and replaces the `- get_dyn_queries[create_missing_columns]` with the the generated queries**  
4️⃣ **Runs the workflow with dynamically generated queries against the destination connection.**

## **🔹 Key Features**

✔ **Fully automated schema updates**  
✔ **Works with flexible schema data (e.g., JSON, CSV, Parquet, etc.)**  
✔ **Reduces manual maintenance when source schemas evolve**  
✔ **Ensures destination tables always match source structure**

---

**With `get_dyn_queries[...]`, your ETLX workflows can now dynamically evolve with changing data structures!**