+++
title = 'Scripts'
weight = 46
draft = false
+++

# Scripts

The **SCRIPTS** section allows you to **execute SQL queries** that **don’t fit into other predefined sections** (ETL, EXPORTS, etc.).

## **🔹 When to Use SCRIPTS?**

✅ **Running cleanup queries after an ETL job**  
✅ **Executing ad-hoc maintenance tasks**  
✅ **Running SQL commands that don’t need to return results**  
✅ **Executing SQL scripts for database optimizations**  

---

## **🛠 Example: Running Cleanup Scripts**    

This example **removes temporary data** after an ETL process.

## **📄 Markdown Configuration**

````markdown
# SCRIPTS

Run Queries that does not need a return

```yaml metadata
name: DailyScripts
description: "Daily Scripts"
connection: "duckdb:"
active: true
```

## SCRIPT1

```yaml metadata
name: SCRIPT1
description: "Clean up auxiliar / temp data"
connection: "duckdb:"
before_sql:
- "INSTALL sqlite"
- "LOAD sqlite"
- "ATTACH 'database/DB.db' AS DB (TYPE SQLITE)"
script_sql: clean_aux_data
on_err_patt: null
on_err_sql: null
after_sql: "DETACH DB"
active: true
```

```sql
-- clean_aux_data
DROP TEMP_TABLE1;
```
````
---

## **🔹 How Scripts It Works**

1️⃣ **Loads necessary extensions and connects to the database.**  
2️⃣ **Executes predefined SQL queries (`script_sql`).**  
3️⃣ **Runs `before_sql` commands before execution.**  
4️⃣ **Runs `after_sql` commands after execution.**

