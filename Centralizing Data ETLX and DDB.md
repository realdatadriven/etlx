# Centralizing Your Data with ETLX and DuckDB

In the world of data engineering, centralizing data from multiple sources is a common yet challenging task. Whether it's pulling data from relational databases, object storage, or APIs, the process often requires meticulous mapping and integration. ETLX simplifies this by combining DuckDB's powerful features with a configuration-based approach.

---

## Why DuckDB?

DuckDB is a modern, lightweight analytical database designed for data science workflows. It supports querying data from:
- **Relational databases**: SQLite, PostgreSQL, MySQL, etc.
- **Object storage**: AWS S3 or local files in CSV, Parquet, or JSON formats.
- **Specialized services**: Hugging Face datasets, MotherDuck, and more.

ETLX leverages DuckDB's capabilities and provides an easy-to-use configuration system for defining ETL workflows.

---

## ETLX in Action

Below, we explore how ETLX can centralize data from various sources.

---

### **Example 1: Centralizing Data from SQLite and PostgreSQL**

````markdown
# ETL
```yaml
type: etl
name: CentralizeSQL
description: "Centralizing data from SQLite and PostgreSQL"
database: CentralDB
connection: "duckdb:CentralDB.duckdb"
```

## Load SQLite Data
```yaml
name: LoadSQLite
description: "Load data from SQLite"
source: SQLITE
extract_conn: "sqlite3:data.sqlite"
extract_sql: "SELECT * FROM sales WHERE date >= '{YYYY-MM-DD}'"
load_conn: "duckdb:"
load_sql: "CREATE OR REPLACE TABLE CentralDB.sales AS SELECT * FROM sales"
```

## Load PostgreSQL Data
```yaml
name: LoadPostgres
description: "Load data from PostgreSQL"
source: POSTGRES
extract_conn: "postgresql://user:password@localhost:5432/dbname"
extract_sql: "SELECT * FROM inventory WHERE last_updated >= '{YYYY-MM-DD}'"
load_conn: "duckdb:"
load_sql: "CREATE OR REPLACE TABLE CentralDB.inventory AS SELECT * FROM inventory"
```
````

---

### **Example 2: Querying Data Directly from S3**

````markdown
# ETL
```yaml
type: etl
name: S3Query
description: "Querying data directly from S3"
connection: "duckdb:"
```

## Query CSV in S3
```yaml
name: SalesS3
description: "Query sales data from S3"
extract_sql: "SELECT * FROM read_csv_auto('s3://bucket-name/sales.csv', HEADER=TRUE)"
load_sql: "CREATE OR REPLACE TABLE Sales AS SELECT * FROM read_csv_auto('s3://bucket-name/sales.csv', HEADER=TRUE)"
```
````

---

### **Example 3: Combining Hugging Face Datasets**

````markdown
# ETL
```yaml
type: etl
name: HuggingFace
description: "Centralizing data from Hugging Face datasets"
connection: "duckdb:"
```

## Load Dataset
```yaml
name: LoadHF
description: "Load dataset from Hugging Face"
extract_sql: "SELECT * FROM 'huggingface://datasets/squad'"
load_sql: "CREATE OR REPLACE TABLE hf_data AS SELECT * FROM 'huggingface://datasets/squad'"
```
````

---

## Benefits of Using ETLX with DuckDB

1. **Versatility**:
   - Query multiple file formats (CSV, Parquet, JSON) and databases using a unified interface.

2. **Performance**:
   - DuckDB optimizes queries for analytical workloads, making ETLX ideal for large-scale transformations.

3. **Flexibility**:
   - Define transformations and validations using simple configuration blocks.

4. **Simplicity**:
   - ETLX abstracts complex SQL workflows into reusable configurations.

---

## Conclusion

ETLX, powered by DuckDB, provides a streamlined way to integrate data from diverse sources. By leveraging ETLX's configuration-driven approach, you can focus on extracting value from your data instead of managing intricate ETL pipelines.

Start centralizing your data with ETLX today and unlock the power of DuckDB!
