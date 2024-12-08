# **DuckDB-Powered Markdown-Driven ETL Framework**

## **Overview**
This project is a high-performance **ETL (Extract, Transform, Load) Framework** powered by **DuckDB**, designed to integrate and process data from diverse sources. It uses Markdown files as configuration inputs, where **YAML metadata** defines data source properties, and **SQL blocks** specify the logic for extraction, transformation, and loading.

The framework supports a variety of data sources, including:
- Relational Databases: **Postgres**, **MySQL**, **SQLite**, **ODBC**.
- Cloud Storage: **S3**.
- File Formats: **CSV**, **Parquet**, **Spreadsheets**.

By leveraging DuckDB's powerful in-memory processing capabilities, this framework enables seamless ETL operations, validation, and data integration.

---

## **Features**

- **Markdown-Driven Configuration**:
  - Use YAML to define ETL metadata (e.g., connection strings, schedules, validations).
  - Embed SQL blocks for data extraction, transformation, and loading.

- **Powerful DuckDB Engine**:
  - In-memory computations for high performance.
  - Supports SQL extensions for multi-source integration.

- **Flexible ETL Workflow**:
  - Modular `Extract`, `Transform`, and `Load` steps.
  - Configurable validations to ensure data integrity.

- **Multi-Source Compatibility**:
  - Relational databases, cloud storage, and file formats all supported.

- **Scheduler Integration**:
  - Define periodicity using cron expressions in YAML metadata.

---

## **How It Works**

### **1. Define ETL Configuration in Markdown**
Create a Markdown file with the ETL process configuration. For example:

````markdown
# ETL
```yaml etl
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres://user:pass@localhost:5432/analytics_db'
periodicity: '0 5 * * *'
```
# EXTRACT
## sales_data
```yaml etl_sales
name: SalesData
description: 'Daily Sales Data'
source: sales_db
extract_conn: 'mysql://user:pass@localhost:3306/sales'
extract_sql: extract_sales
load_conn: 'duckdb:memory'
load_sql: load_sales
```
```sql extract_sales
SELECT * FROM sales WHERE sale_date = '{YYYYMMDD}'
```
```sql load_sales
CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM '<filename>';
```
````

### **2. Parse the Markdown File**
- Parse the Markdown file to extract:
  - **YAML blocks**: For metadata and configuration.
  - **SQL blocks**: For ETL logic.

### **3. Execute the Workflow**
- Use DuckDB to:
  - Extract data from the source.
  - Apply transformations (if specified).
  - Load the processed data into the target.

### **4. Schedule ETL**
- Use the `periodicity` field in YAML for scheduling. For instance, use cron to trigger the ETL process at specified intervals.

---

## **Configuration Details**

### **ETL Metadata (YAML)**
The ETL process is defined using YAML metadata in Markdown. Below is an example:
```markdown
```yaml
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres://user:pass@localhost:5432/analytics_db'
periodicity: '0 5 * * *'
```

### **Validation Rules**
Validation is performed during the load phase using YAML:
```yaml
load_validation:
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
  - type: throw_if_not_empty
    sql: validate_data_duplicates
    msg: 'Duplicate data detected!'
```

---

## **Example Use Case**

Markdown File (`etl_config.md`):
````markdown
# ETL
```yaml etl
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres://user:pass@localhost:5432/analytics_db'
periodicity: '0 5 * * *'
```
# EXTRACT
## sales_data
```yaml etl_sales
name: SalesData
description: 'Daily Sales Data'
source: sales_db
extract_conn: 'mysql://user:pass@localhost:3306/sales'
extract_sql: extract_sales
load_conn: 'duckdb:memory'
load_sql: load_sales
```
```sql extract_sales
SELECT * FROM sales WHERE sale_date = '{YYYYMMDD}'
```
```sql load_sales
CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM '<filename>';
```
````

---

## **Advantages**

- **Human-Readable Configuration**: Easily define ETL workflows in Markdown.
- **Powerful Processing**: Leverage DuckDB’s in-memory analytics engine for high performance.
- **Cross-Platform Compatibility**: Integrates with databases, cloud, and file systems.
- **Extensibility**: Add new data sources or custom transformations by extending Markdown definitions.

---

## **Getting Started**

1. **Install DuckDB**:
   ```bash
   pip install duckdb  # Or download the binary for your platform
   ```

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/markdown-etl
   cd markdown-etl
   ```

3. **Run the ETL Process**:
   ```bash
   go run main.go --config etl_config.md
   ```

4. **Schedule the Process** (Optional):
   - Use cron to schedule the script:
     ```bash
     crontab -e
     ```
     Add:
     ```bash
     0 5 * * * /path/to/etl_runner.sh
     ```

---
### **How the configuration works**

1. **ETL Process Starts**:
   - Begin with the `"ETL"` key.
   - Extract metadata, specifically:
     - `"connection"`: Main connection to the destination database.
     - `"description"`: For logging the start and end time of the ETL process.

2. **Loop through `"EXTRACT"`**:
   - Iterate over each key (e.g., `"sales_data"`) in `"EXTRACT"`.
   - For each key, access its `"metadata"` to process the ETL steps.

3. **ETL Steps**:
   - Each ETL step (`Extract`, `Transform`, `Load`) has:
     - `_before_sql`: Queries to run first (setup).
     - `_sql`: The main query or queries to run.
     - `_after_sql`: Cleanup queries to run afterward.
   - Queries can be:
     - `nil`: Do nothing.
     - `string`: Reference a single query key in the same map.
     - `slice of strings`: Execute all queries in sequence.
   - Use `_conn` for connection settings. If `nil`, fall back to the main connection.

4. **Output Logs**:
   - Log progress (e.g., connection usage, start/end times, descriptions).
   - Gracefully handle missing or `nil` keys.

---

## **License**

This project is licensed under the MIT License.
