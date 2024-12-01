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

```markdown
# ETL
```yaml etl
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres://user:pass@localhost:5432/analytics_db'
periodicity: '0 5 * * *'
```


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
```yaml
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres://user:pass@localhost:5432/analytics_db'
periodicity: '0 5 * * *'
```


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
