# **DuckDB-Powered Markdown-Driven ETL Framework**

## **Overview**
This project is a high-performance **ETL (Extract, Transform, Load) Framework** powered by **DuckDB**, designed to integrate and process data from diverse sources. It uses Markdown as configuration inputs (inspired by evidence.dev), where **YAML|TOML|JSON metadata** defines data source properties, and **SQL blocks** specify the logic for extraction, transformation, and loading.

The framework supports a variety of data sources, including:
- Relational Databases: **Postgres**, **MySQL**, **SQLite**, **ODBC**.
- Cloud Storage: **S3**.
- File Formats: **CSV**, **Parquet**, **Spreadsheets**.

By leveraging DuckDB's powerful in-memory processing capabilities, this framework enables seamless ETL operations, validation, and data integration, template filling ....

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

- **Extensibility**:
  - Designed for embedding in **Go** or **Python** applications.
  - Parse configurations programmatically or pass configurations as form data.

---

## **Command-Line Usage**

The binary supports the following flags:

- `--config`: Path to the Markdown configuration file. *(Default: `config.md`)*
- `--date`: Reference date for the ETL process in `YYYY-MM-DD` format. *(Default: yesterday's date)*
- `--only`: Comma-separated list of keys to run.
- `--skip`: Comma-separated list of keys to skip.
- `--steps`: Steps to run within the ETL process (`extract`, `transform`, `load`).
- `--file`: Path to a specific file to extract data from. Typically used with the `--only` flag.
- `--clean`: Execute `clean_sql` on items (conditional based on `--only` and `--skip`).
- `--drop`: Execute `drop_sql` on items (conditional based on `--only` and `--skip`).
- `--rows`: Retrieve the number of rows in the target table(s).

---

### **Example Command**

```bash
etlx --config etl_config.md --date 2023-10-31 --only sales --steps extract,load
```

## **How It Works**

### **1. Define ETL Configuration in Markdown**
Create a Markdown file with the ETL process configuration. For example:

```markdown
```yaml
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
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
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
```
## sales_data
```yaml etl_sales
name: SalesData
description: 'Daily Sales Data'
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_sql: load_sales
load_after_sql: detaches
```
```sql load_extentions
load mysql;
load postgres;
```
```sql conn
ATTACH 'user=@MYSQL_USER password=A@MYSQL_PASSWORD port=3306 database=sales' AS "ORG" (TYPE MYSQL);
ATTACH 'ser=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable' AS "DST" (TYPE POSTGRES);
```
```sql detaches
DETACH "ORG";
DETACH "DST";
```
```
```sql load_sales
CREATE OR REPLACE TABLE "DST"."analytics_db" AS 
SELECT * 
FROM "ORG"."sales";
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
The ETL process is defined using YAML metadata in Markdown. Below is an example, enviromental variables cam be accessed by puting @ENV. or just @ in front of the name like @ENV.VAR_NAME or @VAR_NAME, the system will recognize it as a potential env variable, and .env fileon the root is suported to laod them:

```markdown
```yaml
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
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
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
```
## sales_data
```yaml etl_sales
name: SalesData
description: 'Daily Sales Data'
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_sql: load_sales
load_after_sql: detaches
```
```sql load_extentions
load mysql;
load postgres;
```
```sql conn
ATTACH 'user=@MYSQL_USER password=A@MYSQL_PASSWORD port=3306 database=sales' AS "ORG" (TYPE MYSQL);
ATTACH 'ser=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable' AS "DST" (TYPE POSTGRES);
```
```sql detaches
DETACH "ORG";
DETACH "DST";
```
```
```sql load_sales
CREATE OR REPLACE TABLE "DST"."analytics_db" AS 
SELECT * 
FROM "ORG"."sales";
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

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/realdatadriven/etlx.git
   cd etlx
   ```

2. **Run the ETL Process**:
   ```bash
   go run main.go --config etl_config.md --date 2023-10-31
   ```
   the same cam be said for build
   On Windows you may have building issues if you keep duckdb, in that case I found out that is esier to just use the latest libduckdb from https://github.com/duckdb/duckdb/releases put it in your path and then build with -tags=duckdb_use_lib

    ```bash
  CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go run -tags=duckdb_use_lib main.go --config etl_config.md --date 2023-10-31
   ```

3. **Schedule the Process** (Optional):
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

2. **Loop through Level 2 key in under `"ETL"` key**:
   - Iterate over each key (e.g., `"sales_data"`).
   - For each key, access its `"metadata"` to process the ETL steps.

3. **ETL Steps**:
   - Each ETL step (`extract`, `transform`, `load`) has:
     - `_before_sql`: Queries to run first (setup).
     - `_sql`: The main query or queries to run.
     - `_after_sql`: Cleanup queries to run afterward.
   - Queries can be:
     - `null`: Do nothing.
     - `string`: Reference a single query key in the same map or the qyery itself.
     - `slice of strings`: Execute all queries in sequence.
     - In case is not null it can be the query itself or just the name of a sql code block under the same key, where `sql [query_name]` or first line `-- [query_name]`
   - Use `_conn` for connection settings. If `null`, fall back to the main connection.

4. **Output Logs**:
   - Log progress (e.g., connection usage, start/end times, descriptions).
   - Gracefully handle missing or `null` keys.


---

## **Embedding in Go**

To embed the ETL framework in a Go application:

```go
package main

import (
	"fmt"
	"time"
	"github.com/realdatadriven/etlx/internal/etlx"
)

func main() {
	etl := &etlx.ETLX{}

	// Load configuration from Markdown text
	err := etl.ConfigFromMDText(`# Your Markdown config here`)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		return
	}

	// Prepare date reference
	dateRef := []time.Time{time.Now().AddDate(0, 0, -1)}

	// Define additional options
	options := map[string]any{
		"only":  []string{"sales"},
		"steps": []string{"extract", "load"},
	}

	// Run ETL process
	logs, err := etl.RunETL(dateRef, nil, options)
	if err != nil {
		fmt.Printf("Error running ETL: %v\n", err)
		return
	}

	// Print logs
	for _, log := range logs {
		fmt.Printf("Log: %+v\n", log)
	}
}
```
---

## **License**

This project is licensed under the MIT License.
