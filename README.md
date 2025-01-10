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

### **Handling Complex Queries in ETL Configuration**

In some ETL processes, particularly during the **Transform** step, queries may become too complex to manage as a single string. To address this, the configuration supports a structured approach where you can break down a query into individual fields and their respective SQL components. This approach improves modularity, readability, and maintainability.

---

#### **Structure**

A complex query is defined as a top-level heading (e.g., `# My Complex Query`) in the configuration. Each field included in the query is represented as a Level 2 heading (e.g., `## Field Name`). 

For each field:
- Metadata can describe the field (e.g., `name`, `description`).
- SQL components like `select`, `from`, `join`, `where`, `group_by`, `order_by`, `having`, and `cte` are specified in separate blocks.

---

#### **Markdown Example**

````markdown
# My Complex Query
This query processes sales and regions data.

```yaml metadata
type: query_doc
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

#### **How It Works**

1. **Parsing the Configuration**:
   - Each query is parsed as a separate section with its metadata stored under the `metadata` key.
   - Fields within the query are parsed as child sections, each containing its own metadata and SQL components.

2. **Combining the Query**:
   - The query is built by iterating over the fields (in the order they appear) and combining their SQL components in the following order:
     - `cte` → `select` → `from` → `join` → `where` → `group_by` → `having` → `order_by`
   - All the resulting parts are concatenated to form the final query.

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
But if you only using the parser for you to document your queries you may want to pass extra information in your metadata to use to generate documentation like data leneage / dictionary, ...
---

#### **Benefits**

- **Modularity**: Each field is defined separately, making the query easier to understand and modify.
- **Reusability**: SQL components like `cte` or `join` can be reused across different queries.
- **Readability**: Breaking down complex queries improves comprehension and reduces debugging time.

---

By leveraging this structure, you can handle even the most complex SQL queries in your ETL process with ease and flexibility. Each query becomes manageable, and you gain the ability to compose intricate SQL logic dynamically.

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

### **Generating Files from Data**

The `EXPORTS` section in the ETL configuration handles exporting data to files. This is particularly useful for generating reports for internal departments, regulators, partners, or saving processed data to a data lake. By leveraging DuckDB's ability to export data in various formats, this section supports file generation with flexibility and precision.

---

#### **Structure**

An export configuration is defined as a top-level heading (e.g., `# EXPORTS`) in the configuration. Within this section:
1. **Exports Metadata**:
   - Metadata defines properties like the database connection, export path, and activation status.
   - Fields like `type`, `name`, `description`, `path`, and `active` control the behavior of each export.

2. **Query-to-File Configuration**:
   - Define the SQL query or sequence of queries used for generating the file.
   - Specify the export format, such as CSV, Parquet, or Excel.

3. **Template-Based Exports**:
   - Templates allow you to map query results into specific cells and sheets of an existing spreadsheet template.

---

#### **Markdown Example**

````markdown
# EXPORTS
Exports data to files.

```yaml metadata
type: exports
name: DailyExports
description: "Daily file exports for various datasets."
database: reporting_db
connection: "duckdb:"
path: "C:/Reports/YYYYMMDD"
active: true
```

## Sales Data Export
```yaml metadata
type: query_to_file
name: SalesExport
description: "Export daily sales data to CSV."
database: reporting_db
connection: "duckdb:"
export_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
  - export
  - "DETACH DB"
active: true
```

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."Sales"
    WHERE "sale_date" = '{YYYY-MM-DD}'
) TO 'C:/Reports/YYYYMMDD/sales_YYYYMMDD.csv' (FORMAT 'csv', HEADER true);
```

## Region Data Export to Excel
```yaml metadata
type: query_to_file
name: RegionExport
description: "Export region data to an Excel file."
database: reporting_db
connection: "duckdb:"
export_sql:
  - "LOAD sqlite"
  - "LOAD Spatial"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
  - export
  - "DETACH DB"
active: true
```

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."Regions"
    WHERE "updated_at" >= '{YYYY-MM-DD}'
) TO 'C:/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx' (FORMAT GDAL, DRIVER 'xlsx');
```

## Sales Report Template
```yaml metadata
type: template
name: SalesReport
description: "Generate a sales report from a template."
database: reporting_db
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
template: "C:/Templates/sales_template.xlsx"
path: "C:/Reports/sales_report_YYYYMMDD.xlsx"
mapping:
  - sheet: Summary
    range: B2
    sql: summary_query
    type: range
    table: SummaryTable
    table_style: TableStyleLight1
    header: true
    if_exists: delete
  - sheet: Details
    range: A1
    sql: details_query
    type: value
    key: total_sales
after_sql: "DETACH DB"
active: true
```

```sql
-- summary_query
SELECT SUM(total_sales) AS total_sales
FROM "DB"."Sales"
WHERE "sale_date" = '{YYYY-MM-DD}'
```

```sql
-- details_query
SELECT *
FROM "DB"."Sales"
WHERE "sale_date" = '{YYYY-MM-DD}';
```
````

---

#### **How It Works**

1. **Parsing the Configuration**:
   - Each export is parsed as a separate section with its metadata stored under the `metadata` key.
   - SQL queries or template mappings are defined as child sections.

2. **File Generation**:
   - The `export_sql` field specifies a sequence of SQL statements used for the export.
   - The final `COPY` statement defines the file format and location.

3. **Template-Based Exports**:
   - Templates map query results to specific sheets and cells in an existing spreadsheet.
   - The `mapping` field defines how query results populate the template:
     - **`sheet`**: The target sheet in the template.
     - **`range`**: The starting cell for the data.
     - **`sql`**: The query generating the data.
     - **`type`**: Indicates whether the data fills a range (`range`) or single value (`value`).
     - **`table_style`**: The table style applied to the range.
     - **`if_exists`**: Specifies how to handle existing data (e.g., delete or append).
the maping can also be a string representig a query and all the mapping cam be loaded from a table in the database to simplifie the config, and also in a real world it can be extensive, would be easier to be done in a spreadsheet and loaded as a table.
---

#### **Resulting Outputs**

1. **CSV File**:
   - Exports sales data to a CSV file located at `C:/Reports/YYYYMMDD/sales_YYYYMMDD.csv`.

2. **Excel File**:
   - Exports region data to an Excel file located at `C:/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx`.

3. **Populated Template**:
   - Generates a sales report from `sales_template.xlsx` and saves it as `sales_report_YYYYMMDD.xlsx`.

---

#### **Benefits**

- **Flexibility**:
  - Export data in multiple formats (e.g., CSV, Excel) using DuckDB's powerful `COPY` command.
- **Reusability**:
  - Use predefined templates to create consistent reports.
- **Customizability**:
  - SQL queries and mappings allow fine-grained control over the exported data.

By leveraging the `EXPORTS` section, you can automate data export processes, making them efficient and repeatable.

---

## **License**

This project is licensed under the MIT License.
