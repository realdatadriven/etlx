# **DuckDB-Powered Markdown-Driven ETL Framework**

## **Overview**
This project is a high-performance **ETL (Extract, Transform, Load) Framework** powered by **DuckDB**, designed to integrate and process data from diverse sources. It uses Markdown as configuration inputs (inspired by evidence.dev), where **YAML|TOML|JSON metadata** defines data source properties, and **SQL blocks** specify the logic for extraction, transformation, and loading.

The framework supports a variety of data sources, including:
- Relational Databases: **Postgres**, **MySQL**, **SQLite**, **ODBC**.
- Cloud Storage: **S3**, **and other S3 compatible Object Storage**.
- File Formats: **CSV**, **Parquet**, **Spreadsheets**.

By leveraging DuckDB's powerful in-memory processing capabilities, this framework enables seamless ETL operations, validation, and data integration, template fill ...

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

Create a Markdown file with the ETL process configuration. For example:

## **Example Use Case**

Markdown File (`etl_config.md`):
````markdown
# ETL
```yaml metadata
name: Daily_ETL
description: 'Daily extraction at 5 AM'
database: analytics_db
connection: 'postgres:user=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable'
```

## sales_data
```yaml metadata
name: SalesData
description: 'Daily Sales Data'
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_validation: # Validation is performed during the load phase using YAML
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
  - type: throw_if_not_empty
    sql: validate_data_duplicates
    msg: 'Duplicate data detected!'
load_sql: load_sales
load_after_sql: detaches
```

```sql 
-- load_extentions
load mysql;
load postgres;
```

```sql 
-- conn
ATTACH 'user=@MYSQL_USER password=A@MYSQL_PASSWORD port=3306 database=sales' AS "ORG" (TYPE MYSQL);
ATTACH 'ser=@PGUSER password=@PGPASSWORD dbname=analytics_db host=localhost port=5432 sslmode=disable' AS "DST" (TYPE POSTGRES);
```

```sql 
-- detaches
DETACH "ORG";
DETACH "DST";
```

```sql load_sales
CREATE OR REPLACE TABLE "DST"."analytics_db" AS 
SELECT * 
FROM "ORG"."sales";
```

```sql 
-- validate_data_not_empty
SELECT * 
FROM "ORG"."sales"
WHERE "date" = '{YYYY-MM-DD}'
LIMIT 10;
```

```sql 
-- throw_if_not_empty
SELECT * 
FROM "DST"."analytics_db"
WHERE "date" = '{YYYY-MM-DD}'
LIMIT 10;
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
The ETL process is defined using YAML metadata in Markdown. Below is an example, environmental variables can be accessed by putting @ENV. or just @ in front of the name like @ENV.VAR_NAME or @VAR_NAME, the system will recognize it as a potential env variable, and .env file on the root is sported to load them:

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
the same can be said for build
On Windows you may have building issues if you keep duckdb, in that case I found out that is easier to just use the latest libduckdb from https://github.com/duckdb/duckdb/releases put it in your path and then build with -tags=duckdb_use_lib

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
     - `string`: Reference a single query key in the same map or the query itself.
     - `array|list`: Execute all queries in sequence.
     - In case is not null it can be the query itself or just the name of a sql code block under the same key, where `sql [query_name]` or first line `-- [query_name]`
   - Use `_conn` for connection settings. If `null`, fall back to the main connection.
   - Additionally, error handling can be defined using `[step]_on_err_match_patt` and `[step]_on_err_match_sql` to handle specific database errors dynamically, where `[step]_on_err_match_patt` is the `regexp` patthern to match error,and if maches the `[step]_on_err_match_sql` is executed, the same can be applyed for `[step]_before_on_err_match_patt` and `[step]_before_on_err_match_sql`.

#### **Error Handling in ETL Steps**

You can define patterns to match specific errors and provide SQL statements to resolve those errors. This feature is useful when working with dynamically created databases, tables, or schemas.


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
path: "/path/to/Reports/YYYYMMDD"
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
) TO '/path/to/Reports/YYYYMMDD/sales_YYYYMMDD.csv' (FORMAT 'csv', HEADER true);
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
) TO '/path/to/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx' (FORMAT GDAL, DRIVER 'xlsx');
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
template: "/path/to/Templates/sales_template.xlsx"
path: "/path/to/Reports/sales_report_YYYYMMDD.xlsx"
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
the maping can also be a string representing a query and all the mapping can be loaded from a table in the database to simplify the config, and also in a real world it can be extensive, would be easier to be done in a spreadsheet and loaded as a table.
---

#### **Resulting Outputs**

1. **CSV File**:
   - Exports sales data to a CSV file located at `/path/to/Reports/YYYYMMDD/sales_YYYYMMDD.csv`.

2. **Excel File**:
   - Exports region data to an Excel file located at `/path/to/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx`.

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

### **Data Quality**

The `DATA_QUALITY` section allows you to define and execute validation rules to ensure the quality of your data. Each rule performs a check using a SQL query to identify records that violate a specific condition. Optionally, you can define a query to fix any identified issues automatically if applicable.

---

#### **Structure**

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

#### **Markdown Example**

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

#### **How It Works**

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

#### **Example Use Case**

For the example above:
1. **Rule0001**:
   - Validates that the `option` field contains only the values `y` and `z`.
   - Updates invalid records to a default value using the fix query.

2. **Rule0002**:
   - Validates that the `option2` field contains only the values `x` and `z`.
   - Updates invalid records to a default value using the fix query.

---

#### **Benefits**

- **Automated Quality Assurance**:
  - Identify and fix data issues programmatically.

- **Customizable Rules**:
  - Define rules tailored to your specific data quality requirements.

- **Flexibility**:
  - Supports pre- and post-SQL commands for advanced workflows.

---

By integrating the `DATA_QUALITY` block, you can ensure the integrity of your data and automate validation processes as part of your ETL pipeline.

---

### **Loading Config Dependencies**

The `REQUIRES` section in the ETL configuration allows you to load dependencies from external Markdown configurations. These dependencies can either be loaded from file paths or dynamically through queries. This feature promotes modularity and reusability by enabling you to define reusable parts of the configuration in separate files or queries.

---

#### **Structure**

1. **Metadata**:
   - The `REQUIRES` section includes metadata describing its purpose and activation status.

2. **Loading Options**:
   - **From Queries**:
     - Dynamically fetch configuration content from a query.
     - Specify the query, column containing the configuration, and optional pre/post SQL scripts.
   - **From Files**:
     - Load configuration content from an external file path.

3. **Integration**:
   - The loaded configuration is merged into the main configuration.
   - Top-level headings in the required configuration that don’t exist in the main configuration are added.

---

#### **Markdown Example**

````markdown
# REQUIRES
```yaml
description: "Load configuration dependencies from files or queries."
active: true
```

## Sales Transformation
```yaml
name: SalesTransform
description: "Load sales transformation config from a query."
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
query: get_sales_conf
column: md_conf_content # Defaults to 'conf' if not provided.
after_sql: "DETACH DB"
active: false
```

```sql
-- get_sales_conf
SELECT "md_conf_content"
FROM "configurations"
WHERE "config_name" = 'Sales'
  AND "active" = true
  AND "excluded" = false;
```

## Inventory Transformation
```yaml
name: InventoryTransform
description: "Load inventory transformation config from a file."
path: "/path/to/Configurations/inventory_transform.md"
active: true
```

````

---

#### **How It Works**

1. **Defining Dependencies**:
   - Dependencies are listed as child sections under the `# REQUIRES` heading.
   - Each dependency specifies its source (`query` or `path`) and associated metadata.

2. **From Queries**:
   - Use the `query` field to specify a SQL query that retrieves the configuration.
   - The `column` field specifies which column contains the Markdown configuration content.
   - Optionally, use `before_sql` and `after_sql` to define scripts to run before or after executing the query.

3. **From Files**:
   - Use the `path` field to specify the file path of an external Markdown configuration.

4. **Merging with Main Configuration**:
   - After loading the configuration, any top-level headings in the loaded configuration that don’t exist in the main configuration are added.

---

#### **Example Use Case**

For the example above, the following happens:
1. **Sales Transformation**:
   - A query retrieves the Markdown configuration content for sales transformations from a database table.
   - The `before_sql` and `after_sql` scripts prepare the environment for the query execution.

2. **Inventory Transformation**:
   - A Markdown configuration is loaded from an external file path (`/path/to/Configurations/inventory_transform.md`).

---

#### **Benefits**

- **Modularity**:
  - Break large configurations into smaller, reusable parts.
- **Dynamic Updates**:
  - Use queries to dynamically load updated configurations from databases.
- **Ease of Maintenance**:
  - Keep configurations for different processes in separate files or sources, simplifying updates and version control.

By leveraging the `REQUIRES` section, you can maintain a clean and scalable ETL configuration structure, promoting reusability and modular design.

---

### **Union Queries for Aggregated Results**

The `MULTI_QUERIES` section allows you to define multiple queries with similar structures and aggregate their results using a SQL `UNION`. This is particularly useful when generating summaries or reports that combine data from multiple queries into a single result set.

---

#### **Structure**

1. **Metadata**:
   - The `MULTI_QUERIES` section includes metadata for connection details, pre/post-SQL commands, and activation status.
   - The `union_key` defines how the queries are combined (e.g., `UNION`, `UNION ALL`).
   - 

2. **Query Definitions**:
   - Each query is defined as a Level 2 heading under the `MULTI_QUERIES` block.
   - Queries inherit metadata from the parent block unless overridden.

3. **Execution**:
   - All queries are combined using the specified `union_key` (default is `UNION`).
   - The combined query is executed as a single statement.
   - The combined query can be save by specifing `save_sql` normally an insert, create [or replace] table or even a copy to file statment, and insert statment should be used in combination with the `save_on_err_patt` and `save_on_err_sql` in case of an error matching the `table does ... not exist` to create the tabe instead.
---

#### **Markdown Example**

````markdown
# MULTI_QUERIES
```yaml
description: "Define multiple structured queries combined with UNION."
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
save_sql: save_mult_query_res
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: create_mult_query_res
after_sql: "DETACH DB"
union_key: "UNION ALL\n" # Defaults to UNION.
active: true
```

```sql
-- save_mult_query_res
INSERT INTO "DB"."MULTI_QUERY" BY NAME
[[final_query]]
```
```sql
-- create_mult_query_res
CREATE OR REPLACE "DB"."MULTI_QUERY" AS
[[final_query]]
```

## Row1
```yaml
name: Row1
description: "Row 1"
connection: "duckdb:"
before_sql:
  - "LOAD sqlite"
  - "ATTACH 'reporting.db' AS DB (TYPE SQLITE)"
query: row_query
active: true
```

```sql
-- row_query
SELECT '# number of rows' AS "variable", COUNT(*) AS "value"
FROM "sales";
```

## Row2
```yaml
name: Row2
description: "Row 2"
query: row_query
active: true
```

```sql
-- row_query
SELECT 'total revenue' AS "variable", SUM("total") AS "value"
FROM "sales";
```

## Row3
```yaml
name: Row3
description: "Row 3"
query: row_query
active: true
```

```sql
-- row_query
SELECT "region" AS "variable", SUM("total") AS "value"
FROM "sales"
GROUP BY "region";
```
````

---

#### **How It Works**

1. **Defining Queries**:
   - Queries are defined as Level 2 headings.
   - Each query can include its own metadata and SQL.

2. **Combining Queries**:
   - All queries are combined using the `union_key` (e.g., `UNION` or `UNION ALL`).
   - The combined query is executed as a single statement.

3. **Execution Flow**:
   - Executes the `before_sql` commands at the start.
   - Combines all active queries with the `union_key`.
   - Executes the combined query.
   - Executes the `after_sql` commands after the query execution.

---

#### **Example Use Case**

For the example above:
1. **Row1**:
   - Counts the number of rows in the `sales` table.
2. **Row2**:
   - Calculates the total revenue from the `sales` table.
3. **Row3**:
   - Sums the revenue for each region in the `sales` table.

The resulting combined query:

```sql
LOAD sqlite;
ATTACH 'reporting.db' AS DB (TYPE SQLITE);

SELECT '# number of rows' AS "variable", COUNT(*) AS "value"
FROM "sales"
UNION ALL
SELECT 'total revenue' AS "variable", SUM("total") AS "value"
FROM "sales"
UNION ALL
SELECT "region" AS "variable", SUM("total") AS "value"
FROM "sales"
GROUP BY "region";

DETACH DB;
```

---

#### **Benefits**

- **Efficiency**:
  - Executes multiple queries in a single statement.
- **Flexibility**:
  - Combines queries using `UNION` or `UNION ALL`.
- **Customizability**:
  - Supports query-specific and parent-level metadata for maximum control.

With the `MULTI_QUERIES` section, you can simplify the process of aggregating data from multiple queries into a unified result set.


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

# ETLX Conclusion

ETLX is a powerful tool for defining and executing ETL processes using Markdown configuration files. It supports complex SQL queries, exports to multiple formats, and dynamic configuration loading. ETLX can be used as a library, CLI tool, or integrated into other systems for advanced data workflows.

---

### **Features**

- **Config Parsing from Markdown**:
  - Supports YAML, TOML, and JSON metadata blocks.
  - Automatically parses and structures Markdown configuration into a nested data structure.

- **ETL Execution**:
  - Handles extract, transform, and load processes using a modular design.
  - Supports complex workflows with customizable steps.

- **Query Documentation**:
  - Define modular SQL queries in sections.
  - Combines query parts dynamically to build complete SQL statements.

- **Exports**:
  - Export data to various formats (e.g., CSV, Excel).
  - Supports template-based exports for custom reports.

- **Requires**:
  - Dynamically load additional configuration from files or database queries.

- **CLI Interface**:
  - Command-line interface for executing ETLX configurations.
  - Supports flags for custom parameters (e.g., `--config`, `--date`, `--steps`).

---

### **To-Do List**

Here is the current progress and planned features for the ETLX project:

#### ✅ **Completed**
- **Config Parsing**:
  - Parses and validates Markdown configurations with nested sections and metadata.
  - Supports YAML, TOML, and JSON for metadata.

- **ETL Execution**:
  - Modular handling of extract, transform, and load processes.
  - Flexible step configuration with before and after SQL.

- **Query Documentation**:
  - Handles complex SQL queries by breaking them into logical components.
  - Dynamically combines query parts to create executable SQL.

- **Exports**:
  - Supports exporting data to files in formats like CSV and Excel.
  - Includes options for templates and data mapping.

- **Requires**:
  - Loads additional configurations dynamically from files or database queries.
  - Integrates loaded configurations into the main process.

- **CLI Interface**:
  - Provides a command-line interface for running configurations.
  - Accepts flags for custom execution parameters.

#### 🕒 **To-Do**
- **Web API**:
  - Create a RESTful web API for executing ETL configurations.
  - Expose endpoints for:
    - Uploading and managing configurations.
    - Triggering ETL workflows.
    - Monitoring job status and logs.
  - Add support for multi-user environments with authentication and authorization.

---

### **Usage**

#### **Command-Line Interface**
Run the ETLX binary with the required flags:

```bash
etlx --config config.md --date 2024-01-01 --steps extract,load
```

#### **Library Integration**
ETLX can be embedded into Go projects as a library:

```go
package main

import (
    "fmt"
    "os"
    "etlx"
)

func main() {
    etl := etlx.NewETLX()
    err := etl.ConfigFromFile("config.md")
    if err != nil {
        fmt.Printf("Error loading config: %v\n", err)
        os.Exit(1)
    }

    // Execute the ETL process
    err = etl.Run(map[string]any{
        "date": "20240101",
    })
    if err != nil {
        fmt.Printf("Error running ETL: %v\n", err)
        os.Exit(1)
    }
}
```
---

By building on the existing features, the ETLX project is moving toward becoming a comprehensive ETL solution for both CLI and web environments.

---

## **License**

This project is licensed under the MIT License.
