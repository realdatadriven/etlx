# ETLX: DuckDB-Powered Markdown-Driven ETL Framework, A Modern Approach to ETL / ELT Workflows

ETLX leverages the power of DuckDB to provide a streamlined, configuration-driven approach to ETL / ELT workflows. With its Markdown-based configuration and extensibility, ETLX simplifies data integration, transformation, automation, quality check, documentation ...

This project is a high-performance **ETL (Extract, Transform, Load) Framework** powered by **DuckDB**, designed to integrate and process data from diverse sources. It uses Markdown as configuration inputs (inspired by evidence.dev), where **YAML|TOML|JSON metadata** defines data source properties, and **SQL blocks** specify the logic for extraction, transformation, and loading.

Supports a variety of data sources, including:

- Relational Databases: **Postgres**, **MySQL**, **SQLite**, **ODBC**.

- Cloud Storage: **S3**, **and other S3 compatible Object Storage**.

- File Formats: **CSV**, **Parquet**, **Spreadsheets**.

By leveraging DuckDB's powerful processing capabilities, this framework enables seamless ETL / ELT operations.

---

## **Features**

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

## **Table of Contents**

1. [Introduction](#1-introduction)
2. [Quick Start](#2-quick-start)
3. [Features](#3-features)
4. [Configuration Details](#4-configuration-details)
   - [ETL](#etl)
   - [Validation Rules](#validation-rules)
   - [Query Documentation](#query-documentation)
   - [Data Quality](#data-quality)
   - [Exports](#exports)
   - [Scripts](#scripts)
   - [Multi-Queries](#multi-queries)
   - [Loading Config Dependencies](#loading-config-dependencies)
   - [Actions](#actions)
   - [Notification](#notify)
5. [Advanced Usage](#5-advanced-usage)
6. [Embedding in Go](#6-embedding-in-go)
7. [Future Plans](#7-future-plans)
8. [License](#8-license)

---

## **1. Introduction**

ETLX introduces an innovative, flexible way to handle ETL processes, data quality validation, and report automation. It empowers users with:

- **Markdown-driven Configuration**: Define your workflows in a structured, human-readable format.
- **DuckDB Integration**: Utilize DuckDB’s capabilities to work with diverse data sources, including databases, object storage, and APIs.
- **Extensibility**: Easily adapt to new use cases with modular configurations.

---

## **2. Quick Start**

### **Installation**

#### **Option 1: Precompiled Binaries**

Precompiled binaries for Linux, macOS, and Windows are available on the [releases page](https://github.com/realdatadriven/etlx/releases). Download the appropriate binary for your system and make it executable:

```bash
# Example for Linux or macOS
chmod +x etlx
./etlx --help
```

#### **Option 2: Install via Go (as a library)**

```bash
# Install ETLX
go get github.com/realdatadriven/etlx
```

#### **Option 3: Clone Repo**

```bash
git clone https://github.com/realdatadriven/etlx.git
cd etlx
```

And then:

```bash
go run cmd/main.go --config etl_config.md --date 2023-10-31
```

the same can be said for build
On Windows you may have build issues, in that case I found out that is easier to just use the latest libduckdb from [duckdb/releases](https://github.com/duckdb/duckdb/releases) put it in your path and then build with -tags=duckdb_use_lib

```bash
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go run -tags=duckdb_use_lib main.go --config etl_config.md --date 2023-10-31
```

### **Running ETLX**

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

```bash
etlx --config etl_config.md --date 2023-10-31 --only sales --steps extract,load
```

---

### **🐳 Running ETLX with Docker**  

You can run **etlx** directly from Docker without installing Go or building locally.

#### Build the Image

Clone the repo and build:

```bash
docker build -t etlx:latest .
```

Or pull the prebuilt image (when published):

```bash
docker pull docker.io/realdatadriven/etlx:latest
```

---

#### Running Commands

The image behaves exactly like the CLI binary.
For example:

```bash
docker run --rm etlx:latest help
docker run --rm etlx:latest version
docker run --rm etlx:latest run --config /app/config.md
```

---

#### Using a `.env` File

If you have a `.env` file with environment variables, mount it into `/app/.env`:

```bash
docker run --rm \
  -v $(pwd)/.env:/app/.env:ro \
  etlx:latest run --config /app/config.md
```

---

#### Mounting Config Files

Mount your config file into the container and reference it by path:

```bash
docker run --rm \
  -v $(pwd)/config.md:/app/config.md:ro \
  etlx:latest run --config /app/config.md
```

---

#### Database Directory

`etlx` can attach a database directory. Mount your local `./database` directory into `/app/database`:

```bash
docker run --rm \
  -v $(pwd)/database:/app/database \
  etlx:latest run --config /app/config.md
```

---

#### Combine All Together

Mount `.env`, config, and database directory:

```bash
docker run --rm \
  -v $(pwd)/.env:/app/.env:ro \
  -v $(pwd)/config.md:/app/config.md:ro \
  -v $(pwd)/database:/app/database \
  etlx:latest run --config /app/config.md
```

---

#### Interactive Mode

For interactive subcommands (like `repl`):

```bash
docker run -it --rm etlx:latest repl
```

---

#### 💡 Pro Tip: Local Alias

You can add an alias so Docker feels like the native binary:

```bash
alias etlx="docker run --rm -v $(pwd):/app etlx:latest"
```

Now you can just run:

```bash
etlx help
etlx run --config /app/config.md
```

---


---

### **How It Works**

Create a Markdown file with the ETL process configuration. For example:

#### **Example Use Case**

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

Run the workflow:

```bash
etlx --config config.md --date 2024-01-01
```

#### **Parse the Markdown File**

- Parse the Markdown file to extract:
  - **YAML|TOML|JSON blocks**: For metadata and configuration.
  - **SQL blocks**: For ETL logic / queries defined in the metadata.

#### **Execute the Workflow**

- Use DuckDB to:
  - Extract data from the source.
  - Apply transformations (if specified).
  - Load the processed data into the target.

---

## **3. Features**

- Markdown-based configurations for easy readability.
- Extensive DuckDB support for various data sources and formats.
- Modular design for reusability and clarity.
- Built-in error handling and validation.
- Export functionality for reports, templates, and data lakes.

---

## **4. Configuration Details**

### **ETL**

- Defines the overall ETL process.
- Example:

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
...
````

#### 1. **ETL Process Starts**

- Begin with the `"ETL"` key;
- Extract metadata, specifically:
  - `"connection"`: Main connection to the destination database.
  - `"description"`: For logging the start and end time of the ETL process.

#### 2. **Loop through Level 2 key in under `"ETL"` key**

- Iterate over each key (e.g., `"sales_data"`)
- For each key, access its `"metadata"` to process the ETL steps.

#### 3. **ETL Steps**

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
- Additionally, error handling can be defined using `[step]_on_err_match_patt` and `[step]_on_err_match_sql` to handle specific database errors dynamically, where `[step]_on_err_match_patt` is the `regexp` patthern to match error,and if maches the `[step]_on_err_match_sql` is executed, the same can be applied for `[step]_before_on_err_match_patt` and `[step]_before_on_err_match_sql`.
   You can define patterns to match specific errors and provide SQL statements to resolve those errors. This feature is useful when working with dynamically created databases, tables, or schemas.

#### 4. **Output Logs**

- Log progress (e.g., connection usage, start/end times, descriptions).
- Gracefully handle missing or `null` keys.

### **Validation Rules**

- Validate data quality during the ETL process by using the key `[step]_validation` in the metadata section.
- Example:

```yaml
...
load_validation:
  - type: throw_if_empty
    sql: validate_data_not_empty
    msg: 'No data extracted for the given date!'
    active: false
  - type: throw_if_not_empty
    sql: validate_data_duplicates
    msg: 'Duplicate data detected!'
...
```

For every object in the `[step]_validation` the `sql` is executed in the `[step]_con` connection, and it cal eather throw error message defined in `msg` or not if the condition (`type:throw_if_empty | type:throw_if_not_empty` or) is met or not.

#### **Extracting Data from Unsupported Databases**

If the database you are using does not have a direct DuckDB scanner, but it is supported by **sqlx** or it has `odbc` support, you must set the `to_csv` option to `true` in the `extract` configuration. This ensures that data is first exported to a CSV file and then on step `load` the file can be processed by DuckDB.  

##### **Example Configuration:**

````markdown
...

## table_from_odbc_source

```yaml metadata
name: table_from_odbc_source
description: 'This is an example o how to extract from databases that does not have a DuckDB scanner'
to_csv: true
extract_conn: 'odbc:DRIVER={ODBC Driver 17 for SQL Server};SERVER=@MSSQL_HOST;UID=@MSSQL_USER;PWD=@MSSQL_PASS;DATABASE=DB'
extract_sql: |
  SELECT [fields]
  FROM [table]
  WHERE [condition]
load_conn: 'duckdb:'
load_before_sql:
  - load_extentions
  - conn
load_sql: load_exported_csv
load_after_sql: detaches
```
...

Once extracted, the CSV file can be loaded by DuckDB using:  

```sql load_exported_csv
CREATE OR REPLACE TABLE DB.target_table AS  
SELECT * FROM READ_CSV('<fname>', HEADER TRUE);  
```  

````

##### **Why Use `to_csv: true`?**

- **Workaround for unsupported databases**: If DuckDB does not have a direct scanner, exporting data to CSV allows and then the CSV can be loaded by the DuckDB.  
- **Ensures compatibility**: ETLX will handle the conversion, making the data accessible for further transformation and loading.  
- **Required for smooth ETL workflows**: Without this option, DuckDB may fail to recognize or query the database directly.  

### **Query Documentation**

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

### **Data Quality**

The `DATA_QUALITY` section allows you to define and execute validation rules to ensure the quality of your data. Each rule performs a check using a SQL query to identify records that violate a specific condition. Optionally, you can define a query to fix any identified issues automatically if applicable.

---

#### **Data Quality Structure**

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

#### **Data Quality Markdown Example**

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
check_only: false # runs only quality check if true
fix_only: false # runs only quality fix if true and available and possible
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

#### **How Data Quality Works**

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

#### **Data Quality Example Use Case**

For the example above:

1. **Rule0001**:
   - Validates that the `option` field contains only the values `y` and `z`.
   - Updates invalid records to a default value using the fix query.

2. **Rule0002**:
   - Validates that the `option2` field contains only the values `x` and `z`.
   - Updates invalid records to a default value using the fix query.

---

#### **Data Quality Benefits**

- **Automated Quality Assurance**:
  - Identify and fix data issues programmatically.

- **Customizable Rules**:
  - Define rules tailored to your specific data quality requirements.

- **Flexibility**:
  - Supports pre- and post-SQL commands for advanced workflows.

---

By integrating the `DATA_QUALITY` block, you can ensure the integrity of your data and automate validation processes as part of your ETL pipeline.

---

### **Exports**

The `EXPORTS` section in the ETL configuration handles exporting data to files. This is particularly useful for generating reports for internal departments, regulators, partners, or saving processed data to a data lake. By leveraging DuckDB's ability to export data in various formats, this section supports file generation with flexibility and precision.

---

#### **Export Structure**

An export configuration is defined as a top-level heading (e.g., `# EXPORTS`) in the configuration. Within this section:

1. **Exports Metadata**:
   - Metadata defines properties like the database connection, export path, and activation status.
   - Fields like `name`, `description`, `path`, and `active` control the behavior of each export.

2. **Query-to-File Configuration**:
   - Define the SQL query or sequence of queries used for generating the file.
   - Specify the export format, such as CSV, Parquet, or Excel.

3. **Template-Based Exports**:
   - Templates allow you to map query results into specific cells and sheets of an existing spreadsheet template.

---

#### **Export Markdown**

````markdown
# EXPORTS
Exports data to files.

```yaml metadata
name: DailyExports
description: "Daily file exports for various datasets."
database: reporting_db
connection: "duckdb:"
path: "/path/to/Reports/YYYYMMDD"
active: true
```

## Sales Data Export
```yaml metadata
name: SalesExport
description: "Export daily sales data to CSV."
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
name: RegionExport
description: "Export region data to an Excel file."
connection: "duckdb:"
export_sql:
  - "LOAD sqlite"
  - "LOAD excel"
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
) TO '/path/to/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx' (FORMAT XLSX, HEADER TRUE);
```

## Sales Report Template
```yaml metadata
name: SalesReport
description: "Generate a sales report from a template."
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

#### **How Exporting It Works**

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

#### **Benefits of this functionality**

- **Flexibility**:
  - Export data in multiple formats (e.g., CSV, Excel) using DuckDB's powerful `COPY` command.
- **Reusability**:
  - Use predefined templates to create consistent reports.
- **Customizability**:
  - SQL queries and mappings allow fine-grained control over the exported data.

By leveraging the `EXPORTS` section, you can automate data export processes, making them efficient and repeatable.

#### 📝 Exporting as Text-Based Template

In addition to exporting structured data formats like CSV or Excel, ETLX also supports exporting reports using plain-text templates such as **HTML**, **XML**, **Markdown**, etc.

These templates are rendered using Go’s `text/template` engine and can use dynamic data from SQL queries declared under the `data_sql` field, just like in the `NOTIFY` section.

This is especially useful for **reporting**, **integration**, or **publishing documents** with dynamic content.

##### 📦 Example

````markdown
...
## TEXT_TMPL

```yaml metadata
name: TEXT_TMPL
description: "Export data to text base template"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
data_sql:
  - logs
  - data
after_sql: "DETACH DB"
tmp_prefix: null
text_template: true
template: template
return_content: false  # if true, returns content instead of writing file
path: "nyc_taxy_YYYYMMDD.html"
active: true
```

```sql
-- data
SELECT *
FROM "DB"."NYC_TAXI"
WHERE "tpep_pickup_datetime"::DATETIME <= '{YYYY-MM-DD}'
LIMIT 100
```

```sql
-- logs
SELECT *
FROM "DB"."etlx_logs"
--WHERE "ref" = '{YYYY-MM-DD}'
```

```html template
<style>
  table {
    border-collapse: collapse;
    width: 100%;
    font-family: sans-serif;
    font-size: 14px;
  }
  th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
  }
  th {
    background-color: #f2f2f2;
    font-weight: bold;
  }
  tr:nth-child(even) {
    background-color: #f9f9f9;
  }
  tr:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    background-color: #eef6ff;
  }
</style>
<b>ETLX Text Template</b><br /><br />
This is gebnerated by ETLX automatically!<br />
{{ with .logs }}
    {{ if eq .success true }}
      <table>
        <tr>
            <th>Name</th>
            <th>Ref</th>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Success</th>
            <th>Message</th>
        </tr>
        {{ range .data }}
        <tr>
            <td>{{ .name }}</td>
            <td>{{ .ref }}</td>
            <td>{{ .start_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ .end_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ divf .duration 1000000000 | printf "%.4fs" }}</td>
            <td>{{ .success }}</td>
            <td><span title="{{ .msg }}">{{ .msg | toString | abbrev 30}}</span></td>
        </tr>
        {{ else }}
        <tr>
          <td colspan="7">No items available</td>
        </tr>
        {{ end }}
      </table>
    {{ else }}
      <p>{{.msg}}</p>
    {{ end }}
{{ else }}
<p>Logs information missing.</p>
{{ end }}
```

````

##### 🧩 Parameters

| Field             | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `text_template`  | Enables text-based template rendering (`true`)                              |
| `template`       | The name of the SQL block that contains the Go template                     |
| `data_sql`       | Slice of named SQL blocks whose results will be passed to the template      |
| `path`           | Output file path (can include placeholders like `{YYYYMMDD}`)               |
| `return_content` | If `true`, does not save to disk and returns rendered text content instead  |

##### 🧰 Advanced Template Functions (Sprig)

ETLX integrates the [`Sprig`](https://github.com/Masterminds/sprig) The Sprig library provides over 70 template functions for Go’s template language, such as:

- String manipulation: `upper`, `lower`, `trim`, `contains`, `replace`
- Math: `add`, `mul`, `round`
- Date formatting: `date`, `now`, `dateModify`
- List operations: `append`, `uniq`, `join`

You can use these helpers directly in your templates:

```gotmpl
{{ .ref | upper }}
{{ .start_at | date "2006-01-02" }}
```

This enables powerful report generation and custom formatting out-of-the-box.

---

### Scripts

The **SCRIPTS** section allows you to **execute SQL queries** that **don’t fit into other predefined sections** (ETL, EXPORTS, etc.).  

#### **🔹 When to Use SCRIPTS?**

✅ **Running cleanup queries after an ETL job**  
✅ **Executing ad-hoc maintenance tasks**  
✅ **Running SQL commands that don’t need to return results**  
✅ **Executing SQL scripts for database optimizations**  

#### **🛠 Example: Running Cleanup Scripts**    

This example **removes temporary data** after an ETL process.

#### **📄 Markdown Configuration**

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

#### **🔹 How Scripts It Works**

1️⃣ **Loads necessary extensions and connects to the database.**  
2️⃣ **Executes predefined SQL queries (`script_sql`).**  
3️⃣ **Runs `before_sql` commands before execution.**  
4️⃣ **Runs `after_sql` commands after execution.**  

#### **🔹 Key Scripts Features**

✔ **Flexible SQL execution for custom scripts**  
✔ **Supports cleanup, maintenance, and database operations**  
✔ **Allows execution of any SQL command that doesn't return data**  
✔ **Easily integrates into automated ETL workflows**  

---

### **Multi-Queries**

The `MULTI_QUERIES` section allows you to define multiple queries with similar structures and aggregate their results using a SQL `UNION`. This is particularly useful when generating summaries or reports that combine data from multiple queries into a single result set.

---

#### **Multi-Queries Structure**

1. **Metadata**:
   - The `MULTI_QUERIES` section includes metadata for connection details, pre/post-SQL commands, and activation status.
   - The `union_key` defines how the queries are combined (e.g., `UNION`, `UNION ALL`).

2. **Query Definitions**:
   - Each query is defined as a Level 2 heading under the `MULTI_QUERIES` block.
   - Queries inherit metadata from the parent block unless overridden.

3. **Execution**:
   - All queries are combined using the specified `union_key` (default is `UNION`).
   - The combined query is executed as a single statement.
   - The combined query can be save by specifying `save_sql` normally an insert, create [or replace] table or even a copy to file statement, and insert statement should be used in combination with the `save_on_err_patt` and `save_on_err_sql` in case of an error matching the `table does ... not exist` to create the table instead.

---

#### **Multi-Queries Markdown Example**

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
CREATE OR REPLACE TABLE "DB"."MULTI_QUERY" AS
[[final_query]]
```

## Row1
```yaml
name: Row1
description: "Row 1"
query: row_query
active: true
```

```sql
-- row_query
SELECT '# number of rows' AS "variable", COUNT(*) AS "value"
FROM "sales"
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
FROM "sales"
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
GROUP BY "region"
```
````

---

#### **Multi-Queries How It Works**

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

#### **Multi-Queries Example Use Case**

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

#### **Multi-Queries Benefits**

- **Efficiency**:
  - Executes multiple queries in a single statement.
- **Flexibility**:
  - Combines queries using `UNION` or `UNION ALL`.
- **Customizability**:
  - Supports query-specific and parent-level metadata for maximum control.

With the `MULTI_QUERIES` section, you can simplify the process of aggregating data from multiple queries into a unified result set.

---

## 📝 Logs Handling (`# LOGS`)

ETLX provides a **logging mechanism** that allows **saving logs** into a database. This is useful for tracking **executions, debugging, and auditing ETL processes**.

### **🔹 How It Works**

- The `LOGS` section defines where and how logs should be saved.
- The process consists of **three main steps**:
  1. **Prepare the environment** using `before_sql` (e.g., loading extensions, attaching databases).
  2. **Execute `save_log_sql`** to store logs in the database.
  3. **Run `after_sql`** for cleanup (e.g., detaching the database).

### **🛠 Example LOGS Configuration**

Below is an example that **saves logs** into a **database**:

#### **📄 LOGS Markdown Configuration**

````md
# LOGS
```yaml metadata
name: LOGS
description: "Example saving logs"
table: logs
connection: "duckdb:"
before_sql:
  - load_extentions
  - attach_db
save_log_sql: load_logs
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: create_logs
after_sql: detach_db
tmp_dir: tmp
active: true
```

```sql
-- load_extentions
INSTALL Sqlite;
LOAD Sqlite;
INSTALL json;
LOAD json;
```

```sql
-- attach_db
ATTACH 'examples/S3_EXTRACT.db' AS "DB" (TYPE SQLITE)
```

```sql
-- detach_db
DETACH "DB";
```

```sql
-- load_logs
INSERT INTO "DB"."<table>" BY NAME
SELECT * 
FROM READ_JSON('<fname>');
```

```sql
-- create_logs
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT * 
FROM READ_JSON('<fname>');
```

````

### **🔹 How to Use**

- This example saves logs into a **SQLite database attached to DuckDB**.
- The **log table (`logs`) is created or replaced** on each run.
- The `<table>` and `<fname>` placeholders are dynamically replaced.

### **🎯 Summary**

✔ **Keeps a persistent log of ETL executions**  
✔ **Uses DuckDB for efficient log storage**  
✔ **Supports preprocessing (`before_sql`) and cleanup (`after_sql`)**  
✔ **Highly customizable to different logging needs**  

### Default logs

By default is generated a sqlite db `etlx_logs.db` in temp folder, that'll depende on the OS, it adds to your config this peace os md:

````markdown
# AUTO_LOGS

```yaml metadata
name: LOGS
description: "Logging"
table: logs
connection: "duckdb:"
before_sql:
  - "LOAD Sqlite"
  - "ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE)"
  - "USE etlx_logs"
  - "LOAD json"
  - "get_dyn_queries[create_missing_columns](ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE),DETACH etlx_logs)"
save_log_sql: |
  INSERT INTO "etlx_logs"."<table>" BY NAME
  SELECT *
  FROM READ_JSON('<fname>');
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: |
  CREATE TABLE "etlx_logs"."<table>" AS
  SELECT *
  FROM READ_JSON('<fname>');
after_sql:
  - 'USE memory'
  - 'DETACH "etlx_logs"'
active: true
```

```sql
-- create_missing_columns
WITH source_columns AS (
    SELECT "column_name", "column_type"
    FROM (DESCRIBE SELECT * FROM READ_JSON('<fname>'))
),
destination_columns AS (
    SELECT "column_name", "data_type" as "column_type"
    FROM "duckdb_columns"
    WHERE "table_name" = '<table>'
),
missing_columns AS (
    SELECT "s"."column_name", "s"."column_type"
    FROM source_columns "s"
    LEFT JOIN destination_columns "d" ON "s"."column_name" = "d"."column_name"
    WHERE "d"."column_name" IS NULL
)
SELECT 'ALTER TABLE "etlx_logs"."<table>" ADD COLUMN "' || "column_name" || '" ' || "column_type" || ';' AS "query"
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```
````

But it can be overiden to be saved on your own database of choice by changing `ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE)`

---

### **Loading Config Dependencies**

The `REQUIRES` section in the ETL configuration allows you to load dependencies from external Markdown configurations. These dependencies can either be loaded from file paths or dynamically through queries. This feature promotes modularity and reusability by enabling you to define reusable parts of the configuration in separate files or queries.

---

#### **Loading Structure**

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

#### **Loading Markdown Example**

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

#### **How Loading Works**

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

#### **Loading - Example Use Case**

For the example above, the following happens:

1. **Sales Transformation**:
   - A query retrieves the Markdown configuration content for sales transformations from a database table.
   - The `before_sql` and `after_sql` scripts prepare the environment for the query execution.

2. **Inventory Transformation**:
   - A Markdown configuration is loaded from an external file path (`/path/to/Configurations/inventory_transform.md`).

---

#### **Loading - Benefits**

- **Modularity**:
  - Break large configurations into smaller, reusable parts.
- **Dynamic Updates**:
  - Use queries to dynamically load updated configurations from databases.
- **Ease of Maintenance**:
  - Keep configurations for different processes in separate files or sources, simplifying updates and version control.

By leveraging the `REQUIRES` section, you can maintain a clean and scalable ETL configuration structure, promoting reusability and modular design.

---

### ACTIONS

(still under development)

There are scenarios in ETL workflows where actions such as downloading, uploading, compressing or copying files cannot be performed using SQL alone (e.g., uploading templates or metadata files to S3, FTP, HTTP, etc.).

To support this, ETLX introduces a special section called **`ACTIONS`**, which allows you to define steps for copying or transferring files using the file system or external protocols.

---

#### **ACTIONS Structure**

Each action under the `ACTIONS` section has the following:

- `name`: Unique name for the action.
- `description`: Human-readable explanation.
- `type`: The kind of action to perform. Options:
  - `copy_file`
  - `compress`
  - `decompress`
  - `ftp_download`
  - `ftp_upload`
  - `sftp_download`
  - `sftp_upload`
  - `http_download`
  - `http_upload`
  - `s3_download`
  - `s3_upload`
  - `db_2_db`
- `params`: A map of input parameters required by the action type.

---

````markdown

# ACTIONS

```yaml metadata
name: FileOperations
description: "Transfer and organize generated reports"
path: examples
active: true
```

---

## COPY LOCAL FILE

```yaml metadata
name: CopyReportToArchive
description: "Move final report to archive folder"
type: copy_file
params:
  source: "C:/reports/final_report.xlsx"
  target: "C:/reports/archive/final_report_YYYYMMDD.xlsx"
active: true
```

---

## Compress to ZIP

```yaml metadata
name: CompressReports
description: "Compress report files into a .zip archive"
type: compress
params:
  compression: zip
  files:
    - "reports/report_1.csv"
    - "reports/report_2.csv"
  output: "archives/reports_YYYYMM.zip"
active: true
```

## UNZIP

```yaml metadata
name: CompressReports
description: "Compress report files into a .zip archive"
type: decompress
params:
  compression: zip
  input: "archives/reports_YYYYMM.zip"
  output: "tmp"
active: true
```

---

## Compress to GZ

```yaml metadata
name: CompressToGZ
description: "Compress a summary file to .gz"
type: compress
params:
  compression: gz
  files:
    - "reports/summary.csv"
  output: "archives/summary_YYYYMM.csv.gz"
active: true
```

---

## HTTP DOWNLOAD

```yaml metadata
name: DownloadFromAPI
description: "Download dataset from HTTP endpoint"
type: http_download
params:
  url: "https://api.example.com/data"
  target: "data/today.json"
  method: GET
  headers:
    Authorization: "Bearer @API_TOKEN"
    Accept: "application/json"
  params:
    date: "YYYYMMDD"
    limit: "1000"
active: true
```

---

## HTTP UPLOAD

```yaml metadata
name: PushReportToWebhook
description: "Upload final report to an HTTP endpoint"
type: http_upload
params:
  url: "https://webhook.example.com/upload"
  method: POST
  source: "reports/final.csv"
  headers:
    Authorization: "Bearer @WEBHOOK_TOKEN"
    Content-Type: "multipart/form-data"
  params:
    type: "summary"
    date: "YYYYMMDD"
active: true
```

---

## FTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external FTP"
type: ftp_download
params:
  host: "ftp.example.com"
  port: "21"
  user: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: true
```

## FTP DOWNLOAD GLOB

```yaml metadata
name: FetchRemoteReport2024
description: "Download data file from external FTP"
type: ftp_download
params:
  host: "ftp.example.com"
  port: "21"
  user: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report_2024*.csv"
  target: "downloads/"
active: true
```

## SFTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external SFTP"
type: stp_download
params:
  host: "sftp.example.com"
  user: "myuser"
  password: "@SFTP_PASSWORD"
  host_key: ~/.ssh/known_hosts # or a specific file
  port: 22
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: true
```

---

## S3 UPLOAD

```yaml metadata
name: ArchiveToS3
description: "Send latest results to S3 bucket"
type: s3_upload
params:
  AWS_ACCESS_KEY_ID: '@AWS_ACCESS_KEY_ID'
  AWS_SECRET_ACCESS_KEY: '@AWS_SECRET_ACCESS_KEY'
  AWS_REGION: '@AWS_REGION'
  AWS_ENDPOINT: 127.0.0.1:3000
  S3_FORCE_PATH_STYLE: true
  S3_DISABLE_SSL: false
  S3_SKIP_SSL_VERIFY: true
  bucket: "my-etlx-bucket"
  key: "exports/summary_YYYYMMDD.xlsx"
  source: "reports/summary.xlsx"
active: true
```

## S3 DOWNLOAD

```yaml metadata
name: DownalodFromS3
description: "Download file S3 from bucket"
type: s3_download
params:
  AWS_ACCESS_KEY_ID: '@AWS_ACCESS_KEY_ID'
  AWS_SECRET_ACCESS_KEY: '@AWS_SECRET_ACCESS_KEY'
  AWS_REGION: '@AWS_REGION'
  AWS_ENDPOINT: 127.0.0.1:3000
  S3_FORCE_PATH_STYLE: true
  S3_DISABLE_SSL: false
  S3_SKIP_SSL_VERIFY: true
  bucket: "my-etlx-bucket"
  key: "exports/summary_YYYYMMDD.xlsx"
  target: "reports/summary.xlsx"
active: true
```

````

##### 📥 ACTIONS – `db_2_db` (Cross-Database Write)

> As of this moment, **DuckDB does not support direct integration** with certain databases like **MSSQL**, **DB2**, or **Oracle**, the same way it does with **SQLite**, **Postgres**, or **MySQL**.

To bridge this gap, the `db_2_db` action type allows you to **query data from one database** (source) and **write the results into another** (target), using ETLX’s internal execution engine (powered by `sqlx` or ODBC).

###### ✅ Use Case

Use `db_2_db` when:

- Your database is not accessible with DuckDB.
- You want to move data from one place to another using **pure SQL**, chunked if necessary.

---

###### 🧩 Example

````markdown
...

## WRITE_RESULTS_MSSQL

```yaml metadata
name: WRITE_RESULTS_MSSQL
description: "MSSQL example – moving logs into a SQL Server database."
type: db_2_db
params:
  source:
    conn: sqlite3:database/HTTP_EXTRACT.db
    before: null
    chunk_size: 1000
    timeout: 30
    sql: origin_query
    after: null
  target:
    conn: mssql:sqlserver://sa:@MSSQL_PASSWORD@localhost?database=master&connection+timeout=30
    timeout: 30
    before:
      - create_schema
    sql: mssql_sql
    after: null
active: true
```

```sql
-- origin_query
SELECT "description", "duration", STRFTIME('%Y-%m-%d %H:%M:%S', "start_at") AS "start_at", "ref"
FROM "etlx_logs" 
ORDER BY "start_at" DESC
```

```sql
-- create_schema
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etlx_logs' AND type = 'U')
CREATE TABLE [dbo].[etlx_logs] (
    [description] NVARCHAR(MAX) NULL,
    [duration] BIGINT NULL,
    [start_at] DATETIME NULL,
    [ref] DATE NULL
);
```

```sql
-- mssql_sql
INSERT INTO [dbo].[etlx_logs] ([:columns]) VALUES 
```

````

---

### 🛠️ Notes

- You can define **`before` and `after` SQL** on both `source` and `target` sides.
- **`[:columns]`** will be automatically expanded with the column list.
- Data is inserted in **chunks** using the provided `chunk_size`.
- Compatible with **any driver supported by `sqlx`** or databse tahat has an ODBC driver.

---

> 📝 **Note:** All paths and dynamic references (like `YYYYMMDD`) are replaced at runtime by the refered date.  
> You can use environmental variables via `@ENV_NAME`.

### ⚠️ **Note on S3 Configuration**

When using `s3_upload` or `s3_download`, ETLX will look for the required AWS credentials and config in the parameters you provide in your `ACTIONS` block, such as:

```yaml
AWS_ACCESS_KEY_ID: '@AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY: '@AWS_SECRET_ACCESS_KEY'
AWS_REGION: '@AWS_REGION'
AWS_ENDPOINT: '127.0.0.1:3000'
S3_FORCE_PATH_STYLE: true
S3_DISABLE_SSL: false
S3_SKIP_SSL_VERIFY: true
```

> 🧠 If these parameters are **not explicitly defined**, ETLX will **fall back** to the system's environment variables with the **same names**. This allows for better compatibility with tools like AWS CLI, Docker secrets, and `.env` files.

This behavior ensures flexible support for local development, staging environments, and production deployments where credentials are injected at runtime.

### ⚠️ **Security Warning: User-Defined Actions**

> ❗ **Dangerous if misused**  
> Allowing users to define or influence `ACTIONS` (e.g. file copy, upload, or download steps) introduces potential security risks such as:
>
> - **Arbitrary file access or overwrite**
> - **Sensitive file exposure (e.g. `/etc/passwd`)**
> - **Remote execution or data exfiltration**
>
> #### 🔐 Best Practices
>
> - **Restrict file paths** using whitelists (`AllowedPaths`) or path validation.
> - Never accept **unvalidated user input** for action parameters like `source`, `target`, or `url`.
> - Use **readonly or sandboxed environments** when possible.
> - Log and audit every `ACTIONS` block executed in production.
>
> 📌 If you're using ETLX as a library (Go or Python), you **must** sanitize and scope what the runtime has access to.

### NOTIFY

A fully automated ETL workflow often requires **notifications** to inform users about the process status. The `NOTIFY` section enables **email notifications via SMTP**, with support for **dynamic templates** populated with SQL query results.

#### **🔹 Why Use NOTIFY?**

✅ **Real-time updates on ETL status**  
✅ **Customizable email templates with dynamic content**  
✅ **Supports attachments for automated reporting**  
✅ **Ensures visibility into ETL success or failure**  

#### **🛠 Example: Sending ETL Status via Email**

This example sends an email **after an ETL process completes**, using **log data from the database**.

#### **📄 NOTIFY Markdown Configuration**

````markdown

# NOTIFY

```yaml metadata
name: Notification
description: "ETL Notification"
connection: "duckdb:"
path: "examples"
active: true
```

## ETL_STATUS

```yaml metadata
name: ETL_STATUS
description: "ETL Status"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
data_sql:
  - logs
after_sql: "DETACH DB"
to:
  - real.datadriven@gmail.com
cc: null
bcc: null
subject: 'ETLX YYYYMMDD'
body: body_tml
attachments:
  - hf.md
  - http.md
active: true
```

The **email body** is defined using a **Golang template**. The results from `data_sql` are available inside the template, also the Spring (`github.com/Masterminds/sprig`) library that provides more than 100 commonly used template functions.

```html body_tml
<style>
  table {
    border-collapse: collapse;
    width: 100%;
    font-family: sans-serif;
    font-size: 14px;
  }
  th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
  }
  th {
    background-color: #f2f2f2;
    font-weight: bold;
  }
  tr:nth-child(even) {
    background-color: #f9f9f9;
  }
  tr:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    background-color: #eef6ff;
  }
</style>
<b>Good Morning!</b><br /><br />
This email was gebnerated by ETLX automatically!<br />
LOGS:<br />
{{ with .logs }}
    {{ if eq .success true }}
      <table>
        <tr>
            <th>Name</th>
            <th>Ref</th>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Success</th>
            <th>Message</th>
        </tr>
        {{ range .data }}
        <tr>
            <td>{{ .name }}</td>
            <td>{{ .ref }}</td>
            <td>{{ .start_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ .end_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ divf .duration 1000000000 | printf "%.4fs" }}</td>
            <td>{{ .success }}</td>
            <td><span title="{{ .msg }}">{{ .msg | toString | abbrev 30}}</span></td>
        </tr>
        {{ else }}
        <tr>
          <td colspan="7">No items available</td>
        </tr>
        {{ end }}
      </table>
    {{ else }}
      <p>{{.msg}}</p>
    {{ end }}
{{ else }}
<p>Logs information missing.</p>
{{ end }}
```

```sql
-- logs
SELECT *
FROM "DB"."etlx_logs"
WHERE "ref" = '{YYYY-MM-DD}'
```

````

#### **🔹 How NOTIFY Works**

1️⃣ **Loads required extensions and connects to the database** (`before_sql`).  
2️⃣ **Executes `data_sql` queries** to retrieve data to be embeded in the body of the email.  
3️⃣ **Uses the results inside the `body` template** (Golang templating).  
4️⃣ **Sends an email with the formatted content and attachments.**  
5️⃣ **Executes cleanup queries (`after_sql`).**  

#### **🔹 Key NOTIFY Features**

✔ **Dynamic email content populated from SQL queries**  
✔ **Supports `to`, `cc`, `bcc`, `attachments`, and templated bodies**  
✔ **Executes SQL before and after sending notifications**  
✔ **Ensures ETL monitoring and alerting**  

---

## **5. Advanced Usage**

- Error Handling:
  Define patterns for resolving errors dynamically during execution.

  ```yaml
  load_on_err_match_patt: "(?i)table.+does.+not.+exist"
  load_on_err_match_sql: "CREATE TABLE sales_table (id INT, total FLOAT)"
  ```

- Modular Configuration:
  Break down workflows into reusable components for better maintainability.

### 🛠️ Advanced Usage: Dynamic Query Generation (`get_dyn_queries[...]`)

In some **advanced ETL workflows**, you may need to **dynamically generate SQL queries** based on metadata or schema differences between the source and destination databases.

#### **🔹 Why Use Dynamic Queries?**

✅ **Schema Flexibility** – Automatically adapt to schema changes in the source system.  
✅ **Self-Evolving Workflows** – ETL jobs can generate and execute additional SQL queries as needed.  
✅ **Automation** – Reduces the need for manual intervention when new columns appear.  

#### **🔹 How `get_dyn_queries[query_name](runs_before,runs_after)` Works**

- Dynamic queries are executed using the **`get_dyn_queries[query_name](runs_before,runs_after)`** pattern.
- During execution, **ETLX runs the query** `query_name` and **retrieves dynamically generated queries**.
- The **resulting queries are then executed automatically**.

#### **🛠 Example: Auto-Adding Missing Columns**

This example **checks for new columns in a JSON file** and **adds them to the destination table**.

##### **📄 Markdown Configuration for `get_dyn_queries[query_name](runs_before,runs_after)`**

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
...

````

#### **🛠 Execution Flow**

1️⃣ **Extract column metadata from the input (in this case a json file, but it could be a table or any other valid query).**  
2️⃣ **Check which columns are missing in the destination table (`<table>`).**  
3️⃣ **Generate `ALTER TABLE` statements for adding missing columns, and replaces the `- get_dyn_queries[create_missing_columns]` with the the generated queries**  
4️⃣ **Runs the workflow with dynamically generated queries against the destination connection.**  

#### **🔹 Key Features**

✔ **Fully automated schema updates**  
✔ **Works with flexible schema data (e.g., JSON, CSV, Parquet, etc.)**  
✔ **Reduces manual maintenance when source schemas evolve**  
✔ **Ensures destination tables always match source structure**  

---

**With `get_dyn_queries[...]`, your ETLX workflows can now dynamically evolve with changing data structures!**

### **🔄 Conditional Execution**

ETLX allows conditional execution of SQL blocks based on the results of a query. This is useful to skip operations dynamically depending on data context (e.g., skip a step if no new data is available, or if a condition in the target is not met.

You can define condition blocks using the following keys:

- For **ETL step-specific conditions**:
  - `extract_condition`
  - `transform_condition`
  - `load_condition`
  - etc.

- For **generic sections** (e.g., DATA_QUALITY, EXPORTS, NOTIFY, etc.):
  - `condition`

You can also specify an optional `*condition_msg` to log a custom message when a condition is not met.

#### **Condition Evaluation Logic**

- The SQL query defined in `*_condition` or `condition` is executed.
- The result must mast be boolean.
- If not met, the corresponding main logig will be skipped.
- If `*_condition_msg` is provided, it will be included in the log entry instead of the default skip message.

#### **Example – Conditional Load Step**

```yaml
load_conn: "duckdb:"
load_condition: check_load_required
load_condition_msg: "No new records to load today"
load_sql: perform_load
```

```sql
-- check_load_required
SELECT COUNT(*) > 0 as _check FROM staging_table WHERE processed = false;
```

```sql
-- perform_load
INSERT INTO target_table
SELECT * FROM staging_table WHERE processed = false;
```

#### **Example – Global Conditional Notification**

```yaml
type: notify
name: notify_if_failures
description: "Send email only if failures occurred"
connection: "duckdb:"
condition: check_failures
condition_msg: "No failures detected, no email sent"
```

```sql
-- check_failures
SELECT COUNT(*) > 0 as chk FROM logs WHERE success = false;
```

> 📝 **Note:** If no `*_condition_msg` is defined and the condition fails, ETLX will simply log the skipped step with a standard message like:  
> `"Condition 'load_condition' was not met. Skipping step 'load'."`

---

### **Advanced Workflow Execution: `runs_as` Override**

By default, the ETLX engine processes each Level 1 section (like `ETL`, `DATA_QUALITY`, `EXPORTS`, `ACTIONS`, `LOGS`, `NOTIFY` etc.) in the order that order. However, in more advanced workflows, it is often necessary to:

- Execute a second ETL process **after** quality validations (`DATA_QUALITY`).
- Reuse intermediate outputs **within the same config**, without having to create and chain multiple `.md` config files.

To enable this behavior, ETLX introduces the `runs_as` field in the **metadata block** of any Level 1 key. This tells ETLX to treat that section **as if it were a specific built-in block** like `ETL`, `EXPORTS`, etc., even if it has a different name.

---

````markdown

# ETL_AFTER_SOME_KEY

```yaml metadata
runs_as: ETL
description: "Post-validation data transformation"
active: true
```

## ETL_OVER_SOME_MAIN_STEP

...

````

In this example:

- ETLX will run the original `ETL` block.
- Then execute `DATA_QUALITY`, an so on.
- Then treat `ETL_AFTER_SOME_KEY` as another `ETL` block (since it contains `runs_as: ETL`) and execute it as such.

This allows chaining of processes within the same configuration file.

---

### **⚠️ Order Matters**

The custom section (e.g. `# ETL_AFTER_SOME_KEY`) is executed **in the order it appears** in the Markdown file after the main keys. That means the flow becomes:

1. `# ETL`
2. `# DATA_QUALITY`
3. `# ETL2` (runs as `ETL`)

This enables advanced chaining like:

- Exporting logs after validation.
- Reapplying transformations based on data quality feedback.
- Generating post-validation reports.

---

## **6. Embedding in Go**

To embed the ETL framework in a Go application:

```go
package main

import (
    "fmt"
    "time"
    "github.com/realdatadriven/etlx"
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

## **7. Future Plans**

ETLX is a powerful tool for defining and executing ETL processes using Markdown configuration files. It supports complex SQL queries, exports to multiple formats, and dynamic configuration loading. ETLX can be used as a library, CLI tool, or integrated into other systems for advanced data workflows.

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

## **8. License**

This project is licensed under the MIT License.
