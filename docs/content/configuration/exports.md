+++
title = 'Exports'
weight = 45
draft = false
+++

# Exports

The `EXPORTS` section in the ETL configuration handles exporting data to files. This is particularly useful for generating reports for internal departments, regulators, partners, or saving processed data to a data lake. By leveraging DuckDB's ability to export data in various formats, this section supports file generation with flexibility and precision.

---

## Sales Data Export

```yaml metadata
name: DailyExports
description: "Daily file exports for various datasets."
database: reporting_db
connection: "duckdb:"
path: "/path/to/Reports/YYYYMMDD"
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

---

## **How Exporting It Works**

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
     - **`header`**: Whether to include headers in the exported table.
     - **`clear_range`**: If true, clears the specified range before populating new data. 
     - **`clear_sheet`**: If true, clears all content from the specified sheet before populating new data. 
     - **`active`**: If false, skips this mapping.
the maping can also be a string representing a query and all the mapping can be loaded from a table in the database to simplify the config, and also in a real world it can be extensive, would be easier to be done in a spreadsheet and loaded as a table.

---

## **Resulting Outputs**

1. **CSV File**:
   - Exports sales data to a CSV file located at `/path/to/Reports/YYYYMMDD/sales_YYYYMMDD.csv`.

2. **Excel File**:
   - Exports region data to an Excel file located at `/path/to/Reports/YYYYMMDD/regions_YYYYMMDD.xlsx`.

3. **Populated Template**:
   - Generates a sales report from `sales_template.xlsx` and saves it as `sales_report_YYYYMMDD.xlsx`.

---

## **Benefits of this functionality**

- **Flexibility**:
  - Export data in multiple formats (e.g., CSV, Excel) using DuckDB's powerful `COPY` command.
- **Reusability**:
  - Use predefined templates to create consistent reports.
- **Customizability**:
  - SQL queries and mappings allow fine-grained control over the exported data.

By leveraging the `EXPORTS` section, you can automate data export processes, making them efficient and repeatable.

## 📝 Exporting as Text-Based Template

In addition to exporting structured data formats like CSV or Excel, ETLX also supports exporting reports using plain-text templates such as **HTML**, **XML**, **Markdown**, etc.

These templates are rendered using Go’s `text/template` engine and can use dynamic data from SQL queries declared under the `data_sql` field, just like in the `NOTIFY` section.

This is especially useful for **reporting**, **integration**, or **publishing documents** with dynamic content.

### 📦 Example

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
... (truncated for space)
```
