+++
title = 'Loading Config Dependencies'
weight = 49
draft = false
+++

### REQUIRES

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