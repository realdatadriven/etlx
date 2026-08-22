# CATALOG_MODEL
```yaml
name: CATALOG
description: Metadata Catalog and data governance model covering domains, glossary terms, data sources, assets, fields, classifications, and quality rules
runs_as: MODEL
admin_conn: '@DB_DRIVER_NAME:@DB_DSN'
create_all: checkfirst
_drop_all: checkfirst
update_table_metadata: true
active: true
cs_app:
  Catalog:
    menu_icon: document-check
    menu_order: 1
    active: true
    tables:
      - stakeholders
      - business_units
      - domains
      - subdomains
      - glossary_terms
      - catalog_assets
      - asset_schemas
      - data_assets
      - asset_fields
  Classifications:
    menu_icon: tag
    menu_order: 2
    active: true
    tables:
      - tag_categories
      - tags
      - asset_term_mappings
      - field_term_mappings
      - asset_tag_mappings
      - field_tag_mappings
  Quality:
    menu_icon: chart-bar
    menu_order: 3
    active: true
    tables:
      - data_quality_rules
      - data_quality_executions
```

## STAKEHOLDERS
```yaml
table: stakeholders
comment: Stakeholder
tooltip: Central repository of individuals who act as technical owners, business stewards, or data producers.
columns:
  stakeholder_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the stakeholder.", form_display: true, table_display: true, order: 1 }
  email: { type: varchar, len: 255, nullable: false, unique: true, comment: "Email", tooltip: "Primary corporate email address of the stakeholder.", form_display: true, table_display: true, order: 2 }
  full_name: { type: varchar, len: 150, nullable: false, comment: "Name", tooltip: "Full name of the stakeholder.", form_display: true, table_display: true, order: 3 }
  title: { type: varchar, len: 100, comment: "Title", tooltip: "Job title or role description of the stakeholder.", form_display: true, table_display: true, order: 4 }
  slack_handle: { type: varchar, len: 50, comment: "Slack", tooltip: "Internal Slack username or handle for rapid communication.", form_display: true, table_display: true, order: 5 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the stakeholder record was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the stakeholder record was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {domains: true, catalog_assets: true, glossary_terms: true}
table_layout:
  default_order: [{field: stakeholder_id, order: DESC}]
```

## BUSINESS_UNITS
```yaml
table: business_units
comment: Business Unit
tooltip: High-level organizational divisions such as Finance, Risk, Marketing, or Product.
columns:
  business_unit_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the business unit.", form_display: true, table_display: true, order: 1 }
  name: { type: varchar, len: 100, nullable: false, unique: true, comment: "Name", tooltip: "Name of the business unit.", form_display: true, table_display: true, order: 2 }
  description: { type: text, comment: "Description", tooltip: "Detailed description of the business unit scope and responsibilities.", form_display: true, table_display: true, order: 3 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the business unit was recorded.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the business unit was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {domains: true}
table_layout:
  default_order: [{field: business_unit_id, order: DESC}]
```

## DOMAINS
```yaml
table: domains
comment: Domain
tooltip: Data Mesh domains representing distinct business bounded contexts such as a Customer Domain or Revenue Domain.
columns:
  domain_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the domain.", form_display: true, table_display: true, order: 1 }
  business_unit_id: { type: integer, fk: "business_units.business_unit_id", comment: "Business Unit", tooltip: "Foreign key referencing the parent business unit.", form_display: true, table_display: true, order: 2 }
  name: { type: varchar, len: 100, nullable: false, unique: true, comment: "Name", tooltip: "Name of the domain.", form_display: true, table_display: true, order: 3 }
  description: { type: text, comment: "Description", tooltip: "Detailed description of the data domain boundary.", form_display: true, table_display: true, order: 4 }
  domain_lead_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Lead", tooltip: "Foreign key referencing the stakeholder acting as domain lead.", form_display: true, table_display: true, order: 5 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the domain was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the domain was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {subdomains: true, glossary_terms: true, catalog_assets: true, data_assets: true}
table_layout:
  default_order: [{field: domain_id, order: DESC}]
```

## SUBDOMAINS
```yaml
table: subdomains
comment: Subdomain
tooltip: More specific business areas within a domain, such as Customer Identity within the Customer domain.
columns:
  subdomain_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the subdomain.", form_display: true, table_display: true, order: 1 }
  domain_id: { type: integer, fk: "domains.domain_id", nullable: false, comment: "Domain", tooltip: "Foreign key referencing the parent domain.", form_display: true, table_display: true, order: 2 }
  name: { type: varchar, len: 100, nullable: false, comment: "Name", tooltip: "Name of the subdomain within its parent domain.", form_display: true, table_display: true, order: 3 }
  description: { type: text, comment: "Description", tooltip: "Detailed description of the subdomain boundary and responsibilities.", form_display: true, table_display: true, order: 4 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the subdomain was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the subdomain was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {catalog_assets: true, data_assets: true}
table_layout:
  default_order: [{field: subdomain_id, order: DESC}]
```

## TAG_CATEGORIES
```yaml
table: tag_categories
comment: Tag Category
tooltip: Taxonomy groupings for tags and classifications such as Sensitivity, GDPR, or Data Tier.
columns:
  category_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the tag category.", form_display: true, table_display: true, order: 1 }
  name: { type: varchar, len: 50, nullable: false, unique: true, comment: "Name", tooltip: "Name of the tag category such as Sensitivity or Data Tier.", form_display: true, table_display: true, order: 2 }
  description: { type: text, comment: "Description", tooltip: "Description of what this category classifies.", form_display: true, table_display: true, order: 3 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the tag category was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the tag category was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {tags: true}
table_layout:
  default_order: [{field: category_id, order: DESC}]
```

## TAGS
```yaml
table: tags
comment: Tag
tooltip: Individual tags or classification labels applied to data assets, tables, or fields.
columns:
  tag_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the tag.", form_display: true, table_display: true, order: 1 }
  category_id: { type: integer, fk: "tag_categories.category_id", comment: "Category", tooltip: "Foreign key referencing the parent tag category.", form_display: true, table_display: true, order: 2 }
  name: { type: varchar, len: 100, nullable: false, unique: true, comment: "Name", tooltip: "Name of the tag such as PII, Confidential, or Gold-Tier.", form_display: true, table_display: true, order: 3 }
  description: { type: text, comment: "Description", tooltip: "Definition and criteria for applying this tag.", form_display: true, table_display: true, order: 4 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the tag was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the tag was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: tag_id, order: DESC}]
```

## GLOSSARY_TERMS
```yaml
table: glossary_terms
comment: Glossary Term
tooltip: Enterprise business glossary establishing authoritative definitions for metrics and concepts.
columns:
  term_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the glossary term.", form_display: true, table_display: true, order: 1 }
  domain_id: { type: integer, fk: "domains.domain_id", comment: "Domain", tooltip: "Foreign key referencing the domain responsible for the term.", form_display: true, table_display: true, order: 2 }
  term_name: { type: varchar, len: 150, nullable: false, unique: true, comment: "Name", tooltip: "Standardized business term name such as Monthly Active User or Net Revenue.", form_display: true, table_display: true, order: 3 }
  definition: { type: text, nullable: false, comment: "Definition", tooltip: "Authoritative business definition of the term.", form_display: true, table_display: true, order: 4 }
  business_rules: { type: text, comment: "Rules", tooltip: "Calculation logic, constraints, or qualitative rules defining the term.", form_display: true, table_display: true, order: 5 }
  status: { type: varchar, len: 30, default: "Draft", comment: "Status", tooltip: "Lifecycle status of the term such as Draft, Approved, or Deprecated.", form_display: true, table_display: true, order: 6 }
  steward_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Steward", tooltip: "Foreign key referencing the business steward responsible for the term.", form_display: true, table_display: true, order: 7 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the term was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the term was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {asset_term_mappings: true, field_term_mappings: true}
table_layout:
  default_order: [{field: term_id, order: DESC}]
```

## CATALOG_ASSETS
```yaml
table: catalog_assets
comment: Catalog Asset
tooltip: Broad catalog of business and technical assets such as tables, views, streams, pipelines, dashboards, topics, and models.
columns:
  asset_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the catalog asset.", form_display: true, table_display: true, order: 1 }
  domain_id: { type: integer, fk: "domains.domain_id", comment: "Domain", tooltip: "Foreign key referencing the domain owning this asset.", form_display: true, table_display: true, order: 2 }
  subdomain_id: { type: integer, fk: "subdomains.subdomain_id", comment: "Subdomain", tooltip: "Foreign key referencing the subdomain owning this asset.", form_display: true, table_display: true, order: 3 }
  name: { type: varchar, len: 255, nullable: false, comment: "Name", tooltip: "Display name of the asset.", form_display: true, table_display: true, order: 4 }
  asset_type: { type: varchar, len: 50, nullable: false, comment: "Type", tooltip: "Asset category such as Table, View, Stream, Pipeline, Dashboard, Topic, or Model.", form_display: true, table_display: true, order: 5 }
  layer_path: { type: varchar, len: 500, comment: "Layer Path", tooltip: "Logical path of the asset such as snowflake.analytics_prod.finance.fct_revenue or airflow.dags.sync_customer.", form_display: true, table_display: true, order: 6 }
  description: { type: text, comment: "Description", tooltip: "Business and technical description of the asset.", form_display: true, table_display: true, order: 7 }
  business_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Business Owner", tooltip: "Foreign key referencing the business owner responsible for the asset.", form_display: true, table_display: true, order: 8 }
  technical_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Tech Owner", tooltip: "Foreign key referencing the technical owner responsible for the asset.", form_display: true, table_display: true, order: 9 }
  row_count: { type: integer, comment: "Rows", tooltip: "Latest observed row count for table-like assets.", form_display: true, table_display: true, order: 10 }
  bytes_size: { type: integer, comment: "Size", tooltip: "Latest observed storage size in bytes for table-like assets.", form_display: true, table_display: true, order: 11 }
  orchestrator_type: { type: varchar, len: 50, comment: "Orchestrator", tooltip: "Platform used for pipelines such as Airflow, dbt, or Dagster.", form_display: true, table_display: true, order: 12 }
  schedule_cron: { type: varchar, len: 100, comment: "Schedule", tooltip: "Cron expression for scheduled execution.", form_display: true, table_display: true, order: 13 }
  code_repo_url: { type: text, comment: "Repo URL", tooltip: "Link to the source code repository for the asset.", form_display: true, table_display: true, order: 14 }
  bi_tool_type: { type: varchar, len: 50, comment: "BI Tool", tooltip: "Business intelligence tool type such as Tableau, Looker, or PowerBI.", form_display: true, table_display: true, order: 15 }
  dashboard_url: { type: text, comment: "Dashboard URL", tooltip: "Web link to the dashboard or report.", form_display: true, table_display: true, order: 16 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the catalog asset was registered.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the catalog asset was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {asset_schemas: true, data_assets: true}
table_layout:
  default_order: [{field: asset_id, order: DESC}]
```

## ASSET_SCHEMAS
```yaml
table: asset_schemas
comment: Asset Schema
tooltip: Database schema or dataset grouping within a catalog asset when the asset is a database-like source.
columns:
  schema_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the schema.", form_display: true, table_display: true, order: 1 }
  catalog_asset_id: { type: integer, fk: "catalog_assets.asset_id", comment: "Catalog Asset", tooltip: "Foreign key referencing the parent catalog asset.", form_display: true, table_display: true, order: 2 }
  name: { type: varchar, len: 150, nullable: false, comment: "Name", tooltip: "Name of the schema or namespace such as public or finance_mart.", form_display: true, table_display: true, order: 3 }
  description: { type: text, comment: "Description", tooltip: "Description of the data contents within this schema.", form_display: true, table_display: true, order: 4 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the schema was recorded.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the schema was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {data_assets: true}
table_layout:
  default_order: [{field: schema_id, order: DESC}]
```

## DATA_ASSETS
```yaml
table: data_assets
comment: Data Asset
tooltip: Tables, views, or streams representing discrete data entities.
columns:
  asset_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the data asset.", form_display: true, table_display: true, order: 1 }
  schema_id: { type: integer, fk: "asset_schemas.schema_id", comment: "Schema", tooltip: "Foreign key referencing the parent schema.", form_display: true, table_display: true, order: 2 }
  domain_id: { type: integer, fk: "domains.domain_id", comment: "Domain", tooltip: "Foreign key referencing the domain owning this asset.", form_display: true, table_display: true, order: 3 }
  subdomain_id: { type: integer, fk: "subdomains.subdomain_id", comment: "Subdomain", tooltip: "Foreign key referencing the subdomain owning this asset.", form_display: true, table_display: true, order: 4 }
  name: { type: varchar, len: 200, nullable: false, comment: "Name", tooltip: "Name of the table, view, or stream such as dim_customer or fct_monthly_revenue.", form_display: true, table_display: true, order: 5 }
  asset_type: { type: varchar, len: 50, default: "Table", comment: "Type", tooltip: "Type of asset such as Table, View, Stream, API, or File.", form_display: true, table_display: true, order: 6 }
  description: { type: text, comment: "Description", tooltip: "Business and technical description of the asset.", form_display: true, table_display: true, order: 7 }
  business_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Business Owner", tooltip: "Foreign key referencing the business owner responsible for asset data quality and meaning.", form_display: true, table_display: true, order: 8 }
  technical_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Tech Owner", tooltip: "Foreign key referencing the technical owner responsible for the ETL or pipeline.", form_display: true, table_display: true, order: 9 }
  row_count: { type: integer, comment: "Rows", tooltip: "Latest observed total row count.", form_display: true, table_display: true, order: 10 }
  bytes_size: { type: integer, comment: "Size", tooltip: "Latest observed storage size in bytes.", form_display: true, table_display: true, order: 11 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the asset was registered.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the asset metadata was last refreshed.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {asset_fields: true, asset_tag_mappings: true, asset_term_mappings: true}
table_layout:
  default_order: [{field: asset_id, order: DESC}]
```

## ASSET_FIELDS
```yaml
table: asset_fields
comment: Asset Field
tooltip: Columns or fields within a data asset including typing, nullability, and relational PK or FK constraints.
columns:
  field_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the field or column.", form_display: true, table_display: true, order: 1 }
  asset_id: { type: integer, fk: "data_assets.asset_id", comment: "Asset", tooltip: "Foreign key referencing the parent data asset or table.", form_display: true, table_display: true, order: 2 }
  name: { type: varchar, len: 150, nullable: false, comment: "Name", tooltip: "Name of the column or field such as customer_id or email_address.", form_display: true, table_display: true, order: 3 }
  data_type: { type: varchar, len: 100, nullable: false, comment: "Type", tooltip: "SQL data type of the column such as VARCHAR(255), TIMESTAMP, INTEGER, or NUMERIC(18,2).", form_display: true, table_display: true, order: 4 }
  max_length: { type: integer, comment: "Length", tooltip: "Maximum character or byte length if applicable.", form_display: true, table_display: true, order: 5 }
  is_nullable: { type: boolean, default: true, comment: "Nullable", tooltip: "Flag indicating whether the column allows NULL values.", form_display: true, table_display: true, order: 6 }
  is_primary_key: { type: boolean, default: false, comment: "PK", tooltip: "Flag indicating whether this field is part of the asset primary key.", form_display: true, table_display: true, order: 7 }
  is_foreign_key: { type: boolean, default: false, comment: "FK", tooltip: "Flag indicating whether this field acts as a foreign key.", form_display: true, table_display: true, order: 8 }
  foreign_key_target_field_id: { type: integer, fk: "asset_fields.field_id", comment: "Target", tooltip: "Self-referencing foreign key linking this column directly to its target primary key field.", form_display: true, table_display: true, order: 9 }
  description: { type: text, comment: "Description", tooltip: "Column-level documentation explaining the meaning of the data stored.", form_display: true, table_display: true, order: 10 }
  business_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Business Owner", tooltip: "Foreign key referencing the business owner responsible for asset data quality and meaning.", form_display: true, table_display: true, order: 6 }
  technical_owner_id: { type: integer, fk: "stakeholders.stakeholder_id", comment: "Tech Owner", tooltip: "Foreign key referencing the technical owner responsible for the ETL or pipeline.", form_display: true, table_display: true, order: 7 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the field metadata was recorded.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the field metadata was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {field_tag_mappings: true, field_term_mappings: true}
table_layout:
  default_order: [{field: field_id, order: DESC}]
```

## ASSET_TERM_MAPPINGS
```yaml
table: asset_term_mappings
comment: Asset Term Mapping
tooltip: Associates data assets with business glossary terms they embody.
columns:
  asset_id: { type: integer, fk: "data_assets.asset_id", comment: "Asset", tooltip: "Foreign key referencing the data asset.", form_display: true, table_display: true, order: 1 }
  term_id: { type: integer, fk: "glossary_terms.term_id", comment: "Term", tooltip: "Foreign key referencing the glossary term.", form_display: true, table_display: true, order: 2 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the mapping was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the mapping was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: asset_id, order: DESC}]
```

## FIELD_TERM_MAPPINGS
```yaml
table: field_term_mappings
comment: Field Term Mapping
tooltip: Associates specific columns or fields with glossary terms.
columns:
  field_id: { type: integer, fk: "asset_fields.field_id", comment: "Field", tooltip: "Foreign key referencing the asset field or column.", form_display: true, table_display: true, order: 1 }
  term_id: { type: integer, fk: "glossary_terms.term_id", comment: "Term", tooltip: "Foreign key referencing the glossary term.", form_display: true, table_display: true, order: 2 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the mapping was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the mapping was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: field_id, order: DESC}]
```

## ASSET_TAG_MAPPINGS
```yaml
table: asset_tag_mappings
comment: Asset Tag Mapping
tooltip: Applies tags and classifications to data assets.
columns:
  asset_id: { type: integer, fk: "data_assets.asset_id", comment: "Asset", tooltip: "Foreign key referencing the data asset.", form_display: true, table_display: true, order: 1 }
  tag_id: { type: integer, fk: "tags.tag_id", comment: "Tag", tooltip: "Foreign key referencing the tag.", form_display: true, table_display: true, order: 2 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the mapping was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the mapping was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: asset_id, order: DESC}]
```

## FIELD_TAG_MAPPINGS
```yaml
table: field_tag_mappings
comment: Field Tag Mapping
tooltip: Applies granular classifications at the individual column level.
columns:
  field_id: { type: integer, fk: "asset_fields.field_id", comment: "Field", tooltip: "Foreign key referencing the asset field or column.", form_display: true, table_display: true, order: 1 }
  tag_id: { type: integer, fk: "tags.tag_id", comment: "Tag", tooltip: "Foreign key referencing the tag.", form_display: true, table_display: true, order: 2 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the mapping was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the mapping was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: field_id, order: DESC}]
```

## DATA_QUALITY_RULES
```yaml
table: data_quality_rules
comment: Data Quality Rule
tooltip: Automated data quality checks and assertions defined for assets or fields.
columns:
  rule_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the data quality rule.", form_display: true, table_display: true, order: 1 }
  asset_id: { type: integer, fk: "data_assets.asset_id", comment: "Asset", tooltip: "Foreign key referencing the target data asset being validated.", form_display: true, table_display: true, order: 2 }
  field_id: { type: integer, fk: "asset_fields.field_id", comment: "Field", tooltip: "Optional foreign key referencing a specific column if the rule is column-level.", form_display: true, table_display: true, order: 3 }
  rule_type: { type: varchar, len: 50, nullable: false, comment: "Type", tooltip: "Type of assertion such as NotNull, Unique, Range, Freshness, or Expression.", form_display: true, table_display: true, order: 4 }
  rule_expression: { type: text, nullable: false, comment: "Expression", tooltip: "Executable condition or expression such as NULL_COUNT == 0 or VALUE >= 0.", form_display: true, table_display: true, order: 5 }
  severity: { type: varchar, len: 20, default: "Warning", comment: "Severity", tooltip: "Alert severity level if the rule fails.", form_display: true, table_display: true, order: 6 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the rule was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the rule was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
  allow_in_subform: {data_quality_executions: true}
table_layout:
  default_order: [{field: rule_id, order: DESC}]
```

## DATA_QUALITY_EXECUTIONS
```yaml
table: data_quality_executions
comment: Data Quality Execution
tooltip: Execution history and logs for data quality rules.
columns:
  execution_id: { type: integer, pk: true, autoincrement: true, comment: "ID", tooltip: "Unique auto-incrementing identifier for the execution record.", form_display: true, table_display: true, order: 1 }
  rule_id: { type: integer, fk: "data_quality_rules.rule_id", comment: "Rule", tooltip: "Foreign key referencing the executed data quality rule.", form_display: true, table_display: true, order: 2 }
  status: { type: varchar, len: 30, nullable: false, comment: "Status", tooltip: "Execution outcome status such as Passed, Failed, or Error.", form_display: true, table_display: true, order: 3 }
  failed_records_count: { type: integer, default: 0, comment: "Failed", tooltip: "Number of records failing the assertion condition.", form_display: true, table_display: true, order: 4 }
  total_records_checked: { type: integer, comment: "Checked", tooltip: "Total number of records scanned during execution.", form_display: true, table_display: true, order: 5 }
  executed_at: { type: datetime, comment: "Executed", tooltip: "Timestamp when the quality check was executed.", form_display: false, table_display: true }
  error_message: { type: text, comment: "Error", tooltip: "Error details if the execution failed due to system exception.", form_display: true, table_display: true, order: 6 }
  user_id: { type: integer, comment: "User ID", tooltip: "Identifier of the user who created or updated the record.", form_display: false, table_display: false }
  created_at: { type: datetime, comment: "Created", tooltip: "Timestamp when the execution was created.", form_display: false, table_display: true }
  updated_at: { type: datetime, comment: "Updated", tooltip: "Timestamp when the execution was last updated.", form_display: false, table_display: true }
  excluded: { type: boolean, default: false, comment: "Excluded", tooltip: "Flag indicating whether the record is excluded from active use.", form_display: false, table_display: false }
form_layout:
  tabs_steps: tabs
  form_in_popup: false
  size: 9
table_layout:
  default_order: [{field: execution_id, order: DESC}]
```
