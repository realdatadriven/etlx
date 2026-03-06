<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->
# ETLX_MODEL
```yaml
name: ETLX
description: ETLX Model
runs_as: MODEL
conn: 'sqlite3:database/ETLX.db'
admin_conn: 'sqlite3:database/ADMIN.db'
create_all: checkfirst
_drop_all: checkfirst
active: true
update_table_metadata: true
cs_app:
  Dashboards:
    menu_icon: document-report
    menu_order: 1
    active: true
    menu_config: '{"label": "dashboard","tooltip": "dashboard_desc","load_items": {"table": "dashboard","tables": ["dashboard"]}}'
    tables:
      - dashboard
  ETLX:
    menu_icon: circle-stack
    menu_order: 2
    active: true
    tables:
      - etlx
      - etlx_conf
      - manage_query
  Notebook:
    menu_icon: book-open
    menu_order: 3
    active: true
    menu_config: '{"label": "notebook","tooltip": "notebook_desc","load_items": {"table": "notebook","tables": ["notebook"]}}'
    tables:
      - notebook
```

## ETLX
```yaml
table: etlx
comment: ETLX
columns:
  etlx_id:          { type: integer, pk: true, autoincrement: true, comment: "ID" }
  etl:              { type: varchar(200), unique: true, nullable: false, comment: "Name" }
  etl_desc:         { type: text, comment: "Description" }
  attach_etlx_conf: { type: varchar(200), comment: "Config File" }
  etlx_conf:        { type: text, comment: "Config Text" }
  active:           { type: boolean, default: true, comment: "Active" }
  user_id:          { type: integer, comment: "User ID" }
  app_id:           { type: integer, comment: "App ID" }
  created_at:       { type: datetime, comment: "Created at" }
  updated_at:       { type: datetime, comment: "Updated at" }
  excluded:         { type: boolean, default: false, comment: "Excluded" }
```

## ETLX_CONF
```yaml
table: etlx_conf
comment: ETLX Extra Cofig
columns:
  etlx_conf_id:    { type: integer, pk: true, autoincrement: true, comment: "ID" }
  etlx_conf:       { type: varchar(200), unique: true, nullable: false, comment: "Name" }
  etlx_conf_desc:  { type: text, comment: "Description" }
  etlx_extra_conf: { type: text, comment: "Config Text" }
  user_id:         { type: integer, comment: "User ID" }
  app_id:          { type: integer, comment: "App ID" }
  created_at:      { type: datetime, comment: "Created at" }
  updated_at:      { type: datetime, comment: "Updated at" }
  excluded:        { type: boolean, default: false, comment: "Excluded" }
```

## MANAGE_QUERY
```yaml
table: manage_query
comment: Queries
columns:
  manage_query_id:   { type: integer, pk: true, autoincrement: true, comment: "ID" }
  manage_query:      { type: varchar(200), nullable: false, comment: "Query Desc" }
  database:          { type: varchar(200), nullable: false, comment: "Database" }
  manage_query_conf: { type: text, comment: "Query Config" }
  active:            { type: boolean, default: true, comment: "Active" }
  user_id:           { type: integer, comment: "User ID" }
  app_id:            { type: integer, comment: "App ID" }
  created_at:        { type: datetime, comment: "Created at" }
  updated_at:        { type: datetime, comment: "Updated at" }
  excluded:          { type: boolean, default: false, comment: "Excluded" }
```

## DASHBOARD
```yaml
table: dashboard
comment: Dashboards
columns:
  dashboard_id:   { type: integer, pk: true, autoincrement: true, comment: "Dashboard ID" }
  dashboard:      { type: varchar(200), comment: "Dashboard" }
  dashboard_desc: { type: text, comment: "Description" }
  dashboard_conf: { type: text, nullable: false, comment: "Conf / Params" }
  order:          { type: integer, comment: "Order" }
  active:         { type: boolean, default: true, comment: "Active" }
  user_id:        { type: integer, comment: "User ID" }
  app_id:         { type: integer, comment: "App ID" }
  created_at:     { type: datetime, comment: "Created at" }
  updated_at:     { type: datetime, comment: "Updated at" }
  excluded:       { type: boolean, default: false, comment: "Excluded" }
```

## DASHBOARD_COMMENT
```yaml
table: dashboard_comment
comment: Dashboards Comments
columns:
  dashboard_comment_id: { type: integer, pk: true, autoincrement: true, comment: "Comment ID" }
  dashboard_comment:    { type: text, comment: "Comments" }
  dashboard:            { type: varchar(200), comment: "Dashboard" }
  active:               { type: boolean, default: true, comment: "Active" }
  user_id:              { type: integer, comment: "User ID" }
  app_id:               { type: integer, comment: "App ID" }
  created_at:           { type: datetime, comment: "Created at" }
  updated_at:           { type: datetime, comment: "Updated at" }
  excluded:             { type: boolean, default: false, comment: "Excluded" }
```

## NOTEBOOK
```yaml
table: notebook
comment: Notebooks
columns:
  notebook_id:   { type: integer, pk: true, autoincrement: true, comment: "Notebook ID" }
  notebook:      { type: varchar(200), comment: "Name" }
  notebook_desc: { type: text, comment: "Description" }
  notebook_conf: { type: text, nullable: false, comment: "Conf / Params" }
  active:        { type: boolean, default: true, comment: "Active" }
  user_id:       { type: integer, comment: "User ID" }
  app_id:        { type: integer, comment: "App ID" }
  created_at:    { type: datetime, comment: "Created at" }
  updated_at:    { type: datetime, comment: "Updated at" }
  excluded:      { type: boolean, default: false, comment: "Excluded" }
```
