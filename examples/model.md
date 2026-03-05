<!-- markdownlint-disable MD022 -->
<!-- markdownlint-disable MD031 -->
# ADMMIN_MODEL
```yaml
name: ADMIN
description: CS ADMIN Model
runs_as: MODEL
conn: 'sqlite3:database/ADMIN.db'
#admin_conn: 'sqlite3:database/ADMIN.db'
create_all: checkfirst
_drop_all: checkfirst
update_table_metadata: true
active: true
cs_app:
    Dashboards:
        menu_icon: document-report
        menu_order: 1
        active: true
        menu_config: '{"label": "dashboard","tooltip": "dashboard_desc","load_items": {"table": "dashboard","tables": ["dashboard"]}}'
        tables:
            - dashboard
    Admin:
        menu_icon: web
        menu_order: 2
        active: true
        tables:
            - app
            - menu
            - role
            - users
            - access_key
            - env
            - {table: arrow_flight, requires_rla: true, active: true}
            - {table: arrow_flight_table, active: false}
            - {table: arrow_flight_table_field, active: false}
            - {table: arrow_flight_table_scope, active: false}
            - user_log
            - custom_table
            - custom_form
            - table
            - table_schema
    Params:
        menu_icon: adjustments
        menu_order: 3
        active: true
        tables:
            - lang
    Jobs Scheduling:
        menu_icon: clock
        menu_order: 4
        active: true
        tables:
            - cron
            - {table: cron_log, active: false}
```

## LANG
```yaml
table: lang
comment: Languages
columns:
  lang_id:     { type: integer, pk: true, autoincrement: true, comment: "Lang ID" }
  lang:        { type: varchar(4), unique: true, nullable: false, comment: "Language" }
  lang_desc:   { type: varchar(200), comment: "Description" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
data:
  - {lang_id: 1, lang: en, lang_desc: English, excluded: false}
```

## ROLE
```yaml
table: role
comment: Roles
columns:
  role_id:     { type: integer, pk: true, autoincrement: true, comment: "Role ID" }
  role:        { type: varchar(20), nullable: false, unique: true, comment: "Role" }
  role_desc:   { type: text, comment: "Description" }
  config:      { type: text, comment: "Config" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
data:
  - {role_id: 1, role: root, role_desc: "Root role", excluded: false}
  - {role_id: 2, role: no-role, role_desc: "No role set", excluded: false}
  - {role_id: 3, role: tenant, role_desc: "Tenant Role", excluded: false}
```

## USERS
```yaml
table: users
comment: Users
columns:
  user_id:              { type: integer, pk: true, autoincrement: true, comment: "User ID" }
  username:             { type: varchar(50), unique: true, nullable: false, comment: "Username" }
  first_name:           { type: varchar(50), nullable: false, comment: "Fisrt Name" }
  last_name:            { type: varchar(50), comment: "Last Name" }
  email:                { type: varchar(50), unique: true, nullable: false, comment: "Email" }
  phone:                { type: varchar(50), unique: true, comment: "Phone" }
  password:             { type: varchar(200), nullable: false, comment: "Password" }
  role_id:              { type: integer, fk: "role.role_id", comment: "Default Role ID" }
  lang_id:              { type: integer, fk: "lang.lang_id", comment: "Lang ID" }
  timezone:             { type: varchar(50), comment: "Timezone" }
  attach_profile_pic:   { type: varchar(200), comment: "Profile Picture" }
  active:               { type: boolean, default: true, comment: "Active" }
  alter_pass_nxt_login: { type: boolean, default: false, comment: "Alter Password on next login" }
  enable_2f_auth:       { type: boolean, default: false, comment: "Enable Two Factor Auth." }
  nxt_code_2f_auth:     { type: varchar(200), comment: "Next Two Factor Code" }
  code_2f_expires_at:   { type: datetime, comment: "2F Code Expires" }
  created_at:           { type: datetime, comment: "Created at" }
  updated_at:           { type: datetime, comment: "Updated at" }
  excluded:             { type: boolean, default: false, comment: "Excluded" }
data:
  - {user_id: 1, username: root, password: '*****', first_name: Super, last_name: Admin, email: real.datadriven@gmail.com, role_id: 1, lang_id: 1, active: true, alter_pass_nxt_login: true, excluded: false}
```

## USER_ROLE
```yaml
table: user_role
comment: User Roles
columns:
  user_role_id: { type: integer, pk: true, autoincrement: true, comment: "User Role ID" }
  user_id:      { type: integer, fk: "users.user_id", comment: "User ID" }
  role_id:      { type: integer, fk: "role.role_id", comment: "Role ID" }
  active:       { type: boolean, default: true, comment: "Active" }
  created_at:   { type: datetime, comment: "Created at" }
  updated_at:   { type: datetime, comment: "Updated at" }
  excluded:     { type: boolean, default: false, comment: "Excluded" }
```

## APP
```yaml
table: app
comment: Applications
columns:
  app_id:      { type: integer, pk: true, autoincrement: true, comment: "App ID" }
  app:         { type: varchar(20), unique: true, nullable: false, comment: "App Name" }
  app_desc:    { type: text, comment: "Description" }
  version:     { type: varchar(10), nullable: false, comment: "Version" }
  email:       { type: varchar(200), comment: "Email" }
  db:          { type: varchar(200), nullable: false, comment: "Database" }
  attach_logo: { type: varchar(200), comment: "Logo" }
  config:      { type: text, comment: "Config" }
  user_id:     { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
data:
  - {app_id: 1, app: ADMIN, app_desc: Admin, db: ADMIN, version: 1.0.0, user_id: 1, excluded: false}
```

## MENU
```yaml
table: menu
comment: Menus
columns:
  menu_id:     { type: integer, pk: true, autoincrement: true, comment: "Menu ID" }
  menu:        { type: varchar(200), nullable: false, comment: "Menu" }
  menu_desc:   { type: text, comment: "Description" }
  menu_icon:   { type: varchar(20), comment: "Icon" }
  menu_order:  { type: integer, comment: "Order" }
  menu_config: { type: text, comment: "Config" }
  app_id:      { type: integer, fk: "app.app_id", nullable: false, comment: "App ID" }
  user_id:     { type: integer, fk: "users.user_id", nullable: false, comment: "User ID" }
  active:      { type: boolean, default: true, comment: "Active" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
data:
  - {menu_id: 1, menu: Admin, menu_desc: Admin, menu_icon: user-group, menu_order: 1, app_id: 1, user_id: 1, active: true, excluded: false}
  - {menu_id: 2, menu: Params, menu_icon: adjustments, menu_order: 2, app_id: 1, user_id: 1, active: true, excluded: false}
```

## TABLE
```yaml
table: table
comment: Tables
columns:
  table_id:     { type: integer, pk: true, autoincrement: true, comment: "Table ID" }
  table:        { type: varchar(50), nullable: false, comment: "Table" }
  table_desc:   { type: varchar(200), comment: "Description" }
  db:           { type: varchar(50), comment: "Database" }
  requires_rla: { type: boolean, default: false, comment: "Requires Row Level Access" }
  user_id:      { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:   { type: datetime, comment: "Created at" }
  updated_at:   { type: datetime, comment: "Updated at" }
  excluded:     { type: boolean, default: false, comment: "Excluded" }
```

## MENU_TABLE
```yaml
table: menu_table
comment: Menu Tables
columns:
  menu_table_id:  { type: integer, pk: true, autoincrement: true, comment: "Menu Table ID" }
  menu_id:        { type: integer, fk: "menu.menu_id", comment: "Menu ID" }
  user_id:        { type: integer, fk: "users.user_id", comment: "User ID" }
  active:         { type: boolean, default: true, comment: "Active" }
  requires_rla:   { type: boolean, default: false, comment: "Requires Row Level Access" }
  menu_table_cnf: { type: text, comment: "Config" }
  created_at:     { type: datetime, comment: "Created at" }
  updated_at:     { type: datetime, comment: "Updated at" }
  excluded:       { type: boolean, default: false, comment: "Excluded" }
```

## ROLE_APP
```yaml
table: role_app
comment: Role Apps
columns:
  role_app_id: { type: integer, pk: true, autoincrement: true, comment: "Role App ID" }
  role_id:     { type: integer, fk: "role.role_id", comment: "Role ID" }
  app_id:      { type: integer, fk: "app.app_id", comment: "App ID" }
  access:      { type: boolean, default: true, comment: "Access" }
  user_id:     { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
```

## ROLE_APP_MENU
```yaml
table: role_app_menu
comment: Role App Menus
columns:
  role_app_menu_id: { type: integer, pk: true, autoincrement: true, comment: "Role App Menu ID" }
  role_id:          { type: integer, fk: "role.role_id", comment: "Role ID" }
  app_id:           { type: integer, fk: "app.app_id", comment: "App ID" }
  menu_id:          { type: integer, fk: "menu.menu_id", comment: "Menu ID" }
  access:           { type: boolean, default: true, comment: "Access" }
  user_id:          { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:       { type: datetime, comment: "Created at" }
  updated_at:       { type: datetime, comment: "Updated at" }
  excluded:         { type: boolean, default: false, comment: "Excluded" }
```

## ROLE_APP_MENU_TABLE
```yaml
table: role_app_menu_table
comment: Role App Menu Tables
columns:
  role_app_menu_table_id: { type: integer, pk: true, autoincrement: true, comment: "Role App Menu Table ID" }
  role_id:                { type: integer, fk: "role.role_id", comment: "Role ID" }
  app_id:                 { type: integer, fk: "app.app_id", comment: "App ID" }
  menu_id:                { type: integer, fk: "menu.menu_id", comment: "Menu ID" }
  table_id:               { type: integer, fk: "table.table_id", comment: "Table ID" }
  create:                 { type: boolean, default: false, comment: "Create" }
  read:                   { type: boolean, default: false, comment: "Read" }
  update:                 { type: boolean, default: false, comment: "Update" }
  delete:                 { type: boolean, default: false, comment: "Delete" }
  user_id:                { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:             { type: datetime, comment: "Created at" }
  updated_at:             { type: datetime, comment: "Updated at" }
  excluded:               { type: boolean, default: false, comment: "Excluded" }
```

## USER_LOG
```yaml
table: user_log
comment: User Logs
columns:
  user_log_id: { type: integer, pk: true, autoincrement: true, comment: "User Log ID" }
  user_id:     { type: integer, fk: "users.user_id", comment: "User ID" }
  action:      { type: varchar(200), nullable: false, comment: "Action" }
  req_ip:      { type: varchar(200), comment: "Request IP" }
  req_at:      { type: datetime, comment: "Request at" }
  req_data:    { type: text, comment: "Request Data" }
  res_at:      { type: datetime, comment: "Response at" }
  res_type:    { type: varchar(200), comment: "Response Type" }
  res_msg:     { type: varchar(500), comment: "Response Message" }
  res_data:    { type: text, comment: "Request Data" }
  table:       { type: varchar(200), comment: "Table" }
  db:          { type: varchar(200), comment: "Database" }
  row_id:      { type: integer, comment: "Database" }
  app_id:      { type: integer, fk: "app.app_id", comment: "App ID" }
  new_data:    { type: text, comment: "New Data" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
```

## CUSTOM_TABLE
```yaml
table: custom_table
comment: Custom Table
columns:
  custom_table_id: { type: integer, pk: true, autoincrement: true, comment: "Custom Table ID" }
  table:           { type: varchar(200), comment: "Table" }
  db:              { type: varchar(200), comment: "Database" }
  config:          { type: text, comment: "Config" }
  app_id:          { type: integer, fk: "app.app_id", comment: "App ID" }
  user_id:         { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:      { type: datetime, comment: "Created at" }
  updated_at:      { type: datetime, comment: "Updated at" }
  excluded:        { type: boolean, default: false, comment: "Excluded" }
```

## CUSTOM_FORM
```yaml
table: custom_form
comment: Custom Form
columns:
  custom_form_id: { type: integer, pk: true, autoincrement: true, comment: "Custom Form ID" }
  table:          { type: varchar(200), comment: "Table" }
  db:             { type: varchar(200), comment: "Database" }
  config:         { type: text, comment: "Config" }
  app_id:         { type: integer, fk: "app.app_id", comment: "App ID" }
  user_id:        { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:     { type: datetime, comment: "Created at" }
  updated_at:     { type: datetime, comment: "Updated at" }
  excluded:       { type: boolean, default: false, comment: "Excluded" }
```

## ROLE_ROW_LEVEL_ACCESS
```yaml
table: role_row_level_access
comment: Role Row Level Access
columns:
  role_row_level_access_id: { type: integer, pk: true, autoincrement: true, comment: "Role Row Level Access ID" }
  role_id:                  { type: integer, fk: "role.role_id", comment: "Role ID" }
  row_id:                   { type: integer, nullable: false, comment: "Row ID" }
  table_id:                 { type: integer, fk: "table.table_id", comment: "Table ID" }
  table:                    { type: varchar(200), nullable: false, comment: "Table" }
  db:                       { type: varchar(200), nullable: false, comment: "Database" }
  user_id:                  { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:                   { type: integer, fk: "app.app_id", comment: "App ID" }
  read:                     { type: boolean, default: false, comment: "Read" }
  update:                   { type: boolean, default: false, comment: "Update" }
  delete:                   { type: boolean, default: false, comment: "Delete" }
  share:                    { type: boolean, default: false, comment: "Share" }
  created_at:               { type: datetime, comment: "Created at" }
  updated_at:               { type: datetime, comment: "Updated at" }
  excluded:                 { type: boolean, default: false, comment: "Excluded" }
```

## COLUMN_LEVEL_ACCESS
```yaml
table: column_level_access
comment: Column Level Access
columns:
  column_level_access_id: { type: integer, pk: true, autoincrement: true, comment: "Column Level Access ID" }
  column:                 { type: integer, nullable: false, comment: "Column" }
  table_id:               { type: integer, fk: "table.table_id", comment: "Table ID" }
  table:                  { type: varchar(200), nullable: false, comment: "Table" }
  db:                     { type: varchar(200), nullable: false, comment: "Database" }
  user_id:                { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:                 { type: integer, fk: "app.app_id", comment: "App ID" }
  create:                 { type: boolean, default: false, comment: "Create" }
  read:                   { type: boolean, default: false, comment: "Read" }
  update:                 { type: boolean, default: false, comment: "Update" }
  created_at:             { type: datetime, comment: "Created at" }
  updated_at:             { type: datetime, comment: "Updated at" }
  excluded:               { type: boolean, default: false, comment: "Excluded" }
```

## ROW_LEVEL_ACCESS
```yaml
table: row_level_access
comment: Row Level Access
columns:
  row_level_access_id: { type: integer, pk: true, autoincrement: true, comment: "Row Level Access ID" }
  row_id:              { type: integer, nullable: false, comment: "Row ID" }
  table_id:            { type: integer, fk: "table.table_id", comment: "Table ID" }
  table:               { type: varchar(200), nullable: false, comment: "Table" }
  db:                  { type: varchar(200), nullable: false, comment: "Database" }
  user_id:             { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:              { type: integer, fk: "app.app_id", comment: "App ID" }
  read:                { type: boolean, default: false, comment: "Read" }
  update:              { type: boolean, default: false, comment: "Update" }
  delete:              { type: boolean, default: false, comment: "Delete" }
  share:               { type: boolean, default: false, comment: "Share" }
  created_at:          { type: datetime, comment: "Created at" }
  updated_at:          { type: datetime, comment: "Updated at" }
  excluded:            { type: boolean, default: false, comment: "Excluded" }
```

## TRANSLATE_TABLE
```yaml
table: translate_table
comment: Translate Table
columns:
  table_org_desc:    { type: varchar(200), nullable: false, comment: "Table Org. Desc" }
  table_transl_desc: { type: varchar(200), nullable: false, comment: "Table Transl. Desc" }
  table:             { type: varchar(200), nullable: false, comment: "Table" }
  db:                { type: varchar(200), nullable: false, comment: "Database" }
  lang:              { type: varchar(5), nullable: false, comment: "Lang" }
  user_id:           { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:            { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:        { type: datetime, comment: "Created at" }
  updated_at:        { type: datetime, comment: "Updated at" }
  excluded:          { type: boolean, default: false, comment: "Excluded" }
```

## TRANSLATE_TABLE_FIELD
```yaml
table: translate_table_field
comment: Translate Table Fields
columns:
  transl_tbl_field_id: { type: integer, pk: true, autoincrement: true, comment: "Translate Table Field ID" }
  field_org_desc:      { type: varchar(200), nullable: false, comment: "Field Org. Desc" }
  field_transl_desc:   { type: varchar(200), nullable: false, comment: "Field Transl. Desc" }
  field:               { type: varchar(200), nullable: false, comment: "Field" }
  table:               { type: varchar(200), nullable: false, comment: "Table" }
  db:                  { type: varchar(200), nullable: false, comment: "Database" }
  lang:                { type: varchar(5), nullable: false, comment: "Lang" }
  user_id:             { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:              { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:          { type: datetime, comment: "Created at" }
  updated_at:          { type: datetime, comment: "Updated at" }
  excluded:            { type: boolean, default: false, comment: "Excluded" }
```

## TABLE_SCHEMA
```yaml
table: table_schema
comment: Table Schema
columns:
  table_schema_id: { type: integer, pk: true, autoincrement: true, comment: "Table field ID" }
  db:              { type: varchar(200), nullable: false, comment: "Database" }
  table:           { type: varchar(200), nullable: false, comment: "Table" }
  field:           { type: varchar(200), nullable: false, comment: "Field" }
  type:            { type: varchar(200), nullable: false, comment: "Type" }
  comment:         { type: varchar(200), comment: "Comment" }
  pk:              { type: boolean, default: false, comment: "Primary Key" }
  autoincrement:   { type: boolean, default: false, comment: "Auto Increment" }
  nullable:        { type: boolean, default: false, comment: "Nullable" }
  computed:        { type: boolean, default: false, comment: "Nullable" }
  default:         { type: boolean, comment: "Default" }
  fk:              { type: boolean, default: false, comment: "Foreign Key" }
  referred_table:  { type: varchar(200), comment: "Ref. Table." }
  referred_column: { type: varchar(200), comment: "Ref. Column" }
  field_order:     { type: integer, comment: "Field Order" }
  user_id:         { type: integer, fk: "users.user_id", comment: "User ID" }
  created_at:      { type: datetime, comment: "Created at" }
  updated_at:      { type: datetime, comment: "Updated at" }
  excluded:        { type: boolean, default: false, comment: "Excluded" }
```

## CRON
```yaml
table: cron
comment: Jobs scheduling
columns:
  cron_id:    { type: integer, pk: true, autoincrement: true, comment: "Cron ID" }
  cron:       { type: varchar(50), nullable: false, comment: "Cron" }
  cron_desc:  { type: varchar(200), nullable: false, comment: "Decription" }
  api:        { type: varchar(200), nullable: false, comment: "API" }
  app_id:     { type: integer, fk: "app.app_id", comment: "App ID" }
  db:         { type: varchar(200), comment: "Database" }
  table:      { type: varchar(50), comment: "Table" }
  active:     { type: boolean, default: true, comment: "Active" }
  created_at: { type: datetime, comment: "Created at" }
  updated_at: { type: datetime, comment: "Updated at" }
  excluded:   { type: boolean, default: false, comment: "Excluded" }
data:
  - {cron_id: 1, cron: "0 0 * * *", cron_desc: Backup, api: buckup, app_id: 1, db: ADMIN, active: false, excluded: false}
  - {cron_id: 2, cron: "0 0 * * *", cron_desc: "Update Env", api: "env/sync", app_id: 1, db: ADMIN, active: false, excluded: false}
  - {cron_id: 3, cron: "0 0 * * *", cron_desc: "ETLX Example", api: "etlx/name/[etlx_name]", app_id: 1, db: ADMIN, active: false, excluded: false}
```

## CRON_LOG
```yaml
table: cron_log
comment: Jobs scheduling logs
columns:
  cron_log_id: { type: integer, pk: true, autoincrement: true, comment: "Cron Log ID" }
  cron_id:     { type: integer, fk: "cron.cron_id", comment: "Cron ID" }
  cron:        { type: varchar(50), nullable: false, comment: "Cron" }
  cron_desc:   { type: varchar(200), nullable: false, comment: "Decription" }
  api:         { type: varchar(200), nullable: false, comment: "API" }
  start_at:    { type: datetime, comment: "Job Start" }
  end_at:      { type: datetime, comment: "Job End" }
  success:     { type: boolean, default: true, comment: "Success" }
  cron_msg:    { type: text, nullable: false, comment: "Message" }
  app_id:      { type: integer, fk: "app.app_id", comment: "App ID" }
  db:          { type: varchar(200), comment: "Database" }
  table:       { type: varchar(50), comment: "Table" }
  created_at:  { type: datetime, comment: "Created at" }
  updated_at:  { type: datetime, comment: "Updated at" }
  excluded:    { type: boolean, default: false, comment: "Excluded" }
```

## ACCESS_KEY
```yaml
table: access_key
comment: Access Keys
columns:
  access_key_id:   { type: integer, pk: true, autoincrement: true, comment: "Access Key ID" }
  access_key_desc: { type: varchar(200), nullable: false, comment: "Description" }
  access_token:    { type: text, nullable: false, comment: "Token" }
  expires_at:      { type: datetime, comment: "Expires at" }
  active:          { type: boolean, default: true, comment: "Active" }
  for_user_id:     { type: integer, fk: "users.user_id", comment: "Created For" }
  user_id:         { type: integer, fk: "users.user_id", comment: "Created BY" }
  created_at:      { type: datetime, comment: "Created at" }
  updated_at:      { type: datetime, comment: "Updated at" }
  excluded:        { type: boolean, default: false, comment: "Excluded" }
```

## ENV
```yaml
table: env
comment: Envariomental Variables
columns:
  env_id:       { type: integer, pk: true, autoincrement: true, comment: "env ID" }
  env_name:     { type: varchar(200), unique: true, nullable: false, comment: "Env Name" }
  env_value:    { type: text, nullable: false, comment: "Env Value" }
  on_srv_start: { type: boolean, default: true, comment: "Set On Server Start" }
  active:       { type: boolean, default: true, comment: "Active" }
  user_id:      { type: integer, fk: "users.user_id", comment: "Created BY" }
  created_at:   { type: datetime, comment: "Created at" }
  updated_at:   { type: datetime, comment: "Updated at" }
  excluded:     { type: boolean, default: false, comment: "Excluded" }
```

## ARROW_FLIGHT
```yaml
table: arrow_flight
comment: Expose Arrow Flight
columns:
  arrow_flight_id:     { type: integer, pk: true, autoincrement: true, comment: "ID" }
  arrow_flight:        { type: varchar(200), unique: true, nullable: false, comment: "Name" }
  arrow_flight_desc:   { type: text, comment: "Description" }
  flight_schema:       { type: varchar(200), unique: true, nullable: false, comment: "Schema Name" }
  startup_sql:         { type: text, comment: "Startup SQL" }
  main_sql:            { type: text, nullable: false, comment: "Main SQL" }
  table_discover_sql:  { type: text, comment: "Table Discover SQL" }
  table_scan_tmpl_sql: { type: text, comment: "Table Scan Template SQL" }
  shutdown_sql:        { type: text, comment: "Shutdown SQL" }
  arrow_flight_conf:   { type: text, comment: "Configuration" }
  active:              { type: boolean, default: true, comment: "Active" }
  user_id:             { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:              { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:          { type: datetime, comment: "Created at" }
  updated_at:          { type: datetime, comment: "Updated at" }
  excluded:            { type: boolean, default: false, comment: "Excluded" }
data:
  - {arrow_flight_id: 1, arrow_flight: "Expose Admin DB", arrow_flight_desc: "Ex. Arrow Flight Schema using ADMIN app", flight_schema: adm, startup_sql: "INSTALL SQLITE;LOAD SQLITE;", main_sql: "ATTACH 'database/ADMIN.db' AS adm (TYPE SQLITE);USE adm;", shutdown_sql: "USE memory;DETACH adm;", active: false, app_id: 1, user_id: 1, excluded: false}
```

## ARROW_FLIGHT_TABLE
```yaml
table: arrow_flight_table
comment: Arrow Flight - Tables
columns:
  arrow_flight_table_id:   { type: integer, pk: true, autoincrement: true, comment: "ID" }
  arrow_flight_table:      { type: varchar(200), nullable: false, comment: "Table Name" }
  arrow_flight_table_desc: { type: text, comment: "Table Description" }
  arrow_flight_id:         { type: integer, fk: "arrow_flight.arrow_flight_id", comment: "Arrow Flight ID" }
  active:                  { type: boolean, default: true, comment: "Active" }
  user_id:                 { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:                  { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:              { type: datetime, comment: "Created at" }
  updated_at:              { type: datetime, comment: "Updated at" }
  excluded:                { type: boolean, default: false, comment: "Excluded" }
```

## ARROW_FLIGHT_TABLE_FIELD
```yaml
table: arrow_flight_table_field
comment: Arrow Flight - Tables Fields
columns:
  arrow_flight_table_field_id:   { type: integer, pk: true, autoincrement: true, comment: "ID" }
  arrow_flight_table_field:      { type: varchar(200), nullable: false, comment: "Field Name" }
  arrow_flight_table_field_desc: { type: text, comment: "Field Description" }
  arrow_flight_table_id:         { type: integer, fk: "arrow_flight_table.arrow_flight_table_id", comment: "Arrow Flight Table ID" }
  arrow_flight_id:               { type: integer, fk: "arrow_flight.arrow_flight_id", comment: "Arrow Flight ID" }
  active:                        { type: boolean, default: true, comment: "Active" }
  user_id:                       { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:                        { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:                    { type: datetime, comment: "Created at" }
  updated_at:                    { type: datetime, comment: "Updated at" }
  excluded:                      { type: boolean, default: false, comment: "Excluded" }
```

## ARROW_FLIGHT_TABLE_SCOPE
```yaml
table: arrow_flight_table_scope
comment: Arrow Flight - Tables Scopes
columns:
  arrow_flight_table_scope_id:   { type: integer, pk: true, autoincrement: true, comment: "ID" }
  arrow_flight_table_scope:      { type: varchar(200), unique: true, nullable: false, comment: "Scope Name" }
  arrow_flight_table_scope_desc: { type: text, comment: "Scope Description" }
  arrow_flight_table_scope_sql:  { type: text, nullable: false, comment: "Scope SQL" }
  arrow_flight_table_id:         { type: integer, fk: "arrow_flight_table.arrow_flight_table_id", comment: "Arrow Flight Table ID" }
  arrow_flight_id:               { type: integer, fk: "arrow_flight.arrow_flight_id", comment: "Arrow Flight ID" }
  active:                        { type: boolean, default: true, comment: "Active" }
  user_id:                       { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:                        { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:                    { type: datetime, comment: "Created at" }
  updated_at:                    { type: datetime, comment: "Updated at" }
  excluded:                      { type: boolean, default: false, comment: "Excluded" }
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
  user_id:        { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:         { type: integer, fk: "app.app_id", comment: "App ID" }
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
  user_id:              { type: integer, fk: "users.user_id", comment: "User ID" }
  app_id:               { type: integer, fk: "app.app_id", comment: "App ID" }
  created_at:           { type: datetime, comment: "Created at" }
  updated_at:           { type: datetime, comment: "Updated at" }
  excluded:             { type: boolean, default: false, comment: "Excluded" }
```
