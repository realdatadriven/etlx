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

