# ROLE_ACCESS
```yaml
name: ROLE_ACCESS
description: Role Model
database: ADMIN #RLA may need to access records on other DBs
runs_as: ROLE
admin_conn: '@DB_DRIVER_NAME:@DB_DSN'
```

## ANONYMOUS
```yaml
name: anonymous
description: Anonymous Role
access:
  - ADMIN:
    - Arrow Flight:
      - {table: flight_catalog, read: true, rla: [{flight_schema: admin, read: true, share: true}]}
      - flight_schema
      - flight_schema_table
active: true
```

# ROLE_USERS
```yaml
name: ROLE_USERS
description: Give user access to a role
runs_as: ROLE
admin_conn: '@DB_DRIVER_NAME:@DB_DSN'
```

## ANONYMOUS
```yaml
name: anonymous
description: Anonymous Role
users: [root, admin@domain.com, anonymous.user]
active: true
```