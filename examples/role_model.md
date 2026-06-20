# ROLE_ACCESS
```yaml
name: ROLE_ACCESS
description: DATA Model ADMIN
database: ADMIN
runs_as: ROLE
admin_conn: '@DB_DRIVER_NAME:@DB_DSN'
```

## ANONYMOUS
```yaml
name: anonymous
description: Anonymous Role
apps:
  - ADMIN:
    - Arrow Flight:
      - {table: flight_schema, read: true, rla: [{flight_schema: admin, read: true, share: true}]}
active: true
```