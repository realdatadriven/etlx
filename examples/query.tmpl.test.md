# DYN_QUERY_TMPL_TEST
```yaml metadata
name: DYN_QUERY_TMPL_TEST
runs_as: ETL
description: |
  this is a test of generating dynamic query from golag template sql, <step>_data → put results in item.data → render Go template → execute generated SQL.
active: true
```

## query.tmpl.test
```yaml
name: "query.tmpl.test"
description: "Test dynamic SQL generation"
load_conn: "duckdb:"
load_data: tmplData
load_sql: load_sql_tmpl
```


```sql
-- tmplData
SELECT *
FROM (VALUES
    ('2026-09-20', 'A'),
    ('2026-09-21', 'B'),
    ('2026-09-22', 'C')
) AS t(date_ref, source_type)
```

```sql
-- load_sql_tmpl
{{- range $i, $row := (index .tmplData).data }}
{{- if $i }} UNION ALL {{ end }}
SELECT DATE '{{$row.date_ref}}' AS date_ref, '{{$row.source_type}}' AS source_type
{{- end }}
```