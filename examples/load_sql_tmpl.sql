{{- range $i, $row := (index .tmplData).data }}
{{- if $i }} UNION ALL {{ end }}
SELECT DATE '{{$row.date_ref}}' AS date_ref, '{{$row.source_type}}' AS source_type
{{- end }}