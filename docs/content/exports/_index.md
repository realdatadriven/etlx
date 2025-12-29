---
title: "Exports"
weight: 40
---

## Exports

ETLX supports exporting query results to files (CSV, Parquet, Excel) and template-based exports.

Example: CSV export

```sql
-- export
COPY (
    SELECT *
    FROM "DB"."Sales"
    WHERE "sale_date" = '{YYYY-MM-DD}'
) TO '/path/to/Reports/YYYYMMDD/sales_YYYYMMDD.csv' (FORMAT 'csv', HEADER true);
```

Template-based exports allow mapping query results into sheets and ranges of an existing Excel template using the `mapping` metadata structure.

Common fields in export metadata:

- `path`: destination path
- `template`: template file path for XLSX exports
- `mapping`: list of sheet/range/sql mappings

See the README for more complex examples (templates, mapping, pre/post SQL).
