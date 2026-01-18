# etlx config: EXTRACT_LOAD example (NYC Yellow Taxi — Jan 2024)

This document shows a practical example of extending the `etlx` config model with **metadata keys** usefull for data governance, using the NYC Yellow Taxi January 2024 Parquet file hosted at `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`.

<!-- markdownlint-disable MD025 -->

# EXTRACT_LOAD

```yaml metadata
name: EXTRACT_LOAD
runs_as: ETL
description: |
  Extracts and Loads the data sets to the local analitical database
connection: "sqlite3:database/sqlite_ex.db"
database: "sqlite3:database/sqlite_ex.db"
active: true
```

## VERSION

```yaml metadata
name: VERSION
description: "DDB Version"
table: VERSION
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
load_sql: 'CREATE OR REPLACE TABLE DB."<table>" AS SELECT version() AS "VERSION";'
load_after_sql: "DETACH DB;"
rows_sql: 'SELECT COUNT(*) AS "nrows" FROM DB."<table>"'
active: true
```

## TRIP_DATA

```yaml metadata
name: TRIP_DATA
description: "Example extrating trip data from web to a local database"
table: TRIP_DATA
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
load_validation:
  - type: throw_if_not_empty
    sql: FROM "DB"."<table>" WHERE "ref_date" = '{YYYY-MM}' LIMIT 10
    msg: "This date is already imported — aborting to avoid duplicates."
load_sql: update_trip_data_table
load_on_err_match_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
load_on_err_match_sql: create_trip_data_table
load_after_sql: DETACH "DB"
_query_doc: QUERY_EXTRACT_TRIP_DATA
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>" WHERE "ref_date" = '{YYYY-MM}'
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>" WHERE "ref_date" = '{YYYY-MM}'
file: "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY-MM}.parquet"
active: true
```

```sql
-- create_trip_data_table
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT *, '{YYYY-MM}' AS "ref_date"
FROM ([[QUERY_EXTRACT_TRIP_DATA]])
```

```sql
-- update_trip_data_table
INSERT INTO "DB"."<table>" BY NAME
SELECT *, '{YYYY-MM}' AS "ref_date"
FROM ([[QUERY_EXTRACT_TRIP_DATA]])
```

```sql
-- extract_load_trip_data_with_out_doc_field_metadata
CREATE OR REPLACE TABLE "DB"."<table>" AS
FROM READ_PARQUET('<fname>')
```

## ZONES

```yaml metadata
name: ZONES
description: "Taxi Zone Lookup Table"
table: ZONES
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
load_sql: extract_load_zones
load_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: true
```

```sql
-- extract_load_zones
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT *
FROM 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv';
```

# QUERY_EXTRACT_TRIP_DATA

This QueryDoc extracts selected fields from the NYC Yellow Taxi Trip Record dataset.

```yaml metadata
name: QUERY_EXTRACT_TRIP_DATA
description: "Extracts essential NYC Yellow Taxi trip fields (with governance metadata)."
is_query: true
owner: taxi-analytics-team
details: "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"
source:
  uri: "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
  format: parquet
```

## VendorID

```yaml metadata
name: VendorID
description: "A code indicating which TPEP provider generated the record.
1=CMT, 2=Curb, 6=Myle, 7=Helix."
type: integer
owner: data-providers
```

```sql
-- select
SELECT VendorID
```

```sql
-- from
FROM READ_PARQUET('<fname>')
```

## tpep_pickup_datetime

```yaml metadata
name: tpep_pickup_datetime
description: "Timestamp when the meter was engaged (trip start)."
type: timestamp
owner: taxi-analytics-team
```

```sql
-- select
    , tpep_pickup_datetime
```

## tpep_dropoff_datetime

```yaml metadata
name: tpep_dropoff_datetime
description: "Timestamp when the meter was disengaged (trip end)."
type: timestamp
owner: taxi-analytics-team
```

```sql
-- select
    , tpep_dropoff_datetime
```

## passenger_count

```yaml metadata
name: passenger_count
description: "Number of passengers, typically entered by the driver."
type: integer
owner: ops
```

```sql
-- select
    , passenger_count
```

## trip_distance

```yaml metadata
name: trip_distance
description: "Elapsed trip distance in miles, reported by the meter."
type: double
owner: taxi-analytics-team
```

```sql
-- select
    , trip_distance
```

## RatecodeID

```yaml metadata
name: RatecodeID
description: "Final rate code at trip end.
1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester,
5=Negotiated fare, 6=Group ride, 99=Unknown."
type: integer
owner: finance
```

```sql
-- select
    , RatecodeID
```

## store_and_fwd_flag

```yaml metadata
name: store_and_fwd_flag
description: "'Y' if the trip record was held in the vehicle before transmission (no server connection). 'N' otherwise."
type: string
owner: platform
```

```sql
-- select
    , store_and_fwd_flag
```

## PULocationID

```yaml metadata
name: PULocationID
description: "TLC Taxi Zone ID for pickup location."
type: integer
owner: geo
```

```sql
-- select
    , PULocationID
```

## DOLocationID

```yaml metadata
name: DOLocationID
description: "TLC Taxi Zone ID for dropoff location."
type: integer
owner: geo
```

```sql
-- select
    , DOLocationID
```

## payment_type

```yaml metadata
name: payment_type
description: "How the passenger paid:
0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip."
type: integer
owner: finance
```

```sql
-- select
    , payment_type
```

## fare_amount

```yaml metadata
name: fare_amount
description: "Time-and-distance fare calculated by the meter."
type: numeric
owner: finance
```

```sql
-- select
    , fare_amount
```

## extra

```yaml metadata
name: extra
description: "Miscellaneous extras and surcharges (e.g., peak surcharge)."
type: numeric
owner: finance
```

```sql
-- select
    , extra
```

## mta_tax

```yaml metadata
name: mta_tax
description: "0.50 USD MTA tax triggered by metered rate."
type: numeric
owner: finance
```

```sql
-- select
    , mta_tax
```

## tip_amount

```yaml metadata
name: tip_amount
description: "Tip in USD. Only includes credit-card tips; cash tips are not recorded."
type: numeric
owner: finance
```

```sql
-- select
    , tip_amount
```

## tolls_amount

```yaml metadata
name: tolls_amount
description: "Total tolls paid for the trip."
type: numeric
owner: finance
```

```sql
-- select
    , tolls_amount
```

## improvement_surcharge

```yaml metadata
name: improvement_surcharge
description: "Flat surcharge added at flag drop. Introduced in 2015."
type: numeric
owner: finance
```

```sql
-- select
    , improvement_surcharge
```

## total_amount

```yaml metadata
name: total_amount
description: "Total charged amount (fare + extras + taxes + tips)."
type: numeric
owner: finance
```

```sql
-- select
    , total_amount
```

## congestion_surcharge

```yaml metadata
name: congestion_surcharge
description: "NY State congestion surcharge assessed per trip."
type: numeric
owner: finance
```

```sql
-- select
    , congestion_surcharge
```

## airport_fee

```yaml metadata
name: airport_fee
description: "Fee for pickups at JFK or LaGuardia airports."
type: numeric
owner: finance
```

```sql
-- select
    , airport_fee
```

## cbd_congestion_fee

```yaml metadata
name: cbd_congestion_fee
description: "MTA Congestion Relief Zone fee (in effect after Jan 5, 2025)."
type: numeric
owner: finance
active: false
```

```sql
-- select
    , cbd_congestion_fee
```

# TRANSFORM

```yaml metadata
name: TRANSFORM
runs_as: ETL
description: Transforms the inputs into to desrable outputs
connection: "sqlite3:database/DB_EX_DGOV.db"
database: "sqlite3:database/DB_EX_DGOV.db"
active: true
```

## MostPopularRoutes

```yaml metadata
name: MostPopularRoutes
description: |
  Most Popular Routes - Identify the most common pickup-dropoff route combinations to understand travel patterns.
table: MostPopularRoutes
transform_conn: "duckdb:"
transform_before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
transform_sql: trf_most_popular_routes
transform_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: true
```

```sql
-- trf_most_popular_routes
CREATE OR REPLACE TABLE "DB"."<table>" AS
[[QUERY_TOP_ZONES]]
LIMIT 15
```

# QUERY_TOP_ZONES

```yaml metadata
name: QUERY_TOP_ZONES
description: "Most common pickup/dropoff zone combinations with aggregated metrics."
owner: taxi-analytics-team
is_query: true
source:
  - TRIP_DATA
  - ZONES
```

## pickup_borough

```yaml metadata
name: pickup_borough
description: "Borough of the pickup location (from ZONES lookup)."
type: string
derived_from:
  - TRIP_DATA.PULocationID
  - ZONES.Borough
formula: |
  This field is a direct lookup of the pickup borough using the pickup location ID.
```

```sql
-- select
SELECT zpu.Borough AS pickup_borough
```

```sql
-- from
FROM DB.TRIP_DATA AS t
```

```sql
-- join
JOIN DB.ZONES AS zpu ON t.PULocationID = zpu.LocationID
```

```sql
-- group_by
GROUP BY pickup_borough
```

## pickup_zone

```yaml metadata
name: pickup_zone
description: "Zone name of the pickup location (from ZONES lookup)."
type: string
derived_from:
  - TRIP_DATA.PULocationID
  - ZONES.Zone
formula: |
  This field is a direct lookup of the pickup zone using the pickup location ID.
```

```sql
-- select
    , zpu.Zone AS pickup_zone
```

```sql
-- group_by
    , pickup_zone
```

## dropoff_borough

```yaml metadata
name: dropoff_borough
description: "Borough of the dropoff location (from ZONES lookup)."
type: string
derived_from:
  - TRIP_DATA.DOLocationID
  - ZONES.Borough
formula: |
  This field is a direct lookup of the dropoff borough using the dropoff location ID.
```

```sql
-- select
    , zdo.Borough AS dropoff_borough
```

```sql
-- join
JOIN DB.ZONES AS zdo ON t.DOLocationID = zdo.LocationID
```

```sql
-- group_by
    , dropoff_borough
```

## dropoff_zone

```yaml metadata
name: dropoff_zone
description: "Zone name of the dropoff location (from ZONES lookup)."
type: string
derived_from:
  - TRIP_DATA.DOLocationID
  - ZONES.Zone
formula: |
  This field is a direct lookup of the dropoff zone using the dropoff location ID.
```

```sql
-- select
    , zdo.Zone AS dropoff_zone
```

```sql
-- group_by
    , dropoff_zone
```

## total_trips

```yaml metadata
name: total_trips
description: "Total number of trips between each pickup/dropoff zone pair."
type: integer
derived_from:
  - TRIP_DATA.*
formula: | 
    "
    Count the number of trips in each pickup/dropoff combination.

    **Formula (LaTeX):**

    ```latex
    \text{total\_trips} = \sum_{i=1}^{N} 1
    ```"
```

```sql
-- select
    , COUNT(*) AS total_trips
```

```sql
-- order_by
ORDER BY total_trips DESC
```

## avg_fare

```yaml metadata
name: avg_fare
description: "Average total fare for trips between the selected pickup and dropoff zones."
type: numeric
derived_from:
  - TRIP_DATA.total_amount
formula: | 
    "
    Compute the arithmetic mean of total fares for the group, rounded to 2 decimals.

    **Formula (LaTeX):**

    ```latex
    \text{avg\_fare} = \text{round}\!\left(\frac{1}{N}\sum_{i=1}^{N} \text{total\_amount}_i,\ 2\right)
    ```"
```

```sql
-- select
    , ROUND(AVG(t.total_amount), 2) AS avg_fare
```

## avg_distance

```yaml metadata
name: avg_distance
description: "Average trip distance (miles)."
type: numeric
derived_from:
  - TRIP_DATA.trip_distance
formula: | 
    "
    Compute the arithmetic mean of trip distances for the group, rounded to 2 decimals.

    **Formula (LaTeX):**

    ```latex
    \text{avg\_distance} = \text{round}\!\left(\frac{1}{N}\sum_{i=1}^{N} \text{trip\_distance}_i,\ 2\right)
    ```"

```

```sql
-- select
    , ROUND(AVG(t.trip_distance), 2) AS avg_distance
```

# QUALITY_CHECK

```yaml
description: "Runs some queries to check quality / validate."
runs_as: DATA_QUALITY
active: true
```

## Rule0001

```yaml
name: Rule0001
description: "Check if the payment_type has only the values 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip."
connection: "duckdb:"
before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
query: quality_check_query
fix_quality_err: fix_quality_err_query
column: total_err # Defaults to 'total'.
check_only: false # runs only quality check if true
fix_only: false # runs only quality fix if true and available and possible
after_sql: "DETACH DB"
active: true
```

```sql
-- quality_check_query
SELECT COUNT(*) AS "total_err"
FROM "TRIP_DATA"
WHERE "payment_type" NOT IN (0,1,2,3,4,5,6);
```

```sql
-- fix_quality_err_query
UPDATE "TRIP_DATA"
SET "payment_type" = 'default value'
WHERE "payment_type" NOT IN (0,1,2,3,4,5,6);
```

## Rule0002

```yaml
name: Rule0002
description: "Check if there is any trip with distance less than or equal to zero."
connection: "duckdb:"
before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
query: quality_check_query
fix_quality_err: null # no automated fixing for this
column: total_err # Defaults to 'total'.
after_sql: "DETACH DB"
active: true
```

```sql
-- quality_check_query
SELECT COUNT(*) AS "total_err"
FROM "TRIP_DATA"
WHERE NOT "trip_distance" > 0;
```

<!-- markdownlint-disable MD025 -->
# GOVERNANCE_ARTIFACTS

```yaml metadata
name: GOVERNANCE_ARTIFACTS
description: "Generates governance artifacts like data dictionary and data quality rules."
runs_as: EXPORTS
active: false
```

## DATA_DICTIONARY

```yaml metadata
name: DATA_DICTIONARY
description: "Generates a data dictionary from the metadata of the queries."
connection: "duckdb:"
before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
data_sql: null
after_sql: "DETACH DB"
tmp_prefix: null
text_template: true
template: governance_template
return_content: false
path: "data_dictionary.html"
active: true
```

```html governance_template
<html encoding="UTF-8" lang="en">
<head>
    <title>Governance Artifacts</title>
    <style>
        body {
            font-family: Arial, sans-serif;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 32px;
        }
        th, td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background-color: #f2f2f2;
        }
        h1, h2, h3 {
            margin-top: 32px;
        }
    </style>
</head>
<body>
<h1>Governance Artifacts</h1>
<h2>Data Dictionary</h2>
{{- range $blockName, $block := .conf }}
    {{- if and (ne $blockName "__order") (ne $blockName "metadata") (kindIs "map" $block)}}
    {{- $meta := index $block "metadata" }}
    <!-- ===================================================== -->
    <!-- DATA DICTIONARY (no runs_as at level 1)               -->
    <!-- ===================================================== -->
    {{- if and (kindIs "map" $meta) (eq (index $meta "is_query") true)}}
        <pre>{{$blockName }}</pre>
        <h3>
            {{ $blockName }}
            {{- with $meta }} — {{ index . "description" }}{{ end }}
        </h3>
        {{- $order := index $block "__order" }}
        {{- if kindIs "slice" $order }}
        <table>
            <tr>
                <th>Field Name</th>
                <th>Description</th>
                <th>Type</th>
                <th>Owner</th>
                <th>Derived From</th>
                <th>Formula</th>
            </tr>
            {{- range $i, $fieldName := $order }}
                {{- $field := index $block $fieldName }}
                {{- if kindIs "map" $field }}
                {{- $fmeta := index $field "metadata" }}
                <tr>
                    <td>{{ $fieldName }}</td>
                    <td>{{ index $fmeta "description" | default "N/A" }}</td>
                    <td>{{ index $fmeta "type" | default "N/A" }}</td>
                    <td>{{ index $fmeta "owner" | default "N/A" }}</td>
                    <td>
                        {{- with index $fmeta "derived_from" }}
                            {{- range $j, $src := . }}
                                {{- if $j }}, {{ end }}{{ $src }}
                            {{- end }}
                        {{- else }}
                            N/A
                        {{- end }}
                    </td>
                    <td>{{ index $fmeta "formula" | default "N/A" }}</td>
                </tr>
                {{- end }}
            {{- end }}
        </table>
        {{- end }}
    {{- end }}
    {{- end }}
{{- end }}
<h2>Data Quality Rules</h2>
{{- range $blockName, $block := .conf }}
    {{- if and (ne $blockName "__order") (ne $blockName "metadata") (kindIs "map" $block)}}
    {{- $meta := index $block "metadata" }}
    <!-- ===================================================== -->
    <!-- DATA QUALITY RULES (runs_as = DATA_QUALITY)            -->
    <!-- ===================================================== -->
    {{- if and $meta (eq (index $meta "runs_as") "DATA_QUALITY")}}
        <h2>
            {{ $blockName }}
            {{- with $meta }} — {{ index . "description" }}{{ end }}
        </h2>
        <table>
            <tr>
                <th>Rule Name</th>
                <th>Description</th>
                <th>Active</th>
            </tr>
            {{- range $ruleName, $rule := $block }}
                {{- if and (ne $ruleName "metadata") (ne $ruleName "__order") (kindIs "map" $rule)}}
                {{- $rmeta := index $rule "metadata" }}
                <tr>
                    <td>{{ $ruleName }}</td>
                    <td>{{ index $rmeta "description" | default "N/A" }}</td>
                    <td>
                        {{- if hasKey $rule "active" -}}
                            {{ index $rule "active" }}
                        {{- else -}}
                            true
                        {{- end -}}
                    </td>
                </tr>
                {{- end }}
            {{- end }}
        </table>
    {{- end }}
    {{- end }}
{{- end }}
</body>
</html>
```

# EXPORT

```yaml metadata
name: EXPORT
description: Export files
runs_as: EXPORTS
connection: "duckdb:"
path: "static/uploads/tmp"
active: true
```

## hist_logs

```yaml metadata
name: hist_logs
description: "Export logs to parquet"
connection: "duckdb:"
before_sql: "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
export_sql: "COPY (FROM DB.etlx_logs) TO '<fname>'"
after_sql: "DETACH DB"
path: 'hist_logs_{YYYYMMDD}.{TSTAMP}.parquet'
tmp_prefix: 'tmp'
active: true
```

# SAVE_LOGS

```yaml metadata
name: SAVE_LOGS
runs_as: LOGS
description: Saving the logs in the same DB instead of the deafult temp style
table: etlx_logs
connection: "duckdb:"
before_sql:
  - "ATTACH 'database/sqlite_ex.db' AS DB (TYPE SQLITE)"
  - "USE DB;"
  - LOAD json
  - "get_dyn_queries[create_columns_missing](ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE), DETACH DB)"
save_log_sql: INSERT INTO "DB"."<table>" BY NAME FROM read_json('<fname>')
save_on_err_patt: "(?i)table.+does.+not.+exist"
save_on_err_sql: CREATE TABLE IF NOT EXISTS "DB"."<table>" AS FROM read_json('<fname>');
after_sql:
  - "USE memory;"
  - DETACH "DB"
active: true
```

```sql
-- create_columns_missing
WITH source_columns AS (
    SELECT column_name, column_type
    FROM (DESCRIBE SELECT * FROM read_json('<fname>'))
),
destination_columns AS (
    SELECT column_name, data_type as column_type
    FROM duckdb_columns
    WHERE table_name = '<table>'
),
missing_columns AS (
    SELECT s.column_name, s.column_type
    FROM source_columns s
    LEFT JOIN destination_columns d ON s.column_name = d.column_name
    WHERE d.column_name IS NULL
)
SELECT 'ALTER TABLE "DB"."<table>" ADD COLUMN "' || column_name || '" ' || column_type || ';' AS query
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```
