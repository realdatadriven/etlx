# etlx config: EXTRACT_LOAD example (NYC Yellow Taxi — Jan 2024)

This document shows a practical example of extending the `etlx` config model with **metadata keys** useful for data governance, using the NYC Yellow Taxi January 2024 Parquet file hosted at `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`.
<!-- markdownlint-disable MD025 -->

# EXTRACT_LOAD

```yaml metadata
name: EXTRACT_LOAD
runs_as: ETL
description: |
  Extracts and Loads the data sets to the local analitical database
connection: "sqlite3:database/DB_EX_DGOV.db"
database: "sqlite3:database/DB_EX_DGOV.db"
active: true
```

## TRIP_DATA

```yaml metadata
name: TRIP_DATA
description: "Example extrating trip data from web to a local database"
table: TRIP_DATA
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
load_sql: extract_load_trip_data
load_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: false
```

```sql
-- extract_load_trip_data
CREATE OR REPLACE TABLE "DB"."<table>" AS
[[QUERY_EXTRACT_TRIP_DATA]]
```

```sql
-- extract_load_trip_data_with_out_doc_field_metadata
CREATE OR REPLACE TABLE "DB"."<table>" AS
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
```

## ZONES

```yaml metadata
name: ZONES
description: "Taxi Zone Lookup Table"
table: ZONES
load_conn: "duckdb:"
load_before_sql: "ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
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
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
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
transform_before_sql: "ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
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
```

```sql
-- select
    , ROUND(AVG(t.trip_distance), 2) AS avg_distance
```

# SAVE_LOGS

```yaml metadata
name: SAVE_LOGS
runs_as: LOGS
description: Saving the logs in the same DB instead of the deafult temp style
table: etlx_logs
connection: "duckdb:"
before_sql:
  - "ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
  - 'USE DB;'
  - LOAD json
  - "get_dyn_queries[create_columns_missing](ATTACH 'database/DB_EX_DGOV.db' AS DB (TYPE SQLITE), DETACH DB)"
save_log_sql: INSERT INTO "DB"."<table>" BY NAME FROM read_json('<fname>')
save_on_err_patt: '(?i)table.+does.+not.+exist'
save_on_err_sql: CREATE TABLE IF NOT EXISTS "DB"."<table>" AS FROM read_json('<fname>');
after_sql:
  - 'USE memory;'
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
