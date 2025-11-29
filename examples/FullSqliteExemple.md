# etlx config: EXTRACT_LOAD example (NYC Yellow Taxi — Jan 2024)

This document shows a practical example of extending the `etlx` config model with **metadata keys** useful for data governance, using the NYC Yellow Taxi January 2024 Parquet file hosted at `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`.
<!-- markdownlint-disable MD025 -->

# EXTRACT_LOAD

```yaml metadata
name: EXTRACT_LOAD
runs_as: ETL
description: |
  Extracts and Loads the data sets to the local analitical database
connection: "sqlite3:DB_EX_DGOV.db"
database: "sqlite3:DB_EX_DGOV.db"
active: true
```

## TRIP_DATA

```yaml metadata
name: TRIP_DATA
description: "Example extrating trip data from web to a local database"
table: TRIP_DATA
load_conn: "duckdb:"
load_before_sql: "ATTACH 'DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
load_sql: extract_load_query
load_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: true
```

```sql
-- extract_load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
[[QUERY_EXTRACT_TRIP_DATA]]
```

## ZONES

```yaml metadata
name: ZONES
description: "Taxi Zone Lookup Table"
table: ZONES
load_conn: "duckdb:"
load_before_sql: "ATTACH 'DB_EX_DGOV.db' AS DB (TYPE SQLITE)"
load_sql: extract_load_query
load_after_sql: DETACH "DB"
drop_sql: DROP TABLE IF EXISTS "DB"."<table>"
clean_sql: DELETE FROM "DB"."<table>"
rows_sql: SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
active: true
```

```sql
-- extract_load_query
CREATE OR REPLACE TABLE zones AS
SELECT *
FROM 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv';
```

# QUERY_EXTRACT_TRIP_DATA

This QueryDoc extracts selected fields from the NYC Yellow Taxi Trip Record dataset.

```yaml metadata
name: QUERY_EXTRACT_TRIP_DATA
description: "Extracts essential NYC Yellow Taxi trip fields (with governance metadata)."
owner: taxi-analytics-team@yourorg.com
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
```

```sql
-- select
    , cbd_congestion_fee
```

# TRANSFORM
