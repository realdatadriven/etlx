---
name: ETLX catalog test
main_conn: 'sqlite3:database/sqlite_ex.db'
author: realdatadriven
version: 1.0.0
---

# ETLX pipeline with catalog import

This is a catalog-oriented replica of `tmpl.sqlite.md`. First create the
catalog database from `examples/catalog_model.md`, using
`sqlite3:database/catalog.db`. Then run this document. The `CATALOG` section
supplies global catalog metadata and imports every eligible pipeline section.

# EXTRACT_LOAD

```yaml metadata
name: EXTRACT_LOAD
runs_as: ETL
description: Extracts and loads NYC taxi data into the local analytical database.
connection: "sqlite3:database/sqlite_ex.db"
database: "sqlite_ex"
business_unit: Analytics
domain: Mobility
subdomain: Taxi Operations
business_owner: analytics@example.com
technical_owner: platform@example.com
glossary_terms:
  - Taxi Trip
  - Trip Distance
active: true
```

## VERSION

```yaml metadata
name: VERSION
description: Database engine version used by this pipeline.
table: VERSION
schema: main
asset_type: Table
business_owner: analytics@example.com
technical_owner: platform@example.com
glossary_term: Database Version
active: true
```

### VERSION

```yaml metadata
name: VERSION
description: Version string returned by the database engine.
type: varchar
nullable: false
glossary_term: Database Version
```

## TRIP_DATA

```yaml metadata
name: TRIP_DATA
description: NYC Yellow Taxi trip records loaded from the public parquet feed.
table: TRIP_DATA
schema: main
asset_type: Table
load_sql: build_trip_data
business_unit: Analytics
domain: Mobility
subdomain: Taxi Operations
business_owner: analytics@example.com
technical_owner: platform@example.com
glossary_terms:
  - Taxi Trip
  - Trip Distance
active: true
```

```sql build_trip_data
CREATE TABLE TRIP_DATA AS
SELECT
  VendorID,
  trip_distance,
  fare_amount
FROM yellow_trip_data;
```

### VendorID

```yaml metadata
name: VendorID
description: Code identifying the TPEP provider that generated the trip record.
type: integer
nullable: false
glossary_term: Taxi Vendor
```

### trip_distance

```yaml metadata
name: trip_distance
description: Travel distance recorded by the taxi meter, in miles.
type: numeric
nullable: true
glossary_term: Trip Distance
```

### fare_amount

```yaml metadata
name: fare_amount
description: Time-and-distance fare charged for the trip.
type: numeric
nullable: true
glossary_term: Trip Fare
```

## ZONES

```yaml metadata
name: ZONES
description: Taxi zone lookup table.
table: ZONES
schema: main
asset_type: Table
business_unit: Analytics
domain: Mobility
subdomain: Taxi Operations
business_owner: analytics@example.com
technical_owner: platform@example.com
glossary_term: Taxi Zone
active: true
```

### LocationID

```yaml metadata
name: LocationID
description: Unique identifier for a taxi zone.
type: integer
nullable: false
primary_key: true
glossary_term: Taxi Zone
```

### Zone

```yaml metadata
name: Zone
description: Human-readable name of a taxi zone.
type: varchar
nullable: false
glossary_term: Taxi Zone
```

# CATALOG
```yaml metadata
name: CATALOG
runs_as: CATALOG
description: Import the EXTRACT_LOAD pipeline metadata into the catalog model.
conn: "sqlite3:database/catalog.db"
# These values override or supplement every eligible source section.
business_unit: Analytics
domain: Mobility
subdomain: Taxi Operations
business_owner: analytics@example.com
technical_owner: platform@example.com
active: true
```
