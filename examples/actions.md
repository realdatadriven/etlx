# ACTIONS

```yaml metadata
name: FileOperations
description: "Transfer and organize generated reports"
path: examples
active: true
```

---

## COPY LOCAL FILE

```yaml metadata
name: CopyReportToArchive
description: "Move final report to archive folder"
type: copy_file
params:
  source: "nyc_taxy_YYYYMMDD.xlsx"
  target: "copy_nyc_taxy_YYYYMMDD.xlsx"
active: false
```

---

## Compress to ZIP

```yaml metadata
name: CompressReports
description: "Compress report files into a .zip archive"
type: compress
params:
  compression: zip
  files:
    - "nyc_taxy_YYYYMMDD.xlsx"
    - "copy_nyc_taxy_YYYYMMDD.xlsx"
  output: "nyc_taxy.zip"
active: false
```

---

## Compress to GZ

```yaml metadata
name: CompressToGZ
description: "Compress a summary file to .gz"
type: compress
params:
  compression: gz
  files:
    - "nyc_taxy_YYYYMMDD.xlsx"
  output: "nyc_taxy_YYYYMMDD.xlsx.gz"
active: false
```

---

## HTTP DOWNLOAD

```yaml metadata
name: DownloadFromAPI
description: "Download dataset from HTTP endpoint"
type: http_download
params:
  url: "https://api.example.com/data"
  target: "data/today.json"
  method: GET
  headers:
    Authorization: "Bearer @API_TOKEN"
    Accept: "application/json"
  params:
    date: "YYYYMMDD"
    limit: "1000"
active: false
```

---

## HTTP UPLOAD

```yaml metadata
name: PushReportToWebhook
description: "Upload final report to an HTTP endpoint"
type: http_upload
params:
  url: "https://webhook.example.com/upload"
  method: POST
  source: "reports/final.csv"
  headers:
    Authorization: "Bearer @WEBHOOK_TOKEN"
    Content-Type: "multipart/form-data"
  params:
    type: "summary"
    date: "YYYYMMDD"
active: false
```

---

## FTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external FTP"
type: ftp_download
params:
  host: "ftp.example.com"
  username: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: false
```

## SFTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external SFTP"
type: sftp_download
params:
  host: "sftp.example.com"
  username: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: false
```

---

## S3 UPLOAD

```yaml metadata
name: ArchiveToS3
description: "Send latest results to S3 bucket"
type: s3_upload
params:
  AWS_ACCESS_KEY_ID: '@AWS_ACCESS_KEY_ID'
  AWS_SECRET_ACCESS_KEY: '@AWS_SECRET_ACCESS_KEY'
  AWS_REGION: '@AWS_REGION'
  AWS_ENDPOINT: 127.0.0.1:3000
  S3_FORCE_PATH_STYLE: true
  S3_DISABLE_SSL: false
  S3_SKIP_SSL_VERIFY: true
  bucket: "my-etlx-bucket"
  key: "exports/summary_YYYYMMDD.xlsx"
  source: "reports/summary.xlsx"
active: false
```

## S3 DOWNLOAD

```yaml metadata
name: DownalodFromS3
description: "Download file S3 from bucket"
type: s3_download
params:
  AWS_ACCESS_KEY_ID: '@AWS_ACCESS_KEY_ID'
  AWS_SECRET_ACCESS_KEY: '@AWS_SECRET_ACCESS_KEY'
  AWS_REGION: '@AWS_REGION'
  AWS_ENDPOINT: 127.0.0.1:3000
  S3_FORCE_PATH_STYLE: true
  S3_DISABLE_SSL: false
  S3_SKIP_SSL_VERIFY: true
  bucket: "my-etlx-bucket"
  key: "exports/summary_YYYYMMDD.xlsx"
  target: "reports/summary.xlsx"
active: false
```

## DB 2 DB EX

```yaml metadata
name: WRITE_RESULTS_MSSQL
description: "MSSQL, as of this moment DDB does not have the same suport to MSSQL like it has for SQLite, PG or MySQL so this could be a way to pu results in db like MSSQL or nay other DB suported by sqlx"
type: db_2_db
params:
  source:
    conn: sqlite3:database/HTTP_EXTRACT.db
    before: null
    chunk_size: 3
    timeout: 30
    sql: origin_query
    after: null
  target:
    conn: mssql:sqlserver://sa:@MSSQL_PASSWORD@localhost?database=master
    timeout: 30
    before:
      - create_schema
    sql: mssql_sql
    after: null
active: true
```

```sql
-- origin_query
SELECT "description", "duration", STRFTIME('%Y-%m-%d %H:%M:%S', "start_at") AS "start_at", "ref"
FROM "etlx_logs" 
ORDER BY "start_at" DESC
```

```sql
-- create_schema
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etlx_logs' AND type = 'U')
CREATE TABLE [dbo].[etlx_logs] (
    [description] NVARCHAR(MAX) NULL,
    [duration] BIGINT NULL,
    [start_at] DATETIME NULL,
    [ref] DATE NULL
);
```

```sql
-- mssql_sql
INSERT INTO [dbo].[etlx_logs] ([:columns]) VALUES 
```

# ETL

```yaml metadata
name: MSSQL_EXTRACT
description: "Example extrating from mssql sqlite3 file"
connection: "duckdb:"
database: MSSQL_EXTRACT.db
active: false
```

## MSSQL_EXTRACT

```yaml metadata
name: MSSQL_EXTRACT
description: "Example extrating from mssql sqlite3 file"
table: logs
to_csv: true
extract_conn: mssql:sqlserver://sa:@MSSQL_PASSWORD@localhost?database=master
extract_sql: SELECT * FROM [dbo].[etlx_logs]
load_conn: "duckdb:"
load_before_sql:
  - load_extentions
  - attach_db
load_sql: load_query
load_after_sql: detach_db
active: true
```

```sql
-- load_extentions
INSTALL sqlite;
LOAD sqlite;
```

```sql
-- attach_db
ATTACH 'database/MSSQL_EXTRACT.db' AS "DB" (TYPE SQLITE)
```

```sql
-- detach_db
DETACH "DB";
```

```sql
-- load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT * 
FROM '<fname>';
```
