+++
title = 'Actions'
weight = 50
draft = false
+++

### ACTIONS

There are scenarios in ETL workflows where actions such as downloading, uploading, compressing or copying files cannot be performed using SQL alone. The `ACTIONS` section allows you to define steps for copying or transferring files using the file system or external protocols.

---

#### **ACTIONS Structure**

Each action under the `ACTIONS` section has the following:

- `name`: Unique name for the action.
- `description`: Human-readable explanation.
- `type`: The kind of action to perform. Options:
  - `copy_file`
  - `compress`
  - `decompress`
  - `ftp_download`
  - `ftp_upload`
  - `sftp_download`
  - `sftp_upload`
  - `http_download`
  - `http_upload`
  - `s3_download`
  - `s3_upload`
  - `db_2_db`
- `params`: A map of input parameters required by the action type.

---

````markdown

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
  source: "C:/reports/final_report.xlsx"
  target: "C:/reports/archive/final_report_YYYYMMDD.xlsx"
active: true
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
    - "reports/report_1.csv"
    - "reports/report_2.csv"
  output: "archives/reports_YYYYMM.zip"
active: true
```

## UNZIP

```yaml metadata
name: CompressReports
description: "Compress report files into a .zip archive"
type: decompress
params:
  compression: zip
  input: "archives/reports_YYYYMM.zip"
  output: "tmp"
active: true
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
    - "reports/summary.csv"
  output: "archives/summary_YYYYMM.csv.gz"
active: true
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
active: true
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
active: true
```

---

## FTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external FTP"
type: ftp_download
params:
  host: "ftp.example.com"
  port: "21"
  user: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: true
```

## FTP DOWNLOAD GLOB

```yaml metadata
name: FetchRemoteReport2024
description: "Download data file from external FTP"
type: ftp_download
params:
  host: "ftp.example.com"
  port: "21"
  user: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report_2024*.csv"
  target: "downloads/"
active: true
```

## SFTP DOWNLOAD

```yaml metadata
name: FetchRemoteReport
description: "Download data file from external SFTP"
type: stp_download
params:
  host: "sftp.example.com"
  user: "myuser"
  password: "@SFTP_PASSWORD"
  host_key: ~/.ssh/known_hosts # or a specific file
  port: 22
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: true
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
active: true
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
active: true
```

---

##### 📥 ACTIONS – `db_2_db` (Cross-Database Write)

> As of this moment, **DuckDB does not support direct integration** with certain databases like **MSSQL**, **DB2**, or **Oracle**, the same way it does with **SQLite**, **Postgres**, or **MySQL**.

To bridge this gap, the `db_2_db` action type allows you to **query data from one database** (source) and **write the results into another** (target), using ETLX’s internal execution engine (powered by `sqlx` or ODBC).

###### ✅ Use Case

Use `db_2_db` when:

- Your database is not accessible with DuckDB.
- You want to move data from one place to another using **pure SQL**, chunked if necessary.

---

###### 🧩 Example

````markdown
...

## WRITE_RESULTS_MSSQL

```yaml metadata
name: WRITE_RESULTS_MSSQL
description: "MSSQL example – moving logs into a SQL Server database."
type: db_2_db
params:
  source:
    conn: sqlite3:database/HTTP_EXTRACT.db
    before: null
    chunk_size: 1000
    timeout: 30
    sql: origin_query
    after: null
  target:
    conn: mssql:sqlserver://sa:@MSSQL_PASSWORD@localhost?database=master&connection+timeout=30
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

```

---

### 🛠️ Notes