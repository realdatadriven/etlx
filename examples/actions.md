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
    - "nyc_taxy_YYYYMMDD.xlsx"
    - "copy_nyc_taxy_YYYYMMDD.xlsx"
  output: "nyc_taxy.zip"
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
    - "nyc_taxy_YYYYMMDD.xlsx"
  output: "nyc_taxy_YYYYMMDD.xlsx.gz"
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
  username: "myuser"
  password: "@FTP_PASSWORD"
  source: "/data/daily_report.csv"
  target: "downloads/daily_report.csv"
active: true
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
