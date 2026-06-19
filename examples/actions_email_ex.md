# ACTIONS

```yaml metadata
name: FileOperations
description: "Transfer and organize generated reports"
path: examples
active: true
```

## EMAILS
```yaml
name: emails
description: Extract Emails
type: IMAP
params:
    protocol: IMAP
    host: imap.gmail.com
    port: 993
    username: "@SMTP_USERNAME"
    password: "@SMTP_PASSWORD"
    folder: INBOX
    download_att: true
    attachment_path: ./examples/downloads
    search:
        _from: supplier@example.com
        subject: C7 Intro
        since: 24h
        _before: 24h
    conn: "duckdb:"
    sqls:
        - ATTACH 'database/etl.db' AS DB (TYPE SQLITE)
        - create_emails
        - merge_into_emails
        - DETACH DB
active: true
```

```sql
-- create_emails
CREATE TABLE IF NOT EXISTS DB.emails  (
    id           BIGINT,
    subject      VARCHAR,
    "from"       VARCHAR,
    "to"         VARCHAR,
    cc           VARCHAR,
    bcc          VARCHAR,
    date         TIMESTAMP,
    body         TEXT,
    attachments  VARCHAR
);
```

```sql
-- merge_into_emails
INSERT INTO DB.emails BY NAME SELECT * FROM READ_JSON('<fname>')
```