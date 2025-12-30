+++
title = 'Notify'
weight = 51
draft = false
+++

### NOTIFY

The `NOTIFY` section enables sending notifications (e.g., email via SMTP) with dynamic templates populated from SQL query results. This is useful for monitoring ETL processes and sending status reports.

---

#### **Why Use NOTIFY?**

✅ **Real-time updates on ETL status**  
✅ **Customizable email templates with dynamic content**  
✅ **Supports attachments for automated reporting**  
✅ **Ensures visibility into ETL success or failure**  

#### **Example: Sending ETL Status via Email**

This example sends an email **after an ETL process completes**, using **log data from the database**.

#### **NOTIFY Markdown Configuration**

````markdown

# NOTIFY

```yaml metadata
name: Notification
description: "ETL Notification"
connection: "duckdb:"
path: "examples"
active: true
```

## ETL_STATUS

```yaml metadata
name: ETL_STATUS
description: "ETL Status"
connection: "duckdb:"
before_sql:
  - "INSTALL sqlite"
  - "LOAD sqlite"
  - "ATTACH 'database/HTTP_EXTRACT.db' AS DB (TYPE SQLITE)"
data_sql:
  - logs
after_sql: "DETACH DB"
to:
  - real.datadriven@gmail.com
cc: null
bcc: null
subject: 'ETLX YYYYMMDD'
body: body_tml
attachments:
  - hf.md
  - http.md
active: true
```

The **email body** is defined using a **Golang template**. The results from `data_sql` are available inside the template, also the Spring (`github.com/Masterminds/sprig`) library that provides more than 100 commonly used template functions.

```html body_tml
<style>
  table {
    border-collapse: collapse;
    width: 100%;
    font-family: sans-serif;
    font-size: 14px;
  }
  th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
  }
  th {
    background-color: #f2f2f2;
    font-weight: bold;
  }
  tr:nth-child(even) {
    background-color: #f9f9f9;
  }
  tr:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    background-color: #eef6ff;
  }
</style>
<b>Good Morning!</b><br /><br />
This email was gebnerated by ETLX automatically!<br />
LOGS:<br />
{{ with .logs }}
    {{ if eq .success true }}
      <table>
        <tr>
            <th>Name</th>
            <th>Ref</th>
            <th>Start</th>
            <th>End</th>
            <th>Duration</th>
            <th>Success</th>
            <th>Message</th>
        </tr>
        {{ range .data }}
        <tr>
            <td>{{ .name }}</td>
            <td>{{ .ref }}</td>
            <td>{{ .start_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ .end_at | date "2006-01-02 15:04:05" }}</td>
            <td>{{ divf .duration 1000000000 | printf "%.4fs" }}</td>
            <td>{{ .success }}</td>
            <td><span title="{{ .msg }}">{{ .msg | toString | abbrev 30}}</span></td>
        </tr>
        {{ else }}
        <tr>
          <td colspan="7">No items available</td>
        </tr>
        {{ end }}
      </table>
    {{ else }}
      <p>{{.msg}}</p>
    {{ end }}
{{ else }}
<p>Logs information missing.</p>
{{ end }}
```

```sql
-- logs
SELECT *
FROM "DB"."etlx_logs"
WHERE "ref" = '{YYYY-MM-DD}'
```

---

#### **How NOTIFY Works**

1️⃣ **Loads required extensions and connects to the database** (`before_sql`).  
2️⃣ **Executes `data_sql` queries** to retrieve data to be embeded in the body of the email.  
3️⃣ **Uses the results inside the `body` template** (Golang templating).  
4️⃣ **Sends an email with the formatted content and attachments.**  
5️⃣ **Executes cleanup queries (`after_sql`).**  

#### **Key NOTIFY Features**

✔ **Dynamic email content populated from SQL queries**  
✔ **Supports `to`, `cc`, `bcc`, `attachments`, and templated bodies**  
✔ **Executes SQL before and after sending notifications**  
✔ **Ensures ETL monitoring and alerting**