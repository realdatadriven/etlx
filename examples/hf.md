The [`httpfs`](https://duckdb.org/docs/extensions/httpfs/overview, "httpfs") extension introduces support for the hf:// protocol to access data sets hosted in [Hugging Face](https://huggingface.co "Hugging Face Homepage") repositories. See the [announcement blog post](https://duckdb.org/2024/05/29/access-150k-plus-datasets-from-hugging-face-with-duckdb.html, "announcement blog post") for details.
# ETL
```yaml metadata
name: HF_EXTRACT
description: "Example extrating from hf to a local sqlite3 file"
connection: "sqlite3:examples/HF_EXTRACT.db"
active: true
```
## HF_EXTRACT
```yaml metadata
name: HF_EXTRACT
description: "Example extrating from hf to a local sqlite3 file"
table: HF_EXTRACT
load_conn: "duckdb:"
load_before_sql:
  - load_extentions
  - attach_db
  - create_hf_token
load_sql: load_query
load_after_sql: detach_db
drop_sql: drop_sql
clean_sql: clean_sql
rows_sql: nrows
active: true
```
```sql
-- load_extentions
INSTALL httpfs;
LOAD httpfs;
```
```sql
-- attach_db
ATTACH 'examples/HF_EXTRACT.db' AS "DB" (TYPE SQLITE)
```
Configure your Hugging Face Token in the DuckDB Secrets Manager to access private or gated datasets. First, [visit Hugging Face Settings – Tokens](https://huggingface.co/settings/tokens) to obtain your access token. Second, set it in your DuckDB session using [DuckDB’s Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html). DuckDB supports two providers for managing secrets:
```sql
-- create_hf_token
CREATE SECRET hf_token (
   TYPE HUGGINGFACE,
   TOKEN '@HF_TOKEN'
);
```
```sql
-- detach_db
DETACH "DB";
```
```sql
-- load_query
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT *
FROM 'hf://datasets/datasets-examples/doc-formats-csv-1/data.csv'
LIMIT 10
```
```sql
-- load_query2
CREATE OR REPLACE TABLE "DB"."<table>" AS
SELECT *
FROM 'hf://datasets/horus-ai-labs/WebInstructSub-150K/data/train-00000-of-00001.parquet'
```
```sql
-- drop_sql
DROP TABLE IF EXISTS "DB"."<table>";
```
```sql
-- clean_sql
DELETE FROM "DB"."<table>";
```
```sql
-- nrows
SELECT COUNT(*) AS "nrows" FROM "DB"."<table>"
```
```shell
bin/etlx --config examples/hf.md
```