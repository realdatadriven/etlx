# parse ETLX config using duckdb Markdown extention

import re
import json
import yaml
import toml
from datetime import datetime
import duckdb
import tempfile

AUTO_LOGS_BLOCK = """# AUTO_LOGS

```yaml metadata
name: LOGS
description: "Logging"
table: logs
connection: "duckdb:"
before_sql:
  - "LOAD Sqlite"
  - "ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE)"
  - "USE etlx_logs"
  - "LOAD json"
  - "get_dyn_queries[create_missing_columns](ATTACH '<tmp>/etlx_logs.db' (TYPE SQLITE),DETACH etlx_logs)"
save_log_sql: |
  INSERT INTO "etlx_logs"."<table>" BY NAME
  SELECT *
  FROM READ_JSON('<fname>');
save_on_err_patt: '(?i)table.+with.+name.+(\\w+).+does.+not.+exist'
save_on_err_sql: |
  CREATE TABLE "etlx_logs"."<table>" AS
  SELECT *
  FROM READ_JSON('<fname>');
after_sql:
  - 'USE memory'
  - 'DETACH "etlx_logs"'
active: true
```

```sql
-- create_missing_columns
WITH source_columns AS (
    SELECT "column_name", "column_type"
    FROM (DESCRIBE SELECT * FROM READ_JSON('<fname>'))
),
destination_columns AS (
    SELECT "column_name", "data_type" as "column_type"
    FROM "duckdb_columns"
    WHERE "table_name" = '<table>'
),
missing_columns AS (
    SELECT "s"."column_name", "s"."column_type"
    FROM source_columns "s"
    LEFT JOIN destination_columns "d"
        ON "s"."column_name" = "d"."column_name"
    WHERE "d"."column_name" IS NULL
)
SELECT 'ALTER TABLE "etlx_logs"."<table>" ADD COLUMN "' || "column_name" || '" ' || "column_type" || ';' AS "query"
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
```
"""

def add_auto_logs(md: str) -> str:
    if "# AUTO_LOGS" in md:
        return md
    return md + AUTO_LOGS_BLOCK

def ipynb_to_markdown(data_bytes: bytes) -> str:
    nb = json.loads(data_bytes)
    parts = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "markdown":
            parts.append("".join(cell.get("source", [])))
        elif cell.get("cell_type") == "code":
            src = "".join(cell.get("source", []))
            parts.append(f"```python\n{src}\n```")
    return "\n\n".join(parts)

class MarkdownConfigParser:
    def __init__(self):
        self.conn = duckdb.connect()
        self.path = './config.md'
        self.conn.execute("INSTALL markdown FROM community;")
        self.conn.execute("LOAD markdown;")
    def parse(self, path: str):
        self.path = path
        self.parse_etlx_md_2_ddb()
        _cnf_level1 = self.conn.sql("""SELECT row, title, metadata_lang, metadata FROM ETLX WHERE level = 1""").pl()
        _config = {'__order': []}
        for l1_row in _cnf_level1.iter_rows(named=True):
            _config['__order'].append(l1_row['title'])
            _config[l1_row['title']] = {'__order': []}
            if l1_row['metadata_lang'] in ['yaml', 'yml'] and 'metadata' in l1_row:
                try:
                    _config[l1_row['title']]['metadata'] = yaml.safe_load(l1_row['metadata'])
                except Exception as e:
                    print(l1_row['title'], 'metadata', l1_row['metadata_lang'], str(e))
                    break
            elif l1_row['metadata_lang'] == 'json' and 'metadata' in l1_row:
                try:
                    _config[l1_row['title']]['metadata'] = json.loads(l1_row['metadata'])
                except Exception as e:
                    print(l1_row['title'], 'metadata', l1_row['metadata_lang'], str(e))
                    break
            elif l1_row['metadata_lang'] == 'toml' and 'metadata' in l1_row:
                try:
                    _config[l1_row['title']]['metadata'] = json.loads(l1_row['metadata'])
                except Exception as e:
                    print(l1_row['title'], 'metadata', l1_row['metadata_lang'], str(e))
                    break
            _cnf_level2 = self.conn.sql(f"""SELECT row, title, metadata_lang, metadata FROM ETLX WHERE LEVEL = 2 AND parent = {l1_row['row']}""").pl()
            for l2_row in _cnf_level2.iter_rows(named=True):
                _config[l1_row['title']]['__order'].append(l2_row['title'])
                _config[l1_row['title']][l2_row['title']] = {}#{'__order': []}
                if l2_row['metadata_lang'] in ['yaml', 'yml'] and 'metadata' in l2_row:
                    try:
                        _config[l1_row['title']][l2_row['title']]['metadata'] = yaml.safe_load(l2_row['metadata'])
                    except Exception as e:
                        print(l1_row['title'], l2_row['title'], 'metadata', l2_row['metadata_lang'], str(e))
                        break
                elif l2_row['metadata_lang'] in 'json' and 'metadata' in l2_row:
                    try:
                        _config[l1_row['title']][l2_row['title']]['metadata'] = toml.loads(l2_row['metadata'])
                    except Exception as e:
                        print(l1_row['title'], l2_row['title'], 'metadata', l2_row['metadata_lang'], str(e))
                        break
                _sql = f"""SELECT row, title, language, code, name FROM ETLX_L2_CODES WHERE LEVEL = 2 AND row = {l2_row['row']} AND language IS NOT NULL AND code IS NOT NULL AND name IS NOT NULL"""
                _cnf_level2_codes = self.conn.sql(_sql).pl()
                for l2_row_code in _cnf_level2_codes.iter_rows(named=True):
                    _config[l1_row['title']][l2_row['title']][l2_row_code['name']] = l2_row_code['code']
        return _config
    def parse_etlx_md_2_ddb(self):
        sql = f"""WITH A AS (
    SELECT row_number() over() as row, title, level, content 
    FROM read_markdown_sections('{self.path}', include_content := true)
),
B AS (
    SELECT A.*
        , CASE 
            WHEN level = 2 
                THEN MAX(row)
                    FILTER (level = 1)
                    OVER (
                        ORDER BY row ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) 
            WHEN level = 3
                THEN MAX(row)
                    FILTER (level = 2)
                    OVER (
                        ORDER BY row ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ) 
            ELSE NULL
        END AS parent
    FROM A
),
C AS (
    SELECT *
        , md_extract_code_blocks(content) as code_blocks
        --, typeof(code_blocks) as _type
        , len(code_blocks) _len
        , case
            when code_blocks[1].language in ('yaml', 'yml', 'json', 'toml') then code_blocks[1].language
            else null
        end as metadata_lang
        , case
            when code_blocks[1].language in ('yaml', 'yml', 'json', 'toml') then code_blocks[1].code
            else null
        end as metadata
        , case
            when len(code_blocks) > 1 then code_blocks[2:]
            else null
        end as others
        , case 
            when level = 1 then row_number() over (partition by level order by row)
            else row_number() over (partition by level, parent order by row)
        end as level_order
    FROM B
),
D AS (
    SELECT C.*, UNNEST(C.others).language as language, UNNEST(C.others).code as code, UNNEST(C.others).info_string as info_string
    FROM C
)
SELECT C.*/*, D.language, D.code, D.info_string
    , case
        when D.language = 'sql' and trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', '')) != ''
            then trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', ''))
        else trim(replace(D.info_string, D.language, ''))
    end AS "name"*/
FROM C
/*LEFT OUTER JOIN D ON D.row = C.ROW*/
ORDER BY C.row;"""
        self.conn.execute(f"""CREATE OR REPLACE TABLE "ETLX" AS {sql}""")
        sql = f"""WITH C AS (FROM "ETLX" ),
D AS (
    SELECT C.*, UNNEST(C.others).language as language, UNNEST(C.others).code as code, UNNEST(C.others).info_string as info_string
    FROM C
)
SELECT C.*, D.language
    , case
        when D.language = 'sql' and trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', '')) != ''
            then replace(D.code, regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)\\n', 0), '')
        else D.code
    end AS code
    , D.info_string
    , case
        when D.language = 'sql' and trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', '')) != ''
            then trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)?', 0), '--', ''))
        else trim(replace(D.info_string, D.language, ''))
    end AS "name"
FROM C
LEFT OUTER JOIN D ON D.row = C.row
ORDER BY C.row;"""
        self.conn.execute(f"""CREATE OR REPLACE TABLE "ETLX_L2_CODES" AS {sql}""")
 
class ETLX:
    """
    Parse configuration from markdown text string.
    
    Automatically adds logging directives to the markdown if auto_logs are enabled.
    Writes the markdown content to a temporary file and parses it using the 
    MarkdownConfigParser.
    
    Args:
        md (str): Markdown text containing configuration to parse.
    
    Returns:
        ETLX: Returns self for method chaining.
    
    Note:
        The temporary file is automatically deleted after parsing.
        If auto_logs_disabled is False, add_auto_logs() is applied to the markdown.
    """
    def __init__(self, auto_logs_disabled=False):
        self.Config = {}
        self.auto_logs_disabled = auto_logs_disabled
        self.parser = MarkdownConfigParser()

    def config_from_file(self, path: str):
        if path.endswith(".ipynb"):
            data = ''
            with open(path, mode='r') as f:
                data = f.read()
                f.close()
            md = ipynb_to_markdown(data)
            path = f'{path}.md'
            if not self.auto_logs_disabled:
                md = add_auto_logs(md)
            with open(path, mode='w', encoding='utf-8') as f:
                f.write(md)
                f.close()
        self.Config = self.parser.parse(path)
        return self

    def config_from_md_text(self, md: str):
        if not self.auto_logs_disabled:
            md = add_auto_logs(md)
        path = f'{path}.md'
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as f:
            f.write(md)
            f.close()
        self.Config = self.parser.parse(path)
        return self

    def print_json(self):
        print(json.dumps(self.Config, indent=2))
