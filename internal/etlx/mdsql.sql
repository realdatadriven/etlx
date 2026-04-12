CREATE OR REPLACE TABLE markdown_sections AS 
WITH A AS (
    select row_number() over() as row, *
    from read_markdown_sections('examples/tmpl.sqlite.md', include_content := true, extract_metadata := true)
),
C AS (
    select *
        , md_extract_code_blocks(content) as code_blocks
        -- , typeof(code_blocks) as _type
        , len(code_blocks) _len
        , case
            when code_blocks[1].language in ('yaml', 'yml', 'json', 'toml') then code_blocks[1].language
            else null
        end as metadata_lang
        , case
            when code_blocks[1].language in ('yaml', 'yml', 'json', 'toml') then code_blocks[1].code
            else null
        end as metadata
        , yaml_extract(metadata::YAML, '$.name') as metadata_name
        , yaml_extract(metadata::YAML, '$.description') as metadata_description
        , yaml_extract(metadata::YAML, '$.source') as metadata_source
        , coalesce(yaml_extract(metadata::YAML, '$.runs_as'), case when level = 1 then /*metadata_name*/ null else null end) as runs_as
        , yaml_extract(metadata::YAML, '$.depends_on') as metadata_depends_on
        , yaml_type(metadata_depends_on) as _type_depends_on
        , yaml_exists(metadata::YAML, '$.depends_on') as _exists_depends_on
        --, yaml_array_length(metadata_depends_on::YAML) as metadata_depends_on_len
        --, yaml_array_elements(metadata_depends_on::YAML) as metadata_depends_on_elements
        --, yaml_extract(metadata_depends_on::YAML, '$.1') as metadata_depends_on_0
        , case
            when len(code_blocks) > 1 then code_blocks[2:]
            else null
        end as others
        , case 
            when level = 1 then row_number() over (partition by level order by row)
            else row_number() over (partition by level, parent_id order by row)
        end as level_order
        -- check with regex if content has [[query_name]] references to other sections. We can use this to build a dependency graph and lineage.
        -- extract only query_name from [[query_name]]
        , regexp_extract_all(content, '\\[\\[(\w+)\\]\\]') as refered_queries
    from A 
),
D AS (
    select C.*, UNNEST(C.others).language as language, UNNEST(C.others).code as code, UNNEST(C.others).info_string as info_string
    from C
),
Q AS (
    /*GET THE CONTENT FROM THE QUERY_DOC REFFERECED IN THE SECTION CONTENT*/
    WITH X AS (
        SELECT C.*, Parent.title as parent_title
        FROM C    
        LEFT OUTER JOIN C AS Parent ON Parent.section_id = C.parent_id
    )
    select X.section_id, X.refered_queries
        , ARRAY_AGG(Q.section_id) as refered_query_section_ids
        , GROUP_CONCAT(DISTINCT Q.content) as refered_query_content
    from X
    JOIN X AS Q ON
        CAST(X.refered_queries AS TEXT) LIKE CONCAT('%', Q.title, '%')
        OR CAST(X.refered_queries AS TEXT) LIKE CONCAT('%', Q.parent_title, '%')
    WHERE LEN(X.refered_queries) > 0
    GROUP BY X.section_id, X.refered_queries
)
select C.*/*, D.language, D.code, D.info_string
    , case
        when D.language = 'sql' and trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', '')) != ''
            then trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', ''))
        else trim(replace(D.info_string, D.language, ''))
    end AS "name"*/
    , parent.title as parent_title
    , parent.level as parent_level
    , parent.metadata_name as parent_metadata_name
    , parent.runs_as as parent_runs_as
    , Q.refered_query_section_ids
    , Q.refered_query_content
from C
LEFT OUTER JOIN C AS Parent ON Parent.section_id = C.parent_id
LEFT OUTER JOIN Q ON Q.section_id = C.section_id
/*LEFT OUTER JOIN D ON D.section_id = C.section_id*/
order by C.row;

CREATE OR REPLACE TABLE edges_est AS
SELECT DISTINCT A.row, A.section_id, A.parent_id, B.row as depends_on_row, B.section_id as depends_on_section_id, B.parent_id as depends_on_parent_id, A.parent_runs_as
FROM markdown_sections AS A
LEFT OUTER JOIN markdown_sections AS B ON
	INSTR(A.content::VARCHAR, B.parent_title::VARCHAR || '.' || B.title::VARCHAR) > 0
	OR INSTR(A.content::VARCHAR, B.parent_metadata_name::VARCHAR || '.' || B.metadata_name::VARCHAR) > 0
	OR INSTR(A.content::VARCHAR, B.metadata_name::VARCHAR) > 0
    OR INSTR(A.refered_query_content::VARCHAR, B.parent_title::VARCHAR || '.' || B.title::VARCHAR) > 0
    OR INSTR(A.refered_query_content::VARCHAR, B.parent_metadata_name::VARCHAR || '.' || B.metadata_name::VARCHAR) > 0
    OR INSTR(A.refered_query_content::VARCHAR, B.metadata_name::VARCHAR) > 0
OR INSTR(A.content::VARCHAR, B.metadata_name::VARCHAR) > 0
WHERE B.section_id IS NOT NULL
	AND B.section_id != A.section_id
	AND A.level = 2
	AND B.level = 2
	AND A.parent_runs_as IS NOT NULL
	AND B.parent_runs_as IS NOT NULL
ORDER BY A.row ASC;