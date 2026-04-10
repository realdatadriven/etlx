package etlxlib

import (
	"fmt"
	"strconv"
	"strings"
)

// GenerateMermaidFlowchart generates an enhanced Mermaid flowchart with animations and click callbacks
func (etlx *ETLX) GenerateMermaidFlowchart(nodes []map[string]any, edges []map[string]any) string {
	var mmd strings.Builder

	mmd.WriteString(`---
config:
  look: handDrawn
  theme: neutral
---
flowchart LR
`)

	// Collect unique parent_ids
	parents := make(map[any]bool)
	for _, row := range nodes {
		if pid, ok := row["parent_id"]; ok && pid != nil {
			parents[pid] = true
		}
	}

	parentList := make([]any, 0, len(parents))
	for p := range parents {
		parentList = append(parentList, p)
	}

	mmd.WriteString("\t%% NODES\n")

	added := make(map[string]bool)

	for _, parent := range parentList {
		children := filterByParentID(nodes, parent)
		if len(children) == 0 {
			continue
		}

		first := children[0]
		parentTitle, _ := getString_(first, "parent_title")
		parentRunsAs, _ := getString_(first, "parent_runs_as")
		parentDescription, _ := getString_(first, "parent_description")
		parentName, _ := getString_(first, "parent_name")

		mmd.WriteString(fmt.Sprintf("\t%% %s %s\n", parentName, parentDescription))
		mmd.WriteString(fmt.Sprintf("\tsubgraph %v[\"%s (%s)\"]\n", parent, parentTitle, parentRunsAs))

		for _, child := range children {
			sectionID := child["section_id"]
			key := fmt.Sprintf("%v_%v", parent, sectionID)

			if !added[key] {
				name, _ := getString_(child, "name")
				description, _ := getString_(child, "description")
				title, _ := getString_(child, "title")

				mmd.WriteString(fmt.Sprintf("\t\t%% %s - %s\n", name, description))
				mmd.WriteString(fmt.Sprintf("\t\t%s[\"%s\"]\n", key, title))
				added[key] = true
			}
		}

		mmd.WriteString("\tend\n\n")
	}

	// === EDGES with animations, click callbacks and success styling ===
	mmd.WriteString("\t%% EDGES\n")
	for i, row := range edges {
		toKey := fmt.Sprintf("%v_%v",
			row["parent_id"],
			row["section_id"])

		fromKey := fmt.Sprintf("%v_%v",
			row["depends_on_parent_id"],
			row["depends_on_section_id"])

		// Animated edge:   from  q0@-->  to
		mmd.WriteString(fmt.Sprintf("\t%s q%d@--> %s\n", fromKey, i, toKey))

		// Animation config
		mmd.WriteString(fmt.Sprintf("\tq%d@{ animate: true }\n", i))

		// Click callback with all four IDs
		callbackStr := fmt.Sprintf("%v|%v|%v|%v",
			getAny_(row, "parent_id"),
			getAny_(row, "section_id"),
			getAny_(row, "depends_on_parent_id"),
			getAny_(row, "depends_on_section_id"))

		mmd.WriteString(fmt.Sprintf("\tclick %s callback \"%s\"\n", fromKey, callbackStr))

		// Success styling on the target node
		if success, ok := getBool_(row, "success"); ok {
			if success {
				mmd.WriteString(fmt.Sprintf("\tstyle %s fill:#0f0,stroke:#333,stroke-width:2px\n", toKey))
			} else {
				mmd.WriteString(fmt.Sprintf("\tstyle %s fill:#f00,stroke:#333,stroke-width:2px\n", toKey))
			}
		}
	}

	return mmd.String()
}

// ==================== Helper functions ====================

func filterByParentID(nodes []map[string]any, parentID any) []map[string]any {
	var result []map[string]any
	for _, row := range nodes {
		if row["parent_id"] == parentID {
			result = append(result, row)
		}
	}
	return result
}

func getString_(m map[string]any, key string) (string, bool) {
	if v, ok := m[key]; ok && v != nil {
		return fmt.Sprintf("%v", v), true
	}
	return "", false
}

// getAny_ returns the value as string (useful for callback)
func getAny_(m map[string]any, key string) string {
	if v, ok := m[key]; ok && v != nil {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// getBool_ safely extracts boolean value
func getBool_(m map[string]any, key string) (bool, bool) {
	if v, ok := m[key]; ok && v != nil {
		switch val := v.(type) {
		case bool:
			return val, true
		case int, int64, float64:
			// Treat non-zero as true (in case it's coming from JSON)
			return val != 0, true
		case string:
			b, _ := strconv.ParseBool(val)
			return b, true
		}
	}
	return false, false
}

// run query using INSTALL markdown FROM community on the md config
func (etlx *ETLX) QueryETLXMD(md string) (map[string]any, error) {
	_conf := etlx.md
	if md != "" {
		_conf = md
	}
	fname, err := etlx.TempFIle("", _conf, "config.*.md")
	if err != nil {
		return nil, err
	}
	fmt.Println(fname)
	// get duckdb conn
	conn, err := etlx.GetDB("duckdb:")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// install markdown and yaml from community
	_, err = conn.ExecuteQuery("INSTALL markdown FROM community")
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecuteQuery("INSTALL yaml FROM community")
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecuteQuery("LOAD markdown;LOAD yaml")
	if err != nil {
		return nil, err
	}
	query := `CREATE OR REPLACE TABLE markdown_sections AS 
WITH A AS (
    select row_number() over() as row, *
    from read_markdown_sections('<filename>', include_content := true, extract_metadata := true)
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
order by C.row;`
	query = etlx.ReplaceFileTablePlaceholder("file", query, fname)
	_, err = conn.ExecuteQuery(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `SELECT * FROM markdown_sections`
	md_data, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `WITH D AS (
    select C.*, UNNEST(C.others).language as language, UNNEST(C.others).code as code, UNNEST(C.others).info_string as info_string
    from markdown_sections
)
SELECT C.* 
	, D.language, D.code, D.info_string
    , case
        when D.language = 'sql' and trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', '')) != ''
            then trim(replace(regexp_extract(D.code, '(?m)^--\\s*([A-Za-z0-9_]+)', 0), '--', ''))
        else trim(replace(D.info_string, D.language, ''))
    end AS "name"
FROM markdown_sections C
LEFT OUTER JOIN D ON D.section_id = C.section_id
ORDER BY C.row;`
	md_data_expanded, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `CREATE OR REPLACE TABLE edges AS
SELECT A.*, A.section_id, A.parent_id, B.row as depends_on_row, B.section_id as depends_on_section_id, B.parent_id as depends_on_parent_id, A.parent_runs_as
FROM markdown_sections AS A
LEFT OUTER JOIN markdown_sections AS B ON 
    A.metadata_depends_on::VARCHAR LIKE ('%' || B.parent_title::VARCHAR || '.' || B.title::VARCHAR || '%') 
    OR INSTR(A.metadata_depends_on::VARCHAR, B.parent_title::VARCHAR || '.' || B.title::VARCHAR) > 0
    OR A.metadata_depends_on::VARCHAR LIKE ('%' || B.parent_metadata_name::VARCHAR || '.' || B.metadata_name::VARCHAR || '%')
    OR INSTR(A.metadata_depends_on::VARCHAR, B.parent_metadata_name::VARCHAR || '.' || B.metadata_name::VARCHAR) > 0
WHERE B.section_id IS NOT NULL`
	_, err = conn.ExecuteQuery(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `WITH NODES AS (
    SELECT DISTINCT row, section_id, parent_id 
    FROM edges
    UNION
    SELECT DISTINCT depends_on_row AS row, depends_on_section_id AS section_id, depends_on_parent_id AS parent_id 
    FROM edges
)
SELECT DISTINCT N.row, N.section_id, N.parent_id, D.title, D.parent_runs_as, P.title as parent_title
    , D.metadata_name AS name, D.metadata_description AS description, D.metadata_source AS source
    , P.metadata_name AS parent_name, P.metadata_description AS parent_description, P.metadata_source AS parent_source
FROM NODES N
JOIN markdown_sections D ON N.section_id = D.section_id
JOIN markdown_sections P ON N.parent_id = P.section_id
ORDER BY N.row ASC;`
	nodes, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `SELECT * FROM edges`
	edges, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = ` CREATE OR REPLACE TABLE edges_est AS
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
ORDER BY A.row ASC`
	_, err = conn.ExecuteQuery(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `SELECT * FROM edges_est`
	edges_est, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	query = `WITH NODES AS (
    SELECT DISTINCT row, section_id, parent_id 
    FROM edges_est
    UNION
    SELECT DISTINCT depends_on_row AS row, depends_on_section_id AS section_id, depends_on_parent_id AS parent_id 
    FROM edges_est
)
SELECT DISTINCT N.row, N.section_id, N.parent_id, D.title, D.parent_runs_as, P.title as parent_title
    , D.metadata_name AS name, D.metadata_description AS description, D.metadata_source AS source
    , P.metadata_name AS parent_name, P.metadata_description AS parent_description, P.metadata_source AS parent_source
FROM NODES N
JOIN markdown_sections D ON N.section_id = D.section_id
JOIN markdown_sections P ON N.parent_id = P.section_id
ORDER BY N.row ASC;`
	nodes_est, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"md_data":          *md_data,
		"md_data_expanded": *md_data_expanded,
		"nodes":            *nodes,
		"edges":            *edges,
		"nodes_est":        *nodes_est,
		"edges_est":        *edges_est,
	}, nil
}
