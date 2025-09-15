package etlxlib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"gopkg.in/yaml.v3"
)

type ETLX struct {
	Config map[string]any
}

func addAutoLoggs(md string) string {
	if !strings.Contains(md, "# AUTO_LOGS") {
		_auto_logs := fmt.Sprintf(`
# AUTO_LOGS

%syaml metadata
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
save_on_err_patt: '(?i)table.+with.+name.+(\w+).+does.+not.+exist'
save_on_err_sql: |
  CREATE TABLE "etlx_logs"."<table>" AS
  SELECT *
  FROM READ_JSON('<fname>');
after_sql:
  - 'USE memory'
  - 'DETACH "etlx_logs"'
active: true
%s

%ssql
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
    LEFT JOIN destination_columns "d" ON "s"."column_name" = "d"."column_name"
    WHERE "d"."column_name" IS NULL
)
SELECT 'ALTER TABLE "etlx_logs"."<table>" ADD COLUMN "' || "column_name" || '" ' || "column_type" || ';' AS "query"
FROM missing_columns
WHERE (SELECT COUNT(*) FROM destination_columns) > 0;
%s
`, "```", "```", "```", "```")
		//fmt.Println(_auto_logs)
		return md + _auto_logs
	}
	return md
}

func (etlx *ETLX) ConfigFromFile(filePath string) error {
	// Read the file content
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	mdText := ""
	if strings.HasSuffix(filePath, ".ipynb") {
		mdText, err = etlx.ConvertIPYNBToMarkdown(data)
		if err != nil {
			return fmt.Errorf("failed convert the Notebook to MDText: %w", err)
		}
		// fmt.Println(mdText)
		data = []byte(addAutoLoggs(mdText))
	}
	// Parse the Markdown content into an AST
	reader := text.NewReader([]byte(addAutoLoggs(string(data))))
	return etlx.ParseMarkdownToConfig(reader, mdText)
}

func (etlx *ETLX) ConfigFromIpynbJSON(ipynbJSON string) error {
	// Parse the Markdown from IPYNB JSON content into an AST
	mdText, err := etlx.ConvertIPYNBToMarkdown([]byte(ipynbJSON))
	if err != nil {
		return fmt.Errorf("failed convert the Notebook JSON content to MDText: %w", err)
	}
	reader := text.NewReader([]byte(addAutoLoggs(mdText)))
	return etlx.ParseMarkdownToConfig(reader, mdText)
}

func (etlx *ETLX) ConfigFromMDText(mdText string) error {
	// Parse the Markdown content into an AST
	reader := text.NewReader([]byte(mdText))
	return etlx.ParseMarkdownToConfig(reader, mdText)
}

// TracebackHeaders traces headers from the current node up to the top-level header.
func (etlx *ETLX) TracebackHeaders(node ast.Node, source []byte) []string {
	var headers []string
	// Traverse up the AST tree
	for current := node; current != nil; current = current.Parent() {
		// Check if the node is a Heading
		if heading, ok := current.(*ast.Heading); ok {
			var headerText bytes.Buffer
			// Collect the text content of the heading
			for child := heading.FirstChild(); child != nil; child = child.NextSibling() {
				if textNode, ok := child.(*ast.Text); ok {
					headerText.Write(textNode.Segment.Value(source))
				}
			}
			// Add the header text to the list
			headers = append([]string{headerText.String()}, headers...)
		}
	}
	return headers
}

// ParseMarkdownToConfig parses a Markdown file into a structured nested map
func (etlx *ETLX) ParseMarkdownToConfig(reader text.Reader, content string) error {
	// Initialize the Markdown parser
	parser := goldmark.DefaultParser()
	root := parser.Parse(reader) // Initialize the result map and a levels map
	config := make(map[string]any)
	config["__order"] = []string{}
	_aux := NewDuckLakeParser().FindDuckLakeStrings(string(content))
	if len(_aux) > 0 {
		config["__lakes"] = []string{}
		for _, s := range _aux {
			if !etlx.Contains(config["__lakes"].([]string), s) {
				config["__lakes"] = append(config["__lakes"].([]string), s)
			}
		}
	}
	levels := make(map[int]map[string]any) // Track the current section for each heading level
	//order := make(map[string][]string)          // Track the order of keys for each top-level section
	// Walk through the AST
	err := ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			switch n := node.(type) {
			case *ast.Heading:
				// Extract the heading text
				var headingText strings.Builder
				for child := n.FirstChild(); child != nil; child = child.NextSibling() {
					if textNode, ok := child.(*ast.Text); ok {
						headingText.WriteString(string(textNode.Value(reader.Source())))
					}
				}
				heading := headingText.String()
				// Handle level 1 headings
				if n.Level == 1 {
					// Level 1 Heading order
					config["__order"] = append(config["__order"].([]string), heading)
					// Reset the context for a new level 1 heading
					if _, exists := config[heading]; !exists {
						config[heading] = make(map[string]any)
					}
					// Add an __order key to track child key order
					if _, exists := config[heading].(map[string]any)["__order"]; !exists {
						config[heading].(map[string]any)["__order"] = []string{}
					}
					levels = map[int]map[string]any{1: config[heading].(map[string]any)}
					//order[heading] = []string{} // Initialize order tracking for this top-level section
				} else {
					// Handle deeper levels (level 2 and beyond)
					parent := levels[n.Level-1]
					if parent == nil {
						return ast.WalkContinue, fmt.Errorf("missing parent section for level %d heading: %s", n.Level, heading)
					}
					// Add to parent's __order slice
					if _, ok := parent["__order"]; ok {
						order := parent["__order"].([]string)
						parent["__order"] = append(order, heading)
					}
					if _, exists := parent[heading]; !exists {
						parent[heading] = make(map[string]any)
					}
					levels[n.Level] = parent[heading].(map[string]any)
				}
				// Clear deeper levels to avoid cross-contamination
				for level := n.Level + 1; level < len(levels); level++ {
					levels[level] = nil
				}
			case *ast.FencedCodeBlock:
				// Extract info and content from the code block
				info := string(n.Info.Segment.Value(reader.Source()))
				content := string(n.Text(reader.Source()))
				// Add to the current section
				current := levels[len(levels)]
				if current != nil {
					if strings.HasPrefix(info, "yaml") || strings.HasPrefix(info, "toml") || strings.HasPrefix(info, "json") {
						// Process YAML or TOML blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
						if strings.HasPrefix(info, "toml") {
							key = strings.TrimSpace(strings.TrimPrefix(info, "toml"))
						}
						if strings.HasPrefix(info, "json") {
							key = strings.TrimSpace(strings.TrimPrefix(info, "json"))
						}
						contentFinal := content
						if key == "" {
							// If no key in the info, try to extract from the first comment line
							key, contentFinal = extracNameFromYamlToml(content)
							// fmt.Println("NOT A NAMED QUERY, FIND # name instead", key, contentFinal)
						}
						if key == "" {
							key = "metadata"
						}
						metaData := make(map[string]any)
						var err error
						if strings.HasPrefix(info, "yaml") {
							// Parse YAML
							err = yaml.Unmarshal([]byte(contentFinal), &metaData)
						} else if strings.HasPrefix(info, "toml") {
							// Parse TOML
							_, err = toml.Decode(contentFinal, &metaData)
						} else if strings.HasPrefix(info, "json") {
							// Parse JSON
							err = json.Unmarshal([]byte(contentFinal), &metaData)
						}
						if err != nil {
							return ast.WalkContinue, fmt.Errorf("error parsing %s block %s: %v, %s", info, key, err, contentFinal)
						}
						current[key] = metaData
						/*/ Add to the current section's __order
						if _, ok := current["__order"]; ok {
							order := current["__order"].([]string)
							current["__order"] = append(order, key)
						}*/
					} else if strings.HasPrefix(info, "sql") {
						// Process SQL blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
						contentFinal := content
						if key == "" {
							// If no key in the info, try to extract from the first comment line
							key, contentFinal = extractQueryNameFromSQL(content)
							// fmt.Println("NOT A NAMED QUERY, FIND -- name instead", key, contentFinal)
						}
						if key == "" {
							fmt.Printf("missing query name for SQL block: %s", content)
						} else {
							current[key] = contentFinal
							/*/ Add to the current section's __order
							if _, ok := current["__order"]; ok {
								order := current["__order"].([]string)
								current["__order"] = append(order, key)
							}*/
						}
					} else if strings.HasPrefix(info, "html") {
						key := strings.TrimSpace(strings.TrimPrefix(info, "html"))
						if key == "" {
							fmt.Printf("missing query name for SQL block: %s", content)
						} else {
							current[key] = content
						}
					} else if strings.HasPrefix(info, "python") {
						key := strings.TrimSpace(strings.TrimPrefix(info, "python"))
						if key == "" {
							fmt.Printf("missing query name for python block: %s", content)
						} else {
							current[key] = content
						}
					} else if strings.HasPrefix(info, "py") {
						key := strings.TrimSpace(strings.TrimPrefix(info, "py"))
						if key == "" {
							fmt.Printf("missing query name for python block: %s", content)
						} else {
							current[key] = content
						}
					}
				}
			}
		}
		return ast.WalkContinue, nil
	})
	if err != nil {
		return err
	}
	// due to toml resulting parsed map been more deeply
	// (going as far as []map[string]any instead of just []any relativally to json and yaml)
	// it was decided to put all to json string an back to go map to guarantee  consistency
	// otherwise schema definition would be needed and that would
	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("err converting to json: %s", err)
	}
	err = json.Unmarshal([]byte(jsonData), &config)
	if err != nil {
		return fmt.Errorf("err converting from json: %s", err)
	}
	etlx.Config = config
	return nil
}

// PrintConfigAsJSON prints the configuration map in JSON format
func (etlx *ETLX) PrintConfigAsJSON(config map[string]any) {
	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("Error converting config to JSON: %v", err)
	}
	if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
		_file, err := etlx.TempFIle("", string(jsonData), "config.*.json")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(_file)
	}
	fmt.Println(string(jsonData))
}

// and removes the matched line along with any leading newline in the remaining content.
func extracNameFromYamlToml(content string) (string, string) {
	// Define a regex to match the first line with a comment containing the name
	re := regexp.MustCompile(`(?m)^#\s*(\w+)\s*$`)
	// Find the name using the regex
	matches := re.FindStringSubmatch(content)
	name := ""
	if len(matches) > 1 {
		name = matches[1] // Capture the name
	}
	// Remove the matched line from the content
	updatedContent := re.ReplaceAllString(content, "")
	// Remove any leading newline or whitespace from the updated content
	updatedContent = strings.TrimLeft(updatedContent, "\n")
	return name, updatedContent
}

// and removes the matched line along with any leading newline in the remaining content.
func extractQueryNameFromSQL(sqlContent string) (string, string) {
	// Define a regex to match the first line with a comment containing the query name
	re := regexp.MustCompile(`(?m)^--\s*(\w+)\s*$`)
	// Find the query name using the regex
	matches := re.FindStringSubmatch(sqlContent)
	queryName := ""
	if len(matches) > 1 {
		queryName = matches[1] // Capture the query name
	}
	// Remove the matched line from the content
	updatedContent := re.ReplaceAllString(sqlContent, "")
	// Remove any leading newline or whitespace from the updated content
	updatedContent = strings.TrimLeft(updatedContent, "\n")
	return queryName, updatedContent
}

// Walk recursively traverses a nested map and processes each key and value.
func (etlx *ETLX) Walk(data map[string]any, path string, fn func(keyPath string, value any)) {
	for key, value := range data {
		// Construct the current path (e.g., "parent.child")
		currentPath := key
		if path != "" {
			currentPath = path + "." + key
		}
		// Call the processing function with the current key path and value
		fn(currentPath, value)
		// If the value is a map, recursively walk through it
		if nestedMap, ok := value.(map[string]any); ok {
			etlx.Walk(nestedMap, currentPath, fn)
		}
	}
}

func (etlx *ETLX) GetRefFromString(file string) time.Time {
	basename := file
	fileRefPats := []struct {
		patt *regexp.Regexp
		fmrt string
	}{
		{patt: regexp.MustCompile(`\d{8}`), fmrt: "20060102"}, // (\d{8})(?!.*\d+)
		{patt: regexp.MustCompile(`\d{6}`), fmrt: "200601"},   // (\d{6})(?!.*\d+)
		{patt: regexp.MustCompile(`\d{4}`), fmrt: "0601"},     // (\d{4})(?!.*\d+)
	}
	// This will hold the final file_ref value
	var fileRef time.Time
	// Loop through the patterns and try to match
	for _, patt := range fileRefPats {
		// Find all matches for the current pattern
		matches := patt.patt.FindAllString(basename, -1)
		if len(matches) > 0 {
			// If a match is found, attempt to parse it into a date
			matchStr := matches[0]
			dt, err := time.Parse(patt.fmrt, matchStr)
			if err != nil {
				// Handle parse error
				fmt.Println("Error parsing date:", err)
				break
			}
			if patt.fmrt == "200601" || patt.fmrt == "0601" {
				// Calculate the last day of the month for the parsed date
				year, month := dt.Year(), dt.Month()
				// Find the last day of the month
				lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
				// Create a new date with the last day of the month
				fileRef = time.Date(year, month, lastDay, 0, 0, 0, 0, time.UTC)
			} else {
				fileRef = dt
			}
			// Break the loop once a match is found and processed
			break
		}
	}
	return fileRef
}

func (etlx *ETLX) ReplaceFileTablePlaceholder(key string, sql string, file_table string) string {
	pats := map[string]*regexp.Regexp{
		"file":  regexp.MustCompile(`<file>|<filename>|<fname>|<file_name>|<path>|<filepath>|<file_path>|{file}|{filename}|{fname}|{file_name}|{path}|{filepath}|{file_path}`), // (?i)
		"table": regexp.MustCompile(`<table>|<table_name>|<tablename>|{table}|{table_name}|{tablename}`),
		"tmp":   regexp.MustCompile(`<tmp_path>|<tmp>|{tmp_path}|{tmp}`), // (?i)
	}
	re := pats[key]
	return re.ReplaceAllString(sql, file_table)
}

func (etlx *ETLX) GetGODateFormat(format string) string {
	goFmrt := format
	formats := []struct {
		frmt   string
		goFmrt string
	}{
		{`YYYY|AAAA`, "2006"},
		{`YY|AA`, "06"},
		{`MM`, "01"},
		{`DD`, "02"},
		{`HH`, "15"},
		{`mm`, "04"},
		{`SS`, "05"},
		{`TSTAMP|STAMP`, "20060102150405"},
	}
	for _, f := range formats {
		re := regexp.MustCompile(f.frmt)
		goFmrt = re.ReplaceAllString(goFmrt, f.goFmrt)
	}
	return goFmrt
}

// setQueryDate formats the query string by inserting the given date reference in place of placeholders
func (etlx *ETLX) ReplaceQueryStringDate(query string, dateRef interface{}) string {
	patt := regexp.MustCompile(`(["]?\w+["]?\.\w+\s?=\s?'\{.*?\}'|["]?\w+["]?\s?=\s?'\{.*?\}')`)
	matches := patt.FindAllString(query, -1)
	if len(matches) == 0 {
		patt = regexp.MustCompile(`["]?\w+["]?\s?=\s?'\{.*?\}'`)
		matches = patt.FindAllString(query, -1)
	}
	if len(matches) > 0 {
		patt2 := regexp.MustCompile(`'\{.*?\}'`)
		for _, m := range matches {
			format := patt2.FindString(m)
			if format != "" {
				frmtFinal := etlx.GetGODateFormat(format)
				//fmt.Println(frmtFinal, format)
				if frmtFinal == format /*&& len(frmtFinal) > 30*/ {
					//fmt.Println("NOT A DATE FORMAT", format)
					continue
				}
				frmtFinal = strings.ReplaceAll(frmtFinal, "{", "")
				frmtFinal = strings.ReplaceAll(frmtFinal, "}", "")
				var procc string
				if dates, ok := dateRef.([]time.Time); ok {
					dts := []string{}
					for _, dt := range dates {
						dts = append(dts, dt.Format(frmtFinal))
					}
					procc = regexp.MustCompile(patt2.String()).ReplaceAllString(m, fmt.Sprintf("(%s)", strings.Join(dts, ",")))
					patt3 := regexp.MustCompile(`\s?=\s?`)
					procc = patt3.ReplaceAllString(procc, " IN ")
				} else if dt, ok := dateRef.(time.Time); ok {
					procc = regexp.MustCompile(patt2.String()).ReplaceAllString(m, dt.Format(frmtFinal))
				}
				patt = regexp.MustCompile(regexp.QuoteMeta(m))
				query = patt.ReplaceAllString(query, procc)
			}
		}
	}
	// Replace remaining date placeholders
	patt = regexp.MustCompile(`'?\{.*?\}'?`)
	matches = patt.FindAllString(query, -1)
	if len(matches) > 0 {
		for _, m := range matches {
			frmtFinal := etlx.GetGODateFormat(m)
			//fmt.Println(2, frmtFinal, m)
			if frmtFinal == m || strings.Contains(strings.ToLower(m), "driver") {
				//fmt.Println("NOT A DATE FORMAT", m)
				continue
			}
			frmtFinal = strings.ReplaceAll(frmtFinal, "{", "")
			frmtFinal = strings.ReplaceAll(frmtFinal, "}", "")
			var procc string
			if dates, ok := dateRef.([]time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dates[0].Format(frmtFinal))
			} else if dt, ok := dateRef.(time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dt.Format(frmtFinal))
			}
			patt = regexp.MustCompile(regexp.QuoteMeta(m))
			query = patt.ReplaceAllString(query, procc)
		}
	}
	// Handle cases for temporary tables with date extensions
	if os.Getenv("ETLX_DONT_RPLC_DT_PLCHLDR_NO_CBRCKTS") == "true" { // DO NOT REPLACE DATE PATT WITH NO CURLLY BRACKETS
		return query
	}
	patt = regexp.MustCompile(
		`YYYY.?MM.?DD|AAAA.?MM.?DD|YY.?MM.?DD|AA.?MM.?DD|YYYY.?MM|AAAA.?MM|YY.?MM|AA.?MM|MM.?DD|DD.?MM.?YYYY|DD.?MM.?AAAA|DD.?MM.?YY|DD.?MM.?AA`,
	)
	matches = patt.FindAllString(query, -1)
	if len(matches) > 0 {
		for _, m := range matches {
			frmtFinal := etlx.GetGODateFormat(m)
			var procc string
			if dates, ok := dateRef.([]time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dates[0].Format(frmtFinal))
			} else if dt, ok := dateRef.(time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dt.Format(frmtFinal))
			}
			patt = regexp.MustCompile(regexp.QuoteMeta(m))
			query = patt.ReplaceAllString(query, procc)
		}
	}
	return query
}

func (etlx *ETLX) ReplaceEnvVariable(input string) string {
	// Replaces @ENV.VARIABLE_NAME or @VARIABLE_NAME to the actual value
	re := regexp.MustCompile(`@ENV\.\w+`)
	matches := re.FindAllString(input, -1)
	i := 0
	if len(matches) > 0 {
		for _, match := range matches {
			envVar := strings.TrimPrefix(match, "@ENV.")
			envValue := os.Getenv(envVar)
			// fmt.Println(match, envVar, envValue)
			if envValue != "" {
				input = strings.ReplaceAll(input, match, envValue)
			} else {
				input = strings.ReplaceAll(input, match, envVar)
			}
		}
		matches = re.FindAllString(input, -1)
		if len(matches) > 0 && i < 3 {
			i += 1
			input = etlx.ReplaceEnvVariable(input)
		}
	} else {
		re = regexp.MustCompile(`@\.\w+`)
		matches = re.FindAllString(input, -1)
		if len(matches) > 0 {
			for _, match := range matches {
				envVar := strings.TrimPrefix(match, "@.")
				envValue := os.Getenv(envVar)
				// fmt.Println(match, envVar, envValue)
				if envValue != "" {
					input = strings.ReplaceAll(input, match, envValue)
				} else {
					input = strings.ReplaceAll(input, match, envVar)
				}
			}
			matches = re.FindAllString(input, -1)
			if len(matches) > 0 && i < 3 {
				i += 1
				input = etlx.ReplaceEnvVariable(input)
			}
		} else {
			re = regexp.MustCompile(`@\w+`)
			matches = re.FindAllString(input, -1)
			if len(matches) > 0 {
				for _, match := range matches {
					envVar := strings.TrimPrefix(match, "@")
					envValue := os.Getenv(envVar)
					// fmt.Println(match, envVar, envValue)
					if envValue != "" {
						input = strings.ReplaceAll(input, match, envValue)
					} else {
						//input = strings.ReplaceAll(input, match, envVar)
					}
				}
			}
			matches = re.FindAllString(input, -1)
			if len(matches) > 0 && i < 3 {
				i += 1
				input = etlx.ReplaceEnvVariable(input)
			}
		}
	}
	return input
}

// RunnerFunc is a function type that executes a query and handles its results.
type RunnerFunc func(conn string, query string, item map[string]any) error

// ProcessETL performs the ETL steps based on the configuration
func (etlx *ETLX) ProcessETL(config map[string]any, runner RunnerFunc) error {
	etl, ok := config["ETL"].(map[string]any)
	if !ok {
		return fmt.Errorf("missing or invalid ETL section")
	}
	// Extract metadata
	metadata, ok := etl["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("missing metadata in ETL section")
	}
	mainConn := metadata["connection"].(string)
	description := metadata["description"].(string)
	fmt.Printf("Starting ETL process: %s\n", description)
	start := time.Now()
	for key, value := range etl {
		if key == "metadata" {
			continue
		}
		fmt.Printf("Processing key: %s\n", key)
		item, ok := value.(map[string]any)
		if !ok {
			log.Printf("Skipping invalid item: %s\n", key)
			continue
		}
		err := etlx.ProcessETLSteps(key, item, mainConn, runner)
		if err != nil {
			log.Printf("Error processing %s: %v\n", key, err)
			continue
		}
	}
	fmt.Printf("ETL process completed: %s (Duration: %v)\n", description, time.Since(start).Seconds())
	return nil
}

func (etlx *ETLX) ProcessETLSteps(key string, item map[string]any, mainConn string, runner RunnerFunc) error {
	metadata, ok := item["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("missing metadata in item: %s", key)
	}
	steps := []string{"extract", "transform", "load"}
	for _, step := range steps {
		beforeSQL := metadata[step+"_before_sql"]
		mainSQL := metadata[step+"_sql"]
		afterSQL := metadata[step+"_after_sql"]
		conn := metadata[step+"_conn"]
		if conn == nil {
			conn = mainConn // Fallback to main connection
		}
		// Process before SQL
		err := etlx.RunQueries(conn.(string), beforeSQL, item, runner)
		if err != nil {
			return fmt.Errorf("error running before SQL for step %s: %v", step, err)
		}
		// Process main SQL
		err = etlx.RunQueries(conn.(string), mainSQL, item, runner)
		if err != nil {
			return fmt.Errorf("error running main SQL for step %s: %v", step, err)
		}
		// Process after SQL
		err = etlx.RunQueries(conn.(string), afterSQL, item, runner)
		if err != nil {
			return fmt.Errorf("error running after SQL for step %s: %v", step, err)
		}
	}
	return nil
}

func (etlx *ETLX) ParseConnection(conn string) (string, string, error) {
	parts := strings.SplitN(conn, ":", 2)
	if len(parts) != 2 {
		return "", conn, nil
		//return "", "", fmt.Errorf("invalid connection string format")
	}
	dl := NewDuckLakeParser().Parse(conn)
	if dl.IsDuckLake {
		return "ducklake", conn, nil
	}
	return parts[0], parts[1], nil
}

func (etlx *ETLX) RunQueries(conn string, sqlData any, item map[string]any, runner RunnerFunc) error {
	switch queries := sqlData.(type) {
	case nil:
		// Do nothing
		return nil
	case string:
		// Single query reference
		query, ok := item[queries].(string)
		if !ok {
			query = queries
		}
		return runner(conn, query, item)
	case []any:
		// Slice of query references
		for _, q := range queries {
			queryKey, ok := q.(string)
			if !ok {
				return fmt.Errorf("invalid query key in slice")
			}
			query, ok := item[queryKey].(string)
			if !ok {
				query = queryKey
			}
			err := runner(conn, query, item)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid SQL data type: %T", sqlData)
	}
	return nil
}

type RunnerFuncKey func(metadata map[string]any, key string, item map[string]any) error

func (etlx *ETLX) ProcessMDKey(key string, config map[string]any, runner RunnerFuncKey) error {
	data, ok := config[key].(map[string]any)
	if !ok {
		return fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("missing metadata in %s section", key)
	}
	// description := metadata["description"].(string)
	// fmt.Printf("Starting %s process: %s\n", key, description)
	// start := time.Now()
	order, okOrder := data["__order"].([]any)
	if okOrder {
		for _, key2 := range order {
			if key2 == "metadata" || key2 == "__order" || key2 == "order" {
				continue
			}
			if _, isMap := data[key2.(string)].(map[string]any); !isMap {
				// fmt.Println(key2, "NOT A MAP:", value)
				continue
			}
			err := runner(metadata, key2.(string), data[key2.(string)].(map[string]any))
			if err != nil {
				return err
			}
		}
	} else {
		for key2, value := range data {
			if key2 == "metadata" || key2 == "__order" || key2 == "order" {
				continue
			}
			if _, isMap := value.(map[string]any); !isMap {
				// fmt.Println(key2, "NOT A MAP:", value)
				continue
			}
			err := runner(metadata, key2, value.(map[string]any))
			if err != nil {
				return err
			}
		}
	}
	// fmt.Printf("%s process completed: %s (Duration: %v)\n", key, description, time.Since(start).Seconds())
	return nil
}

// Create a temporary file in the default temporary directory
func (etlx *ETLX) TempFIle(dir string, content string, name string) (string, error) {
	// Create a temporary file in the default temporary directory
	tempFile, err := os.CreateTemp(dir, name)
	if err != nil {
		return "", fmt.Errorf("error creating temporary file: %s", err)
	}
	// Defer closing the file to ensure it's closed even if an error occurs
	defer tempFile.Close()

	// Write the content to the file
	_, err = tempFile.WriteString(content)
	if err != nil {
		return "", fmt.Errorf("error writing to temporary file: %s", err)
	}
	// Get the name of the temporary file
	tempFileName := tempFile.Name()
	return tempFileName, nil
}
