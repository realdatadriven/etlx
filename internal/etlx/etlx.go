package etlx

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"gopkg.in/yaml.v3"
)

type ETLX struct {
	Config map[string]any
}

func (etlx *ETLX) ConfigFromFile(filePath string) error {
	// Read the file content
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	if strings.HasSuffix(filePath, ".ipynb") {
		mdText, err := etlx.ConvertIPYNBToMarkdown(data)
		if err != nil {
			return fmt.Errorf("failed convert the Notebook to MDText: %w", err)
		}
		// fmt.Println(mdText)
		data = []byte(mdText)
	}
	// Parse the Markdown content into an AST
	reader := text.NewReader(data)
	return etlx.ParseMarkdownToConfig(reader)
}

func (etlx *ETLX) ConfigFromMDText(mdText string) error {
	// Parse the Markdown content into an AST
	reader := text.NewReader([]byte(mdText))
	return etlx.ParseMarkdownToConfig(reader)
}

// ParseMarkdownToConfig parses a Markdown file into a structured nested map
func (etlx *ETLX) ParseMarkdownToConfig_(reader text.Reader) error {
	parser := goldmark.DefaultParser()
	root := parser.Parse(reader)

	// Initialize the result map
	config := make(map[string]any)
	current := config // Reference to the current level of the map

	// Walk through the AST
	ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
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

				// Create a new section under the current map reference
				if _, exists := current[heading]; !exists {
					current[heading] = make(map[string]any)
				}

				// Update the current map reference to the new section
				current = current[heading].(map[string]any)

			case *ast.FencedCodeBlock:
				// Extract info and content from the code block
				info := string(n.Info.Segment.Value(reader.Source()))
				content := string(n.Text(reader.Source()))

				// Process YAML blocks
				if strings.HasPrefix(info, "yaml") {
					key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
					yamlContent := make(map[string]any)
					if err := yaml.Unmarshal([]byte(content), &yamlContent); err != nil {
						log.Printf("Error parsing YAML block %s: %v", key, err)
					} else {
						current[key] = yamlContent
					}
				} else if strings.HasPrefix(info, "sql") {
					// Process SQL blocks
					key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
					current[key] = content
				}
			}
		} else if node.Kind() == ast.KindHeading {
			// Move up one level for closing a heading node
			current = config // Reset to the root on heading exit
		}
		return ast.WalkContinue, nil
	})
	etlx.Config = config
	return nil
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
func (etlx *ETLX) ParseMarkdownToConfigN1(reader text.Reader) error {
	// Initialize the Markdown parser
	parser := goldmark.DefaultParser()
	root := parser.Parse(reader)
	// Initialize the result map and a parent stack
	config := make(map[string]any)
	//config2 := make(map[string]any)
	parents := []map[string]any{config} // Stack of parent references for each level
	// Walk through the AST
	ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
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
				// Ensure the parents stack has enough levels
				for len(parents) <= int(n.Level) {
					parents = append(parents, nil)
				}
				// Reset the stack for deeper levels
				for i := int(n.Level); i < len(parents); i++ {
					parents[i] = nil
				}
				// Create or switch to the appropriate section
				parent := parents[n.Level-1]
				if parent == nil {
					parent = config
				}
				if _, exists := parent[heading]; !exists {
					parent[heading] = make(map[string]any)
				}
				//fmt.Println(n.Level, heading)
				// Update the parent reference for the current level
				parents[n.Level] = parent[heading].(map[string]any)
			case *ast.FencedCodeBlock:
				// Extract info and content from the code block
				info := string(n.Info.Segment.Value(reader.Source()))
				content := string(n.Text(reader.Source()))
				// Add to the appropriate parent
				parent := parents[len(parents)-1]
				/*keys := []string{}
				for key := range parent {
					keys = append(keys, key)
				}*/
				//headersTrace := etlx.TracebackHeaders(n, reader.Source())
				//node.Parent().Lines().Value(reader.Source())
				//fmt.Println(info, len(parents)-1, headersTrace, keys, node.Parent().Lines().Value(reader.Source()))
				if parent != nil {
					if strings.HasPrefix(info, "yaml") {
						// Process YAML blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
						yamlContent := make(map[string]any)
						err := yaml.Unmarshal([]byte(content), &yamlContent)
						if err != nil {
							log.Printf("Error parsing YAML block %s: %v", key, err)
						} else {
							parent[key] = yamlContent
						}
					} else if strings.HasPrefix(info, "sql") {
						// Process SQL blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
						parent[key] = content
					}
				}
			}
		} else if node.Kind() == ast.KindHeading {
			/*/ When exiting a heading, reset the parent stack for deeper levels
			for i := len(parents) - 1; i > 0; i-- {
				parents[i] = nil
			}*/
		}
		return ast.WalkContinue, nil
	})
	etlx.Config = config
	return nil
}

// ParseMarkdownToConfig parses a Markdown file into a structured nested map
func (etlx *ETLX) ParseMarkdownToConfigN_1(reader text.Reader) error {
	// Initialize the Markdown parser
	parser := goldmark.DefaultParser()
	root := parser.Parse(reader)
	// Initialize the result map
	config := make(map[string]any)
	var current map[string]any // Reference to the current map section
	var topLevelKey string     // Current level 1 heading
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
					topLevelKey = heading
					if _, exists := config[topLevelKey]; !exists {
						config[topLevelKey] = make(map[string]any)
					}
					current = config[topLevelKey].(map[string]any)
				} else {
					if current != nil {
						if _, exists := current[heading]; !exists {
							current[heading] = make(map[string]any)
						}
						current = current[heading].(map[string]any)
					}
				}
			case *ast.FencedCodeBlock:
				// Extract info and content from the code block
				info := string(n.Info.Segment.Value(reader.Source()))
				content := string(n.Text(reader.Source()))
				if current != nil {
					if strings.HasPrefix(info, "yaml") {
						// Process YAML blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
						yamlContent := make(map[string]any)
						err := yaml.Unmarshal([]byte(content), &yamlContent)
						if err != nil {
							//log.Printf("Error parsing YAML block %s: %v", key, err)
							return ast.WalkContinue, fmt.Errorf("error parsing YAML block %s: %v", key, err)
						} else {
							current[key] = yamlContent
						}
					} else if strings.HasPrefix(info, "sql") {
						// Process SQL blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
						current[key] = content
					}
				}
			}
		}
		return ast.WalkContinue, nil
	})
	if err != nil {
		return err
	}
	etlx.Config = config
	return nil
}

func (etlx *ETLX) ParseMarkdownToConfig(reader text.Reader) error {
	// Initialize the Markdown parser
	parser := goldmark.DefaultParser()
	root := parser.Parse(reader)

	// Initialize the result map and a levels map
	config := make(map[string]any)
	levels := make(map[int]map[string]any) // Track the current section for each heading level

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
					// Reset the context for a new level 1 heading
					if _, exists := config[heading]; !exists {
						config[heading] = make(map[string]any)
					}
					levels = map[int]map[string]any{1: config[heading].(map[string]any)}
				} else {
					// Handle deeper levels (level 2 and beyond)
					parent := levels[n.Level-1]
					if parent == nil {
						return ast.WalkContinue, fmt.Errorf("missing parent section for level %d heading: %s", n.Level, heading)
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
					if strings.HasPrefix(info, "yaml") {
						// Process YAML blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
						yamlContent := make(map[string]any)
						err := yaml.Unmarshal([]byte(content), &yamlContent)
						if err != nil {
							return ast.WalkContinue, fmt.Errorf("error parsing YAML block %s: %v", key, err)
						}
						current[key] = yamlContent
					} else if strings.HasPrefix(info, "sql") {
						// Process SQL blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
						current[key] = content
					}
				}
			}
		}
		return ast.WalkContinue, nil
	})
	if err != nil {
		return err
	}

	etlx.Config = config
	return nil
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
	// Replaces @ENV.VARIABLE_NAME to the actual value
	re := regexp.MustCompile(`@ENV\.\w+`)
	matches := re.FindAllString(input, -1)
	if len(matches) > 0 {
		for _, match := range matches {
			envVar := strings.TrimPrefix(match, "@ENV.")
			envValue := os.Getenv(envVar)
			// fmt.Println(match, envVar, envValue)
			if envValue != "" {
				input = strings.ReplaceAll(input, match, envValue)
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
	fmt.Printf("ETL process completed: %s (Duration: %v)\n", description, time.Since(start))
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
		return "", "", fmt.Errorf("invalid connection string format")
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
	for key2, value := range data {
		if key2 == "metadata" {
			continue
		}
		err := runner(metadata, key2, value.(map[string]any))
		if err != nil {
			return err
		}
	}
	// fmt.Printf("%s process completed: %s (Duration: %v)\n", key, description, time.Since(start))
	return nil
}

// Create a temporary file in the default temporary directory
func (etlx *ETLX) TempFIle(content string, name string) (string, error) {
	// Create a temporary file in the default temporary directory
	tempFile, err := os.CreateTemp("", name)
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
