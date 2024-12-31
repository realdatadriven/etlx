package etlx

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

func (etlx *ETLX) GetDB(conn string) (db.DBInterface, error) {
	driver, dsn, err := etlx.ParseConnection(conn)
	if err != nil {
		return nil, err
	}
	_dsn := etlx.ReplaceEnvVariable(dsn)
	var dbConn db.DBInterface
	switch driver {
	case "duckdb":
		dbConn, err = db.NewDuckDB(_dsn)
		if err != nil {
			return nil, fmt.Errorf("%s Conn: %s", driver, err)
		}
	case "odbc":
		dbConn, err = db.NewODBC(_dsn)
		if err != nil {
			return nil, fmt.Errorf("ODBC Conn: %s", err)
		}
	default:
		dbConn, err = db.New(driver, _dsn)
		if err != nil {
			return nil, fmt.Errorf("%s Conn: %s", driver, err)
		}
	}
	return dbConn, nil
}

func (etlx *ETLX) SetQueryPlaceholders(query string, table string, path string, dateRef []time.Time) string {
	_query := etlx.ReplaceEnvVariable(query)
	if table != "" {
		_query = etlx.ReplaceFileTablePlaceholder("table", _query, table)
	}
	if path != "" {
		_query = etlx.ReplaceFileTablePlaceholder("file", _query, path)
	}
	_query = etlx.ReplaceQueryStringDate(_query, dateRef)
	return _query
}

func (etlx *ETLX) Query(conn db.DBInterface, query string, item map[string]any, fname string, step string, dateRef []time.Time) (*[]map[string]any, []string, error) {
	table := ""
	metadata, ok := item["metadata"].(map[string]any)
	if ok {
		table, _ = metadata["table"].(string)
	}
	if fname == "" {
		fname = fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
	}
	query = etlx.SetQueryPlaceholders(query, table, fname, dateRef)
	if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
		_file, err := etlx.TempFIle(query, fmt.Sprintf("query.%s_%s.*.sql", "valid", table))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(_file)
	}
	data, cols, _, err := conn.QueryMultiRowsWithCols(query, []any{}...)
	if err != nil {
		return nil, nil, err
	}
	return data, cols, nil
}

// ReplacePlaceholders replaces placeholders in the format [[query_name]] with their corresponding values from the item map.
func ReplacePlaceholders(sql string, item map[string]any) (string, error) {
	// Define a regex to match placeholders in the format [[query_name]]
	re := regexp.MustCompile(`\[\[(\w+)\]\]`)
	// Replace all matches with the corresponding values from the item map
	updatedSQL := re.ReplaceAllStringFunc(sql, func(match string) string {
		// Extract the query_name from the placeholder
		matches := re.FindStringSubmatch(match)
		if len(matches) > 1 {
			queryName := matches[1]
			// fmt.Println(queryName, sql, item[queryName])
			// Check if the query_name exists in the item map
			if replacement, exists := item[queryName]; exists {
				return replacement.(string)
			}
		}
		// If no replacement is found, keep the placeholder as is
		return match
	})
	return updatedSQL, nil
}

// ExtractDistinctQueryNames extracts a slice of distinct query names used in the format [[query_name]].
func ExtractDistinctQueryNames(sql string) []string {
	// Define a regex to match placeholders in the format [[query_name]]
	re := regexp.MustCompile(`\[\[(\w+)\]\]`)
	// Find all matches
	matches := re.FindAllStringSubmatch(sql, -1)
	// Use a map to ensure distinct query names
	uniqueNames := make(map[string]struct{})
	for _, match := range matches {
		if len(match) > 1 {
			queryName := match[1] // Extract the captured query name
			uniqueNames[queryName] = struct{}{}
		}
	}
	// Convert the map keys to a slice
	distinctNames := make([]string, 0, len(uniqueNames))
	for name := range uniqueNames {
		distinctNames = append(distinctNames, name)
	}
	return distinctNames
}

func (etlx *ETLX) ExecuteQuery(conn db.DBInterface, sqlData any, item map[string]any, fname string, step string, dateRef []time.Time) error {
	table := ""
	metadata, ok := item["metadata"].(map[string]any)
	if ok {
		table, _ = metadata["table"].(string)
	}
	odbc2Csv := false
	if _, ok := metadata["odbc_to_csv"]; ok {
		odbc2Csv = metadata["odbc_to_csv"].(bool)
	}
	if fname == "" {
		fname = fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
	}
	fname = etlx.SetQueryPlaceholders(fname, "", "", dateRef)
	switch queries := sqlData.(type) {
	case nil:
		// Do nothing
		return nil
	case string:
		// Single query reference
		query, ok := item[queries].(string)
		_, queryDoc := etlx.Config[queries]
		if !ok && queryDoc {
			query = queries
			_sql, _, _, err := etlx.QueryBuilder(map[string]any{}, queries)
			if err != nil {
				fmt.Printf("QUERY DOC ERR ON KEY %s: %v\n", queries, err)
			} else {
				query = _sql
			}
		} else if !ok {
			query = queries
		}
		updatedSQL, err := ReplacePlaceholders(query, item)
		if err != nil {
			fmt.Println("Error trying to get the placeholder:", err)
		} else {
			query = updatedSQL
		}
		query = etlx.SetQueryPlaceholders(query, table, fname, dateRef)
		if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
			_file, err := etlx.TempFIle(query, fmt.Sprintf("query.%s.*.sql", queries))
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(_file)
		}
		if odbc2Csv && conn.GetDriverName() == "odbc" && step == "extract" {
			_, err := conn.Query2CSV(query, fname)
			if err != nil {
				return err
			}
		} else {
			_, err := conn.ExecuteQuery(query)
			if err != nil {
				return err
			}
		}
		return nil
	case []any:
		// Slice of query references
		for _, q := range queries {
			queryKey, ok := q.(string)
			if !ok {
				return fmt.Errorf("invalid query key in slice")
			}
			//fmt.Println(queryKey)
			query, ok := item[queryKey].(string)
			_, queryDoc := etlx.Config[queryKey]
			if !ok && queryDoc {
				query = queryKey
				_sql, _, _, err := etlx.QueryBuilder(map[string]any{}, queryKey)
				if err != nil {
					fmt.Printf("QUERY DOC ERR ON KEY %s: %v\n", queryKey, err)
				} else {
					query = _sql
				}
			} else if !ok {
				query = queryKey
			}
			updatedSQL, err := ReplacePlaceholders(query, item)
			if err != nil {
				fmt.Println("Error trying to get the placeholder:", err)
			} else {
				query = updatedSQL
			}
			query = etlx.SetQueryPlaceholders(query, table, fname, dateRef)
			if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
				_file, err := etlx.TempFIle(query, fmt.Sprintf("query.%s.*.sql", queryKey))
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println(_file)
			}
			if odbc2Csv && conn.GetDriverName() == "odbc" && step == "extract" {
				_, err := conn.Query2CSV(query, fname)
				if err != nil {
					return err
				}
			} else {
				_, err := conn.ExecuteQuery(query)
				if err != nil {
					//fmt.Println(query, err)
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid SQL data type: %T", sqlData)
	}
}

func (app *ETLX) contains(slice []string, element interface{}) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}

func (app *ETLX) containsAny(slice []interface{}, element interface{}) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}

func (etlx *ETLX) RunETL(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "ETL"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	processLogs = append(processLogs, map[string]any{
		"name":     key,
		"start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	ELTRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				})
				return fmt.Errorf("dectivated %s", "")
			}
		}
		//fmt.Println(metadata, itemKey, item)
		mainConn := metadata["connection"].(string)
		mainDescription = metadata["description"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    time.Now(),
				"end_at":      time.Now(),
				"success":     true,
				"msg":         "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				})
				return nil
			}
		}
		// CHECK CONFIG
		if only, okOnly := extraConf["only"]; okOnly {
			//fmt.Println("ONLY", only, len(only.([]string)))
			if len(only.([]string)) == 0 {
			} else if !etlx.contains(only.([]string), itemKey) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Excluded from the process",
				})
				return nil
			}
		}
		if skip, okSkip := extraConf["skip"]; okSkip {
			//fmt.Println("SKIP", skip, len(skip.([]string)))
			if len(skip.([]string)) == 0 {
			} else if etlx.contains(skip.([]string), itemKey) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Excluded from the process",
				})
				return nil
			}
		}
		start2 := time.Now()
		_log1 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"start_at":    start2,
		}
		_steps := []string{"extract", "transform", "load"}
		for _, step := range _steps {
			// CHECK CLEAN
			clean, ok := extraConf["clean"]
			if ok {
				if clean.(bool) && step != "load" {
					continue
				}
			}
			// CHECK DROP
			drop, ok := extraConf["drop"]
			if ok {
				if drop.(bool) && step != "load" {
					continue
				}
			}
			// CHECK ROWS
			rows, ok := extraConf["rows"]
			if ok {
				if rows.(bool) && step != "load" {
					continue
				}
			}
			// CHECK FILE
			file, ok := extraConf["file"].(string)
			if ok {
				if file != "" && step != "load" {
					continue
				}
			}
			// STEPS
			if steps, ok := extraConf["steps"]; ok {
				if len(steps.([]string)) == 0 {
				} else if !etlx.contains(steps.([]string), step) {
					processLogs = append(processLogs, map[string]any{
						"name":        fmt.Sprintf("%s->%s->%s", key, itemKey, step),
						"description": itemMetadata["description"].(string),
						"start_at":    time.Now(),
						"end_at":      time.Now(),
						"success":     true,
						"msg":         fmt.Sprintf("STEP %s Excluded from the process", step),
					})
					continue
				}
			}
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s->%s", key, itemKey, step),
				"description": itemMetadata["description"].(string),
				"start_at":    start2,
			}
			beforeSQL, okBefore := itemMetadata[step+"_before_sql"]
			onBefErrPatt, okBefErrPatt := itemMetadata[step+"_before_on_err_match_patt"]
			onBefErrSQL, okBefErrSQL := itemMetadata[step+"_before_on_err_match_sql"]
			mainSQL, okMain := itemMetadata[step+"_sql"]
			afterSQL, okAfter := itemMetadata[step+"_after_sql"]
			onAfterErrPatt, okAfterErrPatt := itemMetadata[step+"_after_on_err_match_patt"]
			onAfterErrSQL, okAfterErrSQL := itemMetadata[step+"_after_on_err_match_sql"]
			validation, okValid := itemMetadata[step+"_validation"]
			onErrPatt, okErrPatt := itemMetadata[step+"_on_err_match_patt"]
			onErrSQL, okErrSQL := itemMetadata[step+"_on_err_match_sql"]
			fromFileSQL, okFromFile := itemMetadata[step+"_from_file"].(map[string]any)
			cleanSQL, okClean := itemMetadata["clean_sql"]
			dropSQL, okDrop := itemMetadata["drop_sql"]
			rowsSQL, okRows := itemMetadata["rows_sql"]
			metadataFile, okMetaFile := itemMetadata["file"].(string)
			if rows.(bool) && !okRows {
				rowsSQL = `SELECT COUNT(*) AS "nrows" FROM "<table>"`
			}
			//fmt.Println(step, ok, mainSQL)
			if !ok || mainSQL == nil {
				continue
			}
			conn := itemMetadata[step+"_conn"]
			if conn == nil {
				conn = mainConn // Fallback to main connection
			}
			// CONNECTION
			start4 := time.Now()
			_log3 := map[string]any{
				"name":        fmt.Sprintf("%s->%s->%s:Conn", key, itemKey, step),
				"description": itemMetadata["description"].(string),
				"start_at":    start4,
			}
			dbConn, err := etlx.GetDB(conn.(string))
			if err != nil {
				_log3["success"] = false
				_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: connecting to %s in : %s", key, step, itemKey, conn, err)
				_log3["end_at"] = time.Now()
				_log3["duration"] = time.Since(start4)
				processLogs = append(processLogs, _log3)
				//return fmt.Errorf("%s -> %s -> %s ERR: connecting to %s in : %s", key, step, itemKey, conn, err)
				continue
			}
			defer dbConn.Close()
			_log3["success"] = true
			_log3["msg"] = fmt.Sprintf("%s -> %s -> %s CONN: Connectinon to %s successfull", key, step, itemKey, conn)
			_log3["end_at"] = time.Now()
			_log3["duration"] = time.Since(start4)
			processLogs = append(processLogs, _log3)
			// FILE
			table := itemMetadata["table"].(string)
			fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
			itemHasFile := false
			if okMetaFile && metadataFile != "" {
				fname = metadataFile
				if step != "load" { // FILES ONLY RUN's LOAD STEP
					continue
				}
				itemHasFile = true
				if tmp, ok := itemMetadata["tmp"].(bool); ok {
					if tmp && filepath.Dir(fname) != "" && fname != "." {
						fname = fmt.Sprintf(`%s/%s`, os.TempDir(), filepath.Base(fname))
						//fmt.Println("TMP:", tmp, fname)
					}
				}
			} else if file != "" {
				fname = file
			}
			// Process before SQL
			if okBefore && beforeSQL != nil {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:Before", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				//fmt.Println(_log3)
				//fmt.Println(beforeSQL)
				err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, step, dateRef)
				if err != nil {
					_err_by_pass := false
					if okBefErrPatt && onBefErrPatt != nil && okBefErrSQL && onBefErrSQL != nil {
						//fmt.Println(onErrPatt.(string), onErrSQL.(string))
						re, regex_err := regexp.Compile(onBefErrPatt.(string))
						if regex_err != nil {
							_log3["success"] = false
							_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR Before: fallback regex matching the error failed to compile: %s", key, step, itemKey, err)
							_log3["end_at"] = time.Now()
							_log3["duration"] = time.Since(start4)
						} else if re.MatchString(string(err.Error())) {
							err = etlx.ExecuteQuery(dbConn, onBefErrSQL.(string), item, fname, step, dateRef)
							if err != nil {
								_log3["success"] = false
								_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: Before: %s", key, step, itemKey, err)
								_log3["end_at"] = time.Now()
								_log3["duration"] = time.Since(start4)
							} else {
								_err_by_pass = true
								err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, step, dateRef)
								if err != nil {
									_err_by_pass = false
								}
							}
						}
					}
					if !_err_by_pass {
						_log3["success"] = false
						_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: Before: %s", key, step, itemKey, err)
						_log3["end_at"] = time.Now()
						_log3["duration"] = time.Since(start4)
						processLogs = append(processLogs, _log3)
						//return fmt.Errorf("%s -> %s -> %s ERR: Before: %s", key, step, itemKey, err)
						continue
					}
				} else {
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s Before", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			// Process main SQL
			if okMain && !drop.(bool) && !clean.(bool) && !rows.(bool) {
				// VALIDATION
				isValid := true
				validErr := ""
				if okValid && validation != nil {
					//fmt.Println(1, validation, reflect.TypeOf(validation))
					if _, ok := validation.([]any); ok {
						//fmt.Println(2, validation)
						for _, valid := range validation.([]any) {
							_valid := valid.(map[string]any)
							start4 := time.Now()
							_log3 = map[string]any{
								"name":        fmt.Sprintf("%s->%s->%s:Valid", key, itemKey, step),
								"description": itemMetadata["description"].(string),
								"start_at":    start4,
							}
							rule_active := true
							_rule_active, _ok := _valid["active"]
							if _ok {
								if !_rule_active.(bool) {
									rule_active = false
									//fmt.Println("SKIPING", _valid)
								}
							}
							if !rule_active {
								processLogs = append(processLogs, _log3)
								continue
							}
							//fmt.Println(_valid["type"].(string), _valid["sql"].(string), _valid["msg"].(string))
							_sql := _valid["sql"].(string)
							if _, ok := item[_valid["sql"].(string)]; ok {
								_sql = item[_valid["sql"].(string)].(string)
							}
							_sql = etlx.SetQueryPlaceholders(_sql, table, fname, dateRef)
							res, _, err := etlx.Query(dbConn, _sql, item, fname, step, dateRef)
							if err != nil {
								fmt.Printf("%s -> %s -> %s ERR VALID (%s): %s\n", key, step, itemKey, _valid["sql"], err)
							} else {
								msg := etlx.SetQueryPlaceholders(_valid["msg"].(string), table, fname, dateRef)
								validErr = msg
								//fmt.Println(len(*res), _valid["type"].(string), msg)
								if len(*res) > 0 && _valid["type"].(string) == "trow_if_not_empty" {
									_log3["success"] = false
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s: Validation Error: %s", key, step, itemKey, msg)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
									isValid = false
									processLogs = append(processLogs, _log3)
									break
								} else if len(*res) == 0 && _valid["type"].(string) == "trow_if_empty" {
									_log3["success"] = false
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s: Validation Error: %s", key, step, itemKey, msg)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
									isValid = false
									processLogs = append(processLogs, _log3)
									break
								} else {
									_log3["success"] = true
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s Validation Succefully", key, step, itemKey)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
								}
							}
							processLogs = append(processLogs, _log3)
						}
					}
				}
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:Main", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				if isValid {
					if itemHasFile && fromFileSQL != nil && okFromFile { // IF HAS FILE AND _from_file configuration
						ext := strings.Replace(filepath.Ext(fname), ".", "", 1)
						fmt.Println("FROM FILE:", ext, fromFileSQL[ext])
						if _sql, ok := fromFileSQL[ext]; ok {
							err = etlx.ExecuteQuery(dbConn, _sql, item, fname, step, dateRef)
						} else if _sql, ok := fromFileSQL["others"]; ok {
							err = etlx.ExecuteQuery(dbConn, _sql, item, fname, step, dateRef)
						} else {
							err = etlx.ExecuteQuery(dbConn, mainSQL, item, fname, step, dateRef)
						}
					} else {
						err = etlx.ExecuteQuery(dbConn, mainSQL, item, fname, step, dateRef)
					}
					if err != nil {
						_err_by_pass := false
						if okErrPatt && onErrPatt != nil && okErrSQL && onErrSQL != nil {
							//fmt.Println(onErrPatt.(string), onErrSQL.(string))
							re, regex_err := regexp.Compile(onErrPatt.(string))
							if regex_err != nil {
								_log3["success"] = false
								_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: fallback regex matching the error failed to compile: %s", key, step, itemKey, err)
								_log3["end_at"] = time.Now()
								_log3["duration"] = time.Since(start4)
							} else if re.MatchString(string(err.Error())) {
								err = etlx.ExecuteQuery(dbConn, onErrSQL.(string), item, fname, step, dateRef)
								if err != nil {
									_log3["success"] = false
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
								} else {
									_err_by_pass = true
								}
							}
						}
						if !_err_by_pass {
							_log3["success"] = false
							_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
							_log3["end_at"] = time.Now()
							_log3["duration"] = time.Since(start4)
						}
						//return fmt.Errorf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
					} else {
						_log3["success"] = true
						_log3["msg"] = fmt.Sprintf("%s -> %s -> %s main", key, step, itemKey)
						_log3["end_at"] = time.Now()
						_log3["duration"] = time.Since(start4)
					}
				} else {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s MAIN: Skiped do to validation error: %s", key, step, itemKey, validErr)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			// Process CLEAN SQL
			if clean.(bool) && okClean {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:CLEAN", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				err = etlx.ExecuteQuery(dbConn, cleanSQL, item, fname, step, dateRef)
				if err != nil {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: CLEAN: %s", key, step, itemKey, err)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					//return fmt.Errorf("%s -> %s -> %s ERR: CELAN: %s", key, step, itemKey, err)
				} else {
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s CLEAN", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			// Process DROP SQL
			if drop.(bool) && okDrop {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:DROP", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				err = etlx.ExecuteQuery(dbConn, dropSQL, item, fname, step, dateRef)
				if err != nil {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: DROP: %s", key, step, itemKey, err)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					//return fmt.Errorf("%s -> %s -> %s ERR: DROP: %s", key, step, itemKey, err)
				} else {
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s DROP", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			// Process DROP SQL
			if rows.(bool) && okRows {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:ROWS", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				_sql := rowsSQL.(string)
				if _, ok := item[rowsSQL.(string)]; ok {
					_sql = item[rowsSQL.(string)].(string)
				}
				_sql = etlx.SetQueryPlaceholders(_sql, table, fname, dateRef)
				res, _, err := etlx.Query(dbConn, _sql, item, fname, step, dateRef)
				if err != nil {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: ROWS: %s", key, step, itemKey, err)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					//return fmt.Errorf("%s -> %s -> %s ERR: DROP: %s", key, step, itemKey, err)
				} else {
					_nrows := any(nil)
					if len(*res) > 0 {
						_nrows, ok = (*res)[0]["nrows"]
						if !ok {
							_nrows, ok = (*res)[0]["rows"]
							if !ok {
								_nrows = (*res)[0]["total"]
							}
						}
					}
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ROWS", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					_log3["rows"] = _nrows
				}
				processLogs = append(processLogs, _log3)
			}
			// Process after SQL
			if okAfter && afterSQL != nil {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:After", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				//fmt.Println(afterSQL)
				err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, step, dateRef)
				if err != nil {
					_err_by_pass := false
					if okAfterErrPatt && onAfterErrPatt != nil && okAfterErrSQL && onAfterErrSQL != nil {
						//fmt.Println(onErrPatt.(string), onErrSQL.(string))
						re, regex_err := regexp.Compile(onAfterErrPatt.(string))
						if regex_err != nil {
							_log3["success"] = false
							_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR After: fallback regex matching the error failed to compile: %s", key, step, itemKey, err)
							_log3["end_at"] = time.Now()
							_log3["duration"] = time.Since(start4)
						} else if re.MatchString(string(err.Error())) {
							err = etlx.ExecuteQuery(dbConn, onAfterErrSQL.(string), item, fname, step, dateRef)
							if err != nil {
								_log3["success"] = false
								_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: After: %s", key, step, itemKey, err)
								_log3["end_at"] = time.Now()
								_log3["duration"] = time.Since(start4)
							} else {
								_err_by_pass = true
								err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, step, dateRef)
								if err != nil {
									_err_by_pass = false
								}
							}
						}
					}
					if !_err_by_pass {
						_log3["success"] = false
						_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: After: %s", key, step, itemKey, err)
						_log3["end_at"] = time.Now()
						_log3["duration"] = time.Since(start4)
					}
				} else {
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s After", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3)
			processLogs = append(processLogs, _log3)
		}
		_log1["end_at"] = time.Now()
		_log1["duration"] = time.Since(start2)
		processLogs = append(processLogs, _log1)
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, conf, ELTRunner)
	if err != nil {
		return processLogs, fmt.Errorf("%s failed: %v", key, err)
	}
	processLogs[0] = map[string]any{
		"name":        key,
		"description": mainDescription,
		"start_at":    processLogs[0]["start_at"],
		"end_at":      time.Now(),
		"duration":    time.Since(start),
	}
	return processLogs, nil
}
