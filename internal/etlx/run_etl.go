package etlx

import (
	"fmt"
	"os"
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

func (etlx *ETLX) Query(conn db.DBInterface, query string, item map[string]any, step string, dateRef []time.Time) (*[]map[string]any, error) {
	table := ""
	metadata, ok := item["metadata"].(map[string]any)
	if ok {
		table = metadata["table"].(string)
	}
	fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
	query = etlx.SetQueryPlaceholders(query, table, fname, dateRef)
	if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
		_file, err := etlx.TempFIle(query, fmt.Sprintf("query.%s_%s.*.sql", "valid", table))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(_file)
	}
	data, _, err := conn.QueryMultiRows(query, []any{}...)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (etlx *ETLX) ExecuteQuery(conn db.DBInterface, sqlData any, item map[string]any, step string, dateRef []time.Time) error {
	table := ""
	metadata, ok := item["metadata"].(map[string]any)
	if ok {
		table = metadata["table"].(string)
	}
	odbc2Csv := false
	if _, ok := metadata["odbc_to_csv"]; ok {
		odbc2Csv = metadata["odbc_to_csv"].(bool)
	}
	fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
	fname = etlx.SetQueryPlaceholders(fname, "", "", dateRef)
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
			if !ok {
				query = queryKey
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

func (etlx *ETLX) RunETL(dateRef []time.Time, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
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
			mainSQL, ok := itemMetadata[step+"_sql"]
			afterSQL, okAfter := itemMetadata[step+"_after_sql"]
			validation, okValid := itemMetadata[step+"_validation"]
			cleanSQL, okClean := itemMetadata["clean_sql"]
			dropSQL, okDrop := itemMetadata["drop_sql"]
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
			// Process before SQL
			if okBefore && beforeSQL != nil {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:Before", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				//fmt.Println(_log3)
				err = etlx.ExecuteQuery(dbConn, beforeSQL, item, step, dateRef)
				if err != nil {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: Before: %s", key, step, itemKey, err)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					//return fmt.Errorf("%s -> %s -> %s ERR: Before: %s", key, step, itemKey, err)
					continue
				} else {
					_log3["success"] = true
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s Before", key, step, itemKey)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
				}
				processLogs = append(processLogs, _log3)
			}
			// Process main SQL
			if ok && !drop.(bool) && !clean.(bool) {
				// VALIDATION
				isValid := true
				if okValid && validation != nil {
					table := itemMetadata["table"].(string)
					fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
					//fmt.Println(validation)
					if _, ok := validation.([]any); ok {
						for _, valid := range validation.([]any) {
							_valid := valid.(map[string]any)
							start4 := time.Now()
							_log3 = map[string]any{
								"name":        fmt.Sprintf("%s->%s->%s:Valid", key, itemKey, step),
								"description": itemMetadata["description"].(string),
								"start_at":    start4,
							}
							//fmt.Println(_valid["type"].(string), _valid["sql"].(string), _valid["msg"].(string))
							_sql := _valid["sql"].(string)
							if _, ok := item[_valid["sql"].(string)]; ok {
								_sql = item[_valid["sql"].(string)].(string)
							}
							_sql = etlx.SetQueryPlaceholders(_sql, table, fname, dateRef)
							res, err := etlx.Query(dbConn, _sql, item, step, dateRef)
							if err != nil {
								fmt.Printf("%s -> %s -> %s ERR VALID (%s): %s\n", key, step, itemKey, _valid["sql"], err)
							} else {
								msg := etlx.SetQueryPlaceholders(_valid["msg"].(string), table, fname, dateRef)
								//fmt.Println(len(*res), _valid["type"].(string), msg)
								if len(*res) > 0 && _valid["type"].(string) == "trow_if_not_empty" {
									_log3["success"] = false
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s: Validation Error: %s", key, step, itemKey, msg)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
									//return fmt.Errorf("%s -> %s -> %s Validation Error: %s", key, step, itemKey, msg)
									isValid = false
									break
								} else if len(*res) == 0 && _valid["type"].(string) == "trow_if_empty" {
									_log3["success"] = false
									_log3["msg"] = fmt.Sprintf("%s -> %s -> %s: Validation Error: %s", key, step, itemKey, msg)
									_log3["end_at"] = time.Now()
									_log3["duration"] = time.Since(start4)
									//return fmt.Errorf("%s -> %s -> %s: Validation Error: %s", key, step, itemKey, msg)
									isValid = false
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
					err = etlx.ExecuteQuery(dbConn, mainSQL, item, step, dateRef)
					if err != nil {
						_log3["success"] = false
						_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
						_log3["end_at"] = time.Now()
						_log3["duration"] = time.Since(start4)
						//return fmt.Errorf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
					} else {
						_log3["success"] = true
						_log3["msg"] = fmt.Sprintf("%s -> %s -> %s main", key, step, itemKey)
						_log3["end_at"] = time.Now()
						_log3["duration"] = time.Since(start4)
					}
				} else {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s MAIN: Skiped do to validation error: %s", key, step, itemKey, err)
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
				err = etlx.ExecuteQuery(dbConn, cleanSQL, item, step, dateRef)
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
				err = etlx.ExecuteQuery(dbConn, dropSQL, item, step, dateRef)
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
			// Process after SQL
			if okAfter && afterSQL != nil {
				start4 = time.Now()
				_log3 = map[string]any{
					"name":        fmt.Sprintf("%s->%s->%s:After", key, itemKey, step),
					"description": itemMetadata["description"].(string),
					"start_at":    start4,
				}
				err = etlx.ExecuteQuery(dbConn, afterSQL, item, step, dateRef)
				if err != nil {
					_log3["success"] = false
					_log3["msg"] = fmt.Sprintf("%s -> %s -> %s ERR: After: %s", key, step, itemKey, err)
					_log3["end_at"] = time.Now()
					_log3["duration"] = time.Since(start4)
					//return fmt.Errorf("%s -> %s -> %s ERR: After: %s", key, step, itemKey, err)
					//return nil
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
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, etlx.Config, ELTRunner)
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
