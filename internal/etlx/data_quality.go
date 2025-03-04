package etlxlib

import (
	"fmt"
	"os"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

func (etlx *ETLX) ExecuteQueryWithRowsAffected(conn db.DBInterface, sqlData any, item map[string]any, fname string, step string, dateRef []time.Time) (int64, error) {
	table := ""
	metadata, ok := item["metadata"].(map[string]any)
	if ok {
		table, _ = metadata["table"].(string)
	}
	if fname == "" {
		fname = fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
	}
	fname = etlx.SetQueryPlaceholders(fname, "", "", dateRef)
	switch queries := sqlData.(type) {
	case nil:
		// Do nothing
		return 0, nil
	case string:
		// Single query reference
		query, ok := item[queries].(string)
		_, queryDoc := etlx.Config[queries]
		if !ok && queryDoc {
			query = queries
			_sql, _, _, err := etlx.QueryBuilder(nil, queries)
			if err != nil {
				fmt.Printf("QUERY DOC ERR ON KEY %s: %v\n", queries, err)
			} else {
				query = _sql
			}
		} else if !ok {
			query = queries
		}
		updatedSQL, err := etlx.ReplacePlaceholders(query, item)
		if err != nil {
			fmt.Println("Error trying to get the placeholder:", err)
		} else {
			query = updatedSQL
		}
		query = etlx.SetQueryPlaceholders(query, table, fname, dateRef)
		if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
			_file, err := etlx.TempFIle("", query, fmt.Sprintf("query.%s.*.sql", queries))
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(_file)
		}
		rowsAffected, err := conn.ExecuteQueryRowsAffected(query)
		if err != nil {
			return 0, err
		}
		return rowsAffected, nil
	default:
		return 0, fmt.Errorf("invalid SQL data type: %T", sqlData)
	}
}

func (etlx *ETLX) RunDATA_QUALITY(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "DATA_QUALITY"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	processLogs = append(processLogs, map[string]any{
		"name":     key,
		"start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	DATA_QUALITYRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
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
		mainConn, _ := metadata["connection"].(string)
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
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"start_at":    start3,
		}
		beforeSQL, okBefore := itemMetadata["before_sql"]
		query, okQuery := itemMetadata["query"]
		column, okColumn := itemMetadata["column"]
		fixQuery, okFix := itemMetadata["fix_quality_err"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		if okQuery && query != "" {
			conn, okCon := itemMetadata["connection"]
			if !okCon {
				conn = mainConn
			}
			dbConn, err := etlx.GetDB(conn.(string))
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s ERR: connecting to %s in : %s", key, itemKey, conn, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				return nil
			}
			defer dbConn.Close()
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s -> %s CONN: Connectinon to %s successfull", key, itemKey, conn)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3)
			processLogs = append(processLogs, _log2)
			//  QUERIES TO RUN AT BEGINING
			if okBefore {
				start3 := time.Now()
				_log2 := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
				}
				err = etlx.ExecuteQuery(dbConn, beforeSQL, item, "", "", dateRef)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s Before error: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s Before ", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				}
				processLogs = append(processLogs, _log2)
			}
			// MAIN QUERY
			sql := query.(string)
			if _, ok := item[sql]; ok {
				sql = item[sql].(string)
			}
			sql = etlx.SetQueryPlaceholders(sql, "", "", dateRef)
			rows, _, err := etlx.Query(dbConn, sql, item, "", "", dateRef)
			var nRows any
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to execute query: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				return nil
			}
			if len(*rows) > 0 {
				okConf := false
				if column != nil && okColumn {
					nRows, okConf = (*rows)[0][column.(string)]
				} else {
					nRows, okConf = (*rows)[0]["total"]
				}
				if okConf && nRows != nil {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s -> successfully executed", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					_log2["nrows"] = nRows
					if _, ok := nRows.(int64); ok {
						if nRows.(int64) > 0 && fixQuery != nil && okFix {
							rowsAffected, err := etlx.ExecuteQueryWithRowsAffected(dbConn, beforeSQL, item, "", "", dateRef)
							if err != nil {
								_log2["msg_fix"] = fmt.Sprintf("%s -> %s -> err executing fixes: %s", key, itemKey, err)
							}
							_log2["nrows_fixed"] = rowsAffected
						}
					}
					processLogs = append(processLogs, _log2)
				} else {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to get md conf string query: %s column %s", key, itemKey, query, column)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s -> successfully executed", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				_log2["nrows"] = 0
				processLogs = append(processLogs, _log2)
			}
			// QUERIES TO RUN AT THE END
			if okAfter {
				start3 := time.Now()
				_log2 := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
				}
				err = etlx.ExecuteQuery(dbConn, afterSQL, item, "", "", dateRef)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s After error: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s After ", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				}
				processLogs = append(processLogs, _log2)
			}
		}
		_log2["success"] = true
		_log2["msg"] = "Successfully loaded!"
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3)
		processLogs = append(processLogs, _log2)
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, conf, DATA_QUALITYRunner)
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
