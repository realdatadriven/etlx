package etlx

import (
	"fmt"
	"time"
)

func (etlx *ETLX) LoadREQUIRES(conf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "REQUIRES"
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
	REQUIRESRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
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
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"start_at":    start3,
		}
		path, okPath := itemMetadata["path"]
		beforeSQL, okBefore := itemMetadata["before_sql"]
		query, okQuery := itemMetadata["query"]
		column, okColumn := itemMetadata["column"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		config := make(map[string]any)
		etl := &ETLX{Config: config}
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
				err = etlx.ExecuteQuery(dbConn, beforeSQL, item, "", "", nil)
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
			rows, _, err := etlx.Query(dbConn, query.(string), item, "", "", nil)
			// Fetch data from the database using the provided SQL query
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to execute get md conf query: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				return nil
			}
			if len(*rows) > 0 {
				var mdConf any
				okConf := false
				if column != nil && okColumn {
					mdConf, okConf = (*rows)[0][column.(string)]
				} else {
					mdConf, okConf = (*rows)[0]["conf"]
				}
				if okConf && mdConf != nil {
					err := etl.ConfigFromMDText(mdConf.(string))
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("Error parsing config: %s -> %s", path, err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3)
						processLogs = append(processLogs, _log2)
						return nil
					}
				} else {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to get md conf string query: %s column %s", key, itemKey, query, column)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
			} else {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to execute get md conf query: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				return nil
			}
			// QUERIES TO RUN AT THE END
			if okAfter {
				start3 := time.Now()
				_log2 := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
				}
				err = etlx.ExecuteQuery(dbConn, afterSQL, item, "", "", nil)
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
		} else if path != nil && okPath {
			if ok, _ := fileExists(path.(string)); ok {
				err := etl.ConfigFromFile(path.(string))
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("Error parsing config: %s -> %s", path, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
				}
			} else {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("file doesn't exists: %s", path)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				return nil
			}
		}
		for newConfKey, value := range etl.Config {
			if newConfKey == "metadata" || newConfKey == "__order" || newConfKey == "order" {
				continue
			}
			if _, ok := etlx.Config[newConfKey]; !ok {
				etlx.Config[newConfKey] = value
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
	err := etlx.ProcessMDKey(key, conf, REQUIRESRunner)
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
