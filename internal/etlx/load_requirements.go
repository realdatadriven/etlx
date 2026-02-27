package etlxlib

import (
	"fmt"
	"os"
	"time"
)

func (etlx *ETLX) LoadREQUIRES(conf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "REQUIRES"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	
	// Initialize OpenTelemetry context
	om := GetOTelManager()
	ctx, rootSpan := om.tracer.Start(om.ctx, "LoadREQUIRES")
	defer rootSpan.End()
	
	var processLogs []map[string]any
	start := time.Now()
	
	startLog := map[string]any{
		"name":     key,
		"start_at": start,
	}
	processLogs = append(processLogs, startLog)
	om.processLogs = append(om.processLogs, startLog)
	
	mainDescription := ""
	// Define the runner as a simple function
	REQUIRESRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				return fmt.Errorf("dectivated %s", "")
			}
		}
		mainConn, _ := metadata["connection"].(string)
		mainDescription = metadata["description"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			logEntry := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    time.Now(),
				"end_at":      time.Now(),
				"success":     true,
				"msg":         "Missing metadata in item",
			}
			processLogs = append(processLogs, logEntry)
			om.processLogs = append(om.processLogs, logEntry)
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				return nil
			}
		}
		
		// Create child span for this item
		_, itemSpan := om.tracer.Start(ctx, fmt.Sprintf("%s->%s", key, itemKey))
		defer itemSpan.End()
		
		start3 := time.Now()
		path, okPath := itemMetadata["path"]
		beforeSQL, okBefore := itemMetadata["before_sql"]
		query, okQuery := itemMetadata["query"]
		column, okColumn := itemMetadata["column"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		config := make(map[string]any)
		etl := &ETLX{Config: config, autoLogsDisabled: true}
		var mdConf any
		if okQuery && query != "" {
			conn, okCon := itemMetadata["connection"]
			if !okCon {
				conn = mainConn
			}
			dbConn, err := etlx.GetDB(conn.(string))
			if err != nil {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
					"success":     false,
					"msg":         fmt.Sprintf("%s -> %s ERR: connecting to %s in : %s", key, itemKey, conn, err),
					"end_at":      time.Now(),
					"duration":    time.Since(start3).Seconds(),
					"error":       err.Error(),
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				itemSpan.RecordError(err)
				return nil
			}
			defer dbConn.Close()
			
			logEntry := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    start3,
				"success":     true,
				"msg":         fmt.Sprintf("%s -> %s CONN: Connectinon to %s successfull", key, itemKey, conn),
				"end_at":      time.Now(),
				"duration":    time.Since(start3).Seconds(),
			}
			processLogs = append(processLogs, logEntry)
			om.processLogs = append(om.processLogs, logEntry)
			om.RecordEvent(itemSpan, "Connection established", map[string]any{"connection": conn})
			
			//  QUERIES TO RUN AT BEGINING
			if okBefore {
				start3 := time.Now()
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
				}
				err = etlx.ExecuteQuery(dbConn, beforeSQL, item, "", "", nil)
				if err != nil {
					logEntry["success"] = false
					logEntry["msg"] = fmt.Sprintf("%s -> %s Before error: %s", key, itemKey, err)
					logEntry["end_at"] = time.Now()
					logEntry["duration"] = time.Since(start3).Seconds()
					logEntry["error"] = err.Error()
					itemSpan.RecordError(err)
				} else {
					logEntry["success"] = true
					logEntry["msg"] = fmt.Sprintf("%s -> %s Before ", key, itemKey)
					logEntry["end_at"] = time.Now()
					logEntry["duration"] = time.Since(start3).Seconds()
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
			}
			// MAIN QUERY
			rows, _, err := etlx.Query(dbConn, query.(string), item, "", "", nil)
			// Fetch data from the database using the provided SQL query
			if err != nil {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
					"success":     false,
					"msg":         fmt.Sprintf("%s -> %s -> failed to execute get md conf query: %s", key, itemKey, err),
					"end_at":      time.Now(),
					"duration":    time.Since(start3).Seconds(),
					"error":       err.Error(),
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				itemSpan.RecordError(err)
				return nil
			}
			if len(*rows) > 0 {
				okConf := false
				if column != nil && okColumn {
					mdConf, okConf = (*rows)[0][column.(string)]
				} else {
					mdConf, okConf = (*rows)[0]["conf"]
				}
				if okConf && mdConf != nil {
					err := etl.ConfigFromMDText(mdConf.(string))
					if err != nil {
						logEntry := map[string]any{
							"name":        fmt.Sprintf("%s->%s", key, itemKey),
							"description": itemMetadata["description"].(string),
							"start_at":    start3,
							"success":     false,
							"msg":         fmt.Sprintf("Error parsing config string: %s", err),
							"end_at":      time.Now(),
							"duration":    time.Since(start3).Seconds(),
							"error":       err.Error(),
						}
						processLogs = append(processLogs, logEntry)
						om.processLogs = append(om.processLogs, logEntry)
						itemSpan.RecordError(err)
						return nil
					}
				} else {
					logEntry := map[string]any{
						"name":        fmt.Sprintf("%s->%s", key, itemKey),
						"description": itemMetadata["description"].(string),
						"start_at":    start3,
						"success":     false,
						"msg":         fmt.Sprintf("%s -> %s -> failed to get md conf string query: %s column %s", key, itemKey, query, column),
						"end_at":      time.Now(),
						"duration":    time.Since(start3).Seconds(),
					}
					processLogs = append(processLogs, logEntry)
					om.processLogs = append(om.processLogs, logEntry)
					return nil
				}
			} else {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
					"success":     false,
					"msg":         fmt.Sprintf("%s -> %s -> failed to execute get md conf query: %s", key, itemKey, err),
					"end_at":      time.Now(),
					"duration":    time.Since(start3).Seconds(),
					"error":       err.Error(),
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				return nil
			}
			// QUERIES TO RUN AT THE END
			if okAfter {
				start3 := time.Now()
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
				}
				err = etlx.ExecuteQuery(dbConn, afterSQL, item, "", "", nil)
				if err != nil {
					logEntry["success"] = false
					logEntry["msg"] = fmt.Sprintf("%s -> %s After error: %s", key, itemKey, err)
					logEntry["end_at"] = time.Now()
					logEntry["duration"] = time.Since(start3).Seconds()
					logEntry["error"] = err.Error()
					itemSpan.RecordError(err)
				} else {
					logEntry["success"] = true
					logEntry["msg"] = fmt.Sprintf("%s -> %s After ", key, itemKey)
					logEntry["end_at"] = time.Now()
					logEntry["duration"] = time.Since(start3).Seconds()
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
			}
		} else if path != nil && okPath {
			if ok, _ := fileExists(path.(string)); ok {
				err := etl.ConfigFromFile(path.(string))
				if err != nil {
					logEntry := map[string]any{
						"name":        fmt.Sprintf("%s->%s", key, itemKey),
						"description": itemMetadata["description"].(string),
						"start_at":    start3,
						"success":     false,
						"msg":         fmt.Sprintf("Error parsing config: %s -> %s", path, err),
						"end_at":      time.Now(),
						"duration":    time.Since(start3).Seconds(),
						"error":       err.Error(),
					}
					processLogs = append(processLogs, logEntry)
					om.processLogs = append(om.processLogs, logEntry)
					itemSpan.RecordError(err)
				}
			} else {
				logEntry := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    start3,
					"success":     false,
					"msg":         fmt.Sprintf("file doesn't exists: %s", path),
					"end_at":      time.Now(),
					"duration":    time.Since(start3).Seconds(),
				}
				processLogs = append(processLogs, logEntry)
				om.processLogs = append(om.processLogs, logEntry)
				return nil
			}
		}
		//fmt.Println("LOADED ETLX CONF:", etl.Config)
		if len(etl.Config) == 1 && etl.Config["__order"] != nil {
			etlx.Config[itemKey] = map[string]any{}
			if okQuery && query != "" && mdConf != nil {
				//etlx.Config[itemKey].(map[string]any)[itemKey] = mdConf.(string)
				etlx.Config[itemKey] = mdConf.(string)
			} else if path != nil && okPath {
				data, err := os.ReadFile(path.(string))
				if err != nil {
					fmt.Printf("LOAD RAW FILE: failed to read file: %s", err)
					om.RecordEvent(itemSpan, "File read error", map[string]any{"path": path, "error": err.Error()})
				} else {
					etlx.Config[itemKey] = string(data)
				}
			}
		} else {
			for newConfKey, value := range etl.Config {
				if newConfKey == "metadata" || newConfKey == "__order" || newConfKey == "order" {
					continue
				}
				if _, ok := etlx.Config[newConfKey]; !ok {
					etlx.Config[newConfKey] = value
				} else {
					fmt.Println(newConfKey, "Already exists!")
				}
			}
		}
		logEntry := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"start_at":    start3,
			"success":     true,
			"msg":         "Successfully loaded!",
			"end_at":      time.Now(),
			"duration":    time.Since(start3).Seconds(),
		}
		processLogs = append(processLogs, logEntry)
		om.processLogs = append(om.processLogs, logEntry)
		om.RecordEvent(itemSpan, "Config loaded successfully", map[string]any{})
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
		"duration":    time.Since(start).Seconds(),
	}
	om.processLogs[0] = processLogs[0]
	return processLogs, nil
}

