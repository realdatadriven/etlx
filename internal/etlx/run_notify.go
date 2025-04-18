package etlxlib

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (etlx *ETLX) RunNOTIFY(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "NOTIFY"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	processLogs = append(processLogs, map[string]any{
		"name": key,
		"key":  key, "start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	NOTIFYRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return fmt.Errorf("dectivated %s", "")
			}
		}
		// MAIN PATH
		mainPath, okMainPath := metadata["path"].(string)
		mainConn, _ := metadata["connection"].(string)
		mainDescription = metadata["description"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": time.Now(),
				"end_at":  time.Now(),
				"success": true,
				"msg":     "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return nil
			}
		}
		beforeSQL, okBefore := itemMetadata["before_sql"]
		dataSQL, okData := itemMetadata["data_sql"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		conn, okCon := itemMetadata["connection"]
		if !okCon {
			conn = mainConn
		}
		dtRef, okDtRef := itemMetadata["date_ref"]
		if okDtRef && dtRef != "" {
			_dt, err := time.Parse("2006-01-02", dtRef.(string))
			if err == nil {
				dateRef = append([]time.Time{}, _dt)
			}
		} else {
			if len(dateRef) > 0 {
				dtRef = dateRef[0].Format("2006-01-02")
			}
		}
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"key":         key, "item_key": itemKey, "start_at": start3,
			"ref": dtRef,
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
		// FILE
		table := itemMetadata["name"].(string)
		path, okPath := itemMetadata["path"].(string)
		if !okPath {
			if okMainPath {
				var pth any = mainPath
				itemMetadata["path"] = pth
			}
		}
		fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
		if okPath && path != "" {
			fname = path
			if filepath.IsAbs(fname) {
			} else if filepath.IsLocal(fname) {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			} else if filepath.Dir(fname) != "" && okMainPath && mainPath != "" {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			}
		} else if okMainPath && mainPath != "" {
			fname = fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, mainPath, table)
		}
		// QUERIES TO RUN AT BEGINING
		if okBefore {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, "", dateRef)
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
		// CHECK CONDITION
		condition, okCondition := itemMetadata["condition"].(string)
		condMsg, okCondMsg := itemMetadata["condition_msg"].(string)
		failedCondition := false
		if okCondition && condition != "" {
			cond, err := etlx.ExecuteCondition(dbConn, condition, itemMetadata, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				processLogs = append(processLogs, _log2)
				//return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			} else if !cond {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed the condition %s was not met!", key, itemKey, condition)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				if okCondMsg && condMsg != "" {
					_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, etlx.SetQueryPlaceholders(condMsg, table, fname, dateRef))
				}
				processLogs = append(processLogs, _log2)
				// return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			}
		}
		data := map[string]any{}
		// MAIN QUERIES
		if okData && !failedCondition {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			switch _map := dataSQL.(type) {
			case string:
				sql := _map
				if _, ok := item[_map]; ok {
					sql = item[sql].(string)
				}
				sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
				rows, _, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
				if err != nil {
					data[_map] = map[string]any{
						"success": false,
						"msg":     fmt.Sprintf("Eailed to execute map query %s %s", _map, err),
						"data":    []map[string]any{},
					}
				} else {
					data[_map] = map[string]any{
						"success": true,
						"data":    *rows,
					}
				}
			case []any:
				for _, _sql := range dataSQL.([]any) {
					sql := _sql.(string)
					if _, ok := item[_sql.(string)]; ok {
						sql = item[_sql.(string)].(string)
					}
					sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
					rows, _, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
					if err != nil {
						data[_sql.(string)] = map[string]any{
							"success": false,
							"msg":     fmt.Sprintf("Eailed to execute map query %s %s", _map, err),
							"data":    []map[string]any{},
						}
					} else {
						data[_sql.(string)] = map[string]any{
							"success": true,
							"data":    *rows,
						}
					}
				}
			default:
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s invalid queries data type: %T", key, itemKey, _map)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			}
			if _, ok := itemMetadata["data"].(map[string]any); ok {
				for key, d := range data {
					itemMetadata["data"].(map[string]any)[key] = d
				}
			} else {
				itemMetadata["data"] = data
			}
			itemMetadata["subject"] = etlx.SetQueryPlaceholders(itemMetadata["subject"].(string), table, fname, dateRef)
			body, ok := item[itemMetadata["body"].(string)].(string)
			if ok {
				itemMetadata["body"] = body
			}
			//itemMetadata["body"] = etlx.SetQueryPlaceholders(itemMetadata["body"].(string), table, fname, dateRef)
			attachments, okAtt := itemMetadata["attachments"].([]any)
			atts := []any{}
			var aux_att any
			if okAtt {
				for _, att := range attachments {
					aux_att = etlx.SetQueryPlaceholders(att.(string), table, fname, dateRef)
					// fmt.Println("ATT:", aux_att)
					atts = append(atts, aux_att)
				}
				itemMetadata["attachments"] = atts
			}
			err := etlx.SendEmail(itemMetadata)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s err sending email: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s Notefication sent!", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			}
			//fmt.Println(key, _log2["msg"])
			processLogs = append(processLogs, _log2)
		}
		// QUERIES TO RUN AT THE END
		if okAfter {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, "", dateRef)
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
		// fmt.Println(processLogs)
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, conf, NOTIFYRunner)
	if err != nil {
		return processLogs, fmt.Errorf("%s failed: %v", key, err)
	}
	processLogs[0] = map[string]any{
		"name":        key,
		"description": mainDescription,
		"key":         key, "start_at": processLogs[0]["start_at"],
		"end_at":   time.Now(),
		"duration": time.Since(start),
	}
	return processLogs, nil
}
