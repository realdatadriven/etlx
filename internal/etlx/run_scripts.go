package etlxlib

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (etlx *ETLX) RunSCRIPTS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "SCRIPTS"
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
	SCRIPTSRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
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
		if okMainPath {
			pth := etlx.ReplaceQueryStringDate(mainPath, dateRef)
			//fmt.Println("MAIN PATH", pth)
			if ok, _ := pathExists(pth); !ok {
				err := os.Mkdir(pth, 0755)
				if err != nil {
					return fmt.Errorf("%s ERR: trying to create the script path %s -> %s", key, pth, err)
				}
			}
		} else {

		}
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
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"key":         key, "item_key": itemKey, "start_at": start3,
		}
		beforeSQL, okBefore := itemMetadata["before_sql"]
		scriptSQL, okScript := itemMetadata["script_sql"]
		afterSQL, okAfter := itemMetadata["after_sql"]
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
		// FILE
		table := itemMetadata["name"].(string)
		path, okPath := itemMetadata["path"].(string)
		if !okPath {
			path, okPath = itemMetadata["fname"].(string)
			if !okPath {
				path, okPath = itemMetadata["file"].(string)
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
		// MAIN QUERIES
		if okScript {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, scriptSQL, item, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			}
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
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, conf, SCRIPTSRunner)
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
