package etlxlib

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

func (etlx *ETLX) RunSCRIPTS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "SCRIPTS"
	process := "SCRIPTS"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
	processLogs = append(processLogs, map[string]any{
		"process": process,
		"name":    key,
		"key":     key, "start_at": start,
		"ref":                   nil,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	})
	mainDescription := ""
	// Define the runner as a simple function
	SCRIPTSRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"process":     process,
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return fmt.Errorf("deactivated %s", "")
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
				"process":     process,
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
					"process":     process,
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
		scriptSQL, okScript := itemMetadata["script_sql"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		errPatt, okErrPatt := itemMetadata["on_err_patt"]
		errSQL, okErrSQL := itemMetadata["on_err_sql"]
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
		if processLogs[0]["ref"] == nil {
			processLogs[0]["ref"] = dtRef
		}
		start3 := time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
		_log2 := map[string]any{
			"process":     process,
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"key":         key, "item_key": itemKey, "start_at": start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		dbConn, err := etlx.GetDB(conn.(string))
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s ERR: connecting to %s in : %s", key, itemKey, conn, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			processLogs = append(processLogs, _log2)
			return nil
		}
		defer dbConn.Close()
		_log2["success"] = true
		_log2["msg"] = fmt.Sprintf("%s -> %s CONN: connection to %s successfull", key, itemKey, conn)
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
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
		fname := fmt.Sprintf(`%s/%s_{YYYYMMDD}.csv`, os.TempDir(), table)
		if okPath && path != "" {
			fname = path
			if filepath.IsAbs(fname) {
			} else if filepath.IsLocal(fname) {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			} else if filepath.Dir(fname) != "" && okMainPath && mainPath != "" {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			}
		} else if okMainPath && mainPath != "" {
			fname = fmt.Sprintf(`%s/%s_{YYYYMMDD}.csv`, mainPath, table)
		}
		// QUERIES TO RUN AT beginning
		if okBefore {
			start3 := time.Now()
			_log2 := map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
				"ref":                   dtRef,
				"mem_alloc_start":       mem_alloc,
				"mem_total_alloc_start": mem_total_alloc,
				"mem_sys_start":         mem_sys,
				"num_gc_start":          num_gc,
			}
			err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, "", dateRef)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s Before error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s Before ", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			}
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			processLogs = append(processLogs, _log2)
		}
		// CHECK CONDITION
		condition, okCondition := itemMetadata["condition"].(string)
		condMsg, okCondMsg := itemMetadata["condition_msg"].(string)
		failedCondition := false
		if okCondition && condition != "" {
			cond, err := etlx.ExecuteCondition(dbConn, condition, itemMetadata, fname, "", dateRef)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
				processLogs = append(processLogs, _log2)
				//return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			} else if !cond {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed the condition %s was not met!", key, itemKey, condition)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
				if okCondMsg && condMsg != "" {
					_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, etlx.SetQueryPlaceholders(condMsg, table, fname, dateRef))
				}
				processLogs = append(processLogs, _log2)
				// return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			}
		}
		// MAIN QUERIES
		if okScript && !failedCondition {
			start3 := time.Now()
			mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
			_log2 := map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
				"ref":                   dtRef,
				"mem_alloc_start":       mem_alloc,
				"mem_total_alloc_start": mem_total_alloc,
				"mem_sys_start":         mem_sys,
				"num_gc_start":          num_gc,
			}
			err = etlx.ExecuteQuery(dbConn, scriptSQL, item, fname, "", dateRef)
			if err != nil {
				_err_by_pass := false
				if okErrPatt && errPatt != nil && okErrSQL && errSQL != nil {
					//fmt.Println(onErrPatt.(string), onErrSQL.(string))
					re, regex_err := regexp.Compile(errPatt.(string))
					if regex_err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Errorf("%s ERR: fallback regex matching the error failed to compile: %s", key, regex_err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3).Seconds()
					} else if re.MatchString(string(err.Error())) {
						err = etlx.ExecuteQuery(dbConn, errSQL, item, fname, "", dateRef)
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Errorf("%s ERR: main: %s", key, err)
							_log2["end_at"] = time.Now()
							_log2["duration"] = time.Since(start3).Seconds()
						} else {
							_err_by_pass = true
						}
					}
				}
				if !_err_by_pass {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s error: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s Success", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
				}
				mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s Success", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
			}
			processLogs = append(processLogs, _log2)
		}
		// QUERIES TO RUN AT THE END
		if okAfter {
			start3 := time.Now()
			mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
			_log2 := map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
				"ref":                   dtRef,
				"mem_alloc_start":       mem_alloc,
				"mem_total_alloc_start": mem_total_alloc,
				"mem_sys_start":         mem_sys,
				"num_gc_start":          num_gc,
			}
			err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s After error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s After ", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			}
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
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
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	if err != nil {
		return processLogs, fmt.Errorf("%s failed: %v", key, err)
	}
	processLogs[0] = map[string]any{
		"process":     process,
		"name":        key,
		"description": mainDescription,
		"key":         key, "start_at": processLogs[0]["start_at"],
		"end_at":                time.Now(),
		"duration":              time.Since(start).Seconds(),
		"ref":                   processLogs[0]["ref"],
		"mem_alloc_start":       processLogs[0]["mem_alloc_start"],
		"mem_total_alloc_start": processLogs[0]["mem_total_alloc_start"],
		"mem_sys_start":         processLogs[0]["mem_sys_start"],
		"num_gc_start":          processLogs[0]["num_gc_start"],
	}
	return processLogs, nil
}
