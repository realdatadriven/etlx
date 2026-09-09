package etlxlib

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
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
	var processLogsMu sync.Mutex
	appendLog := func(logEntry map[string]any) {
		processLogsMu.Lock()
		defer processLogsMu.Unlock()
		processLogs = append(processLogs, logEntry)
		formatProcessLogEntry(logEntry)
	}
	setProcessRef := func(ref any) {
		if ref == nil {
			return
		}
		processLogsMu.Lock()
		defer processLogsMu.Unlock()
		if processLogs[0]["ref"] == nil {
			processLogs[0]["ref"] = ref
		}
	}
	start := time.Now().In(etlx.TimeZone)
	mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
	appendLog(map[string]any{
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
		itemDateRef := append([]time.Time(nil), dateRef...)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				appendLog(map[string]any{
					"process":     process,
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now().In(etlx.TimeZone),
					"end_at":  time.Now().In(etlx.TimeZone),
					"success": true,
					"msg":     "Deactivated",
				})
				return fmt.Errorf("deactivated %s", "")
			}
		}
		// MAIN PATH
		mainPath, okMainPath := metadata["path"].(string)
		if okMainPath {
			pth := etlx.ReplaceQueryStringDate(mainPath, itemDateRef)
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
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			appendLog(map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": time.Now().In(etlx.TimeZone),
				"end_at":  time.Now().In(etlx.TimeZone),
				"success": true,
				"msg":     "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				appendLog(map[string]any{
					"process":     process,
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now().In(etlx.TimeZone),
					"end_at":  time.Now().In(etlx.TimeZone),
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
				itemDateRef = []time.Time{_dt}
			}
		} else {
			if len(itemDateRef) > 0 {
				dtRef = itemDateRef[0].Format("2006-01-02")
			}
		}
		setProcessRef(dtRef)
		start3 := time.Now().In(etlx.TimeZone)
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
			_log2["end_at"] = time.Now().In(etlx.TimeZone)
			_log2["duration"] = time.Since(start3).Seconds()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			appendLog(_log2)
			return nil
		}
		defer dbConn.Close()
		_log2["success"] = true
		_log2["msg"] = fmt.Sprintf("%s -> %s CONN: connection to %s successfull", key, itemKey, conn)
		_log2["end_at"] = time.Now().In(etlx.TimeZone)
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		appendLog(_log2)
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
			start3 := time.Now().In(etlx.TimeZone)
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
			err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, "", itemDateRef)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s Before error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s Before ", key, itemKey)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
			}
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			appendLog(_log2)
		}
		// CHECK CONDITION
		condition, okCondition := itemMetadata["condition"].(string)
		condMsg, okCondMsg := itemMetadata["condition_msg"].(string)
		failedCondition := false
		if okCondition && condition != "" {
			cond, err := etlx.ExecuteCondition(dbConn, condition, itemMetadata, fname, "", itemDateRef)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, err)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
				appendLog(_log2)
				//return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			} else if !cond {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed the condition %s was not met!", key, itemKey, condition)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
				if okCondMsg && condMsg != "" {
					_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, etlx.SetQueryPlaceholders(condMsg, table, fname, itemDateRef))
				}
				appendLog(_log2)
				// return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			}
		}
		// MAIN QUERIES
		if okScript && !failedCondition {
			start3 := time.Now().In(etlx.TimeZone)
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
			err = etlx.ExecuteQuery(dbConn, scriptSQL, item, fname, "", itemDateRef)
			if err != nil {
				_err_by_pass := false
				if okErrPatt && errPatt != nil && okErrSQL && errSQL != nil {
					//fmt.Println(onErrPatt.(string), onErrSQL.(string))
					re, regex_err := regexp.Compile(errPatt.(string))
					if regex_err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Errorf("%s ERR: fallback regex matching the error failed to compile: %s", key, regex_err)
						_log2["end_at"] = time.Now().In(etlx.TimeZone)
						_log2["duration"] = time.Since(start3).Seconds()
					} else if re.MatchString(string(err.Error())) {
						err = etlx.ExecuteQuery(dbConn, errSQL, item, fname, "", itemDateRef)
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Errorf("%s ERR: main: %s", key, err)
							_log2["end_at"] = time.Now().In(etlx.TimeZone)
							_log2["duration"] = time.Since(start3).Seconds()
						} else {
							_err_by_pass = true
						}
					}
				}
				if !_err_by_pass {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s error: %s", key, itemKey, err)
					_log2["end_at"] = time.Now().In(etlx.TimeZone)
					_log2["duration"] = time.Since(start3).Seconds()
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s Success", key, itemKey)
					_log2["end_at"] = time.Now().In(etlx.TimeZone)
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
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
				mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
				_log2["mem_alloc_end"] = mem_alloc
				_log2["mem_total_alloc_end"] = mem_total_alloc
				_log2["mem_sys_end"] = mem_sys
				_log2["num_gc_end"] = num_gc
			}
			appendLog(_log2)
		}
		// QUERIES TO RUN AT THE END
		if okAfter {
			start3 := time.Now().In(etlx.TimeZone)
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
			err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, "", itemDateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s After error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s After ", key, itemKey)
				_log2["end_at"] = time.Now().In(etlx.TimeZone)
				_log2["duration"] = time.Since(start3).Seconds()
			}
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			appendLog(_log2)
		}
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	if data, ok := conf[key].(map[string]any); ok {
		if metadata, ok := data["metadata"].(map[string]any); ok {
			mainDescription, _ = metadata["description"].(string)
		}
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
		"end_at":                time.Now().In(etlx.TimeZone),
		"duration":              time.Since(start).Seconds(),
		"ref":                   processLogs[0]["ref"],
		"mem_alloc_start":       processLogs[0]["mem_alloc_start"],
		"mem_total_alloc_start": processLogs[0]["mem_total_alloc_start"],
		"mem_sys_start":         processLogs[0]["mem_sys_start"],
		"num_gc_start":          processLogs[0]["num_gc_start"],
	}
	return processLogs, nil
}
