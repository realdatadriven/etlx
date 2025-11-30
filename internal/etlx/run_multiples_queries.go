package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

func (etlx *ETLX) RunMULTI_QUERIES(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, []map[string]any, error) {
	key := "MULTI_QUERIES"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processData []map[string]any
	var processLogs []map[string]any
	start := time.Now()
	mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
	processLogs = append(processLogs, map[string]any{
		"name": key,
		"key":  key, "start_at": start,
		"ref":                   nil,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	})
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("missing metadata in %s section", key)
	}
	// ACTIVE
	if active, okActive := metadata["active"]; okActive {
		if !active.(bool) {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("KEY %s", key),
				"description": metadata["description"].(string),
				"key":         key,
				"start_at":    time.Now(),
				"end_at":      time.Now(),
				"success":     true,
				"msg":         "Deactivated",
			})
			return nil, nil, fmt.Errorf("%s deactivated", key)
		}
	}
	beforeSQL, okBefore := metadata["before_sql"]
	afterSQL, okAfter := metadata["after_sql"]
	saveSQL, okSave := metadata["save_sql"]
	errPatt, okErrPatt := metadata["save_on_err_patt"]
	errSQL, okErrSQL := metadata["save_on_err_sql"]
	dtRef, okDtRef := metadata["date_ref"]
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
	queries := []string{}
	order := []string{}
	__order, okOrder := data["__order"].([]any)
	if !okOrder {
		for key, _ := range data {
			order = append(order, key)
		}
	} else {
		for _, itemKey := range __order {
			order = append(order, itemKey.(string))
		}
	}
	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			//fmt.Println(itemKey, "NOT A MAP:", item)
			continue
		}
		/*if only, okOnly := extraConf["only"]; okOnly {
			if len(only.([]string)) == 0 {
			} else if !etlx.Contains(only.([]string), itemKey) {
				continue
			}
		}*/
		if skip, okSkip := extraConf["skip"]; okSkip {
			if len(skip.([]string)) == 0 {
			} else if etlx.Contains(skip.([]string), itemKey) {
				continue
			}
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
		// ACTIVE
		if active, okActive := itemMetadata.(map[string]any)["active"]; okActive {
			if !active.(bool) {
				continue
			}
		}
		query, okQuery := itemMetadata.(map[string]any)["query"]
		if query != nil && okQuery {
			sql := query.(string)
			query, ok := item.(map[string]any)[sql].(string)
			_, queryDoc := etlx.Config[sql]
			if !ok && queryDoc {
				query = sql
				_sql, _, _, err := etlx.QueryBuilder(nil, sql)
				if err != nil {
					fmt.Printf("QUERY DOC ERR ON KEY %s: %v\n", queries, err)
					_q, _e := etlx.Config[sql].(string)
					//fmt.Println(sql, "IS A LOADED SQL STR QUERY?", _q, _e)
					if _e {
						query = _q
					}
				} else {
					query = _sql
				}
			}
			sql = etlx.SetQueryPlaceholders(query, "", "", dateRef)
			queries = append(queries, sql)
		}
	}
	conn, okCon := metadata["connection"]
	if !okCon {
		return nil, nil, fmt.Errorf("%s err no connection defined", key)
	}
	start3 := time.Now()
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2 := map[string]any{
		"name":        key,
		"description": metadata["description"].(string),
		"key":         key, "start_at": start3,
		"ref":                   dtRef,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	}
	dbConn, err := etlx.GetDB(conn.(string))
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2["mem_alloc_end"] = mem_alloc
	_log2["mem_total_alloc_end"] = mem_total_alloc
	_log2["mem_sys_end"] = mem_sys
	_log2["num_gc_end"] = num_gc
	if err != nil {
		_log2["success"] = false
		_log2["msg"] = fmt.Sprintf("%s ERR: connecting to %s in : %s", key, conn, err)
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		processLogs = append(processLogs, _log2)
		return nil, nil, fmt.Errorf("%s ERR: connecting to %s in : %s", key, conn, err)
	}
	defer dbConn.Close()
	_log2["success"] = true
	_log2["msg"] = fmt.Sprintf("%s CONN: connection to %s successfull", key, conn)
	_log2["end_at"] = time.Now()
	_log2["duration"] = time.Since(start3).Seconds()
	processLogs = append(processLogs, _log2)
	//  QUERIES TO RUN AT beginning
	if okBefore {
		start3 := time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"name":        key,
			"description": metadata["description"].(string),
			"key":         key, "start_at": start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		err = etlx.ExecuteQuery(dbConn, beforeSQL, data, "", "", dateRef)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s Before error: %s", key, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
		} else {
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s Before ", key)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
		}
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		processLogs = append(processLogs, _log2)
	}
	// MAIN QUERY
	unionKey, ok := metadata["union_key"].(string)
	if !ok {
		unionKey = "UNION\n"
	}
	sql := strings.Join(queries, unionKey)
	// fmt.Println(key, sql)
	start3 = time.Now()
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2 = map[string]any{
		"name":        key,
		"description": metadata["description"].(string),
		"key":         key, "start_at": start3,
		"ref":                   dtRef,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	}
	// CHECK CONDITION
	condition, okCondition := metadata["condition"].(string)
	condMsg, okCondMsg := metadata["condition_msg"].(string)
	failedCondition := false
	if okCondition && condition != "" {
		cond, err := etlx.ExecuteCondition(dbConn, condition, metadata, "", "", dateRef)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, "", err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			processLogs = append(processLogs, _log2)
			//return fmt.Errorf("%s", _log2["msg"])
			failedCondition = true
		} else if !cond {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed the condition %s was not met!", key, "", condition)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			if okCondMsg && condMsg != "" {
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, "", etlx.SetQueryPlaceholders(condMsg, "", "", dateRef))
			}
			processLogs = append(processLogs, _log2)
			// return fmt.Errorf("%s", _log2["msg"])
			failedCondition = true
		}
	}
	if saveSQL != "" && okSave && !failedCondition {
		data["final_query"] = sql // PUT THE QUERY GENERATED IN THE SCOPE
		// fmt.Println(data[saveSQL.(string)])
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["mem_alloc_start"] = mem_alloc
		_log2["mem_total_alloc_start"] = mem_total_alloc
		_log2["mem_sys_start"] = mem_sys
		_log2["num_gc_start"] = num_gc
		err = etlx.ExecuteQuery(dbConn, saveSQL, data, "", "", dateRef)
		if err != nil {
			_err_by_pass := false
			if okErrPatt && errPatt != nil && okErrSQL && errSQL != nil {
				//fmt.Println(onErrPatt.(string), onErrSQL.(string))
				re, regex_err := regexp.Compile(errPatt.(string))
				if regex_err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s ERR: fallback regex matching the error failed to compile: %s", key, regex_err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
				} else if re.MatchString(string(err.Error())) {
					err = etlx.ExecuteQuery(dbConn, errSQL, data, "", "", dateRef)
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("%s ERR: main: %s", key, err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3).Seconds()
					} else {
						_err_by_pass = true
					}
				}
			}
			if !_err_by_pass {
				//return nil, fmt.Errorf("%s ERR: main: %s", key, err)
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s ERR: main: %s", key, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s main ", key)
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
			_log2["msg"] = fmt.Sprintf("%s main ", key)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
		}
		processLogs = append(processLogs, _log2)
	} else if !failedCondition {
		rows, _, err := etlx.Query(dbConn, sql, data, "", "", dateRef)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s After error: %s", key, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
		} else {
			processData = *rows
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s After ", key)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
		}
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		processLogs = append(processLogs, _log2)
	}
	//  QUERIES TO RUN AT THE END
	if okAfter {
		start3 := time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"name":        key,
			"description": metadata["description"].(string),
			"key":         key, "start_at": start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		err = etlx.ExecuteQuery(dbConn, afterSQL, data, "", "", dateRef)
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s After error: %s", key, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
		} else {
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s After ", key)
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
	return processLogs, processData, nil
}
