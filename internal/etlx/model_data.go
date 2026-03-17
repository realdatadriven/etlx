package etlxlib

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func ResolveFileContentSafe(filename string, allowedDir string) (string, error) {

	// Clean and resolve path
	cleanPath := filepath.Clean(filename)
	if cleanPath != filename || strings.Contains(filename, "..") {
		return "", errors.New("invalid filename - path traversal attempt?")
	}

	// Join with base directory and resolve to absolute path
	fullPath := filepath.Join(allowedDir, filename)
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}

	// Security: must be inside allowedDir
	absBase, err := filepath.Abs(allowedDir)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absPath, absBase+string(filepath.Separator)) &&
		absPath != absBase {
		return "", errors.New("file path is outside of allowed directory")
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("cannot read %q: %w", filename, err)
	}

	return string(content), nil
}
func InsertOrUpdate(dbConn any, table string, data map[string]any) (any, error) {
	// This is a placeholder function. You should implement the actual logic to insert or update data in your database.
	// The implementation will depend on the database driver you are using (e.g., database/sql, gorm, etc.).
	// For example, you might want to check if a record with a certain primary key exists and then decide to insert or update accordingly.
	return nil, nil
}
func (etlx *ETLX) RunMODEL_DATA(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "MODEL_DATA"
	process := "MODEL_DATA"
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
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing metadata in %s section", key)
	}
	// ACTIVE
	if active, okActive := metadata["active"]; okActive {
		if !active.(bool) {
			processLogs = append(processLogs, map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("KEY %s", key),
				"description": metadata["description"].(string),
				"key":         key,
				"start_at":    time.Now(),
				"end_at":      time.Now(),
				"success":     true,
				"msg":         "Deactivated",
			})
			return nil, fmt.Errorf("%s deactivated", key)
		}
	}
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
	conn, okCon := metadata["connection"].(string)
	if !okCon {
		conn, okCon = metadata["conn"].(string)
		if !okCon {
			return nil, fmt.Errorf("%s err no connection defined", key)
		}
	}
	start3 := time.Now()
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2 := map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              start3,
		"ref":                   dtRef,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	}
	dbConn, err := etlx.GetDB(conn)
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
		return nil, fmt.Errorf("%s ERR: connecting to %s in : %s", key, conn, err)
	}
	defer dbConn.Close()
	// fmt.Println("CONN:", conn)
	order := []string{}
	__order, okOrder := data["__order"].([]any)
	if !okOrder {
		for key := range data {
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
		// // fmt.Println("ITEM KEY:", itemKey)
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			continue
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
		table, ok := itemMetadata.(map[string]any)["table"].(string)
		if !ok {
			continue
		}
		//fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, driver, comment)
		data, ok := itemMetadata.(map[string]any)["data"].(map[string]any)
		if !ok {
			continue
		}
		start3 = time.Now()
		desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
		if !okDesc {
			desc = fmt.Sprintf("%s->%s", key, itemKey)
		}
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"process":     process,
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": desc,
			"key":         key, "item_key": itemKey, "start_at": start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		//createTableSQL := generateCreateTableSQL(driver, table, comment, create_all, columns)
		// fmt.Println("CREATE TABLE SQL:\n", createTableSQL)
		// each key in data
		fileContentPattern := regexp.MustCompile(`^FileContent\((.+)\)$`)
		nowPattern := regexp.MustCompile(`^Now\(\)$`)
		for colName, input := range data {
			// switch type of _data to string or map
			switch v := input.(type) {
			case string:
				matches := fileContentPattern.FindStringSubmatch(strings.TrimSpace(input.(string)))
				if len(matches) != 2 {
					// pass
				} else {
					filename := strings.TrimSpace(matches[1])
					if filename != "" {
						data[colName], err = ResolveFileContentSafe(filename, "./")
						if err != nil {
							fmt.Printf("%s ERR: resolving file content for %s: %s", key, filename, err)
						}
					}
				}
				matchesNow := nowPattern.FindStringSubmatch(strings.TrimSpace(input.(string)))
				if len(matchesNow) == 1 {
					data[colName] = time.Now() //.Format("2006-01-02 15:04:05")
				}
			case map[string]any:
				// do nothing
			default:
				println(table, colName, v)
			}
		}
		// insert into table dbConn, table, data		
		_, err := InsertOrUpdate(dbConn, table, data)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: creating table %s: %s", key, table, err)
			processLogs = append(processLogs, _log2)
		} else {
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s: table %s created or already exists", key, table)
			processLogs = append(processLogs, _log2)
		}
	}
	mem_alloc2, mem_total_alloc2, mem_sys2, num_gc2 := etlx.RuntimeMemStats()
	processLogs[0] = map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              processLogs[0]["start_at"],
		"end_at":                time.Now(),
		"duration":              time.Since(start).Seconds(),
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
		"mem_alloc_end":         mem_alloc2,
		"mem_total_alloc_end":   mem_total_alloc2,
		"mem_sys_end":           mem_sys2,
		"num_gc_end":            num_gc2,
	}
	return processLogs, nil
}
