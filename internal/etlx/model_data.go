package etlxlib

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
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
func (etlx *ETLX) InsertOrUpdate(dbCon db.DBInterface, table string, cond string, data map[string]any) (any, error) {
	var whereClause string
	var whereClause2 string
	_chk_params := []any{}
	if cond != "" {
		whereClause = cond
		_sql, args, err := etlx.NamedToPositional(cond, data)
		if err != nil {
			return nil, fmt.Errorf("failed to convert named parameters: %w", err)
		}
		whereClause2 = _sql
		_chk_params = args
	}
	dialect := GetDialect(dbCon.GetDriverName())
	var exists bool
	checkQuery := fmt.Sprintf(`SELECT * FROM %s %s LIMIT 1`, dialect.GetTableName(table), whereClause2)
	res, _, err := dbCon.QueryMultiRows(checkQuery, _chk_params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("existence check failed %s : %w", table, err)
	} else if len(*res) > 0 {
		exists = true
	} else {
		exists = false
	}
	if exists {
		updateParts := []string{}
		for k := range data {
			if k == "__order" || k == "__frontmatter" || k == "created_at" {
				continue
			}
			updateParts = append(updateParts, fmt.Sprintf(`%s = :%s`, dialect.GetColumnName(k), k))
		}
		updateQuery := fmt.Sprintf(`UPDATE %s SET %s %s`, dialect.GetTableName(table), strings.Join(updateParts, ", "), whereClause)
		_, err := dbCon.ExecuteNamedQuery(updateQuery, data)
		if err != nil {
			return nil, fmt.Errorf("update failed %s: %w,\n query: %s,\n data: %v", table, err, updateQuery, data)
		}
	} else {
		cols := []string{}
		names := []string{}
		for k := range data {
			if k == "__order" || k == "__frontmatter" {
				continue
			}
			cols = append(cols, dialect.GetColumnName(k))
			names = append(names, ":"+k)
		}
		insertQuery := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, dialect.GetTableName(table), strings.Join(cols, ", "), strings.Join(names, ", "))
		_, err := dbCon.ExecuteNamedQuery(insertQuery, data)
		if err != nil {
			return nil, fmt.Errorf("insert failed %s: %w", table, err)
		}
	}
	return nil, nil
}

// NamedToPositional converts named parameters (:name) to positional (?)
// and returns ordered arguments slice
func (etlx *ETLX) NamedToPositional(sql string, data map[string]any) (string, []any, error) {
	// Find all :param occurrences
	re := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)
	matches := re.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return sql, nil, nil
	}
	// Collect unique parameter names in order of appearance
	seen := make(map[string]bool)
	var paramOrder []string
	for _, match := range matches {
		name := match[1]
		if !seen[name] {
			seen[name] = true
			paramOrder = append(paramOrder, name)
		}
	}
	// Check for missing values
	var args []any
	for _, name := range paramOrder {
		val, ok := data[name]
		if !ok {
			return "", nil, fmt.Errorf("missing value for parameter: %s", name)
		}
		args = append(args, val)
	}
	// Replace :name → ?
	result := re.ReplaceAllStringFunc(sql, func(s string) string {
		// We could also do a map lookup here, but simpler to just replace with ?
		return "?"
	})
	return result, args, nil
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
	database, okDb := metadata["database"].(string)
	if !okDb {
		database, okDb = metadata["name"].(string)
		if !okDb {
			return nil, fmt.Errorf("%s err no database defined", key)
		}
	}
	adminConn, okAdminCon := metadata["admin_connection"].(string)
	if !okAdminCon {
		adminConn, _ = metadata["admin_conn"].(string)
	}
	conn, okCon := metadata["connection"].(string)
	if !okCon {
		conn, okCon = metadata["conn"].(string)
		if database != "" && conn == "" && adminConn != "" {
			// conn will be the admin with the database name replaced
			conn, _ = db.ReplaceDBNameV2(etlx.ReplaceEnvVariable(adminConn), database)
			// fmt.Println("CONN FROM ADMIN CON:", conn, adminConn)
			okCon = true
		} else if !okCon {
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
	dialect := GetDialect(dbConn.GetDriverName())
	app := map[string]any{}
	_sql := `SELECT app_id FROM app WHERE db = ? AND excluded = ? --  LIMIT 1`
	_app, _, err := dbConn.QuerySingleRow(_sql, []any{database, dialect.GetBooleanValue(false)}...)
	if err != nil {
		return fmt.Errorf("find app failed: %w", err)
	}
	if len(*_app) > 0 {
		app = (*_app)
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
		cond, _ := itemMetadata.(map[string]any)["cond"].(string)
		// fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, conn, itemMetadata.(map[string]any)["description"])
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
			"process":               process,
			"name":                  fmt.Sprintf("%s->%s", key, itemKey),
			"description":           desc,
			"key":                   key,
			"item_key":              itemKey,
			"start_at":              start3,
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
		appPatterm := regexp.MustCompile(`^appId\(\)$`) 
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
						data[colName], err = ResolveFileContentSafe(filename, "")
						if err != nil {
							fmt.Printf("%s ERR: resolving file content for %s: %s %v", key, filename, err, v)
						}
					}
				}
				matchesNow := nowPattern.FindStringSubmatch(strings.TrimSpace(input.(string)))
				if len(matchesNow) == 1 {
					data[colName] = time.Now() //.Format("2006-01-02 15:04:05")
				}
				matchesApp := appPatterm.FindStringSubmatch(strings.TrimSpace(input.(string)))
				if len(matchesApp) == 1 {
					data[colName] = app["app_id"]
				}
			case map[string]any:
				// do nothing
			default:
				//println(table, colName, v)
			}
		}
		// insert into table dbConn, table, data
		_, err := etlx.InsertOrUpdate(dbConn, table, cond, data)
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
			_log2["msg"] = fmt.Sprintf("%s: table %s %s", key, table, desc)
			processLogs = append(processLogs, _log2)
		}
		fmt.Println(table, _log2["msg"])
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
