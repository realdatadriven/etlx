package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

func (etlx *ETLX) ResolveModelStringDataFunc(_data, app map[string]any, key string, parent_id any, ids map[string]any) map[string]any {
	fileContentPattern := regexp.MustCompile(`^FileContent\((.+)\)$`)
	nowPattern := regexp.MustCompile(`^Now\(\)$`)
	appPatterm := regexp.MustCompile(`^appId\(\)$`)
	parentPatterm := regexp.MustCompile(`^parentId\(\)$`)
	var err error
	for colName, input := range _data {
		// switch type of _data to string or map
		switch v := input.(type) {
		case string:
			matches := fileContentPattern.FindStringSubmatch(strings.TrimSpace(input.(string)))
			if len(matches) != 2 {
				// pass
			} else {
				filename := strings.TrimSpace(matches[1])
				if filename != "" {
					_data[colName], err = ResolveFileContentSafe(filename, "")
					if err != nil {
						fmt.Printf("%s ERR: resolving file content for %s: %s %v", key, filename, err, v)
					}
				}
			}
			matchesNow := nowPattern.FindStringSubmatch(strings.TrimSpace(input.(string)))
			if len(matchesNow) == 1 {
				_data[colName] = time.Now().In(etlx.TimeZone) //.Format("2006-01-02 15:04:05")
			}
			matchesApp := appPatterm.FindStringSubmatch(strings.TrimSpace(input.(string)))
			if len(matchesApp) == 1 {
				_data[colName] = app["app_id"]
			}
			matchesParent := parentPatterm.FindStringSubmatch(strings.TrimSpace(input.(string)))
			if len(matchesParent) == 1 {
				_data[colName] = parent_id
			}
			for key, value := range ids {
				spatt := fmt.Sprintf(`^%s\\(\\)$`, key)
				fmt.Println(spatt, key, value, input)
				patterm, err := regexp.Compile(spatt)
				if err != nil {
					fmt.Println(spatt, "ERR", err)
					continue
				}
				matches := patterm.FindStringSubmatch(strings.TrimSpace(input.(string)))
				if len(matches) == 1 {
					_data[colName] = value
				}
			}
		case map[string]any:
			// do nothing
		default:
			//println(table, colName, v)
		}
	}
	return _data
}

func (etlx *ETLX) RunWORKFLOW(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "WORKFLOW"
	process := "WORKFLOW"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now().In(etlx.TimeZone)
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
	active := true
	if _actv, okActive := metadata["active"].(bool); okActive {
		active = _actv
		if !_actv {
			processLogs = append(processLogs, map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("KEY %s", key),
				"description": metadata["description"].(string),
				"key":         key,
				"start_at":    time.Now().In(etlx.TimeZone),
				"end_at":      time.Now().In(etlx.TimeZone),
				"success":     true,
				"msg":         "Deactivated",
			})
			// return nil, fmt.Errorf("%s deactivated", key)
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
	start3 := time.Now().In(etlx.TimeZone)
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
		_log2["end_at"] = time.Now().In(etlx.TimeZone)
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
	adminDb, err := etlx.GetDB(adminConn)
	if err != nil {
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		_log2["success"] = false
		_log2["msg"] = fmt.Sprintf("%s ERR: connecting to ADMIN DB %s in : %s", key, adminConn, err)
		_log2["end_at"] = time.Now().In(etlx.TimeZone)
		_log2["duration"] = time.Since(start3).Seconds()
		processLogs = append(processLogs, _log2)
		return nil, fmt.Errorf("%s ERR: connecting to ADMIN DB %s in : %s", key, adminConn, err)
	} else {
		defer adminDb.Close()
	}
	dialect := GetDialect(adminDb.GetDriverName())
	app := map[string]any{}
	_sql := `SELECT * FROM app WHERE db = ? AND excluded = ? --  LIMIT 1`
	_app, _, err := adminDb.QuerySingleRow(_sql, []any{database, dialect.GetBooleanValue(false)}...)
	if err != nil {
		return nil, fmt.Errorf("find app failed: %w", err)
	}
	if len(*_app) > 0 {
		app = (*_app)
		// fmt.Println(app)
	}
	table, ok := metadata["table"].(string)
	if !ok {
		table = "workflow"
	}
	cond := "WHERE workflow = :workflow AND excluded = false"
	name, ok := metadata["name"]
	if !ok {
		name = key
	}
	_data := map[string]any{
		"workflow":          name,
		"workflow_desc":     metadata["description"],
		"workflow_icon":     metadata["icon"],
		"order":             metadata["order"],
		"version":           metadata["version"],
		"steps_orientation": metadata["orientation"],
		"active":            active,
		"user_id":           app["user_id"],
		"app_id":            app["app_id"],
		"created_at":        time.Now().In(etlx.TimeZone),
		"updated_at":        time.Now().In(etlx.TimeZone),
		"excluded":          false,
	}
	if _, ok := metadata["email_template"]; ok {
		_data["email_template"] = metadata["email_template"]
	} else if _, ok := metadata["email"]; ok {
		_data["email_template"] = metadata["email"]
	}
	// insert or update workflow table with the metadata info, and get the workflow_id
	_data = etlx.ResolveModelStringDataFunc(_data, app, key, nil, nil)
	workflow_id, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
	if err != nil {
		return nil, fmt.Errorf("failed to insert/update workflow: %w", err)
	}
	if workflow_id == nil || workflow_id == 0 {
		sql := fmt.Sprintf(`SELECT * FROM %s WHERE workflow = ? AND excluded = false`, dialect.GetTableName(table))
		workflow, _, err := dbConn.QuerySingleRow(sql, []any{name}...)
		if err != nil {
			return nil, fmt.Errorf("failed to query workflow: %w", err)
		}
		if len(*workflow) == 0 {
			return nil, fmt.Errorf("workflow not found after insert/update")
		}
		workflow_id = (*workflow)["workflow_id"]
	}
	fmt.Printf("Workflow: %s ID: %v Generated\n", name, workflow_id)
	// ITENS CAN BE THE STEP, DEPENDECIES, SLA, ETC
	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		fmt.Println("ITEM KEY:", itemKey)
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			continue
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
		// ACTIVE
		active := true
		if _actv, okActive := itemMetadata.(map[string]any)["active"].(bool); okActive {
			active = _actv
		}
		if !active {
			//continue
		}
		table, ok := itemMetadata.(map[string]any)["table"].(string)
		if !ok {
			continue
		}
		cond, ok := itemMetadata.(map[string]any)["cond"].(string)
		if !ok {
			cond = fmt.Sprintf(`WHERE workflow_id = :workflow_id and %s = :%s and excluded = :excluded`, table, table)
		}
		name, ok = itemMetadata.(map[string]any)["name"]
		if !ok {
			name = itemKey
		}
		_data := map[string]any{
			"workflow_id": workflow_id,
			"excluded":    false,
			"app_id":      "appId()",
			"user_id":     app["user_id"],
			"created_at":  "Now()",
			"updated_at":  "Now()",
			// other data from itemMetadata.(map[string]any)["data"]

		}
		if table == "workflow_step" {
			cond = `WHERE workflow_id = :workflow_id and step = :step and excluded = :excluded`
			_data["step"] = name
			_data["step_desc"] = itemMetadata.(map[string]any)["description"]
			_data["step_order"] = itemMetadata.(map[string]any)["order"]
			_data["step_icon"] = itemMetadata.(map[string]any)["icon"]
			_data["step_color"] = itemMetadata.(map[string]any)["color"]
			_data["api"] = itemMetadata.(map[string]any)["api"]
			_data["active"] = active
			if _, ok := itemMetadata.(map[string]any)["email_template"]; ok {
				_data["step_email_template"] = itemMetadata.(map[string]any)["email_template"]
			} else if _, ok := itemMetadata.(map[string]any)["email"]; ok {
				_data["step_email_template"] = itemMetadata.(map[string]any)["email"]
			} else if _, ok := itemMetadata.(map[string]any)["step_email_template"]; ok {
				_data["step_email_template"] = itemMetadata.(map[string]any)["step_email_template"]
			}
			if _, ok := itemMetadata.(map[string]any)["document_template"]; ok {
				_data["document_template"] = itemMetadata.(map[string]any)["document_template"]
			} else if _, ok := itemMetadata.(map[string]any)["doc_template"]; ok {
				_data["document_template"] = itemMetadata.(map[string]any)["doc_template"]
			} else if _, ok := itemMetadata.(map[string]any)["step_document_template"]; ok {
				_data["document_template"] = itemMetadata.(map[string]any)["step_document_template"]
			} else if _, ok := itemMetadata.(map[string]any)["doc"]; ok {
				_data["document_template"] = itemMetadata.(map[string]any)["doc"]
			} else if _, ok := itemMetadata.(map[string]any)["document"]; ok {
				_data["document_template"] = itemMetadata.(map[string]any)["document"]
			}
		}
		start3 = time.Now().In(etlx.TimeZone)
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
		_data = etlx.ResolveModelStringDataFunc(_data, app, key, nil, nil)
		insert_id, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["end_at"] = time.Now().In(etlx.TimeZone)
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: insert/update table %s: %s", key, table, err)
			processLogs = append(processLogs, _log2)
		} else {
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s: table %s %s", key, table, desc)
			processLogs = append(processLogs, _log2)
			if table == "workflow_step" {
				if insert_id == any(nil) || insert_id == 0 {
					sql := fmt.Sprintf(`SELECT * FROM %s WHERE step = ? AND excluded = false`, dialect.GetTableName(table))
					step, _, err := dbConn.QuerySingleRow(sql, []any{name}...)
					if err != nil {
						return nil, fmt.Errorf("failed to query step %s: %w", name, err)
					}
					if len(*step) == 0 {
						return nil, fmt.Errorf("workflow step %s not found after insert/update", name)
					}
					insert_id = (*step)["workflow_step_id"]
				}
				// SCHEMA
				workflow_step_schema, ok := itemMetadata.(map[string]any)["workflow_step_schema"].([]any)
				if !ok {
					workflow_step_schema, ok = itemMetadata.(map[string]any)["step_schema"].([]any)
					if !ok {
						workflow_step_schema, ok = itemMetadata.(map[string]any)["schema"].([]any)
					}
				}
				if workflow_step_schema == nil || !ok {
					fmt.Printf("%s ERR: workflow_step_schema not found for %s", key, insert_id)
				} else {
					table = "workflow_step_schema"
					for order, _field := range workflow_step_schema {
						if _, isMap := _field.(map[string]any); !isMap {
							continue
						}
						field := _field.(map[string]any)
						_data = map[string]any{
							"workflow_step_id": insert_id,
							"workflow_id":      workflow_id,
							"field":            field["field"],
							"order_index":      order,
							"active":           true,
							"app_id":           app["app_id"],
							"user_id":          app["user_id"],
							"created_at":       time.Now().In(etlx.TimeZone),
							"updated_at":       time.Now().In(etlx.TimeZone),
							"excluded":         false,
						}
						if _, ok := field["label"]; ok {
							_data["label"] = field["label"]
						} else {
							_data["label"] = field["field"]
						}
						if _, ok := field["data_type"]; ok {
							_data["data_type"] = field["data_type"]
						}
						if _, ok := field["input_type"]; ok {
							_data["input_type"] = field["input_type"]
						}
						if _, ok := field["nullable"]; ok {
							_data["nullable"] = field["nullable"]
						}
						if _, ok := field["size"]; ok {
							_data["size"] = field["size"]
						}
						if _, ok := field["options"]; ok {
							_data["options"] = field["options"]
						}
						if _, ok := field["active"]; ok {
							_data["active"] = field["active"]
						}
						if _, ok := field["format"]; ok {
							_data["format"] = field["format"]
						}
						if _, ok := field["elipsis"]; ok {
							_data["elipsis"] = field["elipsis"]
						}
						if _, ok := field["order"]; ok {
							_data["order_index"] = field["order"]
						} else if _, ok := field["order_index"]; ok {
							_data["order_index"] = field["order_index"]
						}
						if _, ok := field["default"]; ok {
							_data["default_value"] = field["default"]
						} else if _, ok := field["default"]; ok {
							_data["default_value"] = field["default"]
						}
						if _, ok := field["validation_rule"]; ok {
							_data["validation_rule"] = field["validation_rule"]
						} else if _, ok := field["validation"]; ok {
							_data["validation_rule"] = field["validation"]
						}
						start3 = time.Now().In(etlx.TimeZone)
						desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
						if !okDesc {
							desc = fmt.Sprintf("%s->%s->%s", key, itemKey, field["field"])
						}
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2 = map[string]any{
							"process":               process,
							"name":                  fmt.Sprintf("%s->%s->%s", key, itemKey, field["field"]),
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
						cond = `WHERE workflow_id = :workflow_id and workflow_step_id = :workflow_step_id and field = :field and excluded = :excluded`
						_data = etlx.ResolveModelStringDataFunc(_data, app, key, nil, nil)
						_, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2["end_at"] = time.Now().In(etlx.TimeZone)
						_log2["duration"] = time.Since(start3).Seconds()
						_log2["mem_alloc_end"] = mem_alloc
						_log2["mem_total_alloc_end"] = mem_total_alloc
						_log2["mem_sys_end"] = mem_sys
						_log2["num_gc_end"] = num_gc
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Sprintf("%s ERR: insert/update table %s: %s", key, table, err)
							processLogs = append(processLogs, _log2)
						} else {
							_log2["success"] = true
							_log2["msg"] = fmt.Sprintf("%s: table %s %s", key, table, desc)
							processLogs = append(processLogs, _log2)
						}
					}
				}
				// RESPONSIBLE
				workflow_step_responsible, ok := itemMetadata.(map[string]any)["workflow_step_responsible"].([]any)
				if !ok {
					workflow_step_responsible, ok = itemMetadata.(map[string]any)["step_responsible"].([]any)
					if !ok {
						workflow_step_responsible, ok = itemMetadata.(map[string]any)["responsible"].([]any)
						if !ok {
							workflow_step_responsible, ok = itemMetadata.(map[string]any)["responsibles"].([]any)
						}
					}
				}
				if workflow_step_responsible == nil || !ok {
					fmt.Printf("%s ERR: workflow_step_responsible not found for %s", key, insert_id)
				} else {
					table = "workflow_step_responsible"
					for _, responsible := range workflow_step_responsible {
						if _, isMap := responsible.(map[string]any); !isMap {
							continue
						}
						resp := responsible.(map[string]any)
						_data = map[string]any{
							"workflow_step_id": insert_id,
							//"workflow_id":      workflow_id,
							"email":         resp["email"],
							"first_name":    resp["first_name"],
							"last_name":     resp["last_name"],
							"department_id": resp["department_id"],
							"role":          resp["role"],
							"user_id":       app["user_id"],
							"excluded":      false,
						}
						if _, ok := resp["active"]; ok {
							_data["active"] = resp["active"]
						} else {
							_data["active"] = true
						}
						if _, ok := resp["email_template"]; ok {
							_data["responsible_email_template"] = resp["email_template"]
						} else if _, ok := resp["resp_email_template"]; ok {
							_data["responsible_email_template"] = resp["resp_email_template"]
						} else if _, ok := resp["responsible_email_template"]; ok {
							_data["responsible_email_template"] = resp["responsible_email_template"]
						}
						start3 = time.Now().In(etlx.TimeZone)
						desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
						if !okDesc {
							desc = fmt.Sprintf("%s->%s->%s", key, itemKey, resp["email"])
						}
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2 = map[string]any{
							"process":               process,
							"name":                  fmt.Sprintf("%s->%s->%s", key, itemKey, resp["email"]),
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
						cond = `WHERE workflow_step_id = :workflow_step_id and email = :email and excluded = :excluded`
						_data = etlx.ResolveModelStringDataFunc(_data, app, key, nil, nil)
						_, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2["end_at"] = time.Now().In(etlx.TimeZone)
						_log2["duration"] = time.Since(start3).Seconds()
						_log2["mem_alloc_end"] = mem_alloc
						_log2["mem_total_alloc_end"] = mem_total_alloc
						_log2["mem_sys_end"] = mem_sys
						_log2["num_gc_end"] = num_gc
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Sprintf("%s ERR: insert/update table %s: %s", key, table, err)
							processLogs = append(processLogs, _log2)
						} else {
							_log2["success"] = true
							_log2["msg"] = fmt.Sprintf("%s: table %s %s", key, table, desc)
							processLogs = append(processLogs, _log2)
						}
					}
				}
				// SUBSCRIBERS
				workflow_step_subscriber, ok := itemMetadata.(map[string]any)["workflow_step_subscriber"].([]any)
				if !ok {
					workflow_step_subscriber, ok = itemMetadata.(map[string]any)["step_subscriber"].([]any)
					if !ok {
						workflow_step_subscriber, ok = itemMetadata.(map[string]any)["subscriber"].([]any)
						if !ok {
							workflow_step_subscriber, ok = itemMetadata.(map[string]any)["subscribers"].([]any)
						}
					}
				}
				if workflow_step_subscriber == nil || !ok {
					fmt.Printf("%s ERR: workflow_step_subscriber not found for %s", key, insert_id)
				} else {
					table = "workflow_step_subscriber"
					for _, subscriber := range workflow_step_subscriber {
						if _, isMap := subscriber.(map[string]any); !isMap {
							continue
						}
						sub := subscriber.(map[string]any)
						_data = map[string]any{
							"workflow_step_id": insert_id,
							"email":            sub["email"],
							"first_name":       sub["first_name"],
							"last_name":        sub["last_name"],
							"active":           sub["active"],
							"user_id":          app["user_id"],
							"excluded":         false,
						}

						if _, ok := sub["active"]; ok {
							_data["active"] = sub["active"]
						} else {
							_data["active"] = true
						}
						if _, ok := sub["subscriber_email_template"]; ok {
							_data["subscriber_email_template"] = sub["subscriber_email_template"]
						} else if _, ok := sub["sub_email_template"]; ok {
							_data["subscriber_email_template"] = sub["sub_email_template"]
						} else if _, ok := sub["email_template"]; ok {
							_data["subscriber_email_template"] = sub["email_template"]
						}
						if _, ok := sub["notify_on_start"]; !ok {
							_data["notify_on_start"] = sub["notify_on_start"]
						} else if _, ok := sub["on_start"]; !ok {
							_data["notify_on_start"] = sub["on_start"]
						} else if _, ok := sub["start"]; !ok {
							_data["notify_on_start"] = sub["start"]
						}
						if _, ok := sub["notify_on_complete"]; !ok {
							_data["notify_on_complete"] = sub["notify_on_complete"]
						} else if _, ok := sub["on_complete"]; !ok {
							_data["notify_on_complete"] = sub["on_complete"]
						} else if _, ok := sub["complete"]; !ok {
							_data["notify_on_complete"] = sub["complete"]
						}
						if _, ok := sub["notify_on_escalation"]; !ok {
							_data["notify_on_escalation"] = sub["notify_on_escalation"]
						} else if _, ok := sub["on_escalation"]; !ok {
							_data["notify_on_escalation"] = sub["on_escalation"]
						} else if _, ok := sub["escalation"]; !ok {
							_data["notify_on_escalation"] = sub["escalation"]
						}
						start3 = time.Now().In(etlx.TimeZone)
						desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
						if !okDesc {
							desc = fmt.Sprintf("%s->%s->%s", key, itemKey, sub["email"])
						}
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2 = map[string]any{
							"process":               process,
							"name":                  fmt.Sprintf("%s->%s->%s", key, itemKey, sub["email"]),
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
						cond = `WHERE workflow_step_id = :workflow_step_id and email = :email and excluded = :excluded`
						_data = etlx.ResolveModelStringDataFunc(_data, app, key, nil, nil)
						_, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
						mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
						_log2["end_at"] = time.Now().In(etlx.TimeZone)
						_log2["duration"] = time.Since(start3).Seconds()
						_log2["mem_alloc_end"] = mem_alloc
						_log2["mem_total_alloc_end"] = mem_total_alloc
						_log2["mem_sys_end"] = mem_sys
						_log2["num_gc_end"] = num_gc
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Sprintf("%s ERR: insert/update table %s: %s", key, table, err)
							processLogs = append(processLogs, _log2)
						} else {
							_log2["success"] = true
							_log2["msg"] = fmt.Sprintf("%s: table %s %s", key, table, desc)
							processLogs = append(processLogs, _log2)
						}
					}
				}
				// DEPENDECIES

				// SLA

			}
		}
		// fmt.Println(table, _log2["msg"])
	}
	mem_alloc2, mem_total_alloc2, mem_sys2, num_gc2 := etlx.RuntimeMemStats()
	processLogs[0] = map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              processLogs[0]["start_at"],
		"end_at":                time.Now().In(etlx.TimeZone),
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
