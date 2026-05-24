package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

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
	// MAIN / ROOT SEC IS THE WORKFLOW UPDATES workflow table
	// tbale: workflow
	/*## WORKFLOW
	```yaml
	table: workflow
	comment: "Workflow"
	tooltip: "Defines workflow processes"
	columns:
	  workflow_id:       { type: integer, pk: true, autoincrement: true, comment: "Workflow ID", tooltip: "Unique identifier of the workflow" }
	  workflow:          { type: varchar, len: 200, unique: true, nullable: false, comment: "Workflow", tooltip: "Name of the workflow", form_display: true, table_display: true, form_size_desc: 6, form_order: 1 }
	  workflow_desc:     { type: text, comment: "Workflow Desc", tooltip: "Description of the workflow", form_display: true, table_display: true, form_long_text: true, form_order: 5 }
	  order:             { type: integer, comment: "Order", form_display: true, table_display: true, form_size_desc: 2, form_order: 2 }
	  version:           { type: varchar, len: 200, default: 'v1.0.0', comment: "Version", tooltip: "Version number of the workflow", form_display: true, table_display: true, form_size_desc: 2, form_order: 3 }
	  active:            { type: boolean, default: true, comment: "Active", tooltip: "Indicates whether the workflow is active", form_display: true, table_display: true, form_size_desc: 2, form_order: 4 }
	  schedule:          { type: varchar, len: 200, comment: "Cron Schedule", tooltip: "Cron Representation of when it runs, if so", form_display: true, table_display: true, form_size_desc: 4, form_order: 8 }
	  steps_orientation: { type: varchar, len: 200, comment: "Step Orientation", tooltip: "Vertical / Horizontal", form_display: true, table_display: true, form_size_desc: 4, form_order: 9 }
	  workflow_icon:     { type: varchar, len: 200, comment: "Icon", tooltip: "Workflow Icon - Hero Icon", form_display: true, table_display: true, form_size_desc: 4, form_order: 10 }
	  email_template:    { type: text, comment: "Email Template", tooltip: "Email", form_display: true, form_long_text: true, form_code: html, form_order: 11 }
	  user_id:           { type: integer, comment: "User ID", tooltip: "Identifier of the user responsible for the workflow" }
	  app_id:            { type: integer, comment: "App ID", tooltip: "Identifier of the application context" }
	  created_at:        { type: datetime, comment: "Created AT", tooltip: "Date and time when the workflow was created" }
	  updated_at:        { type: datetime, comment: "Updated AT", tooltip: "Date and time when the workflow was last updated" }
	  excluded:          { type: boolean, default: false, comment: "Excluded", tooltip: "Indicates whether the workflow is excluded from active use" }
	*/
	/* -- data structure example
	   name: WORKFLOW_1
	   table: workflow
	   runs_as: WORKFLOW
	   description: Exemple of a workflow
	   icon: rectangle-group
	   order: 1
	   version: v1.0.0
	   orientation: vertical*/
	table, ok := metadata["table"].(string)
	if !ok {
		table = "workflow"
	}
	cond := "WHERE workflow = :workflow AND excluded = false"
	_data := map[string]any{
		"workflow":          key,
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
	workflow_id, err := etlx.InsertOrUpdate(dbConn, table, cond, _data)
	if err != nil {
		return nil, fmt.Errorf("failed to insert/update workflow: %w", err)
	}
	if workflow_id == nil || workflow_id == 0 {
		sql := fmt.Sprintf(`SELECT * FROM %s WHERE workflow = ? AND excluded = false`, dialect.GetTableName(table))
		workflow, _, err := dbConn.QuerySingleRow(sql, []any{key}...)
		if err != nil {
			return nil, fmt.Errorf("failed to query workflow: %w", err)
		}
		if len(*workflow) == 0 {
			return nil, fmt.Errorf("workflow not found after insert/update")
		}
		workflow_id = (*workflow)["workflow_id"]
	}
	fmt.Printf("Workflow: %s ID: %v Generated\n", key, workflow_id)
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
			/***
			-- workflow step table schema
				table: workflow_step
				comment: "Workflow Step"
				tooltip: "Defines the steps of a workflow"
				columns:
					workflow_step_id:    { type: integer, pk: true, autoincrement: true, comment: "Workflow Step ID", tooltip: "Unique identifier of the workflow step" }
					step:                { type: varchar, len: 200, nullable: false, comment: "Step", tooltip: "Name of the step", form_display: true, table_display: true, form_size_desc: 4, form_order: 1 }
					step_desc:           { type: text, comment: "Step Desc", tooltip: "Description of the step", form_display: true, form_long_text: true, form_order: 6 }
					step_order:          { type: integer, comment: "Step Order", tooltip: "Order of execution of the step", form_display: true, table_display: true, form_size_desc: 2, form_order: 2 }
					workflow_id:         { type: integer, nullable: false, fk: "workflow.workflow_id", comment: "Workflow ID", tooltip: "Identifier of the workflow to which the step belongs", form_display: true, table_display: true, form_size_desc: 4, form_order: 7 }
					step_icon:           { type: varchar, len: 200, comment: "Icon", tooltip: "Step Icon", form_display: true, table_display: true, form_size_desc: 2, form_order: 3 }
					step_color:          { type: varchar, len: 200, comment: "Color", tooltip: "Step Color", form_display: true, table_display: true, form_size_desc: 2, form_order: 4 }
					active:              { type: boolean, default: true, comment: "Active", tooltip: "Indicates whether the step is active", form_display: true, form_size_desc: 2, form_order: 5 }
					step_email_template: { type: text, comment: "Email Template", tooltip: "Email", form_display: true, form_code: html, form_long_text: true, form_order: 8 }
					document_template:   { type: text, comment: "Doc Template", tooltip: "In case the step is suposed to generate some kind of document, here will be the template, and it will be a gostatus templat tha has access to all the data from the previous step, current date, user, and the processes itself", form_display: true, form_code: html, form_long_text: true, form_order: 9 }
					api:                 { type: varchar, len: 255, comment: "Trigers API", tooltip: "API that is called", form_display: true, table_display: false, form_size_desc: 8, form_order: 7 }
					user_id:             { type: integer, comment: "User ID", tooltip: "Identifier of the user responsible for the step definition" }
					app_id:              { type: integer, comment: "App ID", tooltip: "Identifier of the application context" }
					created_at:          { type: datetime, comment: "Created AT", tooltip: "Date and time when the step was created" }
					updated_at:          { type: datetime, comment: "Updated AT", tooltip: "Date and time when the step was last updated" }
					excluded:            { type: boolean, default: false, comment: "E
				-- workflow step data
				name: STEP_1
				table: workflow_step
				description: Step 1
				order: 1
				icon: plus
				color: green
				active: true
				workflow_step_schema:
					- field1: {label: field 1, data_type_desc: text, input_type_desc: radio, nullable: false, size_desc: 3, options: '["A", "B", "C"]'}**/
			cond = `WHERE workflow_id = :workflow_id and step = :step and excluded = :excluded`
			_data["step"] = itemKey
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
		//createTableSQL := generateCreateTableSQL(driver, table, comment, create_all, columns)
		// fmt.Println("CREATE TABLE SQL:\n", createTableSQL)
		// each key in data
		fileContentPattern := regexp.MustCompile(`^FileContent\((.+)\)$`)
		nowPattern := regexp.MustCompile(`^Now\(\)$`)
		appPatterm := regexp.MustCompile(`^appId\(\)$`)
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
			case map[string]any:
				// do nothing
			default:
				//println(table, colName, v)
			}
		}
		// insert into table dbConn, table, data
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
					step, _, err := dbConn.QuerySingleRow(sql, []any{itemKey}...)
					if err != nil {
						return nil, fmt.Errorf("failed to query step %s: %w", itemKey, err)
					}
					if len(*step) == 0 {
						return nil, fmt.Errorf("workflow not found after insert/update")
					}
					insert_id = (*step)["workflow_step_id"]
				}
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
					// parse workflow_step_schema and insert into workflow_step_field table
					/** -- workflow step schema table def
										table: workflow_step_schema
										comment: "Step Schema"
										tooltip: "Defines the data structure required for each step of a workflow"
										columns:
											workflow_step_schema_id: { type: integer, pk: true, autoincrement: true, comment: "Workflow Step Schema ID", tooltip: "Unique identifier of the schema field" }
											workflow_id:             { type: integer, nullable: false, fk: "workflow.workflow_id", comment: "Workflow ID", tooltip: "Identifier of the workflow associated with the field", form_display: true, table_display: true, form_size_desc: 6 }
											workflow_step_id:        { type: integer, nullable: false, fk: "workflow_step.workflow_step_id", comment: "Workflow Step ID", tooltip: "Identifier of the step where the field is collected", form_display: true, table_display: true, form_size_desc: 6 }
											field:                   { type: varchar, len: 200, nullable: false, comment: "Field", tooltip: "Technical identifier of the field", form_display: true, table_display: true, form_size_desc: 4 }
											label:                   { type: varchar, len: 200, nullable: false, comment: "Label", tooltip: "Display name of the field", form_display: true, table_display: true, form_size_desc: 4 }
											data_type:               { type: varchar, fk: "data_type.data_type", nullable: false, comment: "Data Type", tooltip: "Type of data stored in the field", form_display: true, table_display: true, form_size_desc: 4 }
											nullable:                { type: boolean, default: true, comment: "Nullable", tooltip: "Indicates whether the field can be empty", form_display: true, table_display: true, form_size_desc: 3 }
											default_value:           { type: varchar, len: 200, comment: "Default Value", tooltip: "Default value assigned to the field", form_display: true, table_display: false, form_size_desc: 3 }
											validation_rule:         { type: varchar, len: 200, comment: "Validation Rule", tooltip: "Regex validation rule for the field", form_display: true, table_display: flase, form_size_desc: 3 }
											order_index:             { type: integer, comment: "Order Index", tooltip: "Position of the field within the step", form_display: true, table_display: true, form_size_desc: 3}
											format:                  { type: varchar, len: 200, comment: "Format", tooltip: "Format intl.Format", form_display: true, form_size_desc: 4 }
											size:                    { type: integer, fk: "size.size", comment: "Size", tooltip: "1 - 12 size that will be shown in form", form_display: true, form_size_desc: 2 }
											elipsis:                 { type: integer, comment: "Elipsis", tooltip: "Text elipsis", form_display: true, form_size_desc: 2 }
											input_type:              { type: varchar, fk: "input_type.input_type", comment: "Options Input Type", tooltip: "Combobox,Checkbox or Radio", form_display: true, table_display: true, form_size_desc: 2 }
											active:                  { type: boolean, default: true, comment: "Active", tooltip: "Indicates whether the field is active" , form_display: true, table_display: true, form_size_desc: 2 }
											options:                 { type: text, comment: "Options", tooltip: "JSON Array of string or array of objects{label,value}", form_display: true, form_long_text: true, form_size_desc: 12 }
											user_id:                 { type: integer, comment: "User ID", tooltip: "Identifier of the user responsible for the field definition" }
											app_id:                  { type: integer, comment: "App ID", tooltip: "Identifier of the application context" }
											created_at:              { type: datetime, comment: "Created AT", tooltip: "Date and time when the field was created" }
											updated_at:              { type: datetime, comment: "Updated AT", tooltip: "Date and time when the field was last updated" }
											excluded:                { type: boolean, default: false, comment: "Excluded", tooltip: "Indicates whether the field is excluded from active use" }

										---workflow_step_schema example
										workflow_step_schema:
					  						- {field: field1, label: field 1, data_type: text, input_type: radio, nullable: false, size_desc: 3, options: '["A", "B", "C"]'}*/
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
			}
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
