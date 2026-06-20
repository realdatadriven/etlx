package etlxlib

import (
	"fmt"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

func (etlx *ETLX) NamedQuerySingleRow(conn db.DBInterface, sql string, data map[string]any) (map[string]any, error) {
	query, args, err := etlx.NamedToPositional(sql, data)
	if err != nil {
		return nil, err
	}
	_res, _, err := conn.QuerySingleRow(query, args)
	if err != nil {
		return nil, err
	}
	return (*_res), nil
}

func (etlx *ETLX) RunC7ROLE(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "ROLE"
	process := "ROLE"
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
	if active, okActive := metadata["active"]; okActive {
		if !active.(bool) {
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
	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		// fmt.Println("ITEM KEY:", itemKey)
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			continue
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
		role, ok := itemMetadata.(map[string]any)["role"].(string)
		if !ok {
			role, ok = itemMetadata.(map[string]any)["name"].(string)
			if !ok {
				continue
			}
		}
		desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
		if !okDesc {
			desc = fmt.Sprintf("%s->%s", key, itemKey)
		}
		roleData := map[string]any{}
		_sql := `select * from role where role = ?`
		_roleData, _, err := adminDb.QuerySingleRow(_sql, []any{role, dialect.GetBooleanValue(false)}...)
		if err != nil {
			return nil, fmt.Errorf("find role failed: %w", err)
		}
		if len(*_roleData) == 0 {
			ins_sql := `insert into role (role, role_desc, created_at, updated_at, excluded) values (:role, :role_desc, :created_at, :updated_at, :excluded)`
			roleData = map[string]any{"role": role, "role_desc": desc, "created_at": start3, "updated_at": start3, "excluded": dialect.GetBooleanValue(false)}
			insertId, err := adminDb.ExecuteNamedQuery(ins_sql, roleData)
			if err != nil {
				return nil, err
			}
			if insertId > 0 {
				roleData["role_id"] = insertId
			} else {
				_roleData, _, err := adminDb.QuerySingleRow(_sql, []any{role, dialect.GetBooleanValue(false)}...)
				if err != nil {
					return nil, fmt.Errorf("find role failed: %w", err)
				}
				roleData = *_roleData
			}
		} else {
			roleData = *_roleData
			_dbDesc, _ := roleData["role_desc"].(string)
			if _dbDesc != desc && desc != "" {
				roleData["role_desc"] = desc
				ins_sql := `update role set role_desc = :role_desc where role_id = :role_id`
				_, err := adminDb.ExecuteNamedQuery(ins_sql, roleData)
				if err != nil {
					return nil, err
				}
			}
		}
		// fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, conn, itemMetadata.(map[string]any)["description"])
		apps, ok := itemMetadata.(map[string]any)["apps"] //.(map[string]any)
		if !ok {
			continue
		}
		// fmt.Println("APPS", apps)
		_data := roleData
		_data["user_id"] = 1
		for a, _app := range apps.([]any) {
			for app, _menu := range _app.(map[string]any) {
				fmt.Println(a, app, _menu)
				_data["app"] = app
				role_app := map[string]any{}
				sql := `select * from role_app where role_id = :role_id app_id in (select app_id from app where app = :app)`
				_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
				if err != nil {
					return nil, fmt.Errorf("find role failed: %w", err)
				} else if len(_res) > 0 {
					role_app = _res
					_data["app_id"] = role_app["app_id"]
				} else {
					sql := `select app_id from app where app = :app`
					_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
					if err != nil {
						return nil, fmt.Errorf("find app failed: %w", err)
					} else if len(_res) > 0 {
						_data["app_id"] = _res["app_id"]
						role_app = _data
					} else {
						return nil, fmt.Errorf("find app failed: %s", app)
					}
				}
				for m, _menu := range _menu.([]any) {
					for menu, _tables := range _menu.(map[string]any) {
						fmt.Println(m, menu, _tables)
						_data["menu"] = menu
						role_app_menu := map[string]any{}
						sql = `select * from role_app_menu where role_id = :role_id app_id = :app_id, and menu_id in (select menu_id from menu where menu = :menu)`
						_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
						if err != nil {
							return nil, fmt.Errorf("find role failed: %w", err)
						} else if len(_res) > 0 {
							role_app_menu = _res
							_data["menu_id"] = role_app_menu["menu_id"]
						} else {
							sql := `select menu_id from menu where menu = :menu`
							_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
							if err != nil {
								return nil, fmt.Errorf("find app failed: %w", err)
							} else if len(_res) > 0 {
								_data["menu_id"] = _res["menu_id"]
								role_app_menu = _data
							} else {
								return nil, fmt.Errorf("find app failed: %s", app)
							}
						}
					}
				}
			}
		}
		/*switch val := apps.(type) {
		case map[string]any:
			start3 = time.Now().In(etlx.TimeZone)
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
			err := etlx.LoadModelData(dbConn, val, app, table, key, cond, nil, nil)
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
			fmt.Println(table, _log2["msg"])
		case []map[string]any:
			for _, val := range data.([]map[string]any) {
				start3 = time.Now().In(etlx.TimeZone)
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
				err := etlx.LoadModelData(dbConn, val, app, table, key, cond, nil, nil)
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
				fmt.Println(table, _log2["msg"])
			}
		default:
			// pass
		}*/
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
