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
	_res, _, err := conn.QuerySingleRow(query, args...)
	if err != nil {
		fmt.Println(query, args, err)
		return nil, err
	}
	return (*_res), nil
}

func (etlx *ETLX) MergeMapStringAny(first map[string]any, second map[string]any) map[string]any {
	if first == nil {
		first = map[string]any{}
	}
	for key, secondValue := range second {
		/*firstValue, ok := first[key]
		firstMap, firstIsMap := firstValue.(map[string]any)
		secondMap, secondIsMap := secondValue.(map[string]any)
		if ok && firstIsMap && secondIsMap {
			// first[key] = MergeMapStringAny(firstMap, secondMap)
			continue
		}*/
		first[key] = secondValue
	}
	return first
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
			insertId, err := adminDb.ExecuteNamedQuery(ins_sql, dialect.DataTypeConversion(roleData))
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
				_, err := adminDb.ExecuteNamedQuery(ins_sql, dialect.DataTypeConversion(roleData))
				if err != nil {
					return nil, err
				}
			}
		}
		// fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, conn, itemMetadata.(map[string]any)["description"])
		access, ok := itemMetadata.(map[string]any)["access"] //.(map[string]any)
		if !ok {
			continue
		}
		// fmt.Println("ACCESS", access)
		_data := roleData
		//fmt.Println("ROLE:", _data)
		_data["user_id"] = 1
		// role_app
		for _, _app := range access.([]any) {
			for app, _menus := range _app.(map[string]any) {
				// fmt.Println(1, app, _menus)
				if app == "__order" {
					continue
				}
				_data["app"] = app
				role_app := map[string]any{}
				sql := `select * from role_app where role_id = :role_id and app_id in (select app_id from app where app = :app)`
				sql = `select role_app.*, app.db from role_app join app on app.app_id = role_app.app_id where role_app.role_id = :role_id and app.app = :app`
				_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
				if err != nil {
					return nil, fmt.Errorf("find app_role failed: %w", err)
				} else if len(_res) > 0 {
					role_app = _res
					//_data["app_id"] = role_app["app_id"]
					_data = etlx.MergeMapStringAny(_data, _res)
				} else {
					sql := `select * from app where app = :app`
					_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
					if err != nil {
						return nil, fmt.Errorf("find app failed: %w", err)
					} else if len(_res) > 0 {
						// _data["app_id"] = _res["app_id"]
						_data = etlx.MergeMapStringAny(_data, _res)
						role_app = _data
					} else {
						return nil, fmt.Errorf("find app failed: %s", app)
					}
				}
				role_app_id, ok := role_app["role_app_id"]
				_data["excluded"] = dialect.GetBooleanValue(false)
				_data["access"] = dialect.GetBooleanValue(true)
				if !ok {
					_data["created_at"] = time.Now().In(etlx.TimeZone)
					_data["updated_at"] = time.Now().In(etlx.TimeZone)
					sql := `insert into role_app (app_id, role_id, access, user_id, created_at, updated_at, excluded) values (:app_id, :role_id, :access, :user_id, :created_at, :updated_at, :excluded)`
					insertId, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
					if err != nil {
						return nil, err
					}
					role_app["role_app_id"] = insertId
				} else {
					_data["updated_at"] = time.Now().In(etlx.TimeZone)
					sql := `update role_app set access = :access, updated_at = :updated_at, excluded = :excluded where role_app_id = :role_app_id`
					_, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
					if err != nil {
						return nil, err
					}
					_data["role_app_id"] = role_app_id
				}
				// role_app_menu
				for _, _menu := range _menus.([]any) {
					for menu, _tables := range _menu.(map[string]any) {
						if menu == "__order" {
							continue
						}
						// fmt.Println(2, menu, _tables)
						_data["menu"] = menu
						role_app_menu := map[string]any{}
						sql = `select * from role_app_menu where role_id = :role_id and app_id = :app_id and menu_id in (select menu_id from menu where menu = :menu)`
						_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
						if err != nil {
							return nil, fmt.Errorf("find role failed: %w", err)
						} else if len(_res) > 0 {
							role_app_menu = _res
							// _data["menu_id"] = role_app_menu["menu_id"]
							_data = etlx.MergeMapStringAny(_data, role_app_menu)
						} else {
							sql := `select * from menu where menu = :menu and app_id = :app_id`
							_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
							if err != nil {
								return nil, fmt.Errorf("find menu failed: %s -> %s %w", app, menu, err)
							} else if len(_res) > 0 {
								_data["menu_id"] = _res["menu_id"]
								role_app_menu = _data
							} else {
								return nil, fmt.Errorf("find menu failed: %s -> %s", app, menu)
							}
						}
						role_app_menu_id, ok := role_app_menu["role_app_menu_id"]
						_data["excluded"] = dialect.GetBooleanValue(false)
						_data["access"] = dialect.GetBooleanValue(true)
						if !ok {
							_data["created_at"] = time.Now().In(etlx.TimeZone)
							_data["updated_at"] = time.Now().In(etlx.TimeZone)
							sql := `insert into role_app_menu (role_id, app_id, menu_id, access, user_id, created_at, updated_at, excluded) values (:role_id, :app_id, :menu_id, :access, :user_id, :created_at, :updated_at, :excluded)`
							insertId, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
							if err != nil {
								return nil, err
							}
							role_app["role_app_menu_id"] = insertId
						} else {
							_data["updated_at"] = time.Now().In(etlx.TimeZone)
							sql := `update role_app_menu set access = :access, updated_at = :updated_at, excluded = :excluded where role_app_menu_id = :role_app_menu_id`
							_, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
							if err != nil {
								return nil, err
							}
							_data["role_app_menu_id"] = role_app_menu_id
						}
						// role_app_menu_table
						for t, _tbl := range _tables.([]any) {
							var _table map[string]any
							switch tbl := _tbl.(type) {
							case map[string]any:
								_table = tbl
							case string:
								_table = map[string]any{"table": tbl, "create": true, "read": true, "update": true, "delete": true, "share": true}
							default:
								// pass
							}
							table, ok := _table["table"].(string)
							if !ok {
								continue
							}
							crudActs := []any{}
							for _, act := range []string{"create", "read", "update", "delete", "share"} {
								auxVal, _ := _table[act].(bool)
								_data[act] = dialect.GetBooleanValue(auxVal)
								crudActs = append(crudActs, dialect.GetColumnName(act))
							}
							// fmt.Println(crudActs, data)
							_rlas, _ := _table["rla"].([]any)
							if table == "__order" {
								continue
							}
							fmt.Println(t, table, _rlas)
							_data["table"] = table
							role_app_menu_table := map[string]any{}
							dialectTbl := dialect.GetTableName("table")
							sql = fmt.Sprintf(`select * from role_app_menu_table where role_id = :role_id and app_id = :app_id and menu_id = :menu_id and table_id in (select table_id from %s where %s = :table)`, dialectTbl, dialectTbl)
							_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
							if err != nil {
								return nil, fmt.Errorf("find role failed: %w", err)
							} else if len(_res) > 0 {
								role_app_menu_table = _res
								// _data["table_id"] = role_app_menu_table["table_id"]
								_data = etlx.MergeMapStringAny(_data, role_app_menu_table)
								_data["updated_at"] = time.Now().In(etlx.TimeZone)
							} else {
								sql := fmt.Sprintf(`select * from %s where %s = :table and db = :db`, dialectTbl, dialectTbl)
								_res, err := etlx.NamedQuerySingleRow(adminDb, sql, _data)
								if err != nil {
									return nil, fmt.Errorf("find table failed: %s -> %s -> %s %w", app, menu, table, err)
								} else if len(_res) > 0 {
									_data["table_id"] = _res["table_id"]
									_data["created_at"] = time.Now().In(etlx.TimeZone)
									_data["updated_at"] = time.Now().In(etlx.TimeZone)
									role_app_menu_table = _data
								} else {
									return nil, fmt.Errorf("find table failed: %s -> %s -> %s", app, menu, table)
								}
							}
							role_app_menu_table_id, ok := role_app_menu_table["role_app_menu_table_id"]
							_data["excluded"] = dialect.GetBooleanValue(false)
							if !ok {
								sql := fmt.Sprintf(`insert into role_app_menu_table (role_id, app_id, menu_id, table_id, %s, %s, %s, %s, %s, user_id, created_at, updated_at, excluded) values (:role_id, :app_id, :menu_id, :table_id, :create, :read, :update, :delete, :share, :user_id, :created_at, :updated_at, :excluded)`, crudActs...)
								insertId, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
								if err != nil {
									return nil, err
								}
								role_app["role_app_menu_table_id"] = insertId
							} else {
								sql := fmt.Sprintf(`update role_app_menu_table set table_id = :table_id, %s = :create, %s = :read, %s = :update, %s = :delete, %s = :share, updated_at = :updated_at, excluded = :excluded where role_app_menu_table_id = :role_app_menu_table_id`, crudActs...)
								_, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
								if err != nil {
									return nil, err
								}
								_data["role_app_menu_table_id"] = role_app_menu_table_id
							}
						}
						//*/
					}
				}
			}
		}
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

func (etlx *ETLX) RunC7ROLE_USERS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "ROLE_USERS"
	process := "ROLE_USERS"
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
			insertId, err := adminDb.ExecuteNamedQuery(ins_sql, dialect.DataTypeConversion(roleData))
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
				_, err := adminDb.ExecuteNamedQuery(ins_sql, dialect.DataTypeConversion(roleData))
				if err != nil {
					return nil, err
				}
			}
		}
		// fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, conn, itemMetadata.(map[string]any)["description"])
		users, ok := itemMetadata.(map[string]any)["users"] //.(map[string]any)
		if !ok {
			continue
		}
		_data := roleData
		for _, user := range users.([]any) {
			_sql := `select * from user_role where role_id = ? and user_id in (select user_id from users where username = ? or email = ?)`
			userData, _, err := adminDb.QuerySingleRow(_sql, []any{roleData["role_id"], user, user}...)
			if err != nil {
				return nil, fmt.Errorf("find user failed: %w", err)
			} else if len(*userData) == 0 {
				// continue
				// return nil, fmt.Errorf("find user failed: %s -> %w", user, err)
				_sql := `select * from users where username = ? or email = ?)`
				userData, _, err = adminDb.QuerySingleRow(_sql, []any{user, user}...)
				if err != nil {
					return nil, fmt.Errorf("find user failed: %w", err)
				} else if len(*userData) == 0 {
					// continue
					return nil, fmt.Errorf("find user failed: %s -> %w", user, err)
				} else {
					_data = etlx.MergeMapStringAny(_data, (*userData))
				}
			} else {
				_data = etlx.MergeMapStringAny(_data, (*userData))
			}
			_, ok := _data["user_role_id"]
			_data["active"] = dialect.GetBooleanValue(true)
			if !ok {
				_data["created_at"] = time.Now().In(etlx.TimeZone)
				_data["updated_at"] = time.Now().In(etlx.TimeZone)
				sql := `insert into role_app (role_id, user_id, active, created_at, updated_at, excluded) values (:role_id, :user_id, :active, :created_at, :updated_at, :excluded)`
				_, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
				if err != nil {
					return nil, err
				}
			} else {
				_data["updated_at"] = time.Now().In(etlx.TimeZone)
				sql := `update user_role set role_id = :role_id, user_id = :user_id, active = :active, updated_at = :updated_at, excluded = :excluded where user_role_id = :user_role_id`
				_, err := adminDb.ExecuteNamedQuery(sql, dialect.DataTypeConversion(_data))
				if err != nil {
					return nil, err
				}
			}

		}
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
