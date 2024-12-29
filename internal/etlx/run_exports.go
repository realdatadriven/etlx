package etlx

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xuri/excelize/v2"
)

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil // Path exists
	}
	if os.IsNotExist(err) {
		return false, nil // Path does not exist
	}
	return false, err // Some other error occurred
}

func fileExists(filePath string) (bool, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // File does not exist
		}
		return false, err // Some other error occurred
	}
	// Check if it's a file (not a directory)
	return !info.IsDir(), nil
}

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m)) // Preallocate slice with the map size
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func (etlx *ETLX) RunEXPORTS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "EXPORTS"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	processLogs = append(processLogs, map[string]any{
		"name":     key,
		"start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	EXPORTSRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				})
				return fmt.Errorf("dectivated %s", "")
			}
		}
		mainPath, okMainPath := metadata["path"].(string)
		if okMainPath {
			pth := etlx.ReplaceQueryStringDate(mainPath, dateRef)
			if ok, _ := pathExists(pth); !ok {
				os.Mkdir(pth, 0755)
			}
		}
		mainConn := metadata["connection"].(string)
		mainDescription = metadata["description"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    time.Now(),
				"end_at":      time.Now(),
				"success":     true,
				"msg":         "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"start_at":    time.Now(),
					"end_at":      time.Now(),
					"success":     true,
					"msg":         "Deactivated",
				})
				return nil
			}
		}
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"start_at":    start3,
		}
		beforeSQL, okBefore := itemMetadata["before_sql"]
		exportSQL, okExport := itemMetadata["export_sql"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		template, okTemplate := itemMetadata["template"]
		mapping, okMapping := itemMetadata["mapping"]
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
		fname := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
		if okPath && path != "" {
			fname = path
		} else if okMainPath && mainPath != "" {
			fname = mainPath
		}
		//  QUERIES TO RUN AT BEGINING
		if okBefore {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    start3,
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
		if okExport {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    start3,
			}
			err = etlx.ExecuteQuery(dbConn, exportSQL, item, fname, "", dateRef)
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
		} else if okTemplate && okMapping {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    start3,
			}
			fmt.Println(template, mapping)
			if ok, _ := fileExists(template.(string)); !ok {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: %s givem as template does not exists", key, itemKey, template)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			} else {
				// Check for supported spreadsheet extensions
				ext := filepath.Ext(template.(string))
				if ext != ".xlsx" && ext != ".xls" && ext != ".xlsm" {
					return fmt.Errorf("unsupported template file extension: %s", ext)
				}
				// Open or create a new workbook
				var file *excelize.File
				var err error
				if ok, _ := fileExists(template.(string)); ok {
					file, err = excelize.OpenFile(template.(string))
					if err != nil {
						return fmt.Errorf("failed to open template file: %w", err)
					}
				} else {
					return fmt.Errorf("template doesn't exists: %s", template)
				}
				_mapp := []map[string]any{}
				switch _map := mapping.(type) {
				case nil:
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s error mapeamento vazio", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				case string:
					// Single query reference
				case []any:
					for _, item := range mapping.([]any) {
						_mapp = append(_mapp, item.(map[string]any))
					}
				default:
					return fmt.Errorf("invalid mapping data type: %T", _map)
				}
				for _, detail := range _mapp {
					// Skip inactive entries
					if active, _ := detail["active"].(bool); !active {
						continue
					}
					sheet := detail["sheet"].(string)
					table := detail["table"].(string)
					_range := detail["range"].(string)
					_type := detail["type"].(string)
					sql := detail["sql"].(string)
					fmt.Println(sheet, table, sql, _range, _type)
					// Check or create the destination sheet
					sheetIndex, err := file.GetSheetIndex(sheet)
					if sheetIndex == -1 || err != nil {
						file.NewSheet(sheet)
					} else {
						file.DeleteSheet(sheet)
						file.NewSheet(sheet)
					}
					// QUERY
					if _, ok := item[sql]; ok {
						sql = item[sql].(string)
					}
					sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
					rows, columns, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
					// Fetch data from the database using the provided SQL query
					if err != nil {
						return fmt.Errorf("error executing query for detail ID %v: %w", detail, err)
					}
					// Write column headers
					for colIdx, colName := range columns {
						cell, err := excelize.JoinCellName(string('A'+colIdx), 1)
						if err != nil {
							return fmt.Errorf("failed to set columns: %w", err)
						}
						file.SetCellValue(sheet, cell, colName)
					}
					// Write data rows
					rowIdx := 2
					for _, value := range *rows {
						for colIdx, colName := range columns {
							cell, err := excelize.JoinCellName(string('A'+colIdx), rowIdx)
							if err != nil {
								return fmt.Errorf("failed to set columns: %w", err)
							}
							file.SetCellValue(sheet, cell, value[colName])
						}
						rowIdx++
					}
					// Create Excel table if `dest_table_name` is specified
					if table != "" {
						cell, err := excelize.JoinCellName(string('A'+len(columns)-1), rowIdx-1)
						if err != nil {
							return fmt.Errorf("failed to set columns: %w", err)
						}
						tableRange := fmt.Sprintf("A1:%s", cell)
						err = file.AddTable(sheet, &excelize.Table{
							Name:            table,
							Range:           tableRange,
							StyleName:       "TableStyleMedium9",
							ShowFirstColumn: false,
							ShowLastColumn:  false,
							//ShowRowStripes:    true,
							ShowColumnStripes: false,
						})
						if err != nil {
							return fmt.Errorf("failed to create table %s on sheet %s: %w", table, sheet, err)
						}
					}
					outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("%s%s", filepath.Base(template.(string)), ext))
					err = file.SaveAs(outputFile)
					if err != nil {
						return fmt.Errorf("failed to save file: %w", err)
					}
					return nil
				}
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
				"start_at":    start3,
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
	err := etlx.ProcessMDKey(key, conf, EXPORTSRunner)
	if err != nil {
		return processLogs, fmt.Errorf("%s failed: %v", key, err)
	}
	processLogs[0] = map[string]any{
		"name":        key,
		"description": mainDescription,
		"start_at":    processLogs[0]["start_at"],
		"end_at":      time.Now(),
		"duration":    time.Since(start),
	}
	return processLogs, nil
}
