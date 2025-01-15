package etlx

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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

func getStartOfRange(input string) (int, string, error) {
	// Define a regex pattern to match an Excel cell or range
	// This will capture the column letters and row numbers
	re := regexp.MustCompile(`([A-Z]+)(\d+)(?::[A-Z]+\d+)?`)
	// Match the input string
	matches := re.FindStringSubmatch(input)
	if len(matches) < 3 {
		return 0, "", fmt.Errorf("invalid Excel range or cell: %s", input)
	}
	// Extract the column (letters) and row (numbers)
	column := matches[1]
	row, err := strconv.Atoi(matches[2])
	if err != nil {
		return 0, "", fmt.Errorf("invalid row number in input: %s", input)
	}
	return row, column, nil
}

func (etlx *ETLX) RunEXPORTS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "EXPORTS"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
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
		mainConn, _ := metadata["connection"].(string)
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
			fname = fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, mainPath, table)
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
				fname = etlx.SetQueryPlaceholders(fname, table, "", dateRef)
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
				_log2["fname"] = fname
			}
			processLogs = append(processLogs, _log2)
		} else if okTemplate && okMapping {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"start_at":    start3,
			}
			//fmt.Println(template, mapping)
			if ok, _ := fileExists(template.(string)); !ok {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: %s givem as template does not exists", key, itemKey, template)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3)
			} else {
				// Check for supported spreadsheet extensions
				ext := filepath.Ext(template.(string))
				if ext != ".xlsx" && ext != ".xls" && ext != ".xlsm" {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("unsupported template file extension: %s", ext)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
				// Open or create a new workbook
				var file *excelize.File
				var err error
				if ok, _ := fileExists(template.(string)); ok {
					file, err = excelize.OpenFile(template.(string))
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("failed to open template file: %s", err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3)
						processLogs = append(processLogs, _log2)
						return nil
					}
					defer func() {
						if err := file.Close(); err != nil {
							fmt.Printf("failed to close the file: %v", err)
						}
					}()
				} else {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("template doesn't exists: %s", template)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
				_mapp := []map[string]any{}
				switch _map := mapping.(type) {
				case nil:
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s error mapeamento vazio", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				case string:
					// Single query reference
					// QUERY
					sql := _map
					if _, ok := item[_map]; ok {
						sql = item[sql].(string)
					}
					sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
					rows, _, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
					// Fetch data from the database using the provided SQL query
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to execute map query: %s", key, itemKey, err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3)
						processLogs = append(processLogs, _log2)
						return nil
					}
					_mapp = *rows
				case []any:
					for _, item := range mapping.([]any) {
						_mapp = append(_mapp, item.(map[string]any))
					}
				default:
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s invalid mapping data type: %T", key, itemKey, _map)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
				if len(_mapp) == 0 {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s invalid mapping length Zero: ", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					processLogs = append(processLogs, _log2)
					return nil
				}
				for _, detail := range _mapp {
					//fmt.Println(detail)
					// Skip inactive entries
					if active, ok := detail["active"].(bool); ok {
						if !active {
							continue
						}
					}
					sheet, _ := detail["sheet"].(string)
					table, _ := detail["table"].(string)
					_range, _ := detail["range"].(string)
					_type, _ := detail["type"].(string)
					sql, _ := detail["sql"].(string)
					header, _ := detail["header"].(bool)
					if_exists, _ := detail["if_exists"].(string)
					table_style, _ := detail["table_style"].(string)
					formulas, okFormulas := detail["formulas"].([]any)
					formula, okFormula := detail["formula"].(string)
					//fmt.Println(sheet, table, sql, _range, _type, header)
					//fmt.Println(sheet, okFormulas, formulas, detail["formulas"])
					// Check or create the destination sheet
					sheetIndex, err := file.GetSheetIndex(sheet)
					if sheetIndex == -1 || err != nil {
						file.NewSheet(sheet)
					} else {
						if if_exists == "delete" {
							if table != "" {
								if err := file.DeleteTable(table); err != nil {
									fmt.Printf("failed to delete table %s: %s\n", table, err)
								}
							}
							file.DeleteSheet(sheet)
							file.NewSheet(sheet)
						}
					}
					rows := &[]map[string]any{}
					columns := []string{}
					// QUERY
					if sql != "" {
						if _, ok := item[sql]; ok {
							sql = item[sql].(string)
						}
						sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
						rows, columns, err = etlx.Query(dbConn, sql, item, fname, "", dateRef)
						// Fetch data from the database using the provided SQL query
						if err != nil {
							//fmt.Printf("error executing query for detail ID %v: %s\n", detail, err)
							_log2["success"] = false
							_log2["msg"] = fmt.Sprintf("%s -> %s -> %s -> failed to execute query: %s", key, itemKey, sheet, err)
							_log2["end_at"] = time.Now()
							_log2["duration"] = time.Since(start3)
							processLogs = append(processLogs, _log2)
							continue
						}
					}
					startRow, col, err := getStartOfRange(_range)
					if err != nil {
						fmt.Printf("Error for '%s': %v\n", _range, err)
						startRow, col = 1, "A"
					}
					// Convert the start column to an integer offset
					startColIndex := int(strings.ToUpper(col)[0] - 'A')
					if _type == "value" {
						cell, err := excelize.JoinCellName(string(rune('A'+startColIndex)), startRow)
						if err != nil {
							fmt.Printf("failed to set columns: %s\n", err)
						}
						if len((*rows)) > 0 {
							_val := (*rows)[0]
							key, okKey := detail["key"].(string)
							if okKey {
								file.SetCellValue(sheet, cell, _val[key])
							} else {
								file.SetCellValue(sheet, cell, _val["value"])
							}
						} else {
							file.SetCellValue(sheet, cell, nil)
						}
					} else if _type == "formula" {
						if okFormula && formula != "" {
							if err := file.SetCellFormula(sheet, _range, formula); err != nil {
								fmt.Printf("Failed to set formula: %v", err)
							}
						}
					} else {
						rowIdx := startRow
						// Write column headers
						if header {
							for colIdx, colName := range columns {
								cell, err := excelize.JoinCellName(string(rune('A'+startColIndex+colIdx)), startRow)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								file.SetCellValue(sheet, cell, colName)
							}
							rowIdx++
						}
						// Write data rows
						for _, value := range *rows {
							for colIdx, colName := range columns {
								cell, err := excelize.JoinCellName(string(rune('A'+startColIndex+colIdx)), rowIdx)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								file.SetCellValue(sheet, cell, value[colName])
							}
							rowIdx++
						}
						// Create Excel table if `table` is specified
						if table != "" {
							startCell, err := excelize.JoinCellName(string(rune('A'+startColIndex)), startRow)
							if err != nil {
								fmt.Printf("failed to set columns: %s\n", err)
							}
							endCell, err := excelize.JoinCellName(string(rune('A'+startColIndex+len(columns)-1)), rowIdx-1)
							if err != nil {
								fmt.Printf("failed to set columns: %s\n", err)
							}
							tableRange := fmt.Sprintf("%s:%s", startCell, endCell)
							//fmt.Printf("table: %s sheet: %s range: %s\n", table, sheet, tableRange)
							StyleName := ""
							if table_style != "" {
								StyleName = table_style
							}
							//fmt.Println("StyleName:", StyleName)
							err = file.AddTable(sheet, &excelize.Table{
								Name:            table,
								Range:           tableRange,
								StyleName:       StyleName, // "TableStyleMedium9",
								ShowFirstColumn: false,
								ShowLastColumn:  false,
								//ShowRowStripes:    &(true),
								ShowColumnStripes: false,
							})
							if err != nil {
								fmt.Printf("failed to create table %s on sheet %s range %s: %s\n", table, sheet, tableRange, err)
							}
						}
						// FORMULAS
						if okFormulas && formulas != nil {
							//fmt.Println("FORMULAS:", formulas)
							for _, value := range formulas {
								//fmt.Println("FORMULA:", value)
								_formula_value, ok := value.(map[string]any)
								if !ok {
									continue
								}
								if active, ok := _formula_value["active"].(bool); ok {
									if !active {
										continue
									}
								}
								_formula_column := _formula_value["column"].(string)
								_formula := _formula_value["formula"].(string)
								formulaColIndex := int(strings.ToUpper(_formula_column)[0] - 'A')
								startCell, err := excelize.JoinCellName(string(rune('A'+formulaColIndex)), startRow+1)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								endCell, err := excelize.JoinCellName(string(rune('A'+formulaColIndex)), rowIdx-1)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								_range := fmt.Sprintf("%s:%s", startCell, endCell)
								fmt.Println(sheet, _range, _formula)
								if err := file.SetCellFormula(sheet, _range, _formula); err != nil {
									fmt.Printf("Failed to set formula: %v\n", err)
								}
							}
						}
					}
				}
				outputFile := filepath.Join(os.TempDir(), "_", filepath.Base(template.(string)))
				if fname != "" && filepath.Base(fname) != "" {
					outputFile = fname
				} else if fname != "" && filepath.Base(fname) == "" && filepath.Dir(fname) != "" {
					outputFile = filepath.Join(filepath.Dir(fname), "_", filepath.Base(template.(string)))
				}
				outputFile = etlx.ReplaceQueryStringDate(outputFile, dateRef)
				//fmt.Println(outputFile)
				err = file.SaveAs(outputFile)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to save file: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3)
					_log2["fname"] = outputFile
				}
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
