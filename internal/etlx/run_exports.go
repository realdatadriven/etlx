package etlxlib

import (
	"fmt"
	"io"
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

func columnIndexToName(n int) string {
	name := ""
	for n >= 0 {
		name = string(rune('A'+(n%26))) + name
		n = n/26 - 1
	}
	return name
}

func isEmpty(s string) bool {
	return len(s) == 0
}

// clearEntireSheetContent clears all content from a sheet
func clearEntireSheetContent(f *excelize.File, sheetName string) {
	// Get all rows to determine the range
	rows, err := f.GetRows(sheetName)
	if err != nil {
		fmt.Printf("Error getting rows: %v\n", err)
		return
	}

	// Clear all cells
	for rowIndex, row := range rows {
		for colIndex := range row {
			cell, _ := excelize.CoordinatesToCellName(colIndex+1, rowIndex+1)
			f.SetCellValue(sheetName, cell, "")
		}
	}
}

// parseCellReference parses a cell reference like "A2" and returns column and row
func parseCellReference(cellRef string) (col string, row int, err error) {
	// Use regex to separate column letters from row numbers
	// Pattern: one or more letters followed by one or more digits
	re := regexp.MustCompile(`^([A-Z]+)(\d+)$`)
	matches := re.FindStringSubmatch(cellRef)

	if matches == nil {
		return "", 0, fmt.Errorf("invalid cell reference format (expected format: A1, B2, etc.)")
	}

	col = matches[1]
	rowStr := matches[2]

	// Convert row string to integer
	row, err = strconv.Atoi(rowStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid row number '%s'", rowStr)
	}

	// Validate row number (Excel rows are 1-based and max is 1,048,576)
	if row < 1 || row > 1048576 {
		return "", 0, fmt.Errorf("row number %d is out of valid range (1-1048576)", row)
	}

	return col, row, nil
}

func parseRange(cellRange string) (startCol string, startRow int, endCol string, endRow int, err error) {
	// Validate basic format
	if !strings.Contains(cellRange, ":") {
		return "", 0, "", 0, fmt.Errorf("range must contain ':' (e.g., 'A2:F10')")
	}

	parts := strings.Split(cellRange, ":")
	if len(parts) != 2 {
		return "", 0, "", 0, fmt.Errorf("range must have exactly one ':' separator")
	}

	startPart := strings.TrimSpace(strings.ToUpper(parts[0]))
	endPart := strings.TrimSpace(strings.ToUpper(parts[1]))

	if startPart == "" || endPart == "" {
		return "", 0, "", 0, fmt.Errorf("both start and end positions must be specified")
	}

	// Parse start position
	startCol, startRow, err = parseCellReference(startPart)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("invalid start position '%s': %v", startPart, err)
	}

	// Parse end position
	endCol, endRow, err = parseCellReference(endPart)
	if err != nil {
		return "", 0, "", 0, fmt.Errorf("invalid end position '%s': %v", endPart, err)
	}

	return startCol, startRow, endCol, endRow, nil
}

// clearSpecificRange clears a specific range of cells by parsing the range string
// Supports formats like: "A2:C5", "A2:F10", "B3:D3", etc.
// Returns error if the range format is invalid or if clearing fails
func clearSpecificRange(f *excelize.File, sheetName, cellRange string) error {
	startCol, startRow, endCol, endRow, err := parseRange(cellRange)
	if err != nil {
		return fmt.Errorf("invalid range '%s': %v", cellRange, err)
	}

	// Convert column letters to numbers
	startColNum, err := excelize.ColumnNameToNumber(startCol)
	if err != nil {
		return fmt.Errorf("invalid start column '%s': %v", startCol, err)
	}

	endColNum, err := excelize.ColumnNameToNumber(endCol)
	if err != nil {
		return fmt.Errorf("invalid end column '%s': %v", endCol, err)
	}

	// Validate range
	if startRow > endRow {
		return fmt.Errorf("start row (%d) cannot be greater than end row (%d)", startRow, endRow)
	}
	if startColNum > endColNum {
		return fmt.Errorf("start column (%s) cannot be greater than end column (%s)", startCol, endCol)
	}

	// Clear each cell in the range
	for row := startRow; row <= endRow; row++ {
		for col := startColNum; col <= endColNum; col++ {
			cell, err := excelize.CoordinatesToCellName(col, row)
			if err != nil {
				return fmt.Errorf("error creating cell name for column %d, row %d: %v", col, row, err)
			}
			f.SetCellValue(sheetName, cell, "")
		}
	}

	return nil
}

func TableExistsInSheet(f *excelize.File, sheet, tableName string) (bool, *excelize.Table, error) {
	tables, err := f.GetTables(sheet)
	if err != nil {
		return false, nil, err
	}
	for _, tbl := range tables {
		if tbl.Name == tableName {
			return true, &tbl, nil
		}
	}
	return false, nil, nil
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
		"name": key,
		"key":  key, "start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	EXPORTSRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		// fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return fmt.Errorf("deactivated %s", "")
			}
		}
		// MAIN PATH
		mainPath, okMainPath := metadata["path"].(string)
		if okMainPath {
			pth := etlx.ReplaceQueryStringDate(mainPath, dateRef)
			//fmt.Println("MAIN PATH", pth)
			if ok, _ := pathExists(pth); !ok {
				err := os.Mkdir(pth, 0755)
				if err != nil {
					return fmt.Errorf("%s ERR: trying to create the export path %s -> %s", key, pth, err)
				}
			}
		} else {

		}
		mainConn, _ := metadata["connection"].(string)
		mainDescription = metadata["description"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": time.Now(),
				"end_at":  time.Now(),
				"success": true,
				"msg":     "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return nil
			}
		}
		if only, okOnly := extraConf["only"]; okOnly {
			//fmt.Println("ONLY", only, len(only.([]string)))
			if len(only.([]string)) == 0 {
			} else if !etlx.Contains(only.([]string), itemKey) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Excluded from the process",
				})
				return nil
			}
		}
		if skip, okSkip := extraConf["skip"]; okSkip {
			//fmt.Println("SKIP", skip, len(skip.([]string)))
			if len(skip.([]string)) == 0 {
			} else if etlx.Contains(skip.([]string), itemKey) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Excluded from the process",
				})
				return nil
			}
		}
		beforeSQL, okBefore := itemMetadata["before_sql"]
		exportSQL, okExport := itemMetadata["export_sql"]
		dataSQL, okData := itemMetadata["data_sql"]
		afterSQL, okAfter := itemMetadata["after_sql"]
		template, okTemplate := itemMetadata["template"]
		textTemplate, okTextTemplate := itemMetadata["text_template"].(bool)
		/*tmplExt := ""
		if okTemplate {
			tmplExt = filepath.Ext(template.(string))
		}*/
		mapping, okMapping := itemMetadata["mapping"]
		tmpPrefix, okTmpPrefix := itemMetadata["tmp_prefix"]
		conn, okCon := itemMetadata["connection"]
		dtRef, okDtRef := itemMetadata["date_ref"]
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
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"key":         key, "item_key": itemKey, "start_at": start3,
			"ref": dtRef,
		}
		if !okCon {
			conn = mainConn
		}
		dbConn, err := etlx.GetDB(conn.(string))
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s ERR: connecting to %s in : %s", key, itemKey, conn, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			processLogs = append(processLogs, _log2)
			return nil
		}
		defer dbConn.Close()
		_log2["success"] = true
		_log2["msg"] = fmt.Sprintf("%s -> %s CONN: connection to %s successfull", key, itemKey, conn)
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		processLogs = append(processLogs, _log2)
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
		if okPath && path != "" && !isEmpty(path) {
			fname = path
			if filepath.IsAbs(fname) {
			} else if filepath.IsLocal(fname) && !isEmpty(mainPath) {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			} else if filepath.Dir(fname) != "" && okMainPath && mainPath != "" {
				fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
			}
		} else if okMainPath && mainPath != "" && !isEmpty(mainPath) {
			fname = fmt.Sprintf(`%s/%s_{YYYYMMDD}.csv`, mainPath, table)
		}
		// QUERIES TO RUN AT beginning
		if okBefore {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, beforeSQL, item, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s Before error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s Before ", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			}
			processLogs = append(processLogs, _log2)
		}
		// CHECK CONDITION
		condition, okCondition := itemMetadata["condition"].(string)
		condMsg, okCondMsg := itemMetadata["condition_msg"].(string)
		failedCondition := false
		if okCondition && condition != "" {
			cond, err := etlx.ExecuteCondition(dbConn, condition, itemMetadata, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				processLogs = append(processLogs, _log2)
				//return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			} else if !cond {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed the condition %s was not met!", key, itemKey, condition)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				if okCondMsg && condMsg != "" {
					_log2["msg"] = fmt.Sprintf("%s -> %s COND: failed %s", key, itemKey, etlx.SetQueryPlaceholders(condMsg, table, fname, dateRef))
				}
				processLogs = append(processLogs, _log2)
				// return fmt.Errorf("%s", _log2["msg"])
				failedCondition = true
			}
		}
		// MAIN QUERIES
		if okExport && !(okTemplate && okMapping) && !failedCondition {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, exportSQL, item, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				fname = etlx.SetQueryPlaceholders(fname, table, "", dateRef)
				// fmt.Println(1, fname)
				if !filepath.IsAbs(path) {
					if okTmpPrefix && tmpPrefix != "" {
						fname = etlx.SetQueryPlaceholders(fmt.Sprintf("%s/%s", tmpPrefix, path), table, "", dateRef)
					} else {
						fname = etlx.SetQueryPlaceholders(path, table, "", dateRef)
					}
					// fmt.Println(2, fname)
				}
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
				_log2["fname"] = fname
			}
			processLogs = append(processLogs, _log2)
		} else if okTemplate && okMapping && !failedCondition {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			// Check for supported spreadsheet extensions
			tmpl := template.(string)
			// fmt.Printf("%T: %s %v", path, path, path != "")
			if filepath.IsLocal(tmpl) && !isEmpty(mainPath) {
				tmpl = fmt.Sprintf(`%s/%s`, mainPath, tmpl)
			} else if filepath.Dir(tmpl) != "" && okMainPath && mainPath != "" {
				tmpl = fmt.Sprintf(`%s/%s`, mainPath, tmpl)
			}
			template = tmpl
			//fmt.Println(template)
			//fmt.Println(template, mapping)
			if ok, _ := fileExists(template.(string)); !ok {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: %s givem as template does not exists", key, itemKey, template)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				ext := filepath.Ext(template.(string))
				if ext != ".xlsx" && ext != ".xls" && ext != ".xlsm" {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("unsupported template file extension: %s", ext)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
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
						_log2["duration"] = time.Since(start3).Seconds()
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
					_log2["duration"] = time.Since(start3).Seconds()
					processLogs = append(processLogs, _log2)
					return nil
				}
				_mapp := []map[string]any{}
				switch _map := mapping.(type) {
				case nil:
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s error mapping empty", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
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
						_log2["duration"] = time.Since(start3).Seconds()
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
					_log2["duration"] = time.Since(start3).Seconds()
					processLogs = append(processLogs, _log2)
					return nil
				}
				if len(_mapp) == 0 {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s invalid mapping length Zero: ", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
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
					clear_range, _ := detail["clear_range"].(string)
					clear_sheet, _ := detail["clear_sheet"].(bool)
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
						} else if clear_sheet {
							// Clear especific range in the sheet
							clearEntireSheetContent(file, sheet)
						} else if clear_range != "" {
							// Clear especific range in the sheet
							err := clearSpecificRange(file, sheet, clear_range)
							if err != nil {
								fmt.Printf("failed to clear range %s in sheet %s: %s\n", clear_range, sheet, err)
							}
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
							_log2["duration"] = time.Since(start3).Seconds()
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
						// cell, err := excelize.JoinCellName(string(rune('A'+startColIndex)), startRow)
						// fmt.Println(columnIndexToName(startColIndex))
						cell, err := excelize.JoinCellName(columnIndexToName(startColIndex), startRow)
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
								// cell, err := excelize.JoinCellName(string(rune('A'+startColIndex+colIdx)), startRow)
								cell, err := excelize.JoinCellName(columnIndexToName(startColIndex+colIdx), startRow)
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
								//cell, err := excelize.JoinCellName(string(rune('A'+startColIndex+colIdx)), rowIdx)
								cell, err := excelize.JoinCellName(columnIndexToName(startColIndex+colIdx), rowIdx)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								file.SetCellValue(sheet, cell, value[colName])
							}
							rowIdx++
						}
						// Create Excel table if `table` is specified
						if table != "" {
							//startCell, err := excelize.JoinCellName(string(rune('A'+startColIndex)), startRow)
							startCell, err := excelize.JoinCellName(columnIndexToName(startColIndex), startRow)
							if err != nil {
								fmt.Printf("failed to set columns: %s\n", err)
							}
							//endCell, err := excelize.JoinCellName(string(rune('A'+startColIndex+len(columns)-1)), rowIdx-1)
							endCell, err := excelize.JoinCellName(columnIndexToName(startColIndex+len(columns)-1), rowIdx-1)
							if err != nil {
								fmt.Printf("failed to set columns: %s\n", err)
							}
							tableRange := fmt.Sprintf("%s:%s", startCell, endCell)
							//fmt.Printf("table: %s sheet: %s range: %s\n", table, sheet, tableRange)
							StyleName := ""
							if table_style != "" {
								StyleName = table_style
							}
							tableExists, tbl, _ := TableExistsInSheet(file, sheet, table)
							if tableExists {
								tbl.Range = tableRange
							} else {
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
								//startCell, err := excelize.JoinCellName(string(rune('A'+formulaColIndex)), startRow+1)
								startCell, err := excelize.JoinCellName(columnIndexToName(formulaColIndex), startRow+1)
								if err != nil {
									fmt.Printf("failed to set columns: %s\n", err)
								}
								//endCell, err := excelize.JoinCellName(string(rune('A'+formulaColIndex)), rowIdx-1)
								endCell, err := excelize.JoinCellName(columnIndexToName(formulaColIndex), rowIdx-1)
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
					/*if filepath.IsAbs(fname) {
					} else if filepath.IsLocal(fname) {
						fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
					}*/
				} else if fname != "" && filepath.Base(fname) == "" && filepath.Dir(fname) != "" {
					outputFile = filepath.Join(filepath.Dir(fname), "_", filepath.Base(template.(string)))
				}
				outputFile = etlx.ReplaceQueryStringDate(outputFile, dateRef)
				//fmt.Println(outputFile)
				err = file.UpdateLinkedValue()
				if err != nil {
					fmt.Println("UpdateLinkedValue() Error:", err)
				}
				err = file.SaveAs(outputFile)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to save file: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s", key, itemKey)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
					fname = etlx.ReplaceQueryStringDate(outputFile, dateRef)
					// fmt.Println(path, filepath.IsAbs(path))
					if !filepath.IsAbs(path) {
						if okTmpPrefix && tmpPrefix != "" {
							fname = etlx.ReplaceQueryStringDate(fmt.Sprintf("%s/%s", tmpPrefix, path), dateRef)
						} else {
							fname = etlx.ReplaceQueryStringDate(path, dateRef)
						}
					}
					_log2["fname"] = fname
				}
			}
			processLogs = append(processLogs, _log2)

		} else if okTemplate && textTemplate && okTextTemplate && !failedCondition {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			if !okData {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s error: 'data_sql' found!", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				data := map[string]any{}
				start3 := time.Now()
				_log2 := map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": start3,
				}
				switch _map := dataSQL.(type) {
				case string:
					sql := _map
					if _, ok := item[_map]; ok {
						sql = item[sql].(string)
					}
					sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
					rows, _, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
					if err != nil {
						data[_map] = map[string]any{
							"success": false,
							"msg":     fmt.Sprintf("failed to execute map query %s %s", _map, err),
							"data":    []map[string]any{},
						}
					} else {
						data[_map] = map[string]any{
							"success": true,
							"data":    *rows,
						}
					}
				case []any:
					for _, _sql := range dataSQL.([]any) {
						sql := _sql.(string)
						if _, ok := item[_sql.(string)]; ok {
							sql = item[_sql.(string)].(string)
						}
						sql = etlx.SetQueryPlaceholders(sql, table, fname, dateRef)
						rows, _, err := etlx.Query(dbConn, sql, item, fname, "", dateRef)
						if err != nil {
							data[_sql.(string)] = map[string]any{
								"success": false,
								"msg":     fmt.Sprintf("failed to execute map query %s %s", _map, err),
								"data":    []map[string]any{},
							}
						} else {
							data[_sql.(string)] = map[string]any{
								"success": true,
								"data":    *rows,
							}
						}
					}
				default:
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s invalid queries data type: %T", key, itemKey, _map)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
					processLogs = append(processLogs, _log2)
				}
				if _, ok := itemMetadata["data"].(map[string]any); ok {
					for key, d := range itemMetadata["data"].(map[string]any) {
						data[key] = d
					}
				}
				tmpl, ok := item[template.(string)].(string)
				if !ok {
					tmpl = template.(string)
				}
				// render template
				parsedTmpl, err := etlx.RenderTemplate(tmpl, data)
				//fmt.Println(parsedTmpl)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> failed to parse the template: %s", key, itemKey, err)
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
					fmt.Println(0, _log2["msg"])
				} else {
					fname = etlx.ReplaceQueryStringDate(fname, dateRef)
					// Create the file (or truncate if it exists)
					file, err := os.Create(fname)
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("%s -> %s -> Error creating file: %s", key, itemKey, err)
						_log2["end_at"] = time.Now()
						_log2["duration"] = time.Since(start3).Seconds()
						//fmt.Println(1, _log2["msg"])
					} else {
						defer file.Close() // Close the file after the function completes
						// Write the text to the file
						_, err = io.WriteString(file, parsedTmpl)
						if err != nil {
							_log2["success"] = false
							_log2["msg"] = fmt.Sprintf("%s -> %s -> Error writing to file: %s", key, itemKey, err)
							_log2["end_at"] = time.Now()
							_log2["duration"] = time.Since(start3).Seconds()
							//fmt.Println(2, _log2["msg"])
						} else {
							_log2["success"] = true
							_log2["msg"] = fmt.Sprintf("%s -> %s: TXT TMPL Generate!", key, itemKey)
							_log2["end_at"] = time.Now()
							_log2["duration"] = time.Since(start3).Seconds()
							if return_content, ok := itemMetadata["return_content"].(bool); ok && return_content {
								_log2["content"] = parsedTmpl
							}
							fname = etlx.ReplaceQueryStringDate(fname, dateRef)
							if !filepath.IsAbs(path) {
								if okTmpPrefix && tmpPrefix != "" && tmpPrefix != nil {
									fname = etlx.ReplaceQueryStringDate(fmt.Sprintf("%s/%s", tmpPrefix, path), dateRef)
								} else {
									fname = etlx.ReplaceQueryStringDate(path, dateRef)
								}
							}
							_log2["fname"] = fname
						}
					}
				}
				processLogs = append(processLogs, _log2)
			}
		} else {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s: Missconfiguration, it was unable to identify export type", key, itemKey)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			processLogs = append(processLogs, _log2)
			//fmt.Println(4, _log2["msg"])
		}
		// QUERIES TO RUN AT THE END
		if okAfter {
			start3 := time.Now()
			_log2 := map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": start3,
			}
			err = etlx.ExecuteQuery(dbConn, afterSQL, item, fname, "", dateRef)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s After error: %s", key, itemKey, err)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s -> %s After ", key, itemKey)
				_log2["end_at"] = time.Now()
				_log2["duration"] = time.Since(start3).Seconds()
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
		"key":         key, "start_at": processLogs[0]["start_at"],
		"end_at":   time.Now(),
		"duration": time.Since(start).Seconds(),
	}
	return processLogs, nil
}
