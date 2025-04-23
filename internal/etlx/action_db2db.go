package etlxlib

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

func ScanRowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(columns))
	for i := range values {
		valuePointers[i] = &values[i]
	}
	if err := rows.Scan(valuePointers...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	rowMap := make(map[string]interface{})
	for i, colName := range columns {
		rowMap[colName] = values[i]
	}
	return rowMap, nil
}

func (etlx *ETLX) DB2DB(params map[string]any, item map[string]any, dateRef []time.Time) error {
	// Extract and validate required params
	source, _ := params["source"].(map[string]any)
	target, _ := params["target"].(map[string]any)
	source_conn, ok := source["conn"].(string)
	if !ok {
		return fmt.Errorf("no source conn string detected %s", source_conn)
	}
	target_conn, ok := target["conn"].(string)
	if !ok {
		return fmt.Errorf("no target conn string detected %s", target_conn)
	}
	source_sql, ok := source["sql"].(string)
	if !ok {
		return fmt.Errorf("no source conn string detected %s", source_sql)
	}
	target_sql, ok := target["sql"].(string)
	if !ok {
		return fmt.Errorf("no target conn string detected %s", target_sql)
	}
	dbSourceConn, err := etlx.GetDB(source_conn)
	if err != nil {
		return fmt.Errorf("error connecting to source: %s", source_conn)
	}
	defer dbSourceConn.Close()
	dbTargetConn, err := etlx.GetDB(target_conn)
	if err != nil {
		return fmt.Errorf("error connecting to target: %s", target_conn)
	}
	defer dbTargetConn.Close()
	// BEGIN / STARTING QUERIES
	before_source, ok := source["before"]
	if ok {
		err = etlx.ExecuteQuery(dbSourceConn, before_source, item, "", "", dateRef)
		if err != nil {
			return fmt.Errorf("error executing source preparation queries: %s", err)
		}
	}
	//fmt.Println(target_sql, item)
	sql_target := target_sql
	if _, ok := item[target_sql]; ok {
		sql_target = item[target_sql].(string)
	}
	sql := source_sql
	if _, ok := item[source_sql]; ok {
		sql = item[sql].(string)
	}
	chunk_size := 1_000
	if _, ok := source["chunk_size"]; ok {
		j, err := strconv.Atoi(fmt.Sprintf("%v", source["chunk_size"]))
		if err == nil {
			chunk_size = j
		}
	}
	//fmt.Printf("%T->%d", chunk_size, chunk_size)
	timeout := 500
	if _, ok := source["timeout"]; ok {
		j, err := strconv.Atoi(fmt.Sprintf("%v", source["timeout"]))
		if err == nil {
			timeout = j
		}
	}
	sql = etlx.SetQueryPlaceholders(sql, "", "", dateRef)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	rows, err := dbSourceConn.QueryRows(ctx, sql, []any{}...)
	if err != nil {
		return fmt.Errorf("failed to execute source query %s", err)
	}
	defer rows.Close()
	/*columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("failed to get columns: %s", err)
	}*/
	// BEGIN / STARTING QUERIES
	before_target, ok := target["before"]
	if ok {
		err = etlx.ExecuteQuery(dbTargetConn, before_target, item, "", "", dateRef)
		if err != nil {
			return fmt.Errorf("error executing target preparation queries: %s", err)
		}
	}
	i := 0
	var result []map[string]any
	for rows.Next() {
		i += 1
		row, err := ScanRowToMap(rows)
		if err != nil {
			return fmt.Errorf("failed to scan row to map: %w", err)
		}
		result = append(result, row)
		// send to target
		if i >= chunk_size {
			i = 0
			_, err = updateTarget(dbTargetConn, sql_target, result)
			if err != nil {
				// fmt.Printf("failed update the destination: %s", err)
				return fmt.Errorf("failed update the destination: %w", err)
			}
			result = result[:0]
		}
	}
	if len(result) > 0 {
		_, err = updateTarget(dbTargetConn, sql_target, result)
		if err != nil {
			// fmt.Printf("failed update the destination: %s", err)
			return fmt.Errorf("failed update the destination: %w", err)
		}
	}
	// END / CLOSING QUERIES
	after_source, ok := source["after"]
	if ok {
		err = etlx.ExecuteQuery(dbSourceConn, after_source, item, "", "", dateRef)
		if err != nil {
			return fmt.Errorf("error executing source closing queries: %s", err)
		}
	}
	after_target, ok := target["after"]
	if ok {
		err = etlx.ExecuteQuery(dbTargetConn, after_target, item, "", "", dateRef)
		if err != nil {
			return fmt.Errorf("error executing source closing queries: %s", err)
		}
	}
	return nil
}

func BuildInsertSQL(sql_header string, data []map[string]any) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("no data to insert")
	}
	// Use keys from the first map as column names
	columns := make([]string, 0, len(data[0]))
	for k := range data[0] {
		columns = append(columns, k)
	}
	var valueRows []string
	for _, row := range data {
		var values []string
		for _, col := range columns {
			val := row[col]
			values = append(values, formatValue(val))
		}
		valueRows = append(valueRows, "("+strings.Join(values, ", ")+")")
	}
	sql, err := ReplaceColumnsWithDetectedIdentifier(sql_header, columns)
	if err == nil {
		sql = fmt.Sprintf("%s %s;", sql, strings.Join(valueRows, ",\n"))
	} else {
		fmt.Println(err)
		// Escape column names (basic, you might need to adapt for SQL Server specifics)
		colList := strings.Join(columns, ", ")
		/*sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
			table,
			colList,
			strings.Join(valueRows, ",\n"),
		)*/
		re := regexp.MustCompile(`:columns\b`)
		sql_header = re.ReplaceAllString(sql_header, colList)
		sql = fmt.Sprintf("%s %s;", sql_header, strings.Join(valueRows, ",\n"))
	}
	return sql, nil
}

// Detects the quote character around :columns and replaces it with the appropriate formatted column list.
func ReplaceColumnsWithDetectedIdentifier(query string, columns []string) (string, error) {
	// Regex to capture optional identifier wrapping
	re := regexp.MustCompile("([[\"`]?):columns([]\"`]?)")
	matches := re.FindStringSubmatch(query)
	var open, close string
	if len(matches) == 3 {
		open, close = matches[1], matches[2]
	}
	// Default if nothing matched
	if open == "" && close == "" {
		open, close = "", ""
	} else if open == "[" && close != "]" {
		close = "]"
	} else if open == `"` && close != `"` {
		close = `"`
	} else if open == "`" && close != "`" {
		close = "`"
	} else if open == "(" && close == ")" {
		open, close = "", "" // treat as no identifier
	}
	// Escape square brackets inside column names for MSSQL
	formatIdentifier := func(col string) string {
		if open == "[" && close == "]" {
			col = strings.ReplaceAll(col, "]", "]]")
		}
		return open + col + close
	}
	// Apply identifier
	var escapedCols []string
	for _, col := range columns {
		escapedCols = append(escapedCols, formatIdentifier(col))
	}
	finalCols := strings.Join(escapedCols, ", ")
	// Replace the whole match with column list
	finalQuery := re.ReplaceAllString(query, finalCols)
	return finalQuery, nil
}

func formatValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(val), "'", "''") + "'"
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''") + "'"
	}
}

func updateTarget(dbTargetConn db.DBInterface, sql_target string, data []map[string]any) (int, error) {
	sql, err := BuildInsertSQL(sql_target, data)
	if err != nil {
		return 0, err
	}
	fmt.Println(sql)
	return dbTargetConn.ExecuteQuery(sql, []any{}...)
}
