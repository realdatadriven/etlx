package etlxlib

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
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
	sql_target := target_sql
	if _, ok := item[target_sql]; ok {
		sql_target = item[target_sql].(string)
	}
	sql := source_sql
	if _, ok := item[source_sql]; ok {
		sql = item[sql].(string)
	}
	chunk_size := 1_000
	if _, ok := source["chunk_size"].(int); ok {
		chunk_size = source["chunk_size"].(int)
	}
	timeout := 500
	if _, ok := source["timeout"].(int); ok {
		timeout = source["timeout"].(int)
	}
	sql = etlx.SetQueryPlaceholders(sql, "", "", dateRef)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	var result []map[string]any
	rows, err := dbSourceConn.QueryRows(ctx, sql, []any{}...)
	if err != nil {
		return fmt.Errorf("failed to execute source query %s", err)
	}
	defer rows.Close()
	/*columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("failed to get columns: %s", err)
	}*/
	i := 0
	for rows.Next() {
		i += 1
		row, err := ScanRowToMap(rows)
		if err != nil {
			return fmt.Errorf("failed to scan row to map: %w", err)
		}
		result = append(result, row)
		// send to target
		if i == chunk_size {
			i = 0
			_, err = updateTarget(dbTargetConn, sql_target, result)
			if err != nil {
				return fmt.Errorf("failed update the destination: %w", err)
			}
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
	// Escape column names (basic, you might need to adapt for SQL Server specifics)
	colList := strings.Join(columns, ", ")
	/*sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
		table,
		colList,
		strings.Join(valueRows, ",\n"),
	)*/
	re := regexp.MustCompile(`:columns\b`)
	sql_header = re.ReplaceAllString(sql_header, colList)
	sql := fmt.Sprintf("%s %s;", sql_header, strings.Join(valueRows, ",\n"))
	return sql, nil
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
	return dbTargetConn.ExecuteQuery(sql, []any{}...)
}
