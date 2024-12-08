package db

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"

	_ "github.com/alexbrainman/odbc"
)

type ODBC struct {
	*sql.DB
}

func NewODBC(dsn string) (*ODBC, error) {
	//fmt.Printf("DSN: %s\n", dsn)
	db, err := sql.Open("odbc", dsn)
	if err != nil {
		return nil, err
	}
	//fmt.Println(driverName, dsn)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(2 * time.Hour)
	return &ODBC{db}, nil
}

func (db *ODBC) ExecuteQuery(query string, data ...interface{}) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutODBC)
	defer cancel()
	result, err := db.ExecContext(ctx, query, data...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(id), err
}

func (db *ODBC) ExecuteQueryRowsAffected(query string, data ...interface{}) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	result, err := db.ExecContext(ctx, query, data...)
	if err != nil {
		return 0, err
	}
	id, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return id, err
}

func (db *ODBC) QueryMultiRows(query string, params ...interface{}) (*[]map[string]interface{}, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutODBC)
	defer cancel()
	var result []map[string]interface{}
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		row, err := ScanRowToMap(rows)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan row to map: %w", err)
		}
		result = append(result, row)
	}
	return &result, true, err
}

func (db *ODBC) QueryMultiRowsWithCols(query string, params ...interface{}) (*[]map[string]interface{}, []string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	var result []map[string]interface{}
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("failed to get columns: %s", err)
	}
	for rows.Next() {
		row, err := ScanRowToMap(rows)
		if err != nil {
			return nil, nil, false, fmt.Errorf("failed to scan row to map: %w", err)
		}
		result = append(result, row)
	}
	return &result, columns, true, err
}

func (db *ODBC) AllTables(params map[string]interface{}, extra_conf map[string]interface{}) (*[]map[string]interface{}, bool, error) {
	// Logic to get all tables for DuckDB
	return nil, false, nil
}

func (db *ODBC) TableSchema(params map[string]interface{}, table string, dbName string, extra_conf map[string]interface{}) (*[]map[string]interface{}, bool, error) {
	return nil, false, nil
}

func (db *ODBC) ExecuteNamedQuery(query string, data map[string]interface{}) (int, error) {
	return 0, fmt.Errorf("not suported yet %s", "_")
}

func (db *ODBC) ExecuteQueryPGInsertWithLastInsertId(query string, data ...interface{}) (int, error) {
	return 0, fmt.Errorf("not suported %s", "_")
}

func isUTF8(s string) bool {
	return utf8.ValidString(s)
}

func convertToUTF8(isoStr string) (string, error) {
	if isUTF8(isoStr) {
		return isoStr, nil
	}
	reader := strings.NewReader(isoStr)
	transformer := charmap.ISO8859_1.NewDecoder()
	utf8Bytes, err := io.ReadAll(transform.NewReader(reader, transformer))
	if err != nil {
		return "", err
	}
	return string(utf8Bytes), nil
}

func hasDecimalPlace(v interface{}) (bool, error) {
	// Try to cast v to float64
	floatVal, ok := v.(float64)
	if !ok {
		return false, fmt.Errorf("value is not a float64, it is %v", reflect.TypeOf(v))
	}

	// Check if the float has a decimal part
	if floatVal != float64(int(floatVal)) {
		return true, nil
	}
	return false, nil
}

func (db *ODBC) Query2CSV(query string, csv_path string, params ...interface{}) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutODBC)
	defer cancel()
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		//fmt.Println(1, err)
		return false, err
	}
	defer rows.Close()
	csvFile, err := os.Create(csv_path)
	if err != nil {
		return false, fmt.Errorf("error creating CSV file: %w", err)
	}
	defer csvFile.Close()
	// CSV
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("error getting column names: %w", err)
	}
	// Write column names to CSV header
	csvWriter.Write(columns)
	for rows.Next() {
		row, err := ScanRowToMap(rows)
		if err != nil {
			return false, fmt.Errorf("failed to scan row to map: %w", err)
		}
		var rowData []string
		//for _, value := range row {
		for _, col := range columns {
			value := row[col]
			//rowData = append(rowData, fmt.Sprintf("%v", value))
			switch v := value.(type) {
			case nil:
				// Format integer types
				rowData = append(rowData, "")
			case int, int8, int16, int32, int64:
				// Format integer types
				rowData = append(rowData, fmt.Sprintf("%d", v))
			case float64, float32:
				//fmt.Println(col, v)
				// Format large numbers without scientific notation
				hasDec, err := hasDecimalPlace(v)
				if err != nil {
					fmt.Println(err)
					rowData = append(rowData, fmt.Sprintf("%v", value))
				} else if hasDec {
					rowData = append(rowData, fmt.Sprintf("%f", v))
				} else {
					rowData = append(rowData, fmt.Sprintf("%.f", v))
				}
			case []byte:
				// Convert byte slice (UTF-8 data) to a string
				utf8Str, err := convertToUTF8(string(v))
				if err != nil {
					fmt.Println("Failed to convert to UTF-8:", v, err)
				}
				rowData = append(rowData, strings.TrimSpace(string(utf8Str)))
			default:
				// Default formatting for other types
				rowData = append(rowData, fmt.Sprintf("%v", value))
			}
		}
		csvWriter.Write(rowData)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("error iterating rows: %w", err)
	}
	return true, nil
}

func (db *ODBC) QuerySingleRow(query string, params ...interface{}) (*map[string]interface{}, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutODBC)
	defer cancel()
	result := map[string]interface{}{}
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	if rows.Next() {
		/*if err := rows.Scan(result); err != nil {
			return nil, false, err
		}*/
		result, err = ScanRowToMap(rows)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan row to map: %w", err)
		}
	}
	return &result, true, err
}
