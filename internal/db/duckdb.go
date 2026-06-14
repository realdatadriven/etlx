package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/realdatadriven/etlx/internal/env"

	_ "github.com/duckdb/duckdb-go/v2"
)

type DuckDB struct {
	*sql.DB
}

// ScanRowToMap converts a single row into a map[string]any.
func ScanRowToMap(rows *sql.Rows) (map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	values := make([]any, len(columns))
	valuePointers := make([]any, len(columns))
	for i := range values {
		valuePointers[i] = &values[i]
	}
	if err := rows.Scan(valuePointers...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	rowMap := make(map[string]any)
	for i, colName := range columns {
		rowMap[colName] = values[i]
	}
	return rowMap, nil
}

func NewDuckDB(dsn string) (*DuckDB, error) {
	/*if os.Getenv("ETLX_DUCKDB_OPTIONS") != "" {
		_extra_opts := os.Getenv("ETLX_DUCKDB_OPTIONS")
		if !strings.HasPrefix(_extra_opts, "?") {
			_extra_opts = "?" + _extra_opts
		}
		if dsn == "" {
			dsn = ":memory:"
		}
		dsn = fmt.Sprintf("%s%s", dsn, _extra_opts)
	}
	fmt.Printf("DSN: %s\n", dsn)*/
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	defaultTimeoutDuckDB = time.Duration(env.GetInt("DUCKDB_DFLT_TIMEOUT", 15)) * time.Minute
	//fmt.Println(driverName, dsn)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxIdleTime(defaultTimeoutDuckDB)
	db.SetConnMaxLifetime(2 * time.Hour)
	/*if os.Getenv("ETLX_DUCKDB_ALLOWED_DIRECTORIES") != "" {
		_, err = db.ExecContext(context.Background(), os.Getenv("ETLX_DUCKDB_ALLOWED_DIRECTORIES"))
		if err != nil {
			return nil, fmt.Errorf("failed to set allowed directories from ETLX_DUCKDB_ALLOWED_DIRECTORIES: %w", err)
		}
	}
	if os.Getenv("ETLX_DUCKDB_EXTERNAL_ACCESS") != "" {
		_, err = db.ExecContext(context.Background(), os.Getenv("ETLX_DUCKDB_EXTERNAL_ACCESS"))
		if err != nil {
			return nil, fmt.Errorf("failed to set external access from ETLX_DUCKDB_EXTERNAL_ACCESS: %w", err)
		}
	}
	if os.Getenv("ETLX_DUCKDB_ALLOWED_CONFIGS") != "" {
		_, err = db.ExecContext(context.Background(), os.Getenv("ETLX_DUCKDB_ALLOWED_CONFIGS"))
		if err != nil {
			return nil, fmt.Errorf("failed to set allowed configs from ETLX_DUCKDB_ALLOWED_CONFIGS: %w", err)
		}
	}
	if os.Getenv("ETLX_DUCKDB_LOCK_CONFIGURATION") != "" {
		_, err = db.ExecContext(context.Background(), os.Getenv("ETLX_DUCKDB_LOCK_CONFIGURATION"))
		if err != nil {
			return nil, fmt.Errorf("failed to set lock configuration from ETLX_DUCKDB_LOCK_CONFIGURATION: %w", err)
		}
	}
	if os.Getenv("ETLX_DUCKDB_SECURITY_CONFIGS") != "" {
		_, err = db.ExecContext(context.Background(), os.Getenv("ETLX_DUCKDB_SECURITY_CONFIGS"))
		if err != nil {
			return nil, fmt.Errorf("failed to set security configs from ETLX_DUCKDB_SECURITY_CONFIGS: %w", err)
		}
	}*/
	return &DuckDB{db}, nil
}

func (db *DuckDB) New(dsn string) (*DuckDB, error) {
	//fmt.Printf("db DRIVER: %s DSN: %s\n", driverName, dsn)
	_db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	//fmt.Println(driverName, dsn)
	_db.SetMaxOpenConns(25)
	_db.SetMaxIdleConns(25)
	_db.SetConnMaxIdleTime(defaultTimeoutDuckDB)
	_db.SetConnMaxLifetime(2 * time.Hour)
	return &DuckDB{_db}, nil
}

func (db *DuckDB) BeginT() (*sqlx.Tx, error) {
	return nil, fmt.Errorf("Not suported for %s", "DuckDB")
}

func (db *DuckDB) ExecuteQuery(query string, data ...any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
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

func (db *DuckDB) ExecuteQueryRowsAffected(query string, data ...any) (int64, error) {
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

func (db *DuckDB) AllTables(params map[string]any, extra_conf map[string]any) (*[]map[string]any, bool, error) {
	_query := `SELECT table_name as name FROM information_schema.tables`
	_query = `SHOW TABLES`
	// fmt.Println(_query)
	return db.QueryMultiRows(_query, []any{}...)
}

func (db *DuckDB) TableSchema(params map[string]any, table string, dbName string, extra_conf map[string]any) (*[]map[string]any, bool, error) {
	user_id := int(params["user"].(map[string]any)["user_id"].(float64))
	/*_query := fmt.Sprintf(`SELECT ROW_NUMBER() OVER () - 1 AS cid
		, column_name AS name
		, data_type AS type
		, CASE is_nullable WHEN 'NO' THEN 1 ELSE 0 END AS notnull
		, column_default AS dflt_value
		, CASE
			WHEN column_name IN (
				SELECT kcu.column_name
				FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
				JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
				ON kcu.constraint_name = tc.constraint_name
				WHERE tc.constraint_type = 'PRIMARY KEY' AND kcu.table_name = '%s'
			) THEN 1
			ELSE 0
		END AS pk
	FROM information_schema.tables
	WHERE table_schema = 'public'
		AND table_name = '%s';`, table, table)*/
	_query := fmt.Sprintf(`PRAGMA table_info("%s")`, table)
	//fmt.Println(table, _query)
	_aux_data := []map[string]any{}
	_aux_data_fk := map[string]any{}
	res, _, err := db.QueryMultiRows(_query, []any{}...)
	if err != nil {
		return nil, false, err
	}
	_query = fmt.Sprintf(`WITH foreign_keys AS (
		SELECT rc.constraint_name AS fk_name,
			rc.unique_constraint_name AS unique_name,
			kcu.table_name AS table,
			kcu.column_name AS "from",
			kcu.ordinal_position AS seq,
			kcu.table_name AS "to",
			kcu.column_name AS to_column,
			'tc.delete_rule' AS on_delete,
			'tc.update_rule' AS on_update
		FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON rc.constraint_name = kcu.constraint_name
		JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
		WHERE kcu.table_name = '%s'
	)
	SELECT ROW_NUMBER() OVER () - 1 AS id,
		seq,
		"table" AS parent_table,
		"from",
		"to",
		on_update,
		on_delete,
		'NONE' AS match
	FROM  foreign_keys;`, table)
	res_fk, _, err := db.QueryMultiRows(_query, []any{}...)
	if err != nil {
		return nil, false, err
	}
	for _, row := range *res_fk {
		// fmt.Println(row)
		_aux_data_fk[row["from"].(string)] = map[string]any{
			"referred_table":  row["table"].(string),
			"referred_column": row["to"].(string),
		}
	}
	for _, row := range *res {
		fk := false
		var referred_table string
		var referred_column string
		if _, exists := _aux_data_fk[row["name"].(string)]; exists {
			fk = true
			referred_table = _aux_data_fk[row["name"].(string)].(map[string]any)["referred_table"].(string)
			referred_column = _aux_data_fk[row["name"].(string)].(map[string]any)["referred_column"].(string)
		}
		pk := false
		if _pk, ok := row["pk"].(bool); ok {
			pk = _pk
		} else if _pk, ok := row["pk"].(int); ok {
			if _pk == 1 {
				pk = true
			}
		}
		nullable := false
		if notnull, ok := row["notnull"].(bool); ok {
			nullable = notnull
		} else if notnull, ok := row["notnull"].(int); ok {
			if notnull == 0 {
				nullable = true
			}
		}
		_aux_row := map[string]any{
			"db":              dbName,
			"table":           table,
			"field":           row["name"].(string),
			"type":            row["type"].(string),
			"comment":         nil,
			"pk":              pk,
			"autoincrement":   nil,
			"nullable":        nullable,
			"computed":        nil,
			"default":         nil,
			"fk":              fk,
			"referred_table":  referred_table,
			"referred_column": referred_column,
			"user_id":         user_id,
			"created_at":      time.Now(),
			"updated_at":      time.Now(),
			"excluded":        false,
		}
		// fmt.Println(1, row["name"].(string), _aux_row)
		_aux_data = append(_aux_data, _aux_row)
	}
	return &_aux_data, true, nil
}

func (db *DuckDB) QueryMultiRowsWithCols(query string, params ...any) (*[]map[string]any, []string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	var result []map[string]any
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

func (db *DuckDB) QueryMultiRows(query string, params ...any) (*[]map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	var result []map[string]any
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		//fmt.Println(1, err)
		return nil, false, err
	}
	defer rows.Close()
	//for rows.Next() {
	//	row := map[string]any{}
	for rows.Next() {
		row, err := ScanRowToMap(rows)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan row to map: %w", err)
		}
		result = append(result, row)
	}
	/*if err := rows.Scan(row); err != nil {
		return nil, false, err
	}*/
	//	result = append(result, row)
	//}
	return &result, true, err
}

func (db *DuckDB) QueryRows(ctx context.Context, query string, params ...any) (*sql.Rows, error) {
	return db.QueryContext(ctx, query, params...)
}

func (db *DuckDB) QuerySingleRow(query string, params ...any) (*map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	result := map[string]any{}
	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	if rows.Next() {
		result, err = ScanRowToMap(rows)
		if err != nil {
			return nil, false, fmt.Errorf("failed to scan row to map: %w", err)
		}
	}
	return &result, true, err
}

func NamedToPositional(sql string, data map[string]any) (string, []any, error) {
	// Find all :param occurrences
	re := regexp.MustCompile(`:([a-zA-Z_][a-zA-Z0-9_]*)`)
	matches := re.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return sql, nil, nil
	}
	// Collect unique parameter names in order of appearance
	seen := make(map[string]bool)
	var paramOrder []string
	for _, match := range matches {
		name := match[1]
		if !seen[name] {
			seen[name] = true
			paramOrder = append(paramOrder, name)
		}
	}
	// Check for missing values
	var args []any
	for _, name := range paramOrder {
		val, ok := data[name]
		if !ok {
			return "", nil, fmt.Errorf("missing value for parameter: %s", name)
		}
		args = append(args, val)
	}
	// Replace :name → ?
	result := re.ReplaceAllStringFunc(sql, func(s string) string {
		// We could also do a map lookup here, but simpler to just replace with ?
		return "?"
	})
	return result, args, nil
}

func (db *DuckDB) ExecuteNamedQuery(query string, data map[string]any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_positionalQuery, args, err := NamedToPositional(query, data)
	if err != nil {
		return 0, fmt.Errorf("failed to convert named query to positional: %w", err)
	}
	// fmt.Println(_positionalQuery, args)
	result, err := db.ExecContext(ctx, _positionalQuery, args...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	return int(id), nil
}

func (db *DuckDB) ExecuteQueryPGInsertWithLastInsertId(query string, data ...any) (int, error) {
	return db.ExecuteNamedQuery(query, data[0].(map[string]any))
}

func (db *DuckDB) FromParams(params map[string]any, extra_conf map[string]any) (*DB, string, string, error) {
	return nil, "", "", fmt.Errorf("not implemented yet %s", "_")
}

func (db *DuckDB) GetDriverName() string {
	return "duckdb"
}

func (db *DuckDB) GetUserByNameOrEmail(email string) (map[string]any, bool, error) {
	return nil, false, fmt.Errorf("not implemented yet %s", "_")
}

func (db *DuckDB) Query2CSV(query string, csv_path string, params ...any) (bool, error) {
	return false, fmt.Errorf("not implemented yet %s", "_")
}

func (db *DuckDB) IsEmpty(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return len(v) == 0
	case []any:
		return len(v) == 0
	case map[any]any:
		return len(v) == 0
	default:
		return false
	}
}
