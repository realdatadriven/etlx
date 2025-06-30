package db

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
)

// const defaultTimeout = 3 * time.Second
var defaultTimeout = 3 * time.Minute
var defaultTimeoutODBC = 15 * time.Minute
var defaultTimeoutDuckDB = 15 * time.Minute

type DB struct {
	//*sqlx.DB
	*sqlx.DB
}

func New(driverName string, dsn string) (*DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	db, err := sqlx.ConnectContext(ctx, driverName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(2 * time.Hour)
	if driverName == "sqlite3" {
		db.ExecContext(ctx, "PRAGMA journal_mode = wal2")
		db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		db.ExecContext(ctx, "PRAGMA secure_delete = ON")
		//cache_size = -500 * 1024
		cache_size := 2 * 1024 * 1024
		db.ExecContext(ctx, fmt.Sprintf("PRAGMA cache_size = %d", -cache_size))
		// db.ExecContext(ctx, "PRAGMA PAGE_SIZE = {}".format(cache_size))
		// db.ExecContext(ctx, "PRAGMA mmap_size  = {}".format(500 * 1024))
		db.ExecContext(ctx, "PRAGMA synchronous  = 0")
		db.ExecContext(ctx, "PRAGMA TEMP_STORE  = 2")
		db.ExecContext(ctx, "PRAGMA auto_vacuum = FULL")
		busy_timeout := 60 * 1000 * 3 // 60s
		db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", busy_timeout))
	}
	return &DB{db}, nil
}

func (db *DB) New(driverName string, dsn string) (*DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_db, err := sqlx.ConnectContext(ctx, driverName, dsn)
	if err != nil {
		return nil, err
	}
	_db.SetMaxOpenConns(25)
	_db.SetMaxIdleConns(25)
	_db.SetConnMaxIdleTime(5 * time.Minute)
	_db.SetConnMaxLifetime(2 * time.Hour)
	if driverName == "sqlite3" {
		_db.ExecContext(ctx, "PRAGMA journal_mode = wal2")
		_db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		_db.ExecContext(ctx, "PRAGMA secure_delete = ON")
		//cache_size = -500 * 1024
		cache_size := 2 * 1024 * 1024
		_db.ExecContext(ctx, fmt.Sprintf("PRAGMA cache_size = %d", -cache_size))
		// _db.ExecContext(ctx, "PRAGMA PAGE_SIZE = {}".format(cache_size))
		// _db.ExecContext(ctx, "PRAGMA mmap_size  = {}".format(500 * 1024))
		_db.ExecContext(ctx, "PRAGMA synchronous  = 0")
		_db.ExecContext(ctx, "PRAGMA TEMP_STORE  = 2")
		_db.ExecContext(ctx, "PRAGMA auto_vacuum = FULL")
		busy_timeout := 60 * 000 // 60s
		_db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", busy_timeout))
	}
	return &DB{_db}, nil
}

func setStrEnv(input string) string {
	re := regexp.MustCompile(`@ENV\.\w+`)
	matches := re.FindAllString(input, -1)
	if len(matches) > 0 {
		for _, match := range matches {
			envVar := strings.TrimPrefix(match, "@ENV.")
			envValue := os.Getenv(envVar)
			if envValue != "" {
				input = strings.ReplaceAll(input, match, envValue)
			}
		}
	}
	return input
}

// Adjust the query based on the database driver
func adjustQuery(driver, query string) string {
	if driver == "postgres" {
		// Replace ? with $1, $2, $3, etc. for PostgreSQL
		count := 1
		var result strings.Builder
		for _, ch := range query {
			if ch == '?' {
				// Replace ? with positional $n
				result.WriteString(fmt.Sprintf("$%d", count))
				count++
			} else {
				// Copy other characters as-is
				result.WriteRune(ch)
			}
		}
		return result.String()
	} else if driver == "mysql" {
		// Replace double quotes " with backticks ` for MySQL
		return strings.ReplaceAll(query, `"`, "`")
	} else if driver == "mssql" {
		return strings.ReplaceAll(query, `"`, "")
	}
	// SQLite uses ? placeholders, so no changes needed
	return query
}

// isDSN checks if the passed string is a full DSN (contains key-value pairs or URL format).
func isDSN(input string) bool {
	// Check if the input contains typical DSN components like "user=", "host=", or "dbname="
	kvIdentifiers := []string{"user=", "host=", "dbname=", "password=", "sslmode="}
	for _, id := range kvIdentifiers {
		if strings.Contains(input, id) {
			return true
		}
	}
	// Check if it looks like a URL (starts with a scheme like "postgres://")
	u, err := url.Parse(input)
	if err == nil && u.Scheme != "" {
		return true
	}
	return false
}

// ReplaceDBName takes a Postgres connection string (dsn) and replaces the dbname with the newDBName.
func ReplaceDBName(dsn string, newDBName string) (string, error) {
	// Parse the DSN as a URL for easy manipulation
	if isDSN(newDBName) {
		return newDBName, nil
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	// If the DSN is in keyword=value format, split it manually
	if u.Scheme == "" {
		// Parse manually in keyword=value format
		parts := strings.Split(dsn, " ")
		for i, part := range parts {
			if strings.HasPrefix(part, "dbname=") {
				// Replace the dbname with the new database name
				parts[i] = fmt.Sprintf("dbname=%s", newDBName)
				break
			}
		}
		// Join the parts back into a DSN string
		return strings.Join(parts, " "), nil
	}
	// If the DSN is a URL, use query parameters to replace the dbname
	q := u.Query()
	q.Set("dbname", newDBName)
	u.RawQuery = q.Encode()
	// Return the updated DSN as a string
	return u.String(), nil
}

func (db *DB) FromParams(params map[string]any, extra_conf map[string]any) (*DB, string, string, error) {
	var _database interface{}
	if !db.IsEmpty(params["db"]) {
		_database = params["db"]
	} else if !db.IsEmpty(params["data"].(map[string]any)["db"]) {
		_database = params["data"].(map[string]any)["db"]
	} else if !db.IsEmpty(params["data"].(map[string]any)["database"]) {
		_database = params["data"].(map[string]any)["database"]
	} else if !db.IsEmpty(params["app"].(map[string]any)["db"]) {
		_database = params["app"].(map[string]any)["db"]
	}
	/*/ Parse the DSN as a URL for easy manipulation
	u, err := url.Parse(extra_conf["dsn"].(string))
	if err != nil {
		fmt.Println("Err parsing dsm", err)
	}
	fmt.Println(u)*/
	//var newDB DBInterface
	//fmt.Println(_database)
	_not_embed_dbs := []interface{}{"postgres", "postgresql", "pg", "pgql", "mysql"}
	_embed_dbs := []interface{}{"sqlite", "sqlite3", "duckdb", "ducklake"}
	_embed_dbs_ext := []interface{}{".db", ".duckdb", ".ddb", ".sqlite", ".ducklake"}
	switch _database.(type) {
	case nil:
		//return true
		newDB, err := New(extra_conf["driverName"].(string), extra_conf["dsn"].(string))
		_database := filepath.Base(extra_conf["dsn"].(string))
		_db_ext := filepath.Ext(_database)
		_database = _database[:len(_database)-len(_db_ext)]
		return newDB, extra_conf["driverName"].(string), _database, err
	case string:
		_driver := extra_conf["driverName"].(string)
		_dsn := _database.(string)
		dirName := filepath.Dir(_dsn)
		fileName := filepath.Base(_dsn)
		fileExt := filepath.Ext(_dsn)
		if contains(_embed_dbs, _driver) || contains(_embed_dbs_ext, fileExt) {
			//fmt.Println("dirName: ", dirName, "fileName: ", fileName, "fileExt: ", fileExt)
			if filepath.Base(_dsn) == fileName || dirName == "" {
				_dsn = fmt.Sprintf("database/%s", fileName)
			}
			_embed_dbs_ext := []interface{}{".duckdb", ".ddb", ".ducklake"}
			if fileExt == "" {
				_embed_dbs := []interface{}{"sqlite", "sqlite3"}
				if _driver == "duckdb" {
					_dsn = fmt.Sprintf("database/%s.duckdb", fileName)
				} else if contains(_embed_dbs, _driver) {
					_dsn = fmt.Sprintf("database/%s.db", fileName)
				}
			} else if contains(_embed_dbs_ext, fileExt) {
				_driver = "duckdb"
			}
		} else if contains(_not_embed_dbs, _driver) {
			new_dsn, err := ReplaceDBName(extra_conf["dsn"].(string), _dsn)
			if err != nil {
				fmt.Println("Errr getting the DSN for ", _dsn)
			}
			//fmt.Println("NEW DSN", new_dsn)
			_dsn = setStrEnv(new_dsn)
		}
		//fmt.Println("IS STRING:", _database, _dsn)
		if _driver == "duckdb" {
			return nil, _driver, _database.(string), nil
		}
		newDB, err := New(_driver, _dsn)
		return newDB, _driver, _database.(string), err
	case []interface{}:
		//fmt.Println("IS []interface{}:", _database)
		return nil, "", "", errors.New("database conf is of type []interface{}")
	case map[interface{}]interface{}:
		/*_driver := _database["drivername"].(string)
		_dsn := _database["database"].(string)
		fmt.Println("IS map[interface{}]interface{}:", _database, _driver, _dsn)*/
		return nil, "", "", errors.New("database conf is of type map[interface{}]interface{}")
	case map[string]any:
		_aux := _database.(map[string]any)
		_driver := ""
		if _, exists := _aux["drivername"]; exists {
			_driver = _aux["drivername"].(string)
		}
		_db := ""
		_dsn := ""
		if _, exists := _aux["database"]; exists {
			_dsn = _aux["database"].(string)
			_db = _aux["database"].(string)
		}
		if _, exists := _aux["db"]; exists {
			_dsn = _aux["db"].(string)
			_db = _aux["db"].(string)
		}
		if _, exists := _aux["dsn"]; exists {
			_dsn = _aux["dsn"].(string)
			if _db == "" {
				_db = _dsn
			}
		}
		//fmt.Println("IS map[string]any:", _database, _driver, _dsn)
		dirName := filepath.Dir(_dsn)
		fileName := filepath.Base(_dsn)
		fileExt := filepath.Ext(_dsn)
		if contains(_embed_dbs, _driver) || contains(_embed_dbs_ext, fileExt) {
			//fmt.Println("dirName: ", dirName, "fileName: ", fileName, "fileExt: ", fileExt)
			if filepath.Base(_dsn) == fileName || dirName == "" {
				_dsn = fmt.Sprintf("database/%s", fileName)
			}
			_embed_dbs_ext := []interface{}{".duckdb", ".ddb"}
			if fileExt == "" {
				_embed_dbs := []interface{}{"sqlite", "sqlite3"}
				if _driver == "duckdb" {
					_dsn = fmt.Sprintf("database/%s.duckdb", fileName)
				} else if contains(_embed_dbs, _driver) {
					_dsn = fmt.Sprintf("database/%s.db", fileName)
				}
			} else if contains(_embed_dbs_ext, fileExt) {
				_driver = "duckdb"
			}
		} else if contains(_not_embed_dbs, _driver) {
			new_dsn, err := ReplaceDBName(extra_conf["dsn"].(string), _dsn)
			if err != nil {
				fmt.Println("Errr getting the DSN for ", _dsn)
			}
			//fmt.Println("NEW DSN", new_dsn)
			_dsn = setStrEnv(new_dsn)
		}
		//fmt.Println("IS STRING:", _database, _dsn)
		if _driver == "duckdb" {
			// newDB, err := NewDuckDB(_dsn)
			return nil, _driver, _database.(string), nil
		}
		newDB, err := New(_driver, _dsn)
		return newDB, _driver, _db, err
	case interface{}:
		//fmt.Println("IS interface{}:", _database)
		return nil, "", "", errors.New("database conf is of type interface{}")
	default:
		newDB, err := New(extra_conf["driverName"].(string), extra_conf["dsn"].(string))
		_database := filepath.Base(extra_conf["dsn"].(string))
		_db_ext := filepath.Ext(_database)
		_database = _database[:len(_database)-len(_db_ext)]
		return newDB, extra_conf["driverName"].(string), _database, err
	}
}

func (db *DB) GetDriverName() string {
	return db.DriverName()
}

func (db *DB) AllTables(params map[string]any, extra_conf map[string]any) (*[]map[string]any, bool, error) {
	_driver := db.GetDriverName()
	_sqlites_drivers := []interface{}{"sqlite", "sqlite3"}
	_pg_drivers := []interface{}{"pg", "postgres"}
	_ddb_drivers := []interface{}{"ddb", "duckdb"}
	_mysql_drivers := []interface{}{"mysql", "mysql8"}
	if contains(_sqlites_drivers, _driver) {
		_query := `SELECT name FROM sqlite_master WHERE type='table'`
		return db.QueryMultiRows(_query, []interface{}{}...)
	} else if contains(_pg_drivers, _driver) {
		_query := `SELECT table_name::varchar as name FROM information_schema.tables WHERE table_schema = 'public';`
		return db.QueryMultiRows(_query, []interface{}{}...)
	} else if contains(_ddb_drivers, _driver) {
		_query := `SELECT table_name as name FROM information_schema.tables`
		return db.QueryMultiRows(_query, []interface{}{}...)
	} else if contains(_mysql_drivers, _driver) {
		_query := `SHOW TABLES`
		_query = `SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'your_database_name';`
		return db.QueryMultiRows(_query, []interface{}{}...)
	}
	return nil, false, fmt.Errorf("could not handle driver %s", _driver)
}

func (db *DB) TableSchema(params map[string]any, table string, dbName string, extra_conf map[string]any) (*[]map[string]any, bool, error) {
	_driver := db.GetDriverName()
	user_id := int(params["user"].(map[string]any)["user_id"].(float64))
	_sqlites_drivers := []interface{}{"sqlite", "sqlite3"}
	_pg_drivers := []interface{}{"pg", "postgres"}
	_ddb_drivers := []interface{}{"ddb", "duckdb"}
	_mysql_drivers := []interface{}{"mysql", "mysql8"}
	if contains(_sqlites_drivers, _driver) {
		_query := "PRAGMA table_info('" + table + "');"
		_aux_data := []map[string]any{}
		_aux_data_fk := map[string]any{}
		res, _, err := db.QueryMultiRows(_query, []interface{}{}...)
		if err != nil {
			return nil, false, err
		}
		_query = "PRAGMA foreign_key_list('" + table + "');"
		res_fk, _, err := db.QueryMultiRows(_query, []interface{}{}...)
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
	} else if contains(_pg_drivers, _driver) {
		_query := `SELECT table_name as name FROM information_schema.tables WHERE table_schema = 'public';`
		return db.QueryMultiRows(_query, []interface{}{}...)
	} else if contains(_ddb_drivers, _driver) {
		_query := `SELECT table_name as name FROM information_schema.tables`
		return db.QueryMultiRows(_query, []interface{}{}...)
	} else if contains(_mysql_drivers, _driver) {
		_query := `SHOW TABLES`
		_query = `SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'your_database_name';`
		return db.QueryMultiRows(_query, []interface{}{}...)
	}
	return nil, false, fmt.Errorf("could not handle driver %s", _driver)
}

func (db *DB) ExecuteQuery(query string, data ...interface{}) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	query = adjustQuery(db.DriverName(), query)
	result, err := db.ExecContext(ctx, query, data...)
	if err != nil {
		return 0, err
	}
	id := int64(0)
	if db.DriverName() != "postgres" {
		id, err = result.LastInsertId()
		if err != nil {
			//return 0, err
			return 0, nil
		}
	}
	return int(id), nil
}

func (db *DB) ExecuteQueryPGInsertWithLastInsertId(query string, data ...interface{}) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	query = adjustQuery(db.DriverName(), query)
	var id int64
	rows, err := db.NamedQueryContext(ctx, query, data)
	if err != nil {
		//fmt.Println("ExecuteQueryPGInsertWithLastInsertId Err:", err)
		return 0, err
	}
	defer rows.Close()
	if rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			return 0, err
		}
	}
	return int(id), nil
}

func (db *DB) ExecuteQueryRowsAffected(query string, data ...interface{}) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeoutDuckDB)
	defer cancel()
	query = adjustQuery(db.DriverName(), query)
	result, err := db.ExecContext(ctx, query, data...)
	if err != nil {
		return 0, err
	}
	id, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (db *DB) ExecuteNamedQuery(query string, data map[string]any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	query = adjustQuery(db.DriverName(), query)
	result, err := db.NamedExecContext(ctx, query, data)
	if err != nil {
		return 0, err
	}
	id := int64(0)
	if db.DriverName() != "postgres" {
		id, err = result.LastInsertId()
		if err != nil {
			//return 0, err
			return 0, nil
		}
	}
	return int(id), nil
}

func (db *DB) QueryMultiRows(query string, params ...interface{}) (*[]map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	var result []map[string]any
	query = adjustQuery(db.DriverName(), query)
	if db.DriverName() == "postgres" {
		//fmt.Println(query)
	}
	rows, err := db.QueryxContext(ctx, query, params...)
	if err != nil {
		//fmt.Println(1, err)
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		row := map[string]any{}
		if err := rows.MapScan(row); err != nil {
			//fmt.Println(3, err)
			return nil, false, err
		}
		//fmt.Println(2, row)
		result = append(result, row)
	}
	return &result, true, nil
}

func (db *DB) QueryRows(ctx context.Context, query string, params ...interface{}) (*sql.Rows, error) {
	query = adjustQuery(db.DriverName(), query)
	return db.QueryContext(ctx, query, params...)
}

func (db *DB) QueryMultiRowsWithCols(query string, params ...interface{}) (*[]map[string]any, []string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	var result []map[string]any
	query = adjustQuery(db.DriverName(), query)
	rows, err := db.QueryxContext(ctx, query, params...)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("failed to get columns: %s", err)
	}
	for rows.Next() {
		row := map[string]any{}
		if err := rows.MapScan(row); err != nil {
			return nil, nil, false, err
		}
		result = append(result, row)
	}
	return &result, columns, true, nil
}

func (db *DB) QuerySingleRow(query string, params ...interface{}) (*map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	result := map[string]any{}
	query = adjustQuery(db.DriverName(), query)
	rows, err := db.QueryxContext(ctx, query, params...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.MapScan(result); err != nil {
			return nil, false, err
		}
	}
	//fmt.Println(result)
	return &result, true, nil
}

func (db *DB) IsEmpty(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return len(v) == 0
	case []interface{}:
		return len(v) == 0
	case map[interface{}]interface{}:
		return len(v) == 0
	default:
		return false
	}
}

func (db *DB) GetUserByNameOrEmail(email string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	//var user User
	//user2 := map[string]any{}
	user := map[string]any{}

	query := `SELECT * FROM user WHERE email = $1 OR username = $1`

	//err := db.GetContext(ctx, &user2, query, email)
	rows, err := db.QueryxContext(ctx, query, email)
	//err := db.Select(&user2, query, email)
	if rows.Next() {
		errr := rows.MapScan(user)
		if errr != nil {
			fmt.Print(errr)
		}
		//fmt.Print(user["username"])
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}

	return user, true, err
}

// ParsePostgresConnString extracts parameters from a PostgreSQL connection string.
func ParsePostgresConnString(connStr string) (map[string]string, error) {
	result := make(map[string]string)

	// Check for URI format
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		uri, err := url.Parse(connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URI: %w", err)
		}

		// Extract host and port
		result["host"] = uri.Hostname()
		result["port"] = uri.Port()

		// Extract dbname
		if len(uri.Path) > 1 {
			result["dbname"] = strings.TrimPrefix(uri.Path, "/")
		}

		// Extract user (optional)
		if uri.User != nil {
			result["user"] = uri.User.Username()
		}

		// Extract password (optional)
		if password, exists := uri.User.Password(); exists {
			result["password"] = password
		}

		// Extract query parameters (e.g., sslmode)
		for key, values := range uri.Query() {
			result[key] = values[0]
		}

		return result, nil
	}

	// Handle Key-Value format
	re := regexp.MustCompile(`(\w+)=([\S]+)`)
	matches := re.FindAllStringSubmatch(connStr, -1)
	if matches == nil {
		return nil, errors.New("invalid connection string format")
	}

	for _, match := range matches {
		if len(match) == 3 {
			result[match[1]] = match[2]
		}
	}

	return result, nil
}

func (db *DB) Query2CSV(query string, csv_path string, params ...interface{}) (bool, error) {
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

func contains(slice []interface{}, element interface{}) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}
