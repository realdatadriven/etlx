package etlxlib

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

// SQLDialect defines the interface for different SQL database dialects.
type SQLDialect interface {
	GetCreateTable(tableName string) (string, string)
	GetCreateOrReplaceTable(tableName string) (string, string)
	GetCreateTableIfNotExists(tableName string) (string, string)
	GetTableName(tableName string) string
	GetColumnName(fieldName string) string
	GetColumnType(field map[string]any) string
	GetPrimaryKey(field map[string]any) string
	GetAutoIncrement(field map[string]any) string
	GetNullable(field map[string]any) string
	GetUnique(field map[string]any) string
	GetDefaultValue(field map[string]any) string
	GetColumnComment(tableName, columnName, comment string) string
	GetTableComment(tableName, comment string) string
	SupportsInlineColumnComment() bool
	SupportsTableComment() bool
	DropTableIfExists(tableName string) string
}

// BaseDialect provides common implementations for SQLDialect interface.
type BaseDialect struct{}

func (ms *BaseDialect) GetCreateTableIfNotExists(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (`, tableName), ``
}

func (ms *BaseDialect) GetCreateTable(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE %s (`, tableName), ``
}

func (ms *BaseDialect) GetCreateOrReplaceTable(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE OR REPLACE TABLE %s (`, tableName), ``
}

func (b *BaseDialect) DropTableIfExists(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName)
}

func (b *BaseDialect) GetColumnName(fieldName string) string {
	return fieldName
}

func (b *BaseDialect) GetTableName(tableName string) string {
	return tableName
}

func (b *BaseDialect) GetColumnType(field map[string]any) string {
	sqlType := field["type"].(string)
	switch strings.ToUpper(sqlType) {
	case "INTEGER":
		return "INTEGER"
	case "VARCHAR":
		return "TEXT"
	case "TEXT":
		return "TEXT"
	case "DATETIME":
		return "TEXT"
	case "BOOLEAN":
		return "INTEGER"
	default:
		return sqlType
	}
}

func (b *BaseDialect) GetPrimaryKey(field map[string]any) string {
	if pk, ok := field["pk"].(bool); ok && pk {
		return " PRIMARY KEY"
	}
	return ""
}

func (b *BaseDialect) GetAutoIncrement(field map[string]any) string {
	return ""
}

func (b *BaseDialect) GetNullable(field map[string]any) string {
	if nullable, ok := field["nullable"].(bool); ok && !nullable {
		return " NOT NULL"
	}
	return ""
}

func (b *BaseDialect) GetUnique(field map[string]any) string {
	if unique, ok := field["unique"].(bool); ok && unique {
		return " UNIQUE"
	}
	return ""
}

func (b *BaseDialect) GetDefaultValue(field map[string]any) string {
	if defaultVal, ok := field["default"]; ok {
		switch v := defaultVal.(type) {
		case bool:
			return fmt.Sprintf(" DEFAULT %t", v)
		case string:
			// Basic escaping for single quotes
			return fmt.Sprintf(" DEFAULT '%s'", strings.ReplaceAll(v, "'", "''"))
		case int, float64:
			return fmt.Sprintf(" DEFAULT %v", v)
		}
	}
	return ""
}

func (b *BaseDialect) GetColumnComment(tableName, columnName, comment string) string { return "" }
func (b *BaseDialect) GetTableComment(tableName, comment string) string              { return "" }
func (b *BaseDialect) SupportsInlineColumnComment() bool                             { return false }
func (b *BaseDialect) SupportsTableComment() bool                                    { return false }

// PostgresDialect implements SQLDialect for PostgreSQL.
type PostgresDialect struct{ BaseDialect }

func (ms *PostgresDialect) GetCreateTableIfNotExists(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (`, tableName), ``
}

func (ms *PostgresDialect) GetCreateTable(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE "%s" (`, tableName), ``
}

func (ms *PostgresDialect) GetCreateOrReplaceTable(tableName string) (string, string) {
	// PostgreSQL does not support CREATE OR REPLACE TABLE, so we can simulate it with DROP + CREATE
	return fmt.Sprintf(`DROP TABLE IF EXISTS "%s"; CREATE TABLE "%s" (`, tableName, tableName), ``
}

func (p *PostgresDialect) DropTableIfExists(tableName string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS "%s";`, tableName)
}

func (p *PostgresDialect) GetTableName(tableName string) string {
	return fmt.Sprintf(`"%s"`, tableName)
}

func (p *PostgresDialect) GetColumnName(fieldName string) string {
	return fmt.Sprintf(`"%s"`, fieldName)
}

func (p *PostgresDialect) GetColumnType(field map[string]any) string {
	sqlType := field["type"].(string)
	switch strings.ToUpper(sqlType) {
	case "INTEGER":
		if autoincrement, ok := field["autoincrement"].(bool); ok && autoincrement {
			return "SERIAL"
		}
		return "INTEGER"
	case "VARCHAR":
		if length, ok := field["length"].(int); ok {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT" // Default to TEXT if length not specified for VARCHAR
	case "TEXT":
		return "TEXT"
	case "DATETIME":
		return "TIMESTAMP"
	case "BOOLEAN":
		return "BOOLEAN"
	default:
		return sqlType
	}
}

func (p *PostgresDialect) GetPrimaryKey(field map[string]any) string {
	// PostgreSQL primary key is handled by the constraint at the end of the table definition
	return ""
}

func (p *PostgresDialect) GetAutoIncrement(field map[string]any) string {
	// Handled by SERIAL type in GetColumnType
	return ""
}

func (p *PostgresDialect) GetColumnComment(tableName, columnName, comment string) string {
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", tableName, columnName, strings.ReplaceAll(comment, "'", "''"))
}

func (p *PostgresDialect) GetTableComment(tableName, comment string) string {
	return fmt.Sprintf("COMMENT ON TABLE %s IS '%s';", tableName, strings.ReplaceAll(comment, "'", "''"))
}

func (p *PostgresDialect) SupportsTableComment() bool { return true }

// MySQLDialect implements SQLDialect for MySQL.
type MySQLDialect struct{ BaseDialect }

func (ms *MySQLDialect) GetCreateTableIfNotExists(tableName string) (string, string) {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (", tableName), ``
}

func (ms *MySQLDialect) GetCreateOrReplaceTable(tableName string) (string, string) {
	// MySQL does not support CREATE OR REPLACE TABLE, so we can simulate it with DROP + CREATE
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`; CREATE TABLE `%s` (", tableName, tableName), ``
}

func (ms *MySQLDialect) GetCreateTable(tableName string) (string, string) {
	return fmt.Sprintf("CREATE TABLE `%s` (", tableName), ``
}

func (m *MySQLDialect) DropTableIfExists(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)
}

func (m *MySQLDialect) GetTableName(tableName string) string {
	return fmt.Sprintf("`%s`", tableName)
}

func (m *MySQLDialect) GetColumnType(field map[string]any) string {
	sqlType := field["type"].(string)
	switch strings.ToUpper(sqlType) {
	case "INTEGER":
		if autoincrement, ok := field["autoincrement"].(bool); ok && autoincrement {
			return "INT AUTO_INCREMENT"
		}
		return "INT"
	case "VARCHAR":
		if length, ok := field["length"].(int); ok {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT" // Default to TEXT if length not specified for VARCHAR
	case "TEXT":
		return "TEXT"
	case "DATETIME":
		return "DATETIME"
	case "BOOLEAN":
		return "TINYINT(1)"
	default:
		return sqlType
	}
}

func (m *MySQLDialect) GetColumnName(fieldName string) string {
	return fmt.Sprintf("`%s`", fieldName)
}

func (m *MySQLDialect) GetColumnComment(tableName, columnName, comment string) string {
	// MySQL supports inline column comments
	return fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(comment, "'", "''"))
}

func (m *MySQLDialect) GetTableComment(tableName, comment string) string {
	return fmt.Sprintf(" COMMENT='%s'", strings.ReplaceAll(comment, "'", "''"))
}

func (m *MySQLDialect) SupportsInlineColumnComment() bool { return true }
func (m *MySQLDialect) SupportsTableComment() bool        { return true }

// SQLiteDialect implements SQLDialect for SQLite.
type SQLiteDialect struct{ BaseDialect }

func (ms *SQLiteDialect) GetCreateTableIfNotExists(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (`, tableName), ``
}

func (ms *SQLiteDialect) GetCreateTable(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE "%s" (`, tableName), ``
}

func (ms *SQLiteDialect) GetCreateOrReplaceTable(tableName string) (string, string) {
	// SQLite does not support CREATE OR REPLACE TABLE, so we can simulate it with DROP + CREATE
	return fmt.Sprintf(`DROP TABLE IF EXISTS "%s"; CREATE TABLE "%s" (`, tableName, tableName), ``
}

func (s *SQLiteDialect) DropTableIfExists(tableName string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS "%s";`, tableName)
}

func (s *SQLiteDialect) GetTableName(tableName string) string {
	return fmt.Sprintf(`"%s"`, tableName)
}

func (s *SQLiteDialect) GetColumnName(fieldName string) string {
	return fmt.Sprintf(`"%s"`, fieldName)
}

func (s *SQLiteDialect) GetColumnType(field map[string]any) string {
	sqlType := field["type"].(string)
	switch strings.ToUpper(sqlType) {
	case "INTEGER":
		return "INTEGER"
	case "VARCHAR":
		return "TEXT"
	case "TEXT":
		return "TEXT"
	case "DATETIME":
		return "TEXT"
	case "BOOLEAN":
		return "INTEGER"
	default:
		return sqlType
	}
}

func (s *SQLiteDialect) GetAutoIncrement(field map[string]any) string {
	if autoincrement, ok := field["autoincrement"].(bool); ok && autoincrement {
		// AUTOINCREMENT keyword is only for INTEGER PRIMARY KEY
		if pk, ok := field["pk"].(bool); ok && pk {
			return " AUTOINCREMENT"
		}
	}
	return ""
}

// MSSQLDialect implements SQLDialect for Microsoft SQL Server.
type MSSQLDialect struct{ BaseDialect }

func (ms *MSSQLDialect) GetCreateTableIfNotExists(tableName string) (string, string) {
	return fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NULL BEGING CREATE TABLE [%s] ( `, tableName, tableName), ` END `
}

func (ms *MSSQLDialect) GetCreateTable(tableName string) (string, string) {
	return fmt.Sprintf(`CREATE TABLE [%s] (`, tableName), ``
}

func (ms *MSSQLDialect) GetCreateOrReplaceTable(tableName string) (string, string) {
	// MSSQL does not support CREATE OR REPLACE TABLE, so we can simulate it with DROP + CREATE
	return fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE [%s]; CREATE TABLE [%s] (`, tableName, tableName, tableName), ``
}

func (ms *MSSQLDialect) DropTableIfExists(tableName string) string {
	return fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE [%s];`, tableName, tableName)
}

func (ms *MSSQLDialect) GetTableName(tableName string) string {
	return fmt.Sprintf(`[%s]`, tableName)
}

func (ms *MSSQLDialect) GetColumnName(fieldName string) string {
	return fmt.Sprintf(`[%s]`, fieldName)
}

func (ms *MSSQLDialect) GetColumnType(field map[string]any) string {
	sqlType := field["type"].(string)
	switch strings.ToUpper(sqlType) {
	case "INTEGER":
		if autoincrement, ok := field["autoincrement"].(bool); ok && autoincrement {
			return "INT IDENTITY(1,1)"
		}
		return "INT"
	case "VARCHAR":
		if length, ok := field["length"].(int); ok {
			return fmt.Sprintf("NVARCHAR(%d)", length)
		}
		return "NVARCHAR(MAX)"
	case "TEXT":
		return "NVARCHAR(MAX)"
	case "DATETIME":
		return "DATETIME"
	case "BOOLEAN":
		return "BIT"
	default:
		return sqlType
	}
}

// GetDialect returns the appropriate SQLDialect implementation.
func GetDialect(driver string) SQLDialect {
	switch driver {
	case "postgres", "pg":
		return &PostgresDialect{}
	case "mysql", "mariadb":
		return &MySQLDialect{}
	case "sqlite3", "sqlite":
		return &SQLiteDialect{}
	case "sqlserver", "mssql":
		return &MSSQLDialect{}
	default:
		return &BaseDialect{} // Fallback or error handling
	}
}

// Generates CREATE TABLE SQL statements with comments, adapting to SQL dialects
func generateCreateTableSQL(driver, tableName, tableComment, createAll string, fields map[string]any) string {
	dialect := GetDialect(driver)
	var start, end string
	var schema strings.Builder
	switch createAll {
	case "checkfirst":
		start, end = dialect.GetCreateTableIfNotExists(tableName)
		schema.WriteString(start)
	case "replace":
		start, end = dialect.GetCreateOrReplaceTable(tableName)
		schema.WriteString(start)
	default:
		start, end = dialect.GetCreateTable(tableName)
		schema.WriteString(start)
	}

	var columnDefs []string
	var foreignKeyConstraints []string
	var primaryKeyColumns []string
	var postCreateTableSQL []string                // For comments or other post-creation statements
	filedsByOrder, ok := fields["__order"].([]any) // Get the field order from the special __order key
	if !ok {
		filedsByOrder = make([]any, 0)
		// If __order is missing, we can iterate over the map keys in any order (not guaranteed)
		for fieldName := range fields {
			filedsByOrder = append(filedsByOrder, fieldName)
		}
	}
	for _, fieldNameAny := range filedsByOrder {
		fieldName := fieldNameAny.(string)
		_field := fields[fieldName]
		field, ok := _field.(map[string]any) // Type assertion for field definition
		if !ok {
			continue
		}
		if fieldName == "__order" {
			continue
		}
		name := dialect.GetColumnName(fieldName)
		columnType := dialect.GetColumnType(field)
		primaryKey := dialect.GetPrimaryKey(field)
		autoincrement := dialect.GetAutoIncrement(field)
		nullable := dialect.GetNullable(field)
		unique := dialect.GetUnique(field)
		defaultValue := dialect.GetDefaultValue(field)

		columnDef := fmt.Sprintf("    %s %s%s%s%s%s%s", name, columnType, primaryKey, autoincrement, nullable, unique, defaultValue)

		if comment, ok := field["comment"].(string); ok && comment != "" {
			if dialect.SupportsInlineColumnComment() {
				columnDef += dialect.GetColumnComment("", "", comment) // Inline comment, table/column name not needed here
			} else {
				postCreateTableSQL = append(postCreateTableSQL, dialect.GetColumnComment(dialect.GetTableName(tableName), name, comment))
			}
		}
		columnDefs = append(columnDefs, columnDef)

		// Collect primary key columns for a combined PK constraint
		if pk, ok := field["pk"].(bool); ok && pk && primaryKey == "" {
			primaryKeyColumns = append(primaryKeyColumns, name)
		}

		// Handle foreign keys
		if fkRef, ok := field["fk"].(string); ok && fkRef != "" {
			// fkRef format: "referenced_table.referenced_column"
			parts := strings.Split(fkRef, ".")
			if len(parts) == 2 {
				referencedTable := parts[0]
				referencedColumn := parts[1]
				foreignKeyConstraints = append(foreignKeyConstraints, fmt.Sprintf("    FOREIGN KEY (%s) REFERENCES %s(%s)", name, dialect.GetTableName(referencedTable), dialect.GetColumnName(referencedColumn)))
			}
		}
	}

	// Add combined primary key constraint if any
	if len(primaryKeyColumns) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(primaryKeyColumns, ", ")))
	}

	// Add foreign key constraints
	columnDefs = append(columnDefs, foreignKeyConstraints...)

	schema.WriteString(strings.Join(columnDefs, ",\n") + "\n)")

	// Add table-level comments and other post-creation statements
	if tableComment != "" && dialect.SupportsTableComment() {
		schema.WriteString(dialect.GetTableComment(tableName, tableComment))
	}

	// Append any collected post-create SQL (e.g., column comments for Postgres)
	for _, sql := range postCreateTableSQL {
		schema.WriteString("\n" + sql)
	}

	return schema.String() + end + ";\n"
}

// ColumnDefinition represents the structure of a column from the YAML schema.
type ColumnDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Add other fields from your YAML schema as needed, e.g., Pk, Autoincrement, Nullable, etc.
}

// InsertData inserts a slice of data rows into the specified table.
// It automatically handles default values for 'created_at', 'updated_at', and 'excluded'
// if they are defined in the schema but not present in the data row.
func InsertData(dbCon db.DBInterface, tableName string, columns map[string]any, data []any) error {
	// Map schema column names to their properties for quick lookup
	type schemaColInfo struct {
		isCreatedAt bool
		isUpdatedAt bool
		isExcluded  bool
		isNullable  bool
	}
	dialect := GetDialect(dbCon.GetDriverName())
	schemaColumnMap := make(map[string]schemaColInfo)
	var allSchemaColumnNames []string // To maintain order for INSERT statement
	/*filedsByOrder, ok := columns["__order"].([]any) // Get the column order from the special __order key
	if !ok {
		filedsByOrder = make([]any, 0)
		// If __order is missing, we can iterate over the map keys in any order (not guaranteed)
		for colName := range columns {
			filedsByOrder = append(filedsByOrder, colName)
		}
	}*/
	for colName, _col := range columns {
		// fmt.Println(colName, _col)
		if colName == "__order" || colName == "metadata" {
			continue
		}
		col, ok := _col.(map[string]any) // Type assertion for column definition
		if !ok {
			return fmt.Errorf("column (%s) definition is not a valid map[string]any", colName)
		}

		info := schemaColInfo{}
		if colName == "created_at" {
			info.isCreatedAt = true
		}
		if colName == "updated_at" {
			info.isUpdatedAt = true
		}
		if colName == "excluded" {
			info.isExcluded = true
		}
		if _, ok := col["nullable"].(bool); !ok {
			info.isNullable = true
		} else if nullable, ok := col["nullable"].(bool); ok && nullable {
			info.isNullable = true
		}
		schemaColumnMap[colName] = info
		allSchemaColumnNames = append(allSchemaColumnNames, colName)
	}

	for i, _row := range data {
		row, ok := _row.(map[string]any) // Type assertion for data row
		if !ok {
			return fmt.Errorf("row %d is not a valid map[string]any", i)
		}
		insertCols := []string{}
		insertVals := []string{} // For named parameters, e.g., ":colName"
		insertMap := make(map[string]any)
		now := time.Now() // Get current time once per row for consistency

		for _, colName := range allSchemaColumnNames {
			colInfo, existsInSchema := schemaColumnMap[colName]
			if !existsInSchema {
				// This should ideally not happen if allSchemaColumnNames is derived from schemaColumnMap
				continue
			}

			if val, ok := row[colName]; ok {
				// Value exists in the data row, use it
				insertCols = append(insertCols, dialect.GetColumnName(colName))
				insertVals = append(insertVals, ":"+colName)
				insertMap[colName] = val
			} else {
				// Value not in data row, check for defaults based on schema definition
				if colInfo.isCreatedAt {
					insertCols = append(insertCols, dialect.GetColumnName(colName))
					insertVals = append(insertVals, ":"+colName)
					insertMap[colName] = now
				} else if colInfo.isUpdatedAt {
					insertCols = append(insertCols, dialect.GetColumnName(colName))
					insertVals = append(insertVals, ":"+colName)
					insertMap[colName] = now
				} else if colInfo.isExcluded {
					insertCols = append(insertCols, dialect.GetColumnName(colName))
					insertVals = append(insertVals, ":"+colName)
					insertMap[colName] = false
				} else if !colInfo.isNullable {
					// If a non-nullable column is missing from data and has no default, it's an error
					return fmt.Errorf("row %d: non-nullable column '%s' missing from data and no default provided", i, colName)
				}
				// If it's nullable and not provided, it will be omitted from the INSERT, allowing DB default/NULL
			}
		}

		// Construct the INSERT statement
		if len(insertCols) == 0 {
			return fmt.Errorf("row %d: no columns to insert", i)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			dialect.GetTableName(tableName), strings.Join(insertCols, ", "), strings.Join(insertVals, ", "))
		// fmt.Println(query)
		// Execute the insert using NamedExec for safety and convenience
		_, err := dbCon.ExecuteNamedQuery(query, insertMap)
		if err != nil {
			return fmt.Errorf("failed to insert row %d into %s: %w", i, tableName, err)
		}
	}

	return nil
}

func generateDropTableSQL(driver, tableName string) string {
	dialect := GetDialect(driver)
	return dialect.DropTableIfExists(tableName)
}

// METADATA
func generateSeedData(parsedTables map[string]any, dbName string) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339) // or use your preferred format

	data := map[string]any{
		"table":                 []map[string]any{},
		"translate_table":       []map[string]any{},
		"translate_table_field": []map[string]any{},
		"table_schema":          []map[string]any{},
	}

	for tableName, tableDef := range parsedTables {
		// fmt.Println(1, tableName, tableDef)
		commentAny, hasComment := tableDef.(map[string]any)["comment"]
		comment := ""
		if hasComment {
			if s, ok := commentAny.(string); ok {
				comment = s
			}
		}

		// 1) table row
		tableRow := map[string]any{
			"table":      tableName,
			"table_desc": comment,
			"db":         dbName,
			"user_id":    1,
			"created_at": now,
			"updated_at": now,
			"excluded":   false,
		}
		data["table"] = append(data["table"].([]map[string]any), tableRow)

		// 2) translate_table row (english default)
		translateTableRow := map[string]any{
			"table_org_desc":    comment,
			"table_transl_desc": comment, // ← can be empty or later translated
			"table":             tableName,
			"db":                dbName,
			"lang":              "en",
			"user_id":           1,
			"created_at":        now,
			"updated_at":        now,
			"excluded":          false,
		}
		data["translate_table"] = append(data["translate_table"].([]map[string]any), translateTableRow)

		// 3+4) columns → translate_table_field + table_schema
		columnsAny, hasColumns := tableDef.(map[string]any)["columns"]
		//fmt.Println(2, tableName, comment, columnsAny)
		if !hasColumns {
			continue
		}

		columns, ok := columnsAny.(map[string]any)
		if !ok {
			continue
		}
		filedsByOrder, ok := columns["__order"].([]any) // Get the column order from the special __order key
		if !ok {
			filedsByOrder = make([]any, 0)
			// If __order is missing, we can iterate over the map keys in any order (not guaranteed)
			for colName := range columns {
				filedsByOrder = append(filedsByOrder, colName)
			}
		}
		fieldOrder := 0
		for _, colNameAny := range filedsByOrder {
			colName, ok := colNameAny.(string)
			colDefAny, hasColDef := columns[colName]
			//fmt.Println(colName, hasColDef, colDefAny)
			if !ok || !hasColDef {
				continue
			}
			colDef, ok := colDefAny.(map[string]any)
			if !ok {
				continue
			}
			fieldOrder++

			// ──────────────────────────────────────────────
			// extract column properties with safe type assertions
			// ──────────────────────────────────────────────

			colType := getString(colDef, "type", "unknown")
			colComment := getString(colDef, "comment", "")
			pk := getBool(colDef, "pk", false)
			autoincrement := getBool(colDef, "autoincrement", false)
			nullable := getBool(colDef, "nullable", true) // default nullable=true if missing
			defaultVal := getAny(colDef, "default", nil)

			fkRef := getString(colDef, "fk", "")
			var referredTable, referredColumn string
			fk := fkRef != ""
			if fk {
				parts := strings.Split(fkRef, ".")
				if len(parts) == 2 {
					referredTable = parts[0]
					referredColumn = parts[1]
				}
			}

			// ──────────────────────────────────────────────
			// translate_table_field row
			// ──────────────────────────────────────────────
			ttfRow := map[string]any{
				"field_org_desc":    colComment,
				"field_transl_desc": colComment, // ← can be translated later
				"field":             colName,
				"table":             tableName,
				"db":                dbName,
				"lang":              "en",
				"user_id":           1,
				"created_at":        now,
				"updated_at":        now,
				"excluded":          false,
			}
			data["translate_table_field"] = append(data["translate_table_field"].([]map[string]any), ttfRow)

			// ──────────────────────────────────────────────
			// table_schema row
			// ──────────────────────────────────────────────
			schemaRow := map[string]any{
				"db":              dbName,
				"table":           tableName,
				"field":           colName,
				"type":            colType,
				"pk":              pk,
				"autoincrement":   autoincrement,
				"nullable":        nullable,
				"default":         defaultVal,
				"comment":         colComment,
				"fk":              fk,
				"referred_table":  referredTable,
				"referred_column": referredColumn,
				"field_order":     fieldOrder,
				"user_id":         1,
				"created_at":      now,
				"updated_at":      now,
				"excluded":        false,
				// "computed":     ... (add when you start using it)
			}
			data["table_schema"] = append(data["table_schema"].([]map[string]any), schemaRow)
		}
	}

	return data
}

func generateCustomData(parsedTables map[string]any, dbName string) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339) // or use your preferred format
	data := map[string]any{
		"custom_table": []map[string]any{},
		"custom_form":  []map[string]any{},
	}
	for tableName, tableDef := range parsedTables {
		// fmt.Println(1, tableName, tableDef)
		// ──────────────────────────────────────────────
		// custom_form
		// ──────────────────────────────────────────────
		form := map[string]any{
			"table":      tableName,
			"db":         dbName,
			"config":     "{}", // default empty JSON config, can be updated later
			"user_id":    1,
			"created_at": now,
			"updated_at": now,
			"excluded":   false,
		}
		// ──────────────────────────────────────────────
		// custom_table
		// ──────────────────────────────────────────────
		table := map[string]any{
			"table":      tableName,
			"db":         dbName,
			"config":     "{}", // default empty JSON config, can be updated later
			"user_id":    1,
			"created_at": now,
			"updated_at": now,
			"excluded":   false,
		}
		// 3+4) columns → translate_table_field + table_schema
		columnsAny, hasColumns := tableDef.(map[string]any)["columns"]
		//fmt.Println(2, tableName, comment, columnsAny)
		if !hasColumns {
			continue
		}
		columns, ok := columnsAny.(map[string]any)
		if !ok {
			continue
		}
		filedsByOrder, ok := columns["__order"].([]any) // Get the column order from the special __order key
		if !ok {
			filedsByOrder = make([]any, 0)
			// If __order is missing, we can iterate over the map keys in any order (not guaranteed)
			for colName := range columns {
				filedsByOrder = append(filedsByOrder, colName)
			}
		}
		fieldOrder := 0
		form_fields := map[string]any{}
		table_fields := map[string]any{}
		for _, colNameAny := range filedsByOrder {
			colName, ok := colNameAny.(string)
			colDefAny, hasColDef := columns[colName]
			//fmt.Println(colName, hasColDef, colDefAny)
			if !ok || !hasColDef {
				continue
			}
			colDef, ok := colDefAny.(map[string]any)
			if !ok {
				continue
			}
			fieldOrder++
			// ──────────────────────────────────────────────
			// extract column properties with safe type assertions
			// ──────────────────────────────────────────────

			//colType := getString(colDef, "type", "unknown")
			colComment := getString(colDef, "comment", "")
			//pk := getBool(colDef, "pk", false)
			autoincrement := getBool(colDef, "autoincrement", false)
			nullable := getBool(colDef, "nullable", true) // default nullable=true if missing
			// defaultVal := getAny(colDef, "default", nil)

			fkRef := getString(colDef, "fk", "")
			fk := fkRef != ""
			if fk {
			}
			form_field := map[string]any{
				"name":          colNameAny,
				"label":         colComment, // default label same as name, can be updated later
				"display":       false,      // default display=true, can be updated later
				"order":         fieldOrder, // to maintain the order of fields in the form
				"autoincrement": autoincrement,
				"required":      !nullable,
				"sizeXs":        12,
				"sizesm":        12,
				"sizemd":        12,
				"sizelg":        12,
			}
			// for key in colDef if starts with form_ split on underscore and add to field with the suffix as key, e.g. form_label → label
			for k, v := range colDef {
				if strings.HasPrefix(k, "form_") {
					form_field[strings.TrimPrefix(k, "form_")] = v
				}
			}
			form_fields[colName] = form_field
			table_field := map[string]any{
				"name":    colNameAny,
				"label":   colComment, // default label same as name, can be updated later
				"display": true,       // default display=true, can be updated later
				"order":   fieldOrder,
			}
			// for key in colDef if starts with table_ split on underscore and add to field with the suffix as key, e.g. table_label → label
			for k, v := range colDef {
				if strings.HasPrefix(k, "table_") {
					table_field[strings.TrimPrefix(k, "table_")] = v
				}
			}
			table_fields[colName] = table_field
		}
		form_config := map[string]any{
			"fields":        form_fields,
			"layout":        tableDef.(map[string]any)["form_layout"],        // optional, can be generated later based on field order
			"extra_options": tableDef.(map[string]any)["form_extra_options"], // optional, for any additional form-level options
		}
		// config to json
		jsonData, err := json.Marshal(form_config)
		if err != nil {
			log.Fatalf("Error converting config to JSON: %v", err)
		}
		form["config"] = string(jsonData)
		data["custom_form"] = append(data["custom_form"].([]map[string]any), form)
		table_config := map[string]any{
			"fields":        table_fields,
			"layout":        tableDef.(map[string]any)["table_layout"],
			"extra_options": tableDef.(map[string]any)["table_extra_options"],
		}
		// config to json
		jsonData, err = json.Marshal(table_config)
		if err != nil {
			log.Fatalf("Error converting config to JSON: %v", err)
		}
		table["config"] = string(jsonData)
		data["custom_table"] = append(data["custom_table"].([]map[string]any), table)
	}
	return data
}

// ──────────────────────────────────────────────
// small helpers (safe type casting)
// ──────────────────────────────────────────────

func getString(m map[string]any, key string, fallback string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return fallback
}

func getBool(m map[string]any, key string, fallback bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return fallback
}

func getAny(m map[string]any, key string, fallback any) any {
	if v, ok := m[key]; ok {
		return v
	}
	return fallback
}

// SeedData matches what generateSeedData returns
type SeedData map[string]any

// UpsertSeedDataNamed uses named parameters (:name style) + select → update/insert
func UpsertSeedDataNamed(dbCon db.DBInterface, seed SeedData, targetDBName string) error {
	targetTables := []string{
		"table",
		"translate_table",
		"translate_table_field",
		"table_schema",
	}
	dialect := GetDialect(dbCon.GetDriverName())
	now := time.Now().UTC().Format("2006-01-02 15:04:05") // ← adjust to your DB's datetime format

	for _, tableName := range targetTables {
		rows, ok := seed[tableName].([]map[string]any)
		if !ok || len(rows) == 0 {
			log.Printf("No seed data for %q → skipping", tableName)
			continue
		}

		fmt.Printf("\n→ Processing %q (%d rows)\n", tableName, len(rows))

		for _, row := range rows {
			// Prepare the common named params that almost every row has
			params := map[string]any{
				"db":         targetDBName,
				"table":      row["table"],
				"user_id":    row["user_id"],
				"excluded":   row["excluded"],
				"created_at": now,
				"updated_at": now, // always refresh updated_at
			}

			// Add table-specific keys
			if tableName == "translate_table_field" || tableName == "table_schema" {
				params["field"] = row["field"]
			}

			// Decide PK / unique key for existence check & where clause
			var whereClause string
			var whereClause2 string
			var logKey string
			_chk_params := []any{}
			if tableName == "translate_table_field" || tableName == "table_schema" {
				whereClause = `db = ? AND "table" = ? AND field = ? AND excluded = false`
				whereClause2 = `db = :db AND "table" = :table AND field = :field AND excluded = false`
				_chk_params = []any{targetDBName, row["table"], row["field"]}
				logKey = fmt.Sprintf("%v.%v", row["table"], row["field"])
			} else {
				whereClause = `db = :db AND "table" = :table AND excluded = false`
				whereClause2 = `db = :db AND "table" = :table AND excluded = false`
				_chk_params = []any{targetDBName, row["table"]}
				logKey = fmt.Sprintf("%v", row["table"])
			}

			// 1. Check if row exists
			var exists bool
			checkQuery := fmt.Sprintf(`SELECT * FROM %s WHERE %s LIMIT 1`, dialect.GetTableName(tableName), whereClause)
			// fmt.Println("checkQuery:", checkQuery, _chk_params)
			res, _, err := dbCon.QueryMultiRows(checkQuery, _chk_params...)
			if err != nil && err != sql.ErrNoRows {
				return fmt.Errorf("existence check failed %s (%s): %w", tableName, logKey, err)
			} else if len(*res) > 0 {
				exists = true
			} else {
				exists = false
			}

			if exists {
				// 2a. UPDATE – only changeable fields
				updateParts := []string{`"updated_at" = :updated_at`}
				updateParams := map[string]any{"updated_at": now}

				for k, v := range row {
					// Skip identity columns and already handled fields
					if k == "db" || k == "table" || k == "field" || k == "created_at" || k == "updated_at" {
						continue
					}
					updateParts = append(updateParts, fmt.Sprintf(`%s = :%s`, dialect.GetColumnName(k), k))
					updateParams[k] = v
				}

				updateQuery := fmt.Sprintf(`
					UPDATE %s
					SET %s
					WHERE %s
				`, dialect.GetTableName(tableName), strings.Join(updateParts, ", "), whereClause2)

				// Merge where params into update params
				for k, v := range params {
					updateParams[k] = v
				}

				_, err := dbCon.ExecuteNamedQuery(updateQuery, updateParams)
				if err != nil {
					return fmt.Errorf("update failed %s (%s): %w", tableName, logKey, err)
				}

				//fmt.Printf("  updated  %-40s  (%d row(s))\n", logKey, affected)

			} else {
				// 2b. INSERT – all fields from the row + ensure timestamps
				cols := []string{}
				names := []string{}
				// fmt.Println("ROW:", row)
				for k := range row {
					cols = append(cols, dialect.GetColumnName(k))
					names = append(names, ":"+k)
					if _, ok := params[k]; !ok {
						// fmt.Println(k, row[k], params[k])
						params[k] = row[k]
					}
				}

				// Guarantee timestamps if missing in the seed map
				if _, has := row["created_at"]; !has {
					cols = append(cols, `"created_at"`)
					names = append(names, ":created_at")
					params["created_at"] = now
				}
				if _, has := row["updated_at"]; !has {
					cols = append(cols, `"updated_at"`)
					names = append(names, ":updated_at")
					params["updated_at"] = now
				}

				insertQuery := fmt.Sprintf(`
					INSERT INTO %s (%s)
					VALUES (%s)
				`, dialect.GetTableName(tableName), strings.Join(cols, ", "), strings.Join(names, ", "))
				// fmt.Println(insertQuery, params)
				_, err := dbCon.ExecuteNamedQuery(insertQuery, params)
				if err != nil {
					return fmt.Errorf("insert failed %s (%s): %w", tableName, logKey, err)
				}

				//fmt.Printf("  inserted %-40s\n", logKey)
			}
		}
	}

	fmt.Println("\nSeed data load completed successfully.")
	return nil
}

func UpsertCustomFT(dbCon db.DBInterface, seed SeedData, targetDBName string) error {
	app := map[string]any{}
	_sql := `SELECT app_id FROM app WHERE db = ? AND excluded = false LIMIT 1`
	_app, _, err := dbCon.QuerySingleRow(_sql, []any{targetDBName}...)
	if err != nil {
		return fmt.Errorf("find app failed: %w", err)
	}
	if len(*_app) > 0 {
		app = (*_app)
	}
	// fmt.Println("APP ID:", app["app_id"])
	targetTables := []string{
		"custom_form",
		"custom_table",
	}
	dialect := GetDialect(dbCon.GetDriverName())
	now := time.Now().UTC().Format("2006-01-02 15:04:05") // ← adjust to your DB's datetime format
	for _, tableName := range targetTables {
		rows, ok := seed[tableName].([]map[string]any)
		if !ok || len(rows) == 0 {
			log.Printf("\nNo custom data for %q → skipping", tableName)
			continue
		}
		fmt.Printf("\n→ Processing %q (%d rows)\n", tableName, len(rows))
		for _, row := range rows {
			// Prepare the common named params that almost every row has
			params := map[string]any{
				"db":         targetDBName,
				"table":      row["table"],
				"app_id":     app["app_id"],
				"user_id":    row["user_id"],
				"excluded":   row["excluded"],
				"created_at": now,
				"updated_at": now, // always refresh updated_at
			}
			row["app_id"] = app["app_id"] // ensure app_id is in the row for insert/update
			// Decide PK / unique key for existence check & where clause
			var whereClause string
			var whereClause2 string
			var logKey string
			_chk_params := []any{}
			whereClause = `db = ? AND "table" = ? AND excluded = false`
			whereClause2 = `db = :db AND "table" = :table AND excluded = false`
			_chk_params = []any{targetDBName, row["table"]}
			logKey = fmt.Sprintf("%v", row["table"])
			// 1. Check if row exists
			var exists bool
			checkQuery := fmt.Sprintf(`SELECT * FROM %s WHERE %s LIMIT 1`, dialect.GetTableName(tableName), whereClause)
			// fmt.Println("checkQuery:", checkQuery, _chk_params)
			res, _, err := dbCon.QueryMultiRows(checkQuery, _chk_params...)
			if err != nil {
				return fmt.Errorf("existence check failed %s (%s): %w", tableName, logKey, err)
			} else if len(*res) > 0 {
				exists = true
			} else {
				exists = false
			}
			if exists {
				// 2a. UPDATE – only changeable fields
				updateParts := []string{`"updated_at" = :updated_at`}
				updateParams := map[string]any{"updated_at": now}
				for k, v := range row {
					// Skip identity columns and already handled fields
					if k == "db" || k == "table" || k == "created_at" || k == "updated_at" {
						continue
					}
					updateParts = append(updateParts, fmt.Sprintf(`%s = :%s`, dialect.GetColumnName(k), k))
					updateParams[k] = v
				}
				updateQuery := fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, dialect.GetTableName(tableName), strings.Join(updateParts, ", "), whereClause2)
				// Merge where params into update params
				for k, v := range params {
					updateParams[k] = v
				}
				//fmt.Println(updateQuery, updateParams)
				_, err := dbCon.ExecuteNamedQuery(updateQuery, updateParams)
				if err != nil {
					return fmt.Errorf("update failed %s (%s): %w", tableName, logKey, err)
				}
				//fmt.Printf("  updated  %-40s  (%d row(s))\n", logKey, affected)
			} else {
				// 2b. INSERT – all fields from the row + ensure timestamps
				cols := []string{}
				names := []string{}
				// fmt.Println("ROW:", row)
				for k := range row {
					cols = append(cols, dialect.GetColumnName(k))
					names = append(names, ":"+k)
					if _, ok := params[k]; !ok {
						// fmt.Println(k, row[k], params[k])
						params[k] = row[k]
					}
				}
				// Guarantee timestamps if missing in the seed map
				if _, has := row["created_at"]; !has {
					cols = append(cols, `"created_at"`)
					names = append(names, ":created_at")
					params["created_at"] = now
				}
				if _, has := row["updated_at"]; !has {
					cols = append(cols, `"updated_at"`)
					names = append(names, ":updated_at")
					params["updated_at"] = now
				}
				insertQuery := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, dialect.GetTableName(tableName), strings.Join(cols, ", "), strings.Join(names, ", "))
				// fmt.Println(insertQuery, params)
				_, err := dbCon.ExecuteNamedQuery(insertQuery, params)
				if err != nil {
					return fmt.Errorf("insert failed %s (%s): %w", tableName, logKey, err)
				}
				//fmt.Printf("  inserted %-40s\n", logKey)
			}
		}
	}

	fmt.Println("\nSeed data load completed successfully.")
	return nil
}

// InterfaceConf represents the parsed cs_app structure
type InterfaceConf map[string]any

// LoadOrSyncMenusFromConfig creates/updates menus and menu_table links
func LoadOrSyncMenusFromConfig(
	dbCon db.DBInterface,
	conf InterfaceConf,
	dbName string, // e.g. "ADMIN"
	appUserID int, // usually 1
	desc string, // optional app description
) error {
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	// 1. Find the main app record (assuming one app per db)
	app := map[string]any{}
	sql := `SELECT app_id FROM app WHERE db = ? AND excluded = false LIMIT 1`
	_app, _, err := dbCon.QuerySingleRow(sql, []any{dbName}...)
	if err != nil {
		return fmt.Errorf("find app failed: %w", err)
	}
	if len(*_app) == 0 {
		// create the app record if not found
		insertAppSQL := `INSERT INTO app (app, app_desc, db, version, user_id, created_at, updated_at, excluded)
			VALUES (:app, :app_desc, :db, :version, :user_id, :created_at, :updated_at, :excluded)`
		if desc == "" {
			desc = fmt.Sprintf("Auto-created app for db %q", dbName)
		}
		appData := map[string]any{
			"app":        dbName,
			"app_desc":   desc,
			"db":         dbName,
			"version":    "1.0.0",
			"user_id":    appUserID,
			"active":     true,
			"created_at": now,
			"updated_at": now,
			"excluded":   false,
		}
		_, err := dbCon.ExecuteNamedQuery(insertAppSQL, appData)
		if err != nil {
			return fmt.Errorf("create app failed: %w", err)
		} else {
			_app, _, err = dbCon.QuerySingleRow(sql, []any{dbName}...)
			if err != nil {
				return fmt.Errorf("find app failed: %w", err)
			}
		}
	}
	app = (*_app)
	// 2. Process each menu section
	for menuName, _menuCfg := range conf {
		menuCfg, ok := _menuCfg.(map[string]any)
		if !ok {
			continue
		}
		activeAny, hasActive := menuCfg["active"]
		active := false
		if hasActive {
			active, _ = activeAny.(bool)
		}
		if !active {
			continue
		}
		icon := getString(menuCfg, "menu_icon", "")
		order := getFloat64(menuCfg, "menu_order", 999)
		config := getString(menuCfg, "menu_config", "")
		menu := map[string]any{}
		sql = `SELECT menu_id FROM menu WHERE menu = ? AND app_id = ? AND excluded = false LIMIT 1`
		_menu, _, err := dbCon.QuerySingleRow(sql, []any{menuName, app["app_id"]}...)
		if err != nil {
			return fmt.Errorf("check menu %q: %w", menuName, err)
		}
		menu = (*_menu)
		if len(menu) == 0 {
			// INSERT new menu
			sql = `INSERT INTO menu (
				menu, menu_desc, menu_icon, menu_order, menu_config,
				app_id, user_id, active,
				created_at, updated_at, excluded
			) VALUES (
				:menu, :menu_desc, :menu_icon, :menu_order, :menu_config,
				:app_id, :user_id, :active,
				:created_at, :updated_at, :excluded
			)`
			_data := map[string]any{
				"menu":        menuName,
				"menu_desc":   menuName, // or more descriptive if you want
				"menu_icon":   icon,
				"menu_order":  order,
				"menu_config": config,
				"app_id":      app["app_id"],
				"user_id":     appUserID,
				"active":      true,
				"created_at":  now,
				"updated_at":  now,
				"excluded":    false,
			}
			LastInsertId, err := dbCon.ExecuteQueryPGInsertWithLastInsertId(sql, _data)
			if err != nil {
				return fmt.Errorf("insert menu %q: %w", menuName, err)
			}

			// Get the inserted ID (sqlite/mysql/postgres compatible way)
			lastID := LastInsertId
			if lastID == 0 {
				// fallback: query again
				sql = `SELECT menu_id FROM menu WHERE menu = ? AND app_id = ? AND excluded = false LIMIT 1`
				_menu, _, err = dbCon.QuerySingleRow(sql, []any{menuName, app["app_id"]}...)
				if err != nil {
					return fmt.Errorf("retrieve new menu_id for %q: %w", menuName, err)
				}
				menu = (*_menu)
			} else {
				menu["menu_id"] = int(lastID)
			}

			fmt.Printf("Created menu %q → ID %d\n", menuName, menu["menu_id"])

		} else {
			fmt.Printf("Menu %q already exists → ID %d\n", menuName, menu["menu_id"])
			// update existing menu if needed (e.g., active status, icon, order, config)
			updateParts := []string{}
			updateParams := map[string]any{
				"menu_id": menu["menu_id"],
			}
			if icon != "" {
				updateParts = append(updateParts, `menu_icon = :menu_icon`)
				updateParams["menu_icon"] = icon
			}
			if order != 999 {
				updateParts = append(updateParts, `menu_order = :menu_order`)
				updateParams["menu_order"] = order
			}
			if config != "" {
				updateParts = append(updateParts, `menu_config = :menu_config`)
				updateParams["menu_config"] = config
			}
			updateParts = append(updateParts, `active = :active`)
			updateParams["active"] = active
			updateParams["updated_at"] = now
			if len(updateParts) > 0 {
				updateQuery := fmt.Sprintf(`
					UPDATE menu
					SET %s, updated_at = :updated_at
					WHERE menu_id = :menu_id
				`, strings.Join(updateParts, ", "))
				_, err := dbCon.ExecuteNamedQuery(updateQuery, updateParams)
				if err != nil {
					fmt.Printf("update menu %s: %v", menuName, err)
				} else {
					fmt.Printf("Updated menu %s (ID %d)\n", menuName, menu["menu_id"])
				}
			}
		}

		// 3. Link tables (menu_table entries)
		tablesAny, hasTables := menuCfg["tables"]
		if !hasTables {
			continue
		}

		tables, ok := tablesAny.([]any)
		if !ok {
			continue
		}

		for _, tItem := range tables {
			var tblName string
			var linkActive bool = true
			var requiresRLA bool = false

			switch v := tItem.(type) {
			case string:
				tblName = v
			case map[string]any:
				tblName = getString(v, "table", "")
				linkActive = getBool(v, "active", true)
				requiresRLA = getBool(v, "requires_rla", false)
			default:
				continue
			}

			if tblName == "" {
				continue
			}

			// Find table metadata record
			var tblMeta map[string]any
			sql = `SELECT table_id FROM "table"  WHERE "table" = ? AND db = ? AND excluded = false LIMIT 1`
			_tblMeta, _, err := dbCon.QuerySingleRow(sql, []any{tblName, dbName}...)
			if err != nil {
				return fmt.Errorf("find table %q: %w", tblName, err)
			}
			if len(*_tblMeta) == 0 {
				fmt.Printf("Warning: table %q not found in metadata → skipping link\n", tblName)
				continue
			}
			tblMeta = (*_tblMeta)
			// Check if link already exists
			var exists bool
			sql = `SELECT * FROM menu_table  WHERE menu_id = ?  AND table_id = ? AND app_id = ? AND excluded = false LIMIT 1 `
			_res, _, err := dbCon.QuerySingleRow(sql, []any{menu["menu_id"], tblMeta["table_id"], app["app_id"]}...)
			if err != nil {
				return fmt.Errorf("check menu_table link: %w", err)
			}
			if len(*_res) == 0 {
				exists = false
			} else {
				exists = true
			}
			if !exists {
				sql = `INSERT INTO menu_table (
					menu_id, table_id, app_id,
					active, requires_rla,
					user_id, created_at, updated_at, excluded
				) VALUES (
					:menu_id, :table_id, :app_id,
					:active, :requires_rla,
					:user_id, :created_at, :updated_at, :excluded
				)`
				_data := map[string]any{
					"menu_id":      menu["menu_id"],
					"table_id":     tblMeta["table_id"],
					"app_id":       app["app_id"],
					"active":       linkActive,
					"requires_rla": requiresRLA,
					"user_id":      appUserID,
					"created_at":   now,
					"updated_at":   now,
					"excluded":     false,
				}
				_, err = dbCon.ExecuteNamedQuery(sql, _data)
				if err != nil {
					return fmt.Errorf("insert menu_table %q → %q: %w", menuName, tblName, err)
				}

				fmt.Printf("  Linked table %q (active=%v, rla=%v)\n", tblName, linkActive, requiresRLA)
			} else {
				fmt.Printf("  Link already exists for table %q\n", tblName)
				// Optionally update link if active status or requires_rla has changed
				updateParts := []string{}
				updateParams := map[string]any{
					"menu_id":    menu["menu_id"],
					"table_id":   tblMeta["table_id"],
					"app_id":     app["app_id"],
					"updated_at": now,
				}
				if linkActive {
					updateParts = append(updateParts, `active = :active`)
					updateParams["active"] = linkActive
				}
				if requiresRLA {
					updateParts = append(updateParts, `requires_rla = :requires_rla`)
					updateParams["requires_rla"] = requiresRLA
				}
				if len(updateParts) > 0 {
					updateQuery := fmt.Sprintf(`
						UPDATE menu_table
						SET %s, updated_at = :updated_at
						WHERE menu_id = :menu_id AND table_id = :table_id AND app_id = :app_id
					`, strings.Join(updateParts, ", "))
					_, err := dbCon.ExecuteNamedQuery(updateQuery, updateParams)
					if err != nil {
						fmt.Printf("update menu_table link %s → %s: %v", menuName, tblName, err)
					} else {
						fmt.Printf("  Updated link for table %q (active=%v, rla=%v)\n", tblName, linkActive, requiresRLA)
					}
				}
			}
		}
	}
	return nil
}

// ──────────────────────────────────────────────
// Helpers (safe type extraction)
// ──────────────────────────────────────────────

func getFloat64(m map[string]any, key string, fallback float64) float64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return val
		case int:
			return float64(val)
		case int64:
			return float64(val)
		}
	}
	return fallback
}

func (etlx *ETLX) RunMODEL(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "MODEL"
	process := "MODEL"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
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
				"start_at":    time.Now(),
				"end_at":      time.Now(),
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
	conn, okCon := metadata["connection"].(string)
	if !okCon {
		conn, okCon = metadata["conn"].(string)
		if !okCon {
			return nil, fmt.Errorf("%s err no connection defined", key)
		}
	}
	adminConn, okAdminCon := metadata["admin_connection"].(string)
	if !okAdminCon {
		adminConn, _ = metadata["admin_conn"].(string)
	}
	create_all, _ := metadata["create_all"].(string)
	drop_all, _ := metadata["drop_all"].(string)
	start3 := time.Now()
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
		_log2["end_at"] = time.Now()
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
	_tables := map[string]any{}
	if drop_all != "" {
		// loop in reverse order for dropping tables to handle dependencies
		for i := len(order) - 1; i >= 0; i-- {
			itemKey := order[i]
			if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
				continue
			}
			item := data[itemKey]
			if _, isMap := item.(map[string]any); !isMap {
				continue
			}
			itemMetadata, ok := item.(map[string]any)["metadata"]
			if !ok {
				continue
			}
			table, ok := itemMetadata.(map[string]any)["table"].(string)
			if !ok {
				continue
			}
			//driver := dbConn.GetDriverName()
			// fmt.Printf("Dropping table %s (if exists) with driver %s\n", table, driver)
			start3 = time.Now()
			desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
			if !okDesc {
				desc = fmt.Sprintf("%s->%s", key, itemKey)
			}
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2 = map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": desc,
				"key":         key, "item_key": itemKey, "start_at": start3,
				"ref":                   dtRef,
				"mem_alloc_start":       mem_alloc,
				"mem_total_alloc_start": mem_total_alloc,
				"mem_sys_start":         mem_sys,
				"num_gc_start":          num_gc,
			}
			dropTableSQL := generateDropTableSQL(dbConn.GetDriverName(), table)
			_, err := dbConn.ExecuteQuery(dropTableSQL)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			if err != nil {
				fmt.Printf("%s ERR: dropping table %s: %s\n", key, table, err)
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s ERR: dropping table %s: %s", key, table, err)
			} else {
				// fmt.Printf("%s: table %s dropped or did not exist\n", key, table)
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s: table %s dropped or did not exist", key, table)
			}
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			processLogs = append(processLogs, _log2)
		}
	} else {
		for _, itemKey := range order {
			if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
				continue
			}
			// // fmt.Println("ITEM KEY:", itemKey)
			item := data[itemKey]
			if _, isMap := item.(map[string]any); !isMap {
				continue
			}
			itemMetadata, ok := item.(map[string]any)["metadata"]
			if !ok {
				continue
			}
			// ACTIVE
			if active, okActive := itemMetadata.(map[string]any)["active"]; okActive {
				if !active.(bool) {
					continue
				}
			}
			table, ok := itemMetadata.(map[string]any)["table"].(string)
			if !ok {
				continue
			}
			_tables[table] = itemMetadata // just to keep track of tables for potential future use
			comment, _ := itemMetadata.(map[string]any)["comment"].(string)
			driver := dbConn.GetDriverName()
			//fmt.Printf("Processing item %s (table: %s) with driver %s (comment: %s)\n", itemKey, table, driver, comment)
			columns, ok := itemMetadata.(map[string]any)["columns"].(map[string]any)
			if !ok {
				// fmt.Println("COLUMNS NOT FOUND")
				continue
			}
			start3 = time.Now()
			desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
			if !okDesc {
				desc = fmt.Sprintf("%s->%s", key, itemKey)
			}
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2 = map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": desc,
				"key":         key, "item_key": itemKey, "start_at": start3,
				"ref":                   dtRef,
				"mem_alloc_start":       mem_alloc,
				"mem_total_alloc_start": mem_total_alloc,
				"mem_sys_start":         mem_sys,
				"num_gc_start":          num_gc,
			}
			createTableSQL := generateCreateTableSQL(driver, table, comment, create_all, columns)
			// fmt.Println("CREATE TABLE SQL:\n", createTableSQL)
			_, err := dbConn.ExecuteQuery(createTableSQL)
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s ERR: creating table %s: %s", key, table, err)
				processLogs = append(processLogs, _log2)
				fmt.Println(createTableSQL, _log2["msg"])
			} else {
				_log2["success"] = true
				_log2["msg"] = fmt.Sprintf("%s: table %s created or already exists", key, table)
				processLogs = append(processLogs, _log2)
				// ADD DATA
				dataRows, okData := itemMetadata.(map[string]any)["data"].([]any)
				if okData {
					start3 = time.Now()
					mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
					_log2 = map[string]any{
						"process":               process,
						"name":                  fmt.Sprintf("%s->%s", key, itemKey),
						"description":           fmt.Sprintf("Inserting data into %s", table),
						"key":                   key,
						"item_key":              itemKey,
						"start_at":              start3,
						"ref":                   dtRef,
						"mem_alloc_start":       mem_alloc,
						"mem_total_alloc_start": mem_total_alloc,
						"mem_sys_start":         mem_sys,
						"num_gc_start":          num_gc,
					}
					err = InsertData(dbConn, table, columns, dataRows)
					if err != nil {
						_log2["success"] = false
						_log2["msg"] = fmt.Sprintf("%s ERR: inserting data into %s: %s", key, table, err)
						processLogs = append(processLogs, _log2)
						fmt.Println(_log2["msg"])
					} else {
						_log2["success"] = true
						_log2["msg"] = fmt.Sprintf("%s: data inserted into %s", key, table)
					}
					mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
					_log2["end_at"] = time.Now()
					_log2["duration"] = time.Since(start3).Seconds()
					_log2["mem_alloc_end"] = mem_alloc
					_log2["mem_total_alloc_end"] = mem_total_alloc
					_log2["mem_sys_end"] = mem_sys
					_log2["num_gc_end"] = num_gc
					processLogs = append(processLogs, _log2)
				} else {
					// fmt.Printf("No data to insert for %s->%s\n", key, itemKey)
				}
			}
		}
	}
	var adminDb db.DBInterface
	if adminConn == "" {
		// adminDb = dbConn
		start3 = time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
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
		adminDb, err = etlx.GetDB(conn)
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		if err != nil {
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: connecting to %s in : %s", key, conn, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			processLogs = append(processLogs, _log2)
			return nil, fmt.Errorf("%s ERR: connecting to %s in : %s", key, conn, err)
		} else {
			defer adminDb.Close()
		}
	} else {
		//fmt.Println("ADMIN CONN:", adminConn)
		start3 = time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
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
		adminDb, err = etlx.GetDB(adminConn)
		if err != nil {
			mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
			_log2["mem_alloc_end"] = mem_alloc
			_log2["mem_total_alloc_end"] = mem_total_alloc
			_log2["mem_sys_end"] = mem_sys
			_log2["num_gc_end"] = num_gc
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: connecting to ADMIN DB %s in : %s", key, adminConn, err)
			_log2["end_at"] = time.Now()
			_log2["duration"] = time.Since(start3).Seconds()
			processLogs = append(processLogs, _log2)
			return nil, fmt.Errorf("%s ERR: connecting to ADMIN DB %s in : %s", key, adminConn, err)
		} else {
			defer adminDb.Close()
		}
	}
	// ADD TABLE METADATA
	updateTableMetadataSQL, _ := metadata["update_table_metadata"].(bool)
	if drop_all == "" && updateTableMetadataSQL {
		// fmt.Println("UPDATE TABLE METADATA", updateTableMetadataSQL)
		start3 = time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"process":               process,
			"name":                  fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"description":           fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"key":                   key,
			"item_key":              nil,
			"start_at":              start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		_data := generateSeedData(_tables, database)
		err = UpsertSeedDataNamed(adminDb, _data, database)
		if err != nil {
			fmt.Printf("%s ERR: upserting seed data: %s\n", key, err)
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: upserting seed data: %s", key, err)
		} else {
			fmt.Printf("%s: seed data upserted successfully\n", key)
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s: seed data upserted successfully", key)
		}
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		processLogs = append(processLogs, _log2)
	}
	cs_app, ok := metadata["cs_app"].(map[string]any)
	if drop_all == "" && ok && updateTableMetadataSQL {
		// fmt.Println("LOADING/SYNCING MENUS FROM CONFIG", cs_app)
		start3 = time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"process":               process,
			"name":                  fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"description":           fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"key":                   key,
			"item_key":              nil,
			"start_at":              start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		app_desc, _ := metadata["description"].(string)
		err = LoadOrSyncMenusFromConfig(adminDb, cs_app, database, 1, app_desc)
		if err != nil {
			fmt.Printf("%s ERR: loading/syncing menus from config: %s\n", key, err)
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: loading/syncing menus from config: %s", key, err)
		} else {
			fmt.Printf("%s: menus loaded/synced from config successfully\n", key)
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s: menus loaded/synced from config successfully", key)
		}
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		processLogs = append(processLogs, _log2)
	}
	// CUSTOM DATA
	if drop_all == "" && updateTableMetadataSQL {
		// fmt.Println("UPDATE TABLE METADATA", updateTableMetadataSQL)
		start3 = time.Now()
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2 = map[string]any{
			"process":               process,
			"name":                  fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"description":           fmt.Sprintf("%s->%s", key, "Update Table Metadata"),
			"key":                   key,
			"item_key":              nil,
			"start_at":              start3,
			"ref":                   dtRef,
			"mem_alloc_start":       mem_alloc,
			"mem_total_alloc_start": mem_total_alloc,
			"mem_sys_start":         mem_sys,
			"num_gc_start":          num_gc,
		}
		_data := generateCustomData(_tables, database)
		err = UpsertCustomFT(adminDb, _data, database)
		if err != nil {
			fmt.Printf("%s ERR: upserting custom data: %s\n", key, err)
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s ERR: upserting custom data: %s", key, err)
		} else {
			fmt.Printf("%s: custom data upserted successfully\n", key)
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s: custom data upserted successfully", key)
		}
		mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3).Seconds()
		_log2["mem_alloc_end"] = mem_alloc
		_log2["mem_total_alloc_end"] = mem_total_alloc
		_log2["mem_sys_end"] = mem_sys
		_log2["num_gc_end"] = num_gc
		processLogs = append(processLogs, _log2)
	}
	mem_alloc2, mem_total_alloc2, mem_sys2, num_gc2 := etlx.RuntimeMemStats()
	processLogs[0] = map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              processLogs[0]["start_at"],
		"end_at":                time.Now(),
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
