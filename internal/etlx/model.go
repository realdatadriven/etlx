package etlxlib

import (
	"fmt"
	"strings"
)

// SQLDialect defines the interface for different SQL database dialects.
type SQLDialect interface {
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
}

// BaseDialect provides common implementations for SQLDialect interface.
type BaseDialect struct{}

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
func (b *BaseDialect) GetTableComment(tableName, comment string) string { return "" }
func (b *BaseDialect) SupportsInlineColumnComment() bool { return false }
func (b *BaseDialect) SupportsTableComment() bool { return false }

// PostgresDialect implements SQLDialect for PostgreSQL.
type PostgresDialect struct { BaseDialect }

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
type MySQLDialect struct { BaseDialect }

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

func (m *MySQLDialect) GetColumnComment(tableName, columnName, comment string) string {
	// MySQL supports inline column comments
	return fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(comment, "'", "''"))
}

func (m *MySQLDialect) GetTableComment(tableName, comment string) string {
	return fmt.Sprintf(" COMMENT='%s'", strings.ReplaceAll(comment, "'", "''"))
}

func (m *MySQLDialect) SupportsInlineColumnComment() bool { return true }
func (m *MySQLDialect) SupportsTableComment() bool { return true }

// SQLiteDialect implements SQLDialect for SQLite.
type SQLiteDialect struct { BaseDialect }

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
type MSSQLDialect struct { BaseDialect }

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

// getDialect returns the appropriate SQLDialect implementation.
func getDialect(driver string) SQLDialect {
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
func generateCreateTableSQL(driver, tableName, tableComment string, fields []map[string]any) string {
	dialect := getDialect(driver)
	var schema strings.Builder

	schema.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))

	var columnDefs []string
	var foreignKeyConstraints []string
	var primaryKeyColumns []string
	var postCreateTableSQL []string // For comments or other post-creation statements

	for _, field := range fields {
		name := field["name"].(string)
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
				postCreateTableSQL = append(postCreateTableSQL, dialect.GetColumnComment(tableName, name, comment))
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
				foreignKeyConstraints = append(foreignKeyConstraints, fmt.Sprintf("    FOREIGN KEY (%s) REFERENCES %s(%s)", name, referencedTable, referencedColumn))
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

	return schema.String() + ";\n"
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
func InsertData(db *sqlx.DB, tableName string, columns []map[string]any, data []map[string]any) error {
	// Map schema column names to their properties for quick lookup
	type schemaColInfo struct {
		isCreatedAt bool
		isUpdatedAt bool
		isExcluded  bool
		isNullable  bool
	}
	
	schemaColumnMap := make(map[string]schemaColInfo)
	var allSchemaColumnNames []string // To maintain order for INSERT statement

	for _, col := range columns {
		colName, ok := col["name"].(string)
		if !ok {
			return fmt.Errorf("column definition missing 'name' field")
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
		if nullable, ok := col["nullable"].(bool); ok && nullable {
			info.isNullable = true
		}
		schemaColumnMap[colName] = info
		allSchemaColumnNames = append(allSchemaColumnNames, colName)
	}

	for i, row := range data {
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
				insertCols = append(insertCols, colName)
				insertVals = append(insertVals, ":"+colName)
				insertMap[colName] = val
			} else {
				// Value not in data row, check for defaults based on schema definition
				if colInfo.isCreatedAt {
					insertCols = append(insertCols, colName)
					insertVals = append(insertVals, ":"+colName)
					insertMap[colName] = now
				} else if colInfo.isUpdatedAt {
					insertCols = append(insertCols, colName)
					insertVals = append(insertVals, ":"+colName)
					insertMap[colName] = now
				} else if colInfo.isExcluded {
					insertCols = append(insertCols, colName)
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
			tableName, strings.Join(insertCols, ", "), strings.Join(insertVals, ", "))

		// Execute the insert using NamedExec for safety and convenience
		_, err := db.NamedExec(query, insertMap)
		if err != nil {
			return fmt.Errorf("failed to insert row %d into %s: %w", i, tableName, err)
		}
	}

	return nil
}

/*/ Example Usage (for testing purposes)
func main() {
	// Example table definition (similar to your YAML structure)
	fields := []map[string]any{
		{"name": "id", "type": "INTEGER", "pk": true, "autoincrement": true, "comment": "Primary ID"},
		{"name": "name", "type": "VARCHAR", "length": 255, "nullable": false, "unique": true, "comment": "User Name"},
		{"name": "email", "type": "VARCHAR", "length": 100, "nullable": false, "default": "", "comment": "User Email"},
		{"name": "is_active", "type": "BOOLEAN", "default": true, "comment": "Is Active"},
		{"name": "created_at", "type": "DATETIME", "comment": "Creation Timestamp"},
		{"name": "role_id", "type": "INTEGER", "fk": "roles.id", "comment": "Foreign Key to Roles"},
	}

	fmt.Println("--- PostgreSQL ---")
	fmt.Println(generateCreateTableSQL("postgres", "users", "Table of users", fields))

	fmt.Println("--- MySQL ---")
	fmt.Println(generateCreateTableSQL("mysql", "users", "Table of users", fields))

	fmt.Println("--- SQLite3 ---")
	fmt.Println(generateCreateTableSQL("sqlite3", "users", "Table of users", fields))

	fmt.Println("--- MSSQL ---")
	fmt.Println(generateCreateTableSQL("mssql", "users", "Table of users", fields))
}*/
