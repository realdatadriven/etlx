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
	case "postgres":
		return &PostgresDialect{}
	case "mysql":
		return &MySQLDialect{}
	case "sqlite3":
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

// Example Usage (for testing purposes)
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
}
