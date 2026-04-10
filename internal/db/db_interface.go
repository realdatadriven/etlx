package db

import (
	"context"
	"database/sql"
)

type DBInterface interface {
	ExecuteQuery(query string, data ...any) (int, error)
	Query2CSV(query string, csv_path string, params ...any) (bool, error)
	QueryMultiRows(query string, params ...any) (*[]map[string]any, bool, error)
	ExecuteQueryRowsAffected(query string, data ...any) (int64, error)
	QuerySingleRow(query string, params ...any) (*map[string]any, bool, error)
	QueryRows(ctx context.Context, query string, params ...any) (*sql.Rows, error)
	QueryMultiRowsWithCols(query string, params ...any) (*[]map[string]any, []string, bool, error)
	AllTables(params map[string]any, extra_conf map[string]any) (*[]map[string]any, bool, error)
	TableSchema(params map[string]any, table string, dbName string, extra_conf map[string]any) (*[]map[string]any, bool, error)
	ExecuteNamedQuery(query string, data map[string]any) (int, error)
	ExecuteQueryPGInsertWithLastInsertId(query string, data ...any) (int, error)
	GetUserByNameOrEmail(email string) (map[string]any, bool, error)
	GetDriverName() string
	Close() error
	IsEmpty(value any) bool
	FromParams(params map[string]any, extra_conf map[string]any) (*DB, string, string, error)
	Ping() error
}
