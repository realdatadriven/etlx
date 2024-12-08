package db

type DBInterface interface {
	ExecuteQuery(query string, data ...interface{}) (int, error)
	ExecuteQueryRowsAffected(query string, data ...interface{}) (int64, error)
	QuerySingleRow(query string, params ...interface{}) (*map[string]interface{}, bool, error)
	QueryMultiRows(query string, params ...interface{}) (*[]map[string]interface{}, bool, error)
	QueryMultiRowsWithCols(query string, params ...interface{}) (*[]map[string]interface{}, []string, bool, error)
	AllTables(params map[string]interface{}, extra_conf map[string]interface{}) (*[]map[string]interface{}, bool, error)
	TableSchema(params map[string]interface{}, table string, dbName string, extra_conf map[string]interface{}) (*[]map[string]interface{}, bool, error)
	ExecuteNamedQuery(query string, data map[string]interface{}) (int, error)
	ExecuteQueryPGInsertWithLastInsertId(query string, data ...interface{}) (int, error)
	GetUserByNameOrEmail(email string) (map[string]interface{}, bool, error)
	GetDriverName() string
	Close() error
	IsEmpty(value interface{}) bool
	FromParams(params map[string]interface{}, extra_conf map[string]interface{}) (*DB, string, string, error)
	Ping() error
}
