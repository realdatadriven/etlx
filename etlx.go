package etlx

import (
	"fmt"

	"github.com/joho/godotenv"
	"github.com/realdatadriven/etlx/internal/db"
	etlxlib "github.com/realdatadriven/etlx/internal/etlx"
)

// Expose the library functions
type ETLX = etlxlib.ETLX
type DBInterface = db.DBInterface
type DB = db.DB
type DuckDB = db.DuckDB
type ODBC = db.ODBC

func LoadDotEnv() {
	_err := godotenv.Load()
	if _err != nil {
		fmt.Println("Error loading .env file")
	}
}
