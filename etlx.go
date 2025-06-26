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

func New(driverName string, dsn string) (*db.DB, error) {
	return db.New(driverName, dsn)
}

type DuckDB = db.DuckDB

func NewDuckDB(dsn string) (*db.DuckDB, error) {
	return db.NewDuckDB(dsn)
}

type ODBC = db.ODBC

func NewODBC(dsn string) (*db.ODBC, error) {
	return db.NewODBC(dsn)
}

type DuckLakeParseResult = etlxlib.DuckLakeParseResult
type DuckLakeOccurrence = etlxlib.DuckLakeOccurrence
type DuckLakeParser = etlxlib.DuckLakeParser

func NewDuckLakeParser() *etlxlib.DuckLakeParser {
	return etlxlib.NewDuckLakeParser()
}

func LoadDotEnv() {
	_err := godotenv.Load()
	if _err != nil {
		fmt.Println("Error loading .env file")
	}
}
