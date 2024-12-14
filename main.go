package main

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	db, err := sql.Open("duckdb", "foo.db")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()
}
