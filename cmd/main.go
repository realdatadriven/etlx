package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/realdatadriven/etlx/internal/etlx"
)

func main() {
	// Load .env file
	_err := godotenv.Load()
	if _err != nil {
		fmt.Println("Error loading .env file")
	}
	// Config file path
	filePath := flag.String("config", "config.md", "Config File")
	// date of reference
	date_ref := flag.String("date", time.Now().AddDate(0, 0, -1).Format("2006-01-02"), "Date Reference format YYYY-MM-DD")
	// to skip
	skip := flag.String("skip", "", "The keys to skip")
	// to skip
	only := flag.String("only", "", "The only keys to run")
	// to steps
	steps := flag.String("steps", "", "The steps to run")
	// To clean / delete data (execute clean_sql on every item)
	clean := flag.Bool("clean", false, "To clean data (execute clean_sql on every item, conditioned by only and skip)")
	// To drop the table (execute drop_sql on every item condition by only and skip)
	drop := flag.Bool("drop", false, "To drop the table (execute drop_sql on every item, conditioned by only and skip)")
	flag.Parse()
	config := make(map[string]any)
	// Parse the file content
	etl := &etlx.ETLX{Config: config}
	err := etl.ConfigFromFile(*filePath)
	if err != nil {
		log.Fatalf("Error parsing Markdown: %v", err)
	}
	// Print the parsed configuration
	//etl.PrintConfigAsJSON(etl.Config)
	/*/ Walk through the data and process each key-value pair
	etl.Walk(etl.Config, "", func(keyPath string, value any) {
		fmt.Printf("Key: %s, Value: %v\n", keyPath, value)
		if reflect.TypeOf(value).Kind() != reflect.Map {
			fmt.Printf("Key: %s, Value: %v\n", keyPath, value)
		} else {
			fmt.Printf("Entering: %s\n", keyPath)
		}
	})*/
	var dateRef []time.Time
	_dt, _ := time.Parse("2006-01-02", *date_ref)
	dateRef = append(dateRef, _dt)
	// fmt.Println("date_ref:", *date_ref, dateRef)
	extraConf := map[string]any{
		"clean": *clean,
		"drop":  *drop,
	}
	if *only != "" {
		extraConf["only"] = strings.Split(*only, ",")
	}
	if *skip != "" {
		extraConf["skip"] = strings.Split(*skip, ",")
	}
	if *steps != "" {
		extraConf["steps"] = strings.Split(*steps, ",")
	}
	_logs, err := etl.RunETL(dateRef, extraConf)
	if err != nil {
		fmt.Printf("ETL ERR: %v\n", err)
	}
	for _, _log := range _logs {
		fmt.Println(_log["start_at"], _log["end_at"], _log["duration"], _log["name"], _log["success"], _log["msg"])
	}
	//_sql, query_parts, _fields_order, err := etl.QueryBuilder("QUERY_DOC")
	_sql, _, _, err := etl.QueryBuilder("QUERY_DOC")
	if err != nil {
		fmt.Printf("QUERY_DOC ERR: %v\n", err)
	}
	fmt.Println(_sql)
}
