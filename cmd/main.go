package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/joho/godotenv"
	"github.com/realdatadriven/etlx/internal/etlx"
)

// PrintConfigAsJSON prints the configuration map in JSON format
func PrintConfigAsJSON(config map[string]any) {
	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("Error converting config to JSON: %v", err)
	}
	fmt.Println(string(jsonData))
}

func main() {
	// Load .env file
	_err := godotenv.Load()
	if _err != nil {
		fmt.Println("Error loading .env file")
	}
	// Config file path
	filePath := flag.String("config", "config.md", "Config File")
	flag.Parse()
	config := make(map[string]any)
	// Parse the file content
	etl := &etlx.ETLX{Config: config}
	err := etl.ConfigFromFile(*filePath)
	if err != nil {
		log.Fatalf("Error parsing Markdown: %v", err)
	}
	// Print the parsed configuration
	PrintConfigAsJSON(etl.Config)
	/*/ Walk through the data and process each key-value pair
	etl.Walk(etl.Config, "", func(keyPath string, value any) {
		fmt.Printf("Key: %s, Value: %v\n", keyPath, value)
		if reflect.TypeOf(value).Kind() != reflect.Map {
			fmt.Printf("Key: %s, Value: %v\n", keyPath, value)
		} else {
			fmt.Printf("Entering: %s\n", keyPath)
		}
	})*/
	// Define the runner as a simple function
	runner := func(conn string, query string, item map[string]any) error {
		driver, dsn, err := etl.ParseConnection(conn)
		if err != nil {
			return err
		}
		switch driver {
		case "duckdb":
			fmt.Printf("[DuckDB] Running query on DSN [%s]:\n%s\n", dsn, query)
		case "odbc":
			fmt.Printf("[ODBC] Running query on DSN [%s]:\n%s\n", dsn, query)
		default:
			return fmt.Errorf("unsupported driver: %s", driver)
		}
		return nil
	}
	// Process the ETL
	err = etl.ProcessETL(etl.Config, runner)
	if err != nil {
		log.Fatalf("ETL failed: %v", err)
	}

	// Define the runner as a simple function
	runnerKey := func(metadata map[string]any, item map[string]any) error {
		fmt.Println(metadata, item)
		return nil
	}
	// Process the MD KEY
	err = etl.ProcessMDKey("DATA_QUALITY", etl.Config, runnerKey)
	if err != nil {
		log.Fatalf("DATA_QUALITY failed: %v", err)
	}
}
