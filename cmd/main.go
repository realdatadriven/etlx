package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/realdatadriven/etlx/internal/db"
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
	// date of reference
	date_ref := flag.String("date", time.Now().AddDate(0, 0, -1).Format("2006-01-02"), "Date Reference format YYYY-MM-DD")
	flag.Parse()
	config := make(map[string]any)
	// Parse the file content
	etl := &etlx.ETLX{Config: config}
	err := etl.ConfigFromFile(*filePath)
	if err != nil {
		log.Fatalf("Error parsing Markdown: %v", err)
	}
	// Print the parsed configuration
	// PrintConfigAsJSON(etl.Config)
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
	fmt.Println("date_ref:", *date_ref, dateRef)
	// Define the runner as a simple function
	runner := func(conn string, query string, item map[string]any) error {
		driver, dsn, err := etl.ParseConnection(conn)
		if err != nil {
			return err
		}
		table := ""
		metadata, ok := item["metadata"].(map[string]any)
		if ok {
			table = metadata["table"].(string)
		}
		_dsn := etl.ReplaceEnvVariable(dsn)
		_query := etl.ReplaceEnvVariable(query)
		_query = etl.ReplaceQueryStringDate(_query, dateRef)
		_csv_path := fmt.Sprintf(`%s/%s_YYYYMMDD.csv`, os.TempDir(), table)
		if table != "" {
			_query = etl.ReplaceFileTablePlaceholder("table", _query, table)
			_query = etl.ReplaceFileTablePlaceholder("file", _query, _csv_path)
		}
		//fmt.Println("table:", table)
		switch driver {
		case "duckdb":
			//fmt.Printf("[DuckDB] Running query on DSN [%s]:\n%s\n", dsn, query)
			fmt.Printf("[DuckDB] ENV Running query on DSN [%s]:\n%s\n", _dsn, _query)
			db, err := db.NewDuckDB(_dsn)
			if err != nil {
				return fmt.Errorf("%s Conn: %s", driver, err)
			}
			defer db.Close()
			_, err = db.ExecuteQuery(_query)
			if err != nil {
				return fmt.Errorf("%s: %s", driver, err)
			}
		case "odbc":
			//fmt.Printf("[ODBC] Running query on DSN [%s]:\n%s\n", dsn, query)
			fmt.Printf("[ODBC] ENV Running query on DSN [%s]:\n%s\n", _dsn, _query)
			new_odbc, err := db.NewODBC(_dsn)
			if err != nil {
				return fmt.Errorf("ODBC Conn: %s", err)
			}
			defer new_odbc.Close()
			_csv_path = etl.ReplaceQueryStringDate(_csv_path, dateRef)
			fmt.Println(_csv_path)
			_, err = new_odbc.Query2CSV(_query, _csv_path)
			if err != nil {
				return fmt.Errorf("ODBC 2 CSV: %s", err)
			}
		default:
			fmt.Printf("[%s] Running query on DSN [%s]:\n%s\n", driver, _dsn, _query)
			db, err := db.New(driver, _dsn)
			if err != nil {
				return fmt.Errorf("%s Conn: %s", driver, err)
			}
			defer db.Close()
			fmt.Println(db.GetDriverName())
			res, err := db.ExecuteQuery(_query)
			if err != nil {
				return fmt.Errorf("%s: %s", driver, err)
			}
			fmt.Println(db.GetDriverName(), res, err)
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
