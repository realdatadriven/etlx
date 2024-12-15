package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

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
	etl.RunETL(dateRef)
}
