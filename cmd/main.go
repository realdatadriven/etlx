package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/realdatadriven/etlx"
)

func main() {
	etlx.LoadDotEnv()
	
	// Initialize OpenTelemetry
	om, otelErr := etlx.InitializeOTel("etlx-cli")
	if otelErr != nil {
		log.Printf("Warning: Failed to initialize OpenTelemetry: %v\n", otelErr)
	}
	defer func() {
		if om != nil {
			if err := om.Shutdown(); err != nil {
				log.Printf("Error shutting down OpenTelemetry: %v\n", err)
			}
		}
	}()
	
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
	// extrat from a file
	file := flag.String("file", "", "The file to extract data from, the flag shoud be used in combination with the only appointing to the ETL key the data is meant to")
	// To clean / delete data (execute clean_sql on every item)
	clean := flag.Bool("clean", false, "To clean data (execute clean_sql on every item, conditioned by only and skip)")
	// To drop the table (execute drop_sql on every item condition by only and skip)
	drop := flag.Bool("drop", false, "To drop the table (execute drop_sql on every item, conditioned by only and skip)")
	// To get number of rows in the table (execute rows_sql on every item, conditioned by only and skip)
	rows := flag.Bool("rows", false, "To get number of rows in the table (execute rows_sql on every item, conditioned by only and skip)")
	flag.Parse()
	config := make(map[string]any)
	// Parse the file content
	etlxlib := &etlx.ETLX{Config: config}
	err := etlxlib.ConfigFromFile(*filePath)
	if err != nil {
		log.Fatalf("Error parsing Markdown: %v", err)
	}
	if _, ok := etlxlib.Config["REQUIRES"]; ok {
		_logs, err := etlxlib.LoadREQUIRES(nil)
		if err != nil {
			fmt.Printf("REQUIRES ERR: %v\n", err)
		}
		for _, _log := range _logs {
			fmt.Println(_log["start_at"], _log["end_at"], _log["duration"], _log["name"], _log["success"], _log["msg"], _log["rows"])
		}
	}
	// Print the parsed configuration
	if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
		etlxlib.PrintConfigAsJSON(etlxlib.Config)
	}
	/*/ Walk through the data and process each key-value pair
	etlxlib.Walk(etlxlib.Config, "", func(keyPath string, value any) {
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
		"rows":  *rows,
		"file":  *file,
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
	logs := []map[string]any{}
	_keys := []string{"NOTIFY", "LOGS", "SCRIPTS", "MULTI_QUERIES", "EXPORTS", "DATA_QUALITY", "ETL", "ELT", "ACTIONS", "AUTO_LOGS", "REQUIRES"}
	__order, ok := etlxlib.Config["__order"].([]string)
	hasOrderedKeys := false
	if !ok {
		__order2, ok := etlxlib.Config["__order"].([]any)
		if ok {
			hasOrderedKeys = true
			__order = []string{}
			for _, key := range __order2 {
				__order = append(__order, key.(string))
			}
		}
	} else {
		etlxlib.Config["__order"] = []any{}
		for key, _ := range etlxlib.Config {
			etlxlib.Config["__order"] = append(etlxlib.Config["__order"].([]any), key)
		}
		//hasOrderedKeys = true
	}
	// fmt.Println("LEVEL 1 H:", __order, len(__order))
	if !hasOrderedKeys {
	} else if len(__order) > 0 {
		//fmt.Print("LEVEL 1 H:", __order)
		for _, key := range __order {
			if key == "metadata" || key == "__order" || key == "order" {
				continue
			}
			//if !etlxlib.Contains(_keys, any(key)) {
			_key_conf, ok := etlxlib.Config[key].(map[string]any)
			if !ok {
				continue
			}
			_key_conf_metadata, ok := _key_conf["metadata"].(map[string]any)
			if !ok {
				continue
			}
			if _, ok := _key_conf_metadata["runs_as"]; !ok {
				_key_conf_metadata["runs_as"] = strings.ToUpper(key)
			}
			if runs_as, ok := _key_conf_metadata["runs_as"]; ok {
				fmt.Printf("%s RUN AS %s:\n", key, runs_as)
				if etlxlib.Contains(_keys, runs_as) {
					// Create OTel span for this operation
					runOm := om
					if runOm == nil {
						runOm = etlx.GetOTelManager()
					}
					
					switch runs_as {
					case "ETL", "ELT":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("ETL_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunETL(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "DATA_QUALITY", "DATAQUALITY", "QUALITY":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("DataQuality_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunDATA_QUALITY(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "MULTI_QUERIES", "STACKED_QUERIES":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("MultiQueries_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, _, err := etlxlib.RunMULTI_QUERIES(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "EXPORTS":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Exports_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunEXPORTS(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "NOTIFY", "NOTIFICATION":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Notify_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunNOTIFY(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "ACTIONS":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Actions_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunACTIONS(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "SCRIPTS":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Scripts_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunSCRIPTS(dateRef, nil, extraConf, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "LOGS", "OBSERVABILITY":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Logs_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.RunLOGS(dateRef, nil, logs, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					case "REQUIRES", "IMPORTS":
						opSpan, _ := runOm.StartOperationSpan(fmt.Sprintf("Requires_%s", key), map[string]any{"step": runs_as, "key": key})
						_logs, err := etlxlib.LoadREQUIRES(nil, key)
						if err != nil {
							opSpan.RecordError(err)
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlxlib.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlxlib.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
						opSpan.End()
					default:
						//
					}
				}
			}
			//}
		}
	}
}
