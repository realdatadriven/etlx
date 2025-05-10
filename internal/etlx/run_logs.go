package etlxlib

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

func (etlx *ETLX) RunLOGS(dateRef []time.Time, conf map[string]any, logs []map[string]any, keys ...string) ([]map[string]any, error) {
	key := "LOGS"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	// fmt.Println(key, dateRef)
	var processData []map[string]any
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing metadata in %s section", key)
	}
	if active, okActive := metadata["active"]; okActive {
		if !active.(bool) {
			return nil, fmt.Errorf("deactivated %s", key)
		}
	}
	beforeSQL, okBefore := metadata["before_sql"]
	afterSQL, okAfter := metadata["after_sql"]
	saveSQL, okSave := metadata["save_log_sql"]
	errPatt, okErrPatt := metadata["save_on_err_patt"]
	errSQL, okErrSQL := metadata["save_on_err_sql"]
	tmpDir := ""
	if _, ok := metadata["tmp_dir"].(string); ok {
		tmpDir = metadata["tmp_dir"].(string)
	}
	conn, okCon := metadata["connection"]
	if !okCon {
		return nil, fmt.Errorf("%s err no connection defined", key)
	}
	dbConn, err := etlx.GetDB(conn.(string))
	if err != nil {
		return nil, fmt.Errorf("%s ERR: connecting to %s in : %s", key, conn, err)
	}
	defer dbConn.Close()
	jsonData, err := json.MarshalIndent(logs, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("error converting logs to JSON: %v", err)
	}
	fname, err := etlx.TempFIle(tmpDir, string(jsonData), "logs.*.json")
	// println(fname, string(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error saving logs to JSON: %v", err)
	}
	//  QUERIES TO RUN AT beginning
	if okBefore {
		err = etlx.ExecuteQuery(dbConn, beforeSQL, data, fname, "", dateRef)
		if err != nil {
			return nil, fmt.Errorf("%s: Before error: %s", key, err)
		}
	}
	// fmt.Println(key, sql)
	if saveSQL != "" && okSave {
		// fmt.Println(data[saveSQL.(string)])
		err = etlx.ExecuteQuery(dbConn, saveSQL, data, fname, "", dateRef)
		if err != nil {
			_err_by_pass := false
			if okErrPatt && errPatt != nil && okErrSQL && errSQL != nil {
				//fmt.Println(onErrPatt.(string), onErrSQL.(string))
				re, regex_err := regexp.Compile(errPatt.(string))
				if regex_err != nil {
					return nil, fmt.Errorf("%s ERR: fallback regex matching the error failed to compile: %s", key, regex_err)
				} else if re.MatchString(string(err.Error())) {
					err = etlx.ExecuteQuery(dbConn, errSQL, data, fname, "", dateRef)
					if err != nil {
						return nil, fmt.Errorf("%s ERR: main: %s", key, err)
					} else {
						_err_by_pass = true
					}
				}
			}
			if !_err_by_pass {
				return nil, fmt.Errorf("%s ERR: main: %s", key, err)
			}
		}
	}
	//  QUERIES TO RUN AT THE END
	if okAfter {
		err = etlx.ExecuteQuery(dbConn, afterSQL, data, fname, "", dateRef)
		if err != nil {
			return nil, fmt.Errorf("%s: After error: %s", key, err)
		}
	}
	return processData, nil
}
