package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

func (etlx *ETLX) RunMULTI_QUERIES(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "MULTI_QUERIES"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
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
	beforeSQL, okBefore := metadata["before_sql"]
	afterSQL, okAfter := metadata["after_sql"]
	saveSQL, okSave := metadata["save_sql"]
	errPatt, okErrPatt := metadata["save_on_err_patt"]
	errSQL, okErrSQL := metadata["save_on_err_sql"]
	queries := []string{}
	for itemKey, item := range data {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		if _, isMap := item.(map[string]any); !isMap {
			//fmt.Println(itemKey, "NOT A MAP:", item)
			continue
		}
		if only, okOnly := extraConf["only"]; okOnly {
			if len(only.([]string)) == 0 {
			} else if !etlx.contains(only.([]string), itemKey) {
				continue
			}
		}
		if skip, okSkip := extraConf["skip"]; okSkip {
			if len(skip.([]string)) == 0 {
			} else if etlx.contains(skip.([]string), itemKey) {
				continue
			}
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
		query, okQuery := itemMetadata.(map[string]any)["query"]
		if query != nil && okQuery {
			sql := query.(string)
			query, ok := item.(map[string]any)[sql].(string)
			_, queryDoc := etlx.Config[sql]
			if !ok && queryDoc {
				query = sql
				_sql, _, _, err := etlx.QueryBuilder(nil, sql)
				if err != nil {
					fmt.Printf("QUERY DOC ERR ON KEY %s: %v\n", queries, err)
				} else {
					query = _sql
				}
			}
			sql = etlx.SetQueryPlaceholders(query, "", "", dateRef)
			queries = append(queries, sql)
		}
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
	//  QUERIES TO RUN AT BEGINING
	if okBefore {
		err = etlx.ExecuteQuery(dbConn, beforeSQL, data, "", "", dateRef)
		if err != nil {
			return nil, fmt.Errorf("%s: Before error: %s", key, err)
		}
	}
	// MAIN QUERY
	unionKey, ok := metadata["union_key"].(string)
	if !ok {
		unionKey = "UNION\n"
	}
	sql := strings.Join(queries, unionKey)
	fmt.Println(key, sql)
	if saveSQL != "" && okSave {
		data["final_query"] = sql // PUT THE QUERY GENERATED IN THE SCOPE
		fmt.Println(data[saveSQL.(string)])
		err = etlx.ExecuteQuery(dbConn, saveSQL, data, "", "", dateRef)
		if err != nil {
			_err_by_pass := false
			if okErrPatt && errPatt != nil && okErrSQL && errSQL != nil {
				//fmt.Println(onErrPatt.(string), onErrSQL.(string))
				re, regex_err := regexp.Compile(errPatt.(string))
				if regex_err != nil {
					return nil, fmt.Errorf("%s ERR: fallback regex matching the error failed to compile: %s", key, regex_err)
				} else if re.MatchString(string(err.Error())) {
					err = etlx.ExecuteQuery(dbConn, errSQL, data, "", "", dateRef)
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
			//return fmt.Errorf("%s -> %s -> %s ERR: main: %s", key, step, itemKey, err)
		} else {

		}
	} else {
		rows, _, err := etlx.Query(dbConn, sql, data, "", "", dateRef)
		if err != nil {
			return nil, fmt.Errorf("%s: error: %s", key, err)
		}
		processData = *rows
	}
	//  QUERIES TO RUN AT THE END
	if okAfter {
		err = etlx.ExecuteQuery(dbConn, afterSQL, data, "", "", dateRef)
		if err != nil {
			return nil, fmt.Errorf("%s: After error: %s", key, err)
		}
	}
	return processData, nil
}
