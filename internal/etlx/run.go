package etlxlib

import (
	"fmt"
	"strings"
	"time"
)

func (etlx *ETLX) RunETLX(extraConf map[string]any, dateRef []time.Time) ([]map[string]any, map[string]any, error) {
	logs := []map[string]any{}
	data := map[string]any{}
	_keys := []any{"NOTIFY", "NOTIFICATION", "LOGS", "OBSERVABILITY", "SCRIPTS", "MODEL_SQL", "MULTI_QUERIES", "STACKED_QUERIES", "EXPORTS", "DATA_QUALITY", "DATAQUALITY", "QUALITY", "ETL", "ELT", "ACTIONS", "AUTO_LOGS", "REQUIRES", "IMPORTS", "MODEL", "CSMODEL", "C7MODEL", "MODEL_DATA", "CSDATA", "C7DATA", "WORKFLOW", "C7WORKFLOW", "CSWORKFLOW", "C7ROLE", "ROLE", "CSROLE", "C7ROLE_USERS", "CSROLE_USERS", "ROLE_USERS", "REMOTE", "REMOTE_EXEC"}
	__order, ok := etlx.Config["__order"].([]string)
	hasOrderedKeys := false
	if !ok {
		__order2, ok := etlx.Config["__order"].([]any)
		if ok {
			hasOrderedKeys = true
			__order = []string{}
			for _, key := range __order2 {
				__order = append(__order, key.(string))
			}
		}
	} else {
		etlx.Config["__order"] = []any{}
		for key := range etlx.Config {
			etlx.Config["__order"] = append(etlx.Config["__order"].([]any), key)
		}
		//hasOrderedKeys = true
	}
	// fmt.Println("LEVEL 1 H:", __order, len(__order))
	if !hasOrderedKeys {
	} else if len(__order) > 0 {
		//fmt.Print("LEVEL 1 H:", __order)
		ignoreNext := false
		for _, key := range __order {
			if key == "metadata" || key == "__order" || key == "order" {
				continue
			}
			//if !app.contains(_keys, any(key)) {
			_key_conf, ok := etlx.Config[key].(map[string]any)
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
			if ignoreNext {
				continue
			}
			if runs_as, ok := _key_conf_metadata["runs_as"]; ok {
				// fmt.Printf("%s RUN AS %s:\n", key, runs_as)
				if etlx.containsAny(_keys, runs_as) {
					switch runs_as {
					case "ETL", "ELT":
						_logs, err := etlx.RunETL(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "DATA_QUALITY", "DATAQUALITY", "QUALITY":
						_logs, err := etlx.RunDATA_QUALITY(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "MULTI_QUERIES", "STACKED_QUERIES":
						_logs, _, err := etlx.RunMULTI_QUERIES(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "EXPORTS":
						_logs, err := etlx.RunEXPORTS(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "NOTIFY", "NOTIFICATION":
						_logs, err := etlx.RunNOTIFY(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "ACTIONS":
						_logs, err := etlx.RunACTIONS(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "SCRIPTS", "MODEL_SQL":
						_logs, err := etlx.RunSCRIPTS(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "LOGS", "OBSERVABILITY":
						_logs, err := etlx.RunLOGS(dateRef, nil, logs, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							data[key] = map[string]any{
								"success": true,
								"runs_as": runs_as,
								"logs":    _logs,
							}
						}
					case "REQUIRES", "IMPORTS":
						_logs, err := etlx.LoadREQUIRES(nil, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "MODEL", "CSMODEL", "C7MODEL":
						_logs, err := etlx.RunMODEL(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "MODEL_DATA", "CSDATA", "C7DATA":
						//fmt.Printf("%s AS %s START:\n", key, runs_as)
						_logs, err := etlx.RunMODEL_DATA(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									//fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "WORKFLOW", "C7WORKFLOW", "CSWORKFLOW":
						// fmt.Printf("%s AS %s START:\n", key, runs_as)
						_logs, err := etlx.RunWORKFLOW(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "C7ROLE", "CSROLE", "ROLE":
						// fmt.Printf("%s AS %s START:\n", key, runs_as)
						_logs, err := etlx.RunC7ROLE(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "C7ROLE_USERS", "CSROLE_USERS", "ROLE_USERS":
						// fmt.Printf("%s AS %s START:\n", key, runs_as)
						_logs, err := etlx.RunC7ROLE_USERS(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
						}
					case "REMOTE", "REMOTE_EXEC":
						ignoreNext = true
						// fmt.Printf("%s AS %s START:\n", key, runs_as)
						_logs, err := etlx.RunREMOTE(dateRef, nil, extraConf, key)
						if err != nil {
							fmt.Printf("%s AS %s ERR: %v\n", key, runs_as, err)
							if strings.Contains(err.Error(), "deactivated") {
								ignoreNext = false
							}
							// break
						} else {
							if _, ok := etlx.Config["AUTO_LOGS"]; ok && len(_logs) > 0 {
								_, err := etlx.RunLOGS(dateRef, nil, _logs, "AUTO_LOGS")
								if err != nil {
									// fmt.Printf("INCREMENTAL AUTOLOGS ERR: %v\n", err)
								}
							}
							logs = append(logs, _logs...)
							// break
						}
						if etlx.RemoteSkiped {
							ignoreNext = false
						}
						// fmt.Println("etlx.RemoteSkiped:", etlx.RemoteSkiped, "ignoreNext:", ignoreNext, _logs)
					default:
						//
					}
				}
			}
			//}
		}
	}
	return logs, data, nil
}
