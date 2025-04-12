package etlxlib

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (etlx *ETLX) CompressToZip(files []string, output string) error {
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()
	zipWriter := zip.NewWriter(outFile)
	defer zipWriter.Close()
	for _, file := range files {
		inFile, err := os.Open(file)
		if err != nil {
			return err
		}
		defer inFile.Close()

		w, err := zipWriter.Create(filepath.Base(file))
		if err != nil {
			return err
		}
		_, err = io.Copy(w, inFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func (etlx *ETLX) CompressToGZ(input string, output string) error {
	inFile, err := os.Open(input)
	if err != nil {
		return err
	}
	defer inFile.Close()
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()
	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()
	_, err = io.Copy(gzWriter, inFile)
	return err
}

func addMainPath(fname string, mainPath string) string {
	if filepath.IsAbs(fname) {
	} else if filepath.IsLocal(fname) && !isEmpty(mainPath) {
		fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
	} else if filepath.Dir(fname) != "" && mainPath != "" {
		fname = fmt.Sprintf(`%s/%s`, mainPath, fname)
	}
	return fname
}

func (etlx *ETLX) RunACTIONS(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "ACTIONS"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now()
	processLogs = append(processLogs, map[string]any{
		"name": key,
		"key":  key, "start_at": start,
	})
	mainDescription := ""
	// Define the runner as a simple function
	ACTIONSRunner := func(metadata map[string]any, itemKey string, item map[string]any) error {
		//fmt.Println(metadata, itemKey, item)
		// ACTIVE
		if active, okActive := metadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("KEY %s", key),
					"description": metadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return fmt.Errorf("dectivated %s", "")
			}
		}
		//name, _ := metadata["name"].(string)
		mainDescription = metadata["description"].(string)
		mainPath, _ := metadata["path"].(string)
		itemMetadata, ok := item["metadata"].(map[string]any)
		//fmt.Println(itemMetadata, itemKey, item)
		if !ok {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"],
				"key":         key, "item_key": itemKey, "start_at": time.Now(),
				"end_at":  time.Now(),
				"success": true,
				"msg":     "Missing metadata in item",
			})
			return nil
		}
		// ACTIVE
		if active, okActive := itemMetadata["active"]; okActive {
			if !active.(bool) {
				processLogs = append(processLogs, map[string]any{
					"name":        fmt.Sprintf("%s->%s", key, itemKey),
					"description": itemMetadata["description"].(string),
					"key":         key, "item_key": itemKey, "start_at": time.Now(),
					"end_at":  time.Now(),
					"success": true,
					"msg":     "Deactivated",
				})
				return nil
			}
		}
		_type, okType := itemMetadata["type"].(string)
		params, okParams := itemMetadata["params"].(map[string]any)
		if !okType {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": time.Now(),
				"end_at":  time.Now(),
				"success": true,
				"msg":     "Missing Action Type",
			})
			return nil
		}
		if !okParams {
			processLogs = append(processLogs, map[string]any{
				"name":        fmt.Sprintf("%s->%s", key, itemKey),
				"description": itemMetadata["description"].(string),
				"key":         key, "item_key": itemKey, "start_at": time.Now(),
				"end_at":  time.Now(),
				"success": true,
				"msg":     "Missing Action Params",
			})
			return nil
		}
		dtRef, okDtRef := itemMetadata["date_ref"]
		if okDtRef && dtRef != "" {
			_dt, err := time.Parse("2006-01-02", dtRef.(string))
			if err == nil {
				dateRef = append([]time.Time{}, _dt)
			}
		} else {
			if len(dateRef) > 0 {
				dtRef = dateRef[0].Format("2006-01-02")
			}
		}
		start3 := time.Now()
		_log2 := map[string]any{
			"name":        fmt.Sprintf("%s->%s", key, itemKey),
			"description": itemMetadata["description"].(string),
			"key":         key, "item_key": itemKey, "start_at": start3,
			"ref": dtRef,
		}
		switch _type {
		case "copy_file":
			source, hasSource := params["source"].(string)
			target, hasTarget := params["target"].(string)
			if !hasSource || !hasTarget {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: missing required params: source and/or target", key, itemKey, _type)
				break
			}
			source = addMainPath(etlx.SetQueryPlaceholders(source, "", "", dateRef), mainPath)
			data, err := os.ReadFile(source)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Failed to read source: %v", key, itemKey, _type, err)
				break
			}
			target = addMainPath(etlx.SetQueryPlaceholders(target, "", "", dateRef), mainPath)
			err = os.WriteFile(target, data, 0644)
			if err != nil {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Failed to write target: %v", key, itemKey, _type, err)
				break
			}
			_log2["success"] = true
			_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Copy successful", key, itemKey, _type)
		case "compress":
			compression, hasType := params["compression"].(string)
			files, hasFiles := params["files"].([]any) // slice of interface{}
			output, hasOutput := params["output"].(string)
			if !hasType || !hasFiles || !hasOutput {
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: compress missing required params: compression, files, or output", key, itemKey, _type)
				break
			}
			// Convert []any to []string
			filePaths := []string{}
			for _, f := range files {
				if str, ok := f.(string); ok {
					filePaths = append(filePaths, addMainPath(etlx.SetQueryPlaceholders(str, "", "", dateRef), mainPath))
				}
			}
			output = addMainPath(etlx.SetQueryPlaceholders(output, "", "", dateRef), mainPath)
			switch compression {
			case "zip":
				err := etlx.CompressToZip(filePaths, output)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Error compressing to zip: %v", key, itemKey, _type, err)
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: ZIP compression successful.", key, itemKey, _type)
				}
			case "gz":
				if len(filePaths) != 1 {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: GZ compression only supports one input file", key, itemKey, _type)
					break
				}
				err := etlx.CompressToGZ(filePaths[0], output)
				if err != nil {
					_log2["success"] = false
					_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Error compressing to gz: %v", key, itemKey, _type, err)
				} else {
					_log2["success"] = true
					_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: GZ compression successful.", key, itemKey, _type)
				}
			default:
				_log2["success"] = false
				_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Unsupported compression type %s", key, itemKey, _type, compression)
			}
		default:
			_log2["success"] = false
			_log2["msg"] = fmt.Sprintf("%s -> %s -> %s: Unsupported type", key, itemKey, _type)
		}
		_log2["end_at"] = time.Now()
		_log2["duration"] = time.Since(start3)
		processLogs = append(processLogs, _log2)
		return nil
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	// Process the MD KEY
	err := etlx.ProcessMDKey(key, conf, ACTIONSRunner)
	if err != nil {
		return processLogs, fmt.Errorf("%s failed: %v", key, err)
	}
	processLogs[0] = map[string]any{
		"name":        key,
		"description": mainDescription,
		"key":         key, "start_at": processLogs[0]["start_at"],
		"end_at":   time.Now(),
		"duration": time.Since(start),
	}
	return processLogs, nil
}
