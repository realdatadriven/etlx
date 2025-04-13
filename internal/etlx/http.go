package etlxlib

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// HTTPAction executes HTTP uploads and downloads based on the mode and params
func (etlx *ETLX) HTTPAction(mode string, params map[string]any) error {
	url, _ := params["url"].(string)
	method := "GET"
	if m, ok := params["method"].(string); ok {
		method = strings.ToUpper(m)
	}
	headers, _ := params["headers"].(map[string]any)
	contentType, _ := params["content_type"].(string)
	// bodyParams, _ := params["body"].(map[string]any)
	source, _ := params["source"].(string)
	target, _ := params["target"].(string)
	if url == "" {
		return fmt.Errorf("missing 'url' parameter")
	}
	client := &http.Client{}
	switch mode {
	case "download":
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return fmt.Errorf("creating request failed: %w", err)
		}
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("HTTP request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP request returned status: %s", resp.Status)
		}
		outFile, err := os.Create(target)
		if err != nil {
			return fmt.Errorf("creating output file failed: %w", err)
		}
		defer outFile.Close()
		_, err = io.Copy(outFile, resp.Body)
		if err != nil {
			return fmt.Errorf("saving response failed: %w", err)
		}
	case "upload":
		file, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("opening source file failed: %w", err)
		}
		defer file.Close()
		body := &bytes.Buffer{}
		_, err = io.Copy(body, file)
		if err != nil {
			return fmt.Errorf("copying file to body failed: %w", err)
		}
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			return fmt.Errorf("creating HTTP request failed: %w", err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("HTTP upload failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("upload returned status: %s", resp.Status)
		}
	default:
		return fmt.Errorf("unsupported http action: %s", mode)
	}
	return nil
}
