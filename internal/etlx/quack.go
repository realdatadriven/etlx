package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
)

// Handle quack driver specific logic
// to create expects dsn in the format quack:path/to/db.duckdb?token=value&name=value
// to connect to a remote duckdb instance with quack, the dsn should be in the format quack:host:port?token=value&name=value

// IsDuckDBDSN checks if the given DSN is for DuckDB.
func (etlx *ETLX) IsQuackDSN(dsn string) bool {
	// Check if the DSN starts with "quack:"
	return strings.HasPrefix(dsn, "quack:")
}

// ParseQuackDSN parses the given DSN and extracts the host, port, and parameters.
// quack:host:port?token=value&name=value
func (etlx *ETLX) ParseQuackDSN(dsn string) (string, string, map[string]string, error) {
	// Remove the "quack:" prefix
	dsn = strings.TrimPrefix(dsn, "quack:")
	// Use regex to extract host, port, and parameters
	re := regexp.MustCompile(`^(?P<host>[^:]+):(?P<port>\d+)(?:\?(?P<params>.*))?$`)
	matches := re.FindStringSubmatch(dsn)
	if len(matches) == 0 {
		return "", "", nil, fmt.Errorf("invalid DSN format")
	}
	host := matches[1]
	port := matches[2]
	paramsStr := matches[3]
	params := make(map[string]string)
	if paramsStr != "" {
		paramPairs := strings.Split(paramsStr, "&")
		for _, pair := range paramPairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				params[strings.ToLower(kv[0])] = kv[1]
			}
		}
	}
	return host, port, params, nil
}

// to create expects dsn in the format quack:path/to/db.duckdb?host=value&port=value&token=value&name=value
func (etlx *ETLX) ParseQuackFileDSN(dsn string) (string, map[string]string, error) {
	// Remove the "quack:" prefix
	dsn = strings.TrimPrefix(dsn, "quack:")
	// Use regex to extract file path and parameters
	re := regexp.MustCompile(`^(?P<filepath>[^?]+)(?:\?(?P<params>.*))?$`)
	matches := re.FindStringSubmatch(dsn)
	if len(matches) == 0 {
		return "", nil, fmt.Errorf("invalid DSN format")
	}
	filepath := matches[1]
	// check if filepath ends with .duckdb, if not return error
	if !strings.HasSuffix(filepath, ".duckdb") {
		return "", nil, fmt.Errorf("invalid DSN format: file path must end with .duckdb")
	}
	paramsStr := matches[2]
	params := make(map[string]string)
	if paramsStr != "" {
		paramPairs := strings.Split(paramsStr, "&")
		for _, pair := range paramPairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				params[strings.ToLower(kv[0])] = kv[1]
			}
		}
	}
	return filepath, params, nil
}

// check if port is open
func (etlx *ETLX) IsPortOpen(host string, port string) bool {
	return false
}
