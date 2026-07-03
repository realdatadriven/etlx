package etlxlib

import (
    "encoding/json"
    "log"
    "os"
    "strings"
)

var DebugMode bool

var stdLogger *log.Logger

func init() {
    stdLogger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
    rawDebug := os.Getenv("ETL_DEBUG")
    DebugMode = strings.EqualFold(rawDebug, "true") || rawDebug == "1"
}

func SetDebug(d bool) {
    DebugMode = d
}

func Debugf(format string, args ...any) {
    if DebugMode {
        stdLogger.Printf("[DEBUG] "+format, args...)
    }
}

func Infof(format string, args ...any) {
    stdLogger.Printf("[INFO] "+format, args...)
}

func Errorf(format string, args ...any) {
    stdLogger.Printf("[ERROR] "+format, args...)
}

// AppendProcessLog appends an entry to processLogs and emits a debug log
// when DebugMode is enabled. The entry is marshaled to JSON for readability.
func AppendProcessLog(processLogs *[]map[string]any, entry map[string]any) {
    *processLogs = append(*processLogs, entry)
    if DebugMode {
        if b, err := json.Marshal(entry); err == nil {
            stdLogger.Printf("[PROCESS_LOG] %s", string(b))
        } else {
            stdLogger.Printf("[PROCESS_LOG] (marshal error): %v", err)
        }
    }
}
