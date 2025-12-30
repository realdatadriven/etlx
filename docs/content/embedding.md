+++
title = 'Embedding in Go'
weight = 70
draft = false
+++

## Embedding in Go

To embed the ETL framework in a Go application, you can use the `etlx` package and call `ConfigFromMDText` and `RunETL`. Example (from README):

```go
package main

import (
    "fmt"
    "time"
    "github.com/realdatadriven/etlx"
)

func main() {
    etl := &etlx.ETLX{}

    // Load configuration from Markdown text
    err := etl.ConfigFromMDText(`# Your Markdown config here`)
    if err != nil {
        fmt.Printf("Error loading config: %v\n", err)
        return
    }

    // Prepare date reference
    dateRef := []time.Time{time.Now().AddDate(0, 0, -1)}

    // Define additional options
    options := map[string]any{
        "only":  []string{"sales"},
        "steps": []string{"extract", "load"},
    }

    // Run ETL process
    logs, err := etl.RunETL(dateRef, nil, options)
    if err != nil {
        fmt.Printf("Error running ETL: %v\n", err)
        return
    }

    // Print logs
    for _, log := range logs {
        fmt.Printf("Log: %+v\n", log)
    }
}
```
