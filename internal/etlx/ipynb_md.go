package etlx

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Notebook represents the structure of a Jupyter notebook.
type Notebook struct {
	Cells []Cell `json:"cells"`
}

// Cell represents a single cell in the notebook.
type Cell struct {
	CellType string   `json:"cell_type"`
	Source   []string `json:"source"`
}

// ConvertIPYNBToMarkdown converts the content of a .ipynb file to Markdown text.
func (etlx *ETLX) ConvertIPYNBToMarkdown(ipynbContent []byte) (string, error) {
	// Parse the .ipynb content
	var notebook Notebook
	if err := json.Unmarshal(ipynbContent, &notebook); err != nil {
		return "", fmt.Errorf("error parsing JSON: %w", err)
	}
	// Build the Markdown output
	var mdBuilder strings.Builder
	for _, cell := range notebook.Cells {
		// Skip empty cells
		if len(cell.Source) == 0 {
			continue
		}
		switch cell.CellType {
		case "markdown":
			// Add Markdown content directly
			for _, line := range cell.Source {
				mdBuilder.WriteString(line)
			}
			mdBuilder.WriteString("\n\n") // Add spacing between cells
		case "code":
			// Wrap code content in a Markdown code block
			mdBuilder.WriteString("```\n")
			for _, line := range cell.Source {
				mdBuilder.WriteString(line)
			}
			mdBuilder.WriteString("```\n\n")
		}
	}
	if os.Getenv("ETLX_DEBUG_QUERY") == "true" {
		_, err := etlx.TempFIle(mdBuilder.String(), "ipymd2md.*.md")
		if err != nil {
			fmt.Println(err)
		}
		//fmt.Println(_file)
	}
	return mdBuilder.String(), nil
}
