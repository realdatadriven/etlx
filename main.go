package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"gopkg.in/yaml.v3"
)

// ParseMarkdownToConfig parses a Markdown file into a structured nested map
func ParseMarkdownToConfig(filePath string) (map[string]any, error) {
	// Read the file content
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Initialize the Markdown parser
	parser := goldmark.DefaultParser()

	// Parse the Markdown content into an AST
	reader := text.NewReader(data)
	root := parser.Parse(reader)

	// Initialize the result map and a parent stack
	config := make(map[string]any)
	parents := []map[string]any{config} // Stack of parent references for each level

	// Walk through the AST
	ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			switch n := node.(type) {
			case *ast.Heading:
				// Extract the heading text
				var headingText strings.Builder
				for child := n.FirstChild(); child != nil; child = child.NextSibling() {
					if textNode, ok := child.(*ast.Text); ok {
						headingText.WriteString(string(textNode.Text(reader.Source())))
					}
				}

				heading := headingText.String()

				// Ensure the parents stack has enough levels
				for len(parents) <= int(n.Level) {
					parents = append(parents, nil)
				}

				// Create or switch to the appropriate section
				parent := parents[n.Level-1]
				if parent == nil {
					parent = config
				}

				if _, exists := parent[heading]; !exists {
					parent[heading] = make(map[string]any)
				}

				// Update the parent reference for the current level
				parents[n.Level] = parent[heading].(map[string]any)

			case *ast.FencedCodeBlock:
				// Extract info and content from the code block
				info := string(n.Info.Segment.Value(reader.Source()))
				content := string(n.Text(reader.Source()))

				// Add to the appropriate parent
				parent := parents[len(parents)-1]
				if parent != nil {
					if strings.HasPrefix(info, "yaml") {
						// Process YAML blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "yaml"))
						yamlContent := make(map[string]any)
						if err := yaml.Unmarshal([]byte(content), &yamlContent); err != nil {
							log.Printf("Error parsing YAML block %s: %v", key, err)
						} else {
							parent[key] = yamlContent
						}
					} else if strings.HasPrefix(info, "sql") {
						// Process SQL blocks
						key := strings.TrimSpace(strings.TrimPrefix(info, "sql"))
						parent[key] = content
					}
				}
			}
		}
		return ast.WalkContinue, nil
	})

	return config, nil
}

// PrintConfigAsJSON prints the configuration map in JSON format
func PrintConfigAsJSON(config map[string]any) {
	jsonData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("Error converting config to JSON: %v", err)
	}
	fmt.Println(string(jsonData))
}

func main() {
	// Config file path
	filePath := flag.String("config", "config.md", "Config File")
	flag.Parse()
	// Parse the file content
	config, err := ParseMarkdownToConfig(*filePath)
	if err != nil {
		log.Fatalf("Error parsing Markdown: %v", err)
	}

	// Print the parsed configuration
	PrintConfigAsJSON(config)
}
