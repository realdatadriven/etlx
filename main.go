package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"gopkg.in/yaml.v3"
	"github.com/russross/blackfriday/v2"
	"strings"
)

// Function to parse the markdown config file
func parseMarkdownToConfig(filePath string) (map[string]map[string]any, error) {
	// Read the markdown file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Parse the markdown into an AST
	parsed := blackfriday.New().Parse(data)

	// Initialize the result map
	config := make(map[string]map[string]any)

	var currentTitle string
	var yamlContent map[string]any
	var sqlContent map[string]string

	// Walk the parsed AST
	blackfriday.Walk(parsed, func(node *blackfriday.Node, entering bool) blackfriday.WalkStatus {
		// If we are entering a heading (section title)
		if entering && node.Type == blackfriday.Heading {
			// Extract section title (e.g., "EXTRACT")
			currentTitle = string(node.Literal)

			// Initialize the section map for the title
			config[currentTitle] = make(map[string]any)
		}

		// If we are entering a YAML block
		if entering && node.Type == blackfriday.CodeBlock && strings.HasPrefix(string(node.Info), "yaml") {
			// Extract the name after "yaml" as the key
			key := strings.TrimSpace(strings.TrimPrefix(string(node.Info), "yaml"))
			yamlContent = make(map[string]any)

			// Store the YAML content for parsing later
			err := yaml.Unmarshal(node.Literal, &yamlContent)
			if err != nil {
				log.Printf("Error parsing YAML block for key %s: %v", key, err)
			} else {
				// Store the YAML content under the appropriate key in the section
				config[currentTitle][key] = yamlContent
			}
		}

		// If we are entering a SQL block
		if entering && node.Type == blackfriday.CodeBlock && strings.HasPrefix(string(node.Info), "sql") {
			// Extract the name after "sql" as the key
			key := strings.TrimSpace(strings.TrimPrefix(string(node.Info), "sql"))
			sqlContent = make(map[string]string)

			// Store SQL content for the block
			sqlContent[key] = string(node.Literal)
		}

		// If we are leaving a SQL block, add it to the section
		if !entering && node.Type == blackfriday.CodeBlock && strings.HasPrefix(string(node.Info), "sql") {
			// Store the SQL content under the appropriate key in the section
			key := strings.TrimSpace(strings.TrimPrefix(string(node.Info), "sql"))
			config[currentTitle][key] = sqlContent[key]
		}

		return blackfriday.GoToNext
	})

	return config, nil
}

// Function to walk through the parsed config map
func walkConfig(config map[string]map[string]any) {
	for title, blocks := range config {
		fmt.Printf("Section: %s\n", title)

		// Print YAML content
		if yamlContent, ok := blocks["yaml"]; ok {
			fmt.Println("  YAML:")
			yamlBytes, _ := yaml.Marshal(yamlContent)
			fmt.Println(string(yamlBytes))
		}

		// Print SQL content
		if sqlContent, ok := blocks["sql"]; ok {
			fmt.Println("  SQL Blocks:")
			for key, sql := range sqlContent.(map[string]string) {
				fmt.Printf("    %s:\n    %s\n", key, sql)
			}
		}
	}
}

func main() {
	// Parse the markdown file
	config, err := parseMarkdownToConfig("etl_config.md")
	if err != nil {
		log.Fatalf("Error parsing markdown: %v", err)
	}

	// Walk through the parsed config and print
	walkConfig(config)
}
