package etlxlib

import (
	"regexp"
	"strings"
)

// DuckLakeParseResult represents the result of parsing a DuckLake string
type DuckLakeParseResult struct {
	IsDuckLake   bool   `json:"is_ducklake"`
	HasAttach    bool   `json:"has_attach"`
	DSN          string `json:"dsn"`
	DuckLakeName string `json:"ducklake_name"`
	DataPath     string `json:"data_path"`
}

// DuckLakeOccurrence represents a single DuckLake occurrence found in text
type DuckLakeOccurrence struct {
	DuckLakeString string `json:"ducklake_string"`
	HasAttach      bool   `json:"has_attach"`
	DSN            string `json:"dsn"`
	DuckLakeName   string `json:"ducklake_name"`
	DataPath       string `json:"data_path"`
}

// DuckLakeParser handles parsing of DuckLake format strings
type DuckLakeParser struct {
	mainPattern     *regexp.Regexp
	dataPathPattern *regexp.Regexp
	scanPattern     *regexp.Regexp
}

// NewDuckLakeParser creates a new DuckLakeParser instance
func NewDuckLakeParser() *DuckLakeParser {
	// Main regex pattern to match ducklake format
	// This pattern handles:
	// 1. Optional ATTACH keyword at the beginning
	// 2. Required 'ducklake:' prefix
	// 3. DSN (data source name) - can contain various characters
	// 4. Optional AS clause with ducklake_name
	// 5. Optional parameters like DATA_PATH
	mainPattern := regexp.MustCompile(`(?i)^\s*(ATTACH\s+)?['"]?ducklake:([^'"\)\s]+)['"]?(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?(?:\s*\(([^)]*)\))?\s*;?\s*$`)

	// Pattern to extract DATA_PATH from parameters
	dataPathPattern := regexp.MustCompile(`(?i)DATA_PATH\s+['"]([^'"]+)['"]`)

	// Pattern for finding potential ducklake occurrences in logs
	// This is more flexible and can find partial matches
	scanPattern := regexp.MustCompile(`(?i)(?:(?:^|\s)(ATTACH)\s+)?['"]?(ducklake:[^'"\)\s;]+)['"]?(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?(?:\s*\([^)]*\))?\s*;?`)

	return &DuckLakeParser{
		mainPattern:     mainPattern,
		dataPathPattern: dataPathPattern,
		scanPattern:     scanPattern,
	}
}

// Parse parses a string to check if it's in ducklake format and extract components
func (p *DuckLakeParser) Parse(input string) DuckLakeParseResult {
	result := DuckLakeParseResult{
		IsDuckLake:   false,
		HasAttach:    false,
		DSN:          "",
		DuckLakeName: "",
		DataPath:     "",
	}

	if input == "" {
		return result
	}

	matches := p.mainPattern.FindStringSubmatch(strings.TrimSpace(input))

	if len(matches) > 0 {
		result.IsDuckLake = true

		// Check if ATTACH keyword is present (group 1)
		if matches[1] != "" {
			result.HasAttach = true
		}

		// Extract DSN (group 2)
		if len(matches) > 2 && matches[2] != "" {
			result.DSN = matches[2]
		}

		// Extract ducklake name (group 3)
		if len(matches) > 3 && matches[3] != "" {
			result.DuckLakeName = matches[3]
		}

		// Extract DATA_PATH from parameters (group 4)
		if len(matches) > 4 && matches[4] != "" {
			dataPathMatches := p.dataPathPattern.FindStringSubmatch(matches[4])
			if len(dataPathMatches) > 1 {
				result.DataPath = dataPathMatches[1]
			}
		}
	}

	return result
}

// FindDuckLakeOccurrences finds all DuckLake occurrences in a multi-line string
func (p *DuckLakeParser) FindDuckLakeOccurrences(text string) []DuckLakeOccurrence {
	if text == "" {
		return []DuckLakeOccurrence{}
	}

	var occurrences []DuckLakeOccurrence
	matches := p.scanPattern.FindAllStringSubmatch(text, -1)

	for _, match := range matches {
		if len(match) > 2 {
			// Extract components
			hasAttach := match[1] != ""
			fullDSN := match[2]
			duckLakeName := ""
			if len(match) > 3 {
				duckLakeName = match[3]
			}
			parameters := ""
			if len(match) > 4 {
				parameters = match[4]
			}

			// Extract DSN (remove ducklake: prefix)
			dsn := fullDSN
			if strings.HasPrefix(fullDSN, "ducklake:") {
				dsn = fullDSN[9:]
			}

			// Extract DATA_PATH if present
			dataPath := ""
			if parameters != "" {
				dataPathMatch := p.dataPathPattern.FindStringSubmatch(parameters)
				if len(dataPathMatch) > 1 {
					dataPath = dataPathMatch[1]
				}
			}

			// Create occurrence record
			occurrence := DuckLakeOccurrence{
				DuckLakeString: strings.TrimSpace(match[0]),
				HasAttach:      hasAttach,
				DSN:            dsn,
				DuckLakeName:   duckLakeName,
				DataPath:       dataPath,
			}

			occurrences = append(occurrences, occurrence)
		}
	}

	return occurrences
}

// FindDuckLakeStrings finds all DuckLake strings in a multi-line text (simple version)
func (p *DuckLakeParser) FindDuckLakeStrings(text string) []string {
	occurrences := p.FindDuckLakeOccurrences(text)
	var strings []string

	for _, occ := range occurrences {
		strings = append(strings, occ.DuckLakeString)
	}

	return strings
}

// FindDuckLakeDSNs finds all unique DSNs in a multi-line text
func (p *DuckLakeParser) FindDuckLakeDSNs(text string) []string {
	occurrences := p.FindDuckLakeOccurrences(text)
	dsnMap := make(map[string]bool)
	var dsns []string

	for _, occ := range occurrences {
		if occ.DSN != "" && !dsnMap[occ.DSN] {
			dsnMap[occ.DSN] = true
			dsns = append(dsns, occ.DSN)
		}
	}

	return dsns
}

// IsDuckLakeFormat quickly checks if a string is in ducklake format
func (p *DuckLakeParser) IsDuckLakeFormat(input string) bool {
	return p.Parse(input).IsDuckLake
}

// ExtractDSN extracts just the DSN from a ducklake format string
func (p *DuckLakeParser) ExtractDSN(input string) string {
	return p.Parse(input).DSN
}

// ExtractDuckLakeName extracts just the ducklake name from a ducklake format string
func (p *DuckLakeParser) ExtractDuckLakeName(input string) string {
	return p.Parse(input).DuckLakeName
}

// ExtractDataPath extracts just the DATA_PATH value from a ducklake format string
func (p *DuckLakeParser) ExtractDataPath(input string) string {
	return p.Parse(input).DataPath
}

// HasAttachKeyword checks if the string contains the ATTACH keyword
func (p *DuckLakeParser) HasAttachKeyword(input string) bool {
	return p.Parse(input).HasAttach
}

// ParseDuckLakeString is a convenience function to parse a ducklake string
func ParseDuckLakeString(input string) DuckLakeParseResult {
	parser := NewDuckLakeParser()
	return parser.Parse(input)
}

// FindDuckLakeOccurrences is a convenience function to find all DuckLake occurrences in a multi-line string
func FindDuckLakeOccurrences(text string) []DuckLakeOccurrence {
	parser := NewDuckLakeParser()
	return parser.FindDuckLakeOccurrences(text)
}

// FindDuckLakeStrings is a convenience function to find all DuckLake strings in a multi-line text
func FindDuckLakeStrings(text string) []string {
	parser := NewDuckLakeParser()
	return parser.FindDuckLakeStrings(text)
}

// FindDuckLakeDSNs is a convenience function to find all unique DSNs in a multi-line text
func FindDuckLakeDSNs(text string) []string {
	parser := NewDuckLakeParser()
	return parser.FindDuckLakeDSNs(text)
}
