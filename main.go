package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/xuri/excelize/v2"
)

func writeDataToSheet(data []map[string]any, sheetName, fileName, startColumn string) error {
	// Create a new Excel file
	f := excelize.NewFile()

	// Ensure the sheet exists
	if sheetName != "Sheet1" {
		index, _ := f.NewSheet(sheetName)
		f.SetActiveSheet(index)
	}

	// Check if data is empty
	if len(data) == 0 {
		return fmt.Errorf("data is empty")
	}

	// Convert the start column to an integer offset
	startColIndex := int(strings.ToUpper(startColumn)[0] - 'A')

	// Write headers (keys of the first map)
	headers := make([]string, 0, len(data[0]))
	for key := range data[0] {
		headers = append(headers, key)
	}
	for colIdx, header := range headers {
		cell := fmt.Sprintf("%s1", string(rune('A'+startColIndex+colIdx)))
		if err := f.SetCellValue(sheetName, cell, header); err != nil {
			return err
		}
	}

	// Write data rows
	for rowIdx, row := range data {
		for colIdx, header := range headers {
			cell := fmt.Sprintf("%s%d", string(rune('A'+startColIndex+colIdx)), rowIdx+2)
			if value, exists := row[header]; exists {
				if err := f.SetCellValue(sheetName, cell, value); err != nil {
					return err
				}
			}
		}
	}

	// Save the Excel file
	if err := f.SaveAs(fileName); err != nil {
		return err
	}

	return nil
}

func main() {
	// Example data
	data := []map[string]any{
		{"Name": "Alice", "Age": 30, "Country": "USA"},
		{"Name": "Bob", "Age": 25, "Country": "Canada"},
		{"Name": "Charlie", "Age": 35, "Country": "UK"},
	}

	// File and sheet details
	fileName := "output.xlsx"
	sheetName := "People"
	startColumn := "B" // Specify the starting column

	// Write data to the sheet
	if err := writeDataToSheet(data, sheetName, fileName, startColumn); err != nil {
		log.Fatalf("Failed to write data to sheet: %v", err)
	}

	fmt.Printf("Data successfully written to %s starting at column %s\n", fileName, startColumn)
}
