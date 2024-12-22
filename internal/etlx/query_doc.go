package etlx

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Field represents a query field or variable
type Field struct {
	Name    string
	Desc    string
	CTE     string
	Select  string
	From    string
	Join    string
	Where   string
	GroupBy string
	OrderBy string
	Having  string
	Window  string
	Extras  map[string]string
	Active  bool
}

// GetMap returns the field in the map format
func (f *Field) GetMap() map[string]string {
	fieldMap := map[string]string{
		"name":     f.Name,
		"desc":     f.Desc,
		"cte":      f.CTE,
		"select":   f.Select,
		"from":     f.From,
		"join":     f.Join,
		"where":    f.Where,
		"group_by": f.GroupBy,
		"order_by": f.OrderBy,
		"having":   f.Having,
		"window":   f.Window,
	}
	for k, v := range f.Extras {
		fieldMap[k] = v
	}
	return fieldMap
}

// QueryDoc is the main struct that manages query parts
type QueryDoc struct {
	QueryParts  map[string]Field
	FieldOrders []string
}

// NewQueryDoc initializes a new QueryDoc
func NewQueryDoc() *QueryDoc {
	return &QueryDoc{
		QueryParts: make(map[string]Field),
	}
}

// AddField adds a field to the main query parts map
func (q *QueryDoc) AddField(f Field) {
	if f.Name == "" {
		fmt.Println("Field.Name is required")
		return
	}
	if f.Select == "" {
		fmt.Println("Field.Select is required")
		return
	}
	q.QueryParts[f.Name] = f
}

// RemoveField removes a field by name from the query parts
func (q *QueryDoc) RemoveField(name string) {
	delete(q.QueryParts, name)
}

// GetQueryParts returns the query parts as a map
func (q *QueryDoc) GetQueryParts() map[string]Field {
	return q.QueryParts
}

func (q *QueryDoc) GetQueryPartsAsMap() map[string]map[string]interface{} {
	result := map[string]map[string]interface{}{}
	for key, f := range q.QueryParts {
		// Assuming `Field` has some properties, let's say Name and Value
		result[key] = map[string]interface{}{
			"name":     f.Name,
			"desc":     f.Desc,
			"cte":      f.CTE,
			"select":   f.Select,
			"from":     f.From,
			"join":     f.Join,
			"where":    f.Where,
			"group_by": f.GroupBy,
			"order_by": f.OrderBy,
			"having":   f.Having,
			"window":   f.Window,
		}
	}
	return result
}

// cleanSelectPart processes the Select string:
// - Removes newlines
// - Removes "AS FieldName"
// - Removes leading commas
func cleanSelectPart(selectClause, fieldName string) string {
	// Remove newlines and trim the string
	selectClause = strings.ReplaceAll(selectClause, "\n", " ")
	selectClause = strings.TrimSpace(selectClause)
	// Replace the matched "SELECT" with an empty string
	re := regexp.MustCompile(`(?i)^\s*SELECT\s+`)
	selectClause = re.ReplaceAllString(selectClause, "")
	// Remove "AS FieldName" part (case-insensitive)
	asPattern := regexp.MustCompile(`(?i)\s+AS\s+"?` + fieldName + `"?`)
	selectClause = asPattern.ReplaceAllString(selectClause, "")
	// Trim spaces again after removing the AS part
	selectClause = strings.TrimSpace(selectClause)
	// Remove any leading commas
	selectClause = strings.TrimPrefix(selectClause, ",")
	return strings.TrimSpace(selectClause)
}

// replaceFieldPlaceholders replaces @Field placeholders with the corresponding field's Select part
func (q *QueryDoc) replaceFieldPlaceholders(sqlClause string, queryParts map[string]Field) string {
	// Define the regex to find @FieldName pattern
	regex := regexp.MustCompile(`@\w+`)
	// Find all matches in the clause
	matches := regex.FindAllString(sqlClause, -1)
	//fmt.Println(matches)
	// Loop over all matches and replace them with the corresponding field's Select part
	for _, match := range matches {
		// Remove the '@' sign to get the field name
		// fmt.Println(match)
		fieldName := match[1:]
		// Check if the field exists in QueryParts
		if field, ok := queryParts[fieldName]; ok {
			// Replace the @FieldName with the corresponding field's Select part
			_select := cleanSelectPart(field.Select, fieldName)
			_select = fmt.Sprintf(`(%s)`, _select)
			//re := regexp.MustCompile(fmt.Sprintf(`\b@%s\b`, fieldName))
			re := regexp.MustCompile(fmt.Sprintf(`[@]%s(\b|::\w+|\(\))?`, fieldName))
			sqlClause = re.ReplaceAllString(sqlClause, _select)
			// field.Select = sqlClause
			queryParts[fieldName] = field // Ensure to update the map with the modified field
		}
	}
	return sqlClause
}

// replaceFieldPlaceholders replaces @Field placeholders with the corresponding field's Select part
func (q *QueryDoc) replaceFieldPlaceholdersMap(sqlClause string, queryParts *map[string]map[string]interface{}) string {
	// Define the regex to find @FieldName pattern
	regex := regexp.MustCompile(`@\w+`)
	// Find all matches in the clause
	matches := regex.FindAllString(sqlClause, -1)
	//fmt.Println(matches)
	// Loop over all matches and replace them with the corresponding field's Select part
	for _, match := range matches {
		// Remove the '@' sign to get the field name
		// fmt.Println(match)
		fieldName := match[1:]
		// Check if the field exists in QueryParts
		if field, ok := (*queryParts)[fieldName]; ok {
			// Replace the @FieldName with the corresponding field's Select part
			_select := cleanSelectPart(field["select"].(string), fieldName)
			_select = fmt.Sprintf(`(%s)`, _select)
			//re := regexp.MustCompile(fmt.Sprintf(`\b@%s\b`, fieldName))
			re := regexp.MustCompile(fmt.Sprintf(`[@]%s(\b|::\w+|\(\))?`, fieldName))
			sqlClause = re.ReplaceAllString(sqlClause, _select)
		}
	}
	return sqlClause
}

// GetQuerySQL generates the SQL query string from the query parts
func (q *QueryDoc) GetQuerySQL() string {
	var queryParts = q.GetQueryParts()
	var query Field
	// Compile the final SQL from different parts
	for _, f := range q.FieldOrders {
		field := queryParts[f]
		if !field.Active {
			continue
		}
		// For each clause, replace @FieldName with corresponding field's Select part
		field.CTE = q.replaceFieldPlaceholders(field.CTE, queryParts)
		field.Select = q.replaceFieldPlaceholders(field.Select, queryParts)
		field.From = q.replaceFieldPlaceholders(field.From, queryParts)
		field.Join = q.replaceFieldPlaceholders(field.Join, queryParts)
		field.Where = q.replaceFieldPlaceholders(field.Where, queryParts)
		field.GroupBy = q.replaceFieldPlaceholders(field.GroupBy, queryParts)
		field.OrderBy = q.replaceFieldPlaceholders(field.OrderBy, queryParts)
		field.Having = q.replaceFieldPlaceholders(field.Having, queryParts)
		field.Window = q.replaceFieldPlaceholders(field.Window, queryParts)
		// Concat all the string parts in a sigle sql string
		query.CTE += field.CTE
		query.Select += field.Select
		query.From += field.From
		query.Join += field.Join
		query.Where += field.Where
		query.GroupBy += field.GroupBy
		query.OrderBy += field.OrderBy
		query.Having += field.Having
		query.Window += field.Window
	}
	return fmt.Sprintf("%s%s%s%s%s%s%s%s%s",
		query.CTE, query.Select, query.From, query.Join, query.Where,
		query.GroupBy, query.Window, query.OrderBy, query.Having)
}

func (q *QueryDoc) GetQuerySQLFromMap() string {
	var queryParts = q.GetQueryPartsAsMap()
	var query Field
	// fmt.Println(queryParts, q.FieldOrders)
	// Compile the final SQL from different parts
	for _, f := range q.FieldOrders {
		field := queryParts[f]
		// fmt.Println(field)
		if _, ok := field["active"].(bool); ok {
			if !field["active"].(bool) {
				continue
			}
		}
		// For each clause, replace @FieldName with corresponding field's Select part
		queryParts[f]["cte"] = q.replaceFieldPlaceholdersMap(field["cte"].(string), &queryParts)
		queryParts[f]["select"] = q.replaceFieldPlaceholdersMap(field["select"].(string), &queryParts)
		queryParts[f]["from"] = q.replaceFieldPlaceholdersMap(field["from"].(string), &queryParts)
		queryParts[f]["join"] = q.replaceFieldPlaceholdersMap(field["join"].(string), &queryParts)
		queryParts[f]["where"] = q.replaceFieldPlaceholdersMap(field["where"].(string), &queryParts)
		queryParts[f]["group_by"] = q.replaceFieldPlaceholdersMap(field["group_by"].(string), &queryParts)
		queryParts[f]["order_by"] = q.replaceFieldPlaceholdersMap(field["order_by"].(string), &queryParts)
		queryParts[f]["having"] = q.replaceFieldPlaceholdersMap(field["having"].(string), &queryParts)
		queryParts[f]["window"] = q.replaceFieldPlaceholdersMap(field["window"].(string), &queryParts)
		// Concat all the string parts in a sigle sql string
		query.CTE += queryParts[f]["cte"].(string)
		query.Select += queryParts[f]["select"].(string)
		query.From += queryParts[f]["from"].(string)
		query.Join += queryParts[f]["join"].(string)
		query.Where += queryParts[f]["where"].(string)
		query.GroupBy += queryParts[f]["group_by"].(string)
		query.OrderBy += queryParts[f]["order_by"].(string)
		query.Having += queryParts[f]["having"].(string)
		query.Window += queryParts[f]["window"].(string)
	}
	return fmt.Sprintf("%s%s%s%s%s%s%s%s%s", query.CTE, query.Select, query.From, query.Join, query.Where, query.GroupBy, query.Window, query.OrderBy, query.Having)
}

// SetMap takes a map[string]interface{} and sets the corresponding fields in the Field struct
func (f *Field) SetMap(fieldMap map[string]interface{}) error {
	for key, value := range fieldMap {
		switch key {
		case "name":
			if strVal, ok := value.(string); ok {
				f.Name = strVal
			} else {
				return fmt.Errorf("invalid type for 'name', expected string")
			}
		case "desc":
			if strVal, ok := value.(string); ok {
				f.Desc = strVal
			} else {
				f.Desc = ""
			}
		case "description":
			if strVal, ok := value.(string); ok {
				f.Desc = strVal
			} else {
				f.Desc = ""
			}
		case "cte":
			if strVal, ok := value.(string); ok {
				f.CTE = strVal
			} else {
				f.CTE = ""
			}
		case "select":
			if strVal, ok := value.(string); ok {
				f.Select = strVal
			} else {
				return fmt.Errorf("invalid type for 'select', expected string")
			}
		case "from":
			if strVal, ok := value.(string); ok {
				f.From = strVal
			} else {
				f.From = ""
			}
		case "join":
			if strVal, ok := value.(string); ok {
				f.Join = strVal
			} else {
				f.Join = ""
			}
		case "where":
			if strVal, ok := value.(string); ok {
				f.Where = strVal
			} else {
				f.Where = ""
			}
		case "group_by":
			if strVal, ok := value.(string); ok {
				f.GroupBy = strVal
			} else {
				f.GroupBy = ""
			}
		case "order_by":
			if strVal, ok := value.(string); ok {
				f.OrderBy = strVal
			} else {
				f.OrderBy = ""
			}
		case "having":
			if strVal, ok := value.(string); ok {
				f.Having = strVal
			} else {
				f.Having = ""
			}
		case "window":
			if strVal, ok := value.(string); ok {
				f.Window = strVal
			} else {
				f.Window = ""
			}
		case "active":
			f.Active = true
			if boolVal, ok := value.(bool); ok {
				f.Active = boolVal
			} else if _, ok := value.(int); ok {
				if value.(int) == 1 {
					f.Active = true
				} else {
					f.Active = false
				}
			} else {
				f.Active = false
			}
		default:
			// If the key doesn't match any of the predefined fields, put it in Extras
		}
	}
	return nil
}

// SetQueryPartsFromMap sets the QueryParts of QueryDoc using a map of field names to field properties
func (q *QueryDoc) SetQueryPartsFromMap(fieldMap map[string]interface{}) error {
	for fieldName, fieldData := range fieldMap {
		//fmt.Println(fieldName, fieldData.(map[string]interface{}))
		var field Field
		err := field.SetMap(fieldData.(map[string]interface{}))
		if err != nil {
			return err
		}
		//fmt.Println(fieldName, field)
		q.QueryParts[fieldName] = field
	}
	return nil
}

// ReplacePlaceholders replaces placeholders in the SQL query with actual values
func (q *QueryDoc) ReplacePlaceholders(sql string, replacements map[string]string) string {
	for placeholder, value := range replacements {
		re := regexp.MustCompile(fmt.Sprintf(`@%s`, placeholder))
		sql = re.ReplaceAllString(sql, value)
	}
	return sql
}

func getDtFmrt(format string) string {
	go_fmrt := format
	formats := []struct {
		frmt    string
		go_fmrt string
	}{
		{`YYYY|AAAA`, "2006"},
		{`YY|AA`, "06"},
		{`MM`, "01"},
		{`DD`, "02"},
	}
	for _, f := range formats {
		re := regexp.MustCompile(f.frmt)
		go_fmrt = re.ReplaceAllString(go_fmrt, f.go_fmrt)
	}
	return go_fmrt
}

// setQueryDate formats the query string by inserting the given date reference in place of placeholders
func (q *QueryDoc) setQueryDate(query string, dateRef interface{}) string {
	patt := regexp.MustCompile(`(["]?\w+["]?\.\w+\s?=\s?'\{.*?\}'|["]?\w+["]?\s?=\s?'\{.*?\}')`)
	matches := patt.FindAllString(query, -1)
	if len(matches) == 0 {
		patt = regexp.MustCompile(`["]?\w+["]?\s?=\s?'\{.*?\}'`)
		matches = patt.FindAllString(query, -1)
	}
	if len(matches) > 0 {
		patt2 := regexp.MustCompile(`'\{.*?\}'`)
		for _, m := range matches {
			format := patt2.FindString(m)
			if format != "" {
				frmtFinal := getDtFmrt(format)
				frmtFinal = strings.ReplaceAll(frmtFinal, "{", "")
				frmtFinal = strings.ReplaceAll(frmtFinal, "}", "")
				var procc string
				if dates, ok := dateRef.([]time.Time); ok {
					dts := []string{}
					for _, dt := range dates {
						dts = append(dts, dt.Format(frmtFinal))
					}
					procc = regexp.MustCompile(patt2.String()).ReplaceAllString(m, fmt.Sprintf("(%s)", strings.Join(dts, ",")))
					patt3 := regexp.MustCompile(`\s?=\s?`)
					procc = patt3.ReplaceAllString(procc, " IN ")
				} else if dt, ok := dateRef.(time.Time); ok {
					procc = regexp.MustCompile(patt2.String()).ReplaceAllString(m, dt.Format(frmtFinal))
				}
				patt = regexp.MustCompile(regexp.QuoteMeta(m))
				query = patt.ReplaceAllString(query, procc)
			}
		}
	}
	// Replace remaining date placeholders
	patt = regexp.MustCompile(`'?\{.*?\}'?`)
	matches = patt.FindAllString(query, -1)
	if len(matches) > 0 {
		for _, m := range matches {
			frmtFinal := getDtFmrt(m)
			frmtFinal = strings.ReplaceAll(frmtFinal, "{", "")
			frmtFinal = strings.ReplaceAll(frmtFinal, "}", "")
			var procc string
			if dates, ok := dateRef.([]time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dates[0].Format(frmtFinal))
			} else if dt, ok := dateRef.(time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dt.Format(frmtFinal))
			}
			patt = regexp.MustCompile(regexp.QuoteMeta(m))
			query = patt.ReplaceAllString(query, procc)
		}
	}
	// Handle cases for temporary tables with date extensions
	patt = regexp.MustCompile(
		`YYYY.?MM.?DD|AAAA.?MM.?DD|YY.?MM.?DD|AA.?MM.?DD|YYYY.?MM|AAAA.?MM|YY.?MM|AA.?MM|MM.?DD|DD.?MM.?YYYY|DD.?MM.?AAAA|DD.?MM.?YY|DD.?MM.?AA`,
	)
	matches = patt.FindAllString(query, -1)
	if len(matches) > 0 {
		for _, m := range matches {
			frmtFinal := getDtFmrt(m)
			var procc string
			if dates, ok := dateRef.([]time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dates[0].Format(frmtFinal))
			} else if dt, ok := dateRef.(time.Time); ok {
				procc = regexp.MustCompile(patt.String()).ReplaceAllString(m, dt.Format(frmtFinal))
			}
			patt = regexp.MustCompile(regexp.QuoteMeta(m))
			query = patt.ReplaceAllString(query, procc)
		}
	}
	return query
}
