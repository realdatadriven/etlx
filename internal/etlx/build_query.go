package etlx

import (
	"fmt"
)

// Builds query from the query doc MD config
//
// Input:
//
//	-key: Represnting the markdown Level 1 Heading where the query begins
//
// Output:
//
//	-sql: The SQL query generated
//	-query_parts: The query parts parsed from the md config input
//	-field_orders: The order of the fields in the parts
//	-error: Error returned in case something goes wrong
func (etlx *ETLX) QueryBuilder(keys ...string) (string, map[string]any, []string, error) { // dateRef []time.Time, extraConf map[string]any,
	key := "QUERY_DOC"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	data, ok := etlx.Config[key].(map[string]any)
	if !ok {
		return "", nil, nil, fmt.Errorf("missing or invalid %s section", key)
	}
	//fmt.Println(data)
	/*/ Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return "", nil, nil, fmt.Errorf("missing metadata in %s section", key)
	}*/
	//fmt.Println(key, metadata["description"])
	// Extract metadata
	fields, ok := data["FIELDS"].(map[string]any)
	if !ok {
		return "", nil, nil, fmt.Errorf("missing metadata in %s section", key)
	}
	query_parts := map[string]interface{}{}
	_fields_order := []string{}
	for key2, value := range fields {
		if key2 == "metadata" {
			continue
		}
		_field := value.(map[string]any)
		field_metadata, ok := _field["metadata"].(map[string]any)
		if !ok {
			return "", nil, nil, fmt.Errorf("missing metadata in %s section", key)
		}
		_fields_order = append(_fields_order, field_metadata["name"].(string))
		query_parts[field_metadata["name"].(string)] = map[string]any{
			"name":     field_metadata["name"],
			"desc":     field_metadata["description"],
			"cte":      _field["cte"],
			"select":   _field["select"],
			"from":     _field["from"],
			"join":     _field["join"],
			"where":    _field["where"],
			"group_by": _field["group_by"],
			"order_by": _field["order_by"],
			"having":   _field["having"],
			"window":   _field["window"],
			"active":   _field["active"],
			"key":      key,
			"metadata": field_metadata,
		}
	}
	qd := QueryDoc{
		QueryParts:  make(map[string]Field),
		FieldOrders: _fields_order,
	}
	err := qd.SetQueryPartsFromMap(query_parts)
	if err != nil {
		return "", nil, nil, fmt.Errorf("error setting field: %s", err)
	}
	_sql := qd.GetQuerySQLFromMap()
	//_sql = app.setQueryDate(_sql, date_ref)
	//fmt.Println("SQL", _sql)
	return _sql, query_parts, _fields_order, nil
}
