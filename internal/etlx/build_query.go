package etlxlib

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
func (etlx *ETLX) QueryBuilder(conf map[string]any, keys ...string) (string, map[string]any, []string, error) { // dateRef []time.Time, extraConf map[string]any,
	key := "QUERY_DOC"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return "", nil, nil, fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	fields, ok := data["FIELDS"].(map[string]any)
	if !ok {
		fields = data
	}
	query_parts := map[string]interface{}{}
	_fields_order := []string{}
	for key2, value := range fields {
		if key2 == "metadata" || key2 == "__order" || key2 == "order" {
			continue
		}
		_field := value.(map[string]any)
		field_metadata, ok := _field["metadata"].(map[string]any)
		//fmt.Println(1, field_metadata, len(field_metadata))
		if !ok {
			// return "", nil, nil, fmt.Errorf("missing metadata in query %s and field %s", key, _field)
			field_metadata = map[string]any{
				"name":        key2,
				"description": key2,
			}
		} else if len(field_metadata) == 0 {
			field_metadata = map[string]any{
				"name":        key2,
				"description": key2,
			}
		}
		_fields_order = append(_fields_order, field_metadata["name"].(string))
		active, ok := field_metadata["active"].(bool)
		if !ok {
			active = true
		}
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
			"active":   active,
			"key":      key,
			"metadata": field_metadata,
		}
	}
	__order, ok := data["__order"].([]any)
	//fmt.Printf("%s -> %v, %v, %t", key, ok, data["__order"], data["__order"])
	if ok {
		_fields_order = []string{}
		for _, o := range __order {
			if _, ok := o.(string); ok {
				_field_data, _ok := data[o.(string)].(map[string]any)
				if _ok {
					_metadata, _ok := _field_data["metadata"].(map[string]any)
					if _ok {
						_name, _ok := _metadata["name"].(string)
						if _ok {
							_fields_order = append(_fields_order, _name)
						} else {
							_fields_order = append(_fields_order, o.(string))
						}
					} else {
						_fields_order = append(_fields_order, o.(string))
					}
				} else {
					_fields_order = append(_fields_order, o.(string))
				}
			}
		}
		//fmt.Println("QD ORDER:", _fields_order)
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
