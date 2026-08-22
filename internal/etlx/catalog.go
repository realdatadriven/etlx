package etlxlib

import (
	"fmt"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

// RunCATALOG imports the metadata of a normal ETLX pipeline into a database
// created from examples/catalog_model.md.  It deliberately does not execute
// the pipeline: a level-two section becomes a catalog asset, its schema (or
// "main" when none is declared), and a data asset.
//
// A CATALOG section needs catalog_connection (or catalog_conn) in its level-one
// metadata.  Governance names may be supplied on that metadata and inherited
// by its level-two items: stakeholder(s), business_unit, domain, subdomain and
// glossary_term(s).  Existing names are resolved to IDs; unknown names point
// to the respective, lazily-created "undefined" record.
func (etlx *ETLX) RunCATALOG(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "CATALOG"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	if conf == nil {
		conf = etlx.Config
	}
	section, ok := conf[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing or invalid %s section", key)
	}
	metadata, ok := section["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing metadata in %s section", key)
	}
	if active, ok := metadata["active"].(bool); ok && !active {
		return []map[string]any{{"process": "CATALOG", "key": key, "success": true, "msg": "Deactivated"}}, nil
	}
	conn := catalogString(metadata, "catalog_connection", "catalog_conn", "connection", "conn")
	if conn == "" {
		return nil, fmt.Errorf("%s requires metadata.catalog_connection (or catalog_conn)", key)
	}
	catalogDB, err := etlx.GetDB(etlx.ReplaceEnvVariable(conn))
	if err != nil {
		return nil, fmt.Errorf("connect catalog database: %w", err)
	}
	defer catalogDB.Close()

	loader := &catalogLoader{etlx: etlx, db: catalogDB, names: map[string]map[string]any{}}
	order := catalogOrder(section)
	logs := make([]map[string]any, 0, len(order)+1)
	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		item, ok := section[itemKey].(map[string]any)
		if !ok {
			continue
		}
		itemMetadata, ok := item["metadata"].(map[string]any)
		if !ok {
			return logs, fmt.Errorf("%s.%s is missing metadata", key, itemKey)
		}
		if active, ok := itemMetadata["active"].(bool); ok && !active {
			continue
		}
		start := time.Now().In(etlx.TimeZone)
		if err := loader.loadAsset(key, itemKey, metadata, item, itemMetadata); err != nil {
			return logs, fmt.Errorf("catalog %s.%s: %w", key, itemKey, err)
		}
		logs = append(logs, map[string]any{
			"process": "CATALOG", "name": key + "->" + itemKey, "key": key,
			"item_key": itemKey, "start_at": start, "end_at": time.Now().In(etlx.TimeZone),
			"duration": time.Since(start).Seconds(), "success": true,
			"msg": "catalog asset registered",
		})
	}
	return logs, nil
}

type catalogLoader struct {
	etlx  *ETLX
	db    db.DBInterface
	names map[string]map[string]any // table -> source name -> resolved ID
}

func (l *catalogLoader) loadAsset(sectionKey, itemKey string, parent, item map[string]any, itemMetadata map[string]any) error {
	governance := catalogMerged(parent, itemMetadata)
	businessOwner, err := l.resolveStakeholder(catalogString(governance, "business_owner", "owner", "stakeholder"))
	if err != nil {
		return err
	}
	technicalOwner, err := l.resolveStakeholder(catalogString(governance, "technical_owner", "owner", "stakeholder"))
	if err != nil {
		return err
	}
	businessUnit, err := l.resolveBusinessUnit(catalogString(governance, "business_unit", "businessUnit"))
	if err != nil {
		return err
	}
	domain, err := l.resolveDomain(catalogString(governance, "domain"), businessUnit, businessOwner)
	if err != nil {
		return err
	}
	subdomain, err := l.resolveSubdomain(catalogString(governance, "subdomain"), domain)
	if err != nil {
		return err
	}

	name := catalogString(itemMetadata, "name")
	if name == "" {
		name = itemKey
	}
	assetType := catalogString(itemMetadata, "catalog_asset_type", "asset_type")
	if assetType == "" {
		if isQuery, _ := itemMetadata["is_query"].(bool); isQuery {
			assetType = "Query"
		} else {
			assetType = "Pipeline"
		}
	}
	assetID, err := l.upsert("catalog_assets", "name = :name", map[string]any{
		"name": name, "asset_type": assetType, "description": catalogString(itemMetadata, "description"),
		"domain_id": domain, "subdomain_id": subdomain, "business_owner_id": businessOwner,
		"technical_owner_id": technicalOwner, "layer_path": sectionKey + "." + itemKey,
		"orchestrator_type": catalogString(parent, "orchestrator_type", "orchestrator"),
		"schedule_cron":     catalogString(itemMetadata, "schedule_cron", "cron"),
		"code_repo_url":     catalogString(parent, "code_repo_url", "repo_url"),
	})
	if err != nil {
		return err
	}
	schemaName := catalogString(itemMetadata, "schema")
	if schemaName == "" {
		schemaName = "main"
	}
	schemaID, err := l.upsert("asset_schemas", "catalog_asset_id = :catalog_asset_id AND name = :name", map[string]any{
		"catalog_asset_id": assetID, "name": schemaName, "description": catalogString(itemMetadata, "schema_description"),
	})
	if err != nil {
		return err
	}
	dataAssetID, err := l.upsert("data_assets", "schema_id = :schema_id AND name = :name", map[string]any{
		"schema_id": schemaID, "name": name, "asset_type": catalogStringOr(itemMetadata, "Table", "data_asset_type", "asset_type"),
		"description": catalogString(itemMetadata, "description"), "domain_id": domain, "subdomain_id": subdomain,
		"business_owner_id": businessOwner, "technical_owner_id": technicalOwner,
	})
	if err != nil {
		return err
	}
	if err := l.mapTerms(dataAssetID, catalogStrings(governance, "glossary_terms", "glossary_term", "terms"), domain, businessOwner); err != nil {
		return err
	}
	return l.loadFields(item, dataAssetID, domain, businessOwner, technicalOwner)
}

func (l *catalogLoader) loadFields(item map[string]any, dataAssetID any, domain, businessOwner, technicalOwner any) error {
	for _, fieldKey := range catalogOrder(item) {
		if fieldKey == "metadata" || fieldKey == "__order" || fieldKey == "order" {
			continue
		}
		field, ok := item[fieldKey].(map[string]any)
		if !ok {
			continue
		}
		metadata, ok := field["metadata"].(map[string]any)
		if !ok {
			continue
		}
		name := catalogString(metadata, "name")
		if name == "" {
			name = fieldKey
		}
		dataType := catalogString(metadata, "data_type", "type")
		if dataType == "" {
			dataType = "unknown"
		}
		fieldID, err := l.upsert("asset_fields", "asset_id = :asset_id AND name = :name", map[string]any{
			"asset_id": dataAssetID, "name": name, "data_type": dataType,
			"description": catalogString(metadata, "description"), "business_owner_id": businessOwner,
			"technical_owner_id": technicalOwner, "is_nullable": catalogBool(metadata, "nullable", true),
			"is_primary_key": catalogBool(metadata, "primary_key", false), "is_foreign_key": catalogBool(metadata, "foreign_key", false),
		})
		if err != nil {
			return err
		}
		if err := l.mapFieldTerms(fieldID, catalogStrings(metadata, "glossary_terms", "glossary_term", "terms"), domain, businessOwner); err != nil {
			return err
		}
	}
	return nil
}

func (l *catalogLoader) mapTerms(assetID any, terms []string, domain, steward any) error {
	for _, term := range terms {
		termID, err := l.resolveTerm(term, domain, steward)
		if err != nil {
			return err
		}
		if _, err = l.upsert("asset_term_mappings", "asset_id = :asset_id AND term_id = :term_id", map[string]any{"asset_id": assetID, "term_id": termID}); err != nil {
			return err
		}
	}
	return nil
}

func (l *catalogLoader) mapFieldTerms(fieldID any, terms []string, domain, steward any) error {
	for _, term := range terms {
		termID, err := l.resolveTerm(term, domain, steward)
		if err != nil {
			return err
		}
		if _, err = l.upsert("field_term_mappings", "field_id = :field_id AND term_id = :term_id", map[string]any{"field_id": fieldID, "term_id": termID}); err != nil {
			return err
		}
	}
	return nil
}

func (l *catalogLoader) resolveStakeholder(name string) (any, error) {
	return l.resolve("stakeholders", name, "stakeholder_id", "full_name", map[string]any{"email": "undefined@etlx.local", "full_name": "undefined"})
}
func (l *catalogLoader) resolveBusinessUnit(name string) (any, error) {
	return l.resolve("business_units", name, "business_unit_id", "name", map[string]any{"name": "undefined", "description": "Fallback ETLX catalog business unit"})
}
func (l *catalogLoader) resolveDomain(name string, businessUnit, lead any) (any, error) {
	return l.resolve("domains", name, "domain_id", "name", map[string]any{"name": "undefined", "description": "Fallback ETLX catalog domain", "business_unit_id": businessUnit, "domain_lead_id": lead})
}
func (l *catalogLoader) resolveSubdomain(name string, domain any) (any, error) {
	return l.resolve("subdomains", name, "subdomain_id", "name", map[string]any{"name": "undefined", "description": "Fallback ETLX catalog subdomain", "domain_id": domain})
}
func (l *catalogLoader) resolveTerm(name string, domain, steward any) (any, error) {
	return l.resolve("glossary_terms", name, "term_id", "term_name", map[string]any{"term_name": "undefined", "definition": "Fallback ETLX catalog glossary term", "domain_id": domain, "steward_id": steward})
}

// resolve never invents a supplied governance entity.  A found entity is used;
// otherwise the supplied name maps to the catalog's explicit undefined record.
func (l *catalogLoader) resolve(table, name, idColumn, nameColumn string, undefined map[string]any) (any, error) {
	if l.names[table] == nil {
		l.names[table] = map[string]any{}
	}
	cacheKey := name
	if cacheKey == "" {
		cacheKey = "undefined"
	}
	if id, ok := l.names[table][cacheKey]; ok {
		return id, nil
	}
	if name != "" {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? LIMIT 1", l.column(idColumn), l.table(table), l.column(nameColumn))
		row, _, err := l.db.QuerySingleRow(query, name)
		if err != nil {
			return nil, fmt.Errorf("find %s %q: %w", table, name, err)
		}
		if id, ok := (*row)[idColumn]; ok {
			l.names[table][cacheKey] = id
			return id, nil
		}
	}
	id, err := l.upsert(table, fmt.Sprintf("where %s = :%s", nameColumn, nameColumn), undefined)
	if err != nil {
		return nil, err
	}
	l.names[table][cacheKey] = id
	return id, nil
}

func (l *catalogLoader) upsert(table, condition string, data map[string]any) (any, error) {
	l.defaults(data)
	if _, err := l.etlx.InsertOrUpdate(l.db, table, condition, data); err != nil {
		return nil, err
	}
	primaryKey, ok := catalogPrimaryKeys[table]
	if !ok {
		return nil, fmt.Errorf("no primary key configured for %s", table)
	}
	query, args, err := l.etlx.NamedToPositional(fmt.Sprintf("SELECT %s FROM %s WHERE %s LIMIT 1", l.column(primaryKey), l.table(table), condition), data)
	if err != nil {
		return nil, err
	}
	row, _, err := l.db.QuerySingleRow(query, args...)
	if err != nil {
		return nil, err
	}
	id, ok := (*row)[primaryKey]
	if !ok {
		return nil, fmt.Errorf("upsert %s did not return %s", table, primaryKey)
	}
	return id, nil
}

func (l *catalogLoader) defaults(data map[string]any) {
	if _, ok := data["user_id"]; !ok {
		data["user_id"] = 1
	}
	if _, ok := data["excluded"]; !ok {
		data["excluded"] = false
	}
	now := time.Now().In(l.etlx.TimeZone)
	if _, ok := data["created_at"]; !ok {
		data["created_at"] = now
	}
	data["updated_at"] = now
}
func (l *catalogLoader) table(name string) string {
	return GetDialect(l.db.GetDriverName()).GetTableName(name)
}
func (l *catalogLoader) column(name string) string {
	return GetDialect(l.db.GetDriverName()).GetColumnName(name)
}

var catalogPrimaryKeys = map[string]string{"stakeholders": "stakeholder_id", "business_units": "business_unit_id", "domains": "domain_id", "subdomains": "subdomain_id", "glossary_terms": "term_id", "catalog_assets": "asset_id", "asset_schemas": "schema_id", "data_assets": "asset_id", "asset_fields": "field_id", "asset_term_mappings": "asset_id", "field_term_mappings": "field_id"}

func catalogMerged(parent, child map[string]any) map[string]any {
	result := map[string]any{}
	for k, v := range parent {
		result[k] = v
	}
	for k, v := range child {
		result[k] = v
	}
	return result
}
func catalogString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func catalogStringOr(data map[string]any, fallback string, keys ...string) string {
	if value := catalogString(data, keys...); value != "" {
		return value
	}
	return fallback
}
func catalogBool(data map[string]any, key string, fallback bool) bool {
	if value, ok := data[key].(bool); ok {
		return value
	}
	return fallback
}
func catalogStrings(data map[string]any, keys ...string) []string {
	for _, key := range keys {
		switch values := data[key].(type) {
		case string:
			if value := strings.TrimSpace(values); value != "" {
				return []string{value}
			}
		case []any:
			result := []string{}
			for _, value := range values {
				if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
					result = append(result, strings.TrimSpace(text))
				}
			}
			return result
		case []string:
			return values
		}
	}
	return nil
}
func catalogOrder(data map[string]any) []string {
	if items, ok := data["__order"].([]any); ok {
		result := make([]string, 0, len(items))
		for _, item := range items {
			if key, ok := item.(string); ok {
				result = append(result, key)
			}
		}
		return result
	}
	result := []string{}
	for key := range data {
		result = append(result, key)
	}
	return result
}
