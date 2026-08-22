package etlxlib

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

// RunCATALOG imports every eligible level-one pipeline section into a database
// created from examples/catalog_model.md. It deliberately never executes a
// pipeline. CATALOG is global configuration only: its metadata supplies the
// catalog connection and governance defaults for all imported sections.
//
// A CATALOG section needs catalog_connection (or catalog_conn) in its level-one
// metadata.  Governance names may be supplied on that metadata and inherited
// by its level-two items: stakeholder(s), business_unit, domain, subdomain and
// glossary_term(s).  Existing names are resolved to IDs; unknown names point
// to the respective, lazily-created "undefined" record.
func (etlx *ETLX) RunCATALOG(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "CATALOG"
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
		logs := []map[string]any{}
		catalogAppendLog(&logs, map[string]any{"process": "CATALOG", "key": key, "success": true, "msg": "Deactivated"})
		return logs, nil
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

	logs := []map[string]any{}
	loader := &catalogLoader{etlx: etlx, db: catalogDB, names: map[string]map[string]any{}, logs: &logs, catalogKey: key}
	for _, sectionKey := range catalogOrder(conf) {
		if sectionKey == key || sectionKey == "metadata" || sectionKey == "__order" || sectionKey == "order" {
			continue
		}
		pipeline, ok := conf[sectionKey].(map[string]any)
		if !ok {
			continue
		}
		pipelineMetadata, ok := pipeline["metadata"].(map[string]any)
		if !ok {
			continue
		}
		if !catalogPipelineCandidate(sectionKey, pipelineMetadata) {
			continue
		}
		if active, ok := pipelineMetadata["active"].(bool); ok && !active {
			continue
		}
		sectionStart := time.Now().In(etlx.TimeZone)
		catalogAppendLog(&logs, map[string]any{"process": "CATALOG", "name": key + "->" + sectionKey, "key": key, "item_key": sectionKey, "start_at": sectionStart, "success": true, "msg": "catalog pipeline section started"})
		defaults := catalogMerged(pipelineMetadata, metadata)
		itemCount := 0
		for _, itemKey := range catalogOrder(pipeline) {
			if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
				continue
			}
			item, ok := pipeline[itemKey].(map[string]any)
			if !ok {
				continue
			}
			itemMetadata, ok := item["metadata"].(map[string]any)
			if !ok {
				continue
			}
			if active, ok := itemMetadata["active"].(bool); ok && !active {
				continue
			}
			start := time.Now().In(etlx.TimeZone)
			analysis := catalogDiscoverSQL(item, itemMetadata)
			loader.sectionKey, loader.itemKey = sectionKey, itemKey
			if err := loader.loadAsset(sectionKey, itemKey, defaults, itemMetadata, analysis); err != nil {
				catalogAppendLog(&logs, map[string]any{"process": "CATALOG", "name": sectionKey + "->" + itemKey, "key": key, "item_key": itemKey, "start_at": start, "end_at": time.Now().In(etlx.TimeZone), "duration": time.Since(start).Seconds(), "success": false, "msg": err.Error()})
				return logs, fmt.Errorf("catalog %s.%s: %w", sectionKey, itemKey, err)
			}
			itemCount++
			catalogAppendLog(&logs, map[string]any{"process": "CATALOG", "name": sectionKey + "->" + itemKey, "key": key, "item_key": itemKey, "start_at": start, "end_at": time.Now().In(etlx.TimeZone), "duration": time.Since(start).Seconds(), "success": true, "msg": analysis.Summary()})
		}
		catalogAppendLog(&logs, map[string]any{"process": "CATALOG", "name": key + "->" + sectionKey, "key": key, "item_key": sectionKey, "start_at": sectionStart, "end_at": time.Now().In(etlx.TimeZone), "duration": time.Since(sectionStart).Seconds(), "success": true, "msg": fmt.Sprintf("catalog pipeline section completed: %d items", itemCount)})
	}
	return logs, nil
}

type catalogLoader struct {
	etlx       *ETLX
	db         db.DBInterface
	names      map[string]map[string]any // table -> source name -> resolved ID
	logs       *[]map[string]any
	catalogKey string
	sectionKey string
	itemKey    string
}

// CatalogSQLAnalysis is the phase-one contract between SQL discovery and the
// DuckDB parser_tools integration. Phase two will persist its table, field and
// join results as data_assets, asset_fields and lineage relations.
type CatalogSQLAnalysis struct {
	Statements []string
	Kinds      []string // select, create_table_as_select, insert_from_select
}

func (a CatalogSQLAnalysis) Summary() string {
	if len(a.Kinds) == 0 {
		return "catalog asset registered; no supported SQL discovered"
	}
	return "catalog asset registered; SQL: " + strings.Join(a.Kinds, ", ")
}

var (
	catalogSQLKeyPattern = regexp.MustCompile(`(?i)(^sql$|^query$|(^|_)sql$|(^|_)query$|^step.*_(sql|query)$)`)
	catalogSelectPattern = regexp.MustCompile(`(?is)^\s*(with\b.*?\bselect\b|select\b)`)
	catalogCTASPattern   = regexp.MustCompile(`(?is)^\s*create\s+(or\s+replace\s+)?table\b.*?\bas\s+(with\b.*?\bselect\b|select\b)`)
	catalogInsertPattern = regexp.MustCompile(`(?is)^\s*insert\s+into\b.*?\b(with\b.*?\bselect\b|select\b)`)
)

func catalogPipelineCandidate(sectionKey string, metadata map[string]any) bool {
	for _, value := range []string{sectionKey, catalogString(metadata, "name"), catalogString(metadata, "runs_as")} {
		switch strings.ToUpper(strings.TrimSpace(value)) {
		case "ETL", "ELT", "ETL_PROCESS", "DATA_QUALITY", "DATAQUALITY", "QUALITY", "SCRIPTS", "MULTI_QUERIES", "STACKED_QUERIES":
			return true
		}
	}
	return false
}

// catalogDiscoverSQL resolves metadata references such as load_sql: my_query
// against the named SQL block stored alongside metadata. Only SELECT, CTAS and
// INSERT ... SELECT statements are phase-one candidates. parser_tools only
// supports SELECT parsing, so CTAS/INSERT candidates are retained here for
// later extraction of their SELECT source.
func catalogDiscoverSQL(item, metadata map[string]any) CatalogSQLAnalysis {
	result := CatalogSQLAnalysis{}
	seen := map[string]bool{}
	for metaKey, raw := range metadata {
		if !catalogSQLKeyPattern.MatchString(metaKey) {
			continue
		}
		value, ok := raw.(string)
		if !ok {
			continue
		}
		sql := strings.TrimSpace(value)
		if named, ok := item[sql].(string); ok {
			sql = strings.TrimSpace(named)
		}
		if sql == "" || seen[sql] {
			continue
		}
		kind := ""
		switch {
		case catalogCTASPattern.MatchString(sql):
			kind = "create_table_as_select"
		case catalogInsertPattern.MatchString(sql):
			kind = "insert_from_select"
		case catalogSelectPattern.MatchString(sql):
			kind = "select"
		}
		if kind != "" {
			seen[sql] = true
			result.Statements = append(result.Statements, sql)
			result.Kinds = append(result.Kinds, kind)
		}
	}
	return result
}

// DuckDBParserToolsSetupSQL is intentionally data-only in phase one. A later
// analyzer can execute this against DuckDB and then query
// SELECT * FROM parse_tables(?) for every eligible SELECT statement.
const DuckDBParserToolsSetupSQL = "INSTALL parser_tools FROM community; LOAD parser_tools;"

func (l *catalogLoader) loadAsset(sectionKey, itemKey string, parent, itemMetadata map[string]any, analysis CatalogSQLAnalysis) error {
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
	_, err = l.upsert("asset_schemas", "catalog_asset_id = :catalog_asset_id AND name = :name", map[string]any{
		"catalog_asset_id": assetID, "name": schemaName, "description": catalogString(itemMetadata, "schema_description"),
	})
	if err != nil {
		return err
	}
	_ = analysis // reserved for parser_tools table/column/lineage persistence in phase two.
	return nil
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
		l.logComponent(table, "resolved from cache")
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
			l.logComponent(table, "resolved")
			return id, nil
		}
	}
	id, err := l.upsert(table, fmt.Sprintf("%s = :%s", nameColumn, nameColumn), undefined)
	if err != nil {
		return nil, err
	}
	l.names[table][cacheKey] = id
	return id, nil
}

func (l *catalogLoader) upsert(table, condition string, data map[string]any) (any, error) {
	l.defaults(data)
	if _, err := l.etlx.InsertOrUpdate(l.db, table, "WHERE "+condition, data); err != nil {
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
	l.logComponent(table, "registered")
	return id, nil
}

func (l *catalogLoader) logComponent(component, message string) {
	if l.logs == nil {
		return
	}
	now := time.Now().In(l.etlx.TimeZone)
	catalogAppendLog(l.logs, map[string]any{
		"process": "CATALOG", "name": l.sectionKey + "->" + l.itemKey + "->" + component,
		"key": l.catalogKey, "item_key": l.itemKey, "start_at": now, "end_at": now,
		"duration": 0.0, "success": true, "msg": component + " " + message,
	})
}

func catalogAppendLog(logs *[]map[string]any, entry map[string]any) {
	*logs = append(*logs, entry)
	formatProcessLogEntry(entry)
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

func (etlx *ETLX) getMapString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (etlx *ETLX) getMapAny(data map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			return value
		}
	}
	return nil
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
