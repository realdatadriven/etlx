package etlxlib

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/realdatadriven/etlx/internal/db"
)

type bpmDefinition struct {
	Source string
	Nodes  []bpmNodeDefinition
	Flows  []bpmFlowDefinition
}

type bpmNodeDefinition struct {
	Code       string
	Type       string
	ParentCode string
	Global     bool
	Metadata   map[string]any
}

type bpmFlowDefinition struct {
	Code       string
	ParentCode string
	Lines      []string
}

type bpmEdgeDefinition struct {
	From        string
	To          string
	Condition   string
	BranchValue *bool
}

var bpmNodeCodePattern = regexp.MustCompile(`^(BP|BA|ST|SP|EP|DP)[A-Za-z0-9_-]*$`)
var bpmFlowCodePattern = regexp.MustCompile(`^FL[A-Za-z0-9_-]*$`)
var bpmDecisionPattern = regexp.MustCompile(`^([A-Z]{2}[A-Za-z0-9_-]*)\(([^,]+),([^,]+),([^)]+)\)$`)

func (etlx *ETLX) RunBPM(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "BPM"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing or invalid %s section", key)
	}
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing metadata in %s section", key)
	}
	if runsAs, _ := metadata["runs_as"].(string); runsAs != "" && !strings.EqualFold(runsAs, "BPM") {
		return nil, fmt.Errorf("%s must set runs_as: BPM", key)
	}
	if active, ok := metadata["active"].(bool); ok && !active {
		return nil, fmt.Errorf("%s deactivated", key)
	}

	definition, err := parseBPMDefinition(data)
	if err != nil {
		return nil, err
	}
	connection, err := bpmConnection(etlx, metadata)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}

	start := time.Now().In(etlx.TimeZone)
	memAlloc, memTotalAlloc, memSys, numGC := etlx.RuntimeMemStats()
	ref := any(nil)
	if configuredRef, ok := metadata["date_ref"].(string); ok && configuredRef != "" {
		ref = configuredRef
	} else if len(dateRef) > 0 {
		ref = dateRef[0].Format("2006-01-02")
	}
	entry := map[string]any{
		"process":               "BPM",
		"name":                  key,
		"description":           bpmString(metadata, "description", "desc"),
		"key":                   key,
		"ref":                   ref,
		"start_at":              start,
		"mem_alloc_start":       memAlloc,
		"mem_total_alloc_start": memTotalAlloc,
		"mem_sys_start":         memSys,
		"num_gc_start":          numGC,
	}
	logs := []map[string]any{entry}

	connectionDB, err := etlx.GetDB(connection)
	if err != nil {
		bpmFinishLog(etlx, entry, start, false, fmt.Sprintf("connecting to %s: %v", connection, err))
		formatProcessLogEntry(entry)
		return logs, fmt.Errorf("%s ERR: connecting to %s: %w", key, connection, err)
	}
	defer connectionDB.Close()

	store := &bpmSyncStore{
		db:      connectionDB,
		dialect: GetDialect(connectionDB.GetDriverName()),
		now:     time.Now().In(etlx.TimeZone),
	}
	if err := store.persist(definition); err != nil {
		bpmFinishLog(etlx, entry, start, false, err.Error())
		formatProcessLogEntry(entry)
		return logs, err
	}

	bpmFinishLog(
		etlx,
		entry,
		start,
		true,
		fmt.Sprintf("synchronized %d BPM nodes and %d flows", len(definition.Nodes), len(definition.Flows)),
	)
	formatProcessLogEntry(entry)
	return logs, nil
}

func bpmFinishLog(etlx *ETLX, entry map[string]any, start time.Time, success bool, message string) {
	memAlloc, memTotalAlloc, memSys, numGC := etlx.RuntimeMemStats()
	entry["success"] = success
	entry["msg"] = message
	entry["end_at"] = time.Now().In(etlx.TimeZone)
	entry["duration"] = time.Since(start).Seconds()
	entry["mem_alloc_end"] = memAlloc
	entry["mem_total_alloc_end"] = memTotalAlloc
	entry["mem_sys_end"] = memSys
	entry["num_gc_end"] = numGC
}

func bpmConnection(etlx *ETLX, metadata map[string]any) (string, error) {
	connection := bpmString(metadata, "connection", "conn")
	if connection != "" {
		return connection, nil
	}
	adminConnection := bpmString(metadata, "admin_connection", "admin_conn")
	database := bpmString(metadata, "database", "name")
	if adminConnection != "" && database != "" {
		connection, err := db.ReplaceDBNameV2(etlx.ReplaceEnvVariable(adminConnection), database)
		if err != nil {
			return "", fmt.Errorf("derive database connection: %w", err)
		}
		return connection, nil
	}
	if adminConnection != "" {
		return adminConnection, nil
	}
	return "", fmt.Errorf("no connection defined; set connection/conn, or admin_conn with an optional database/name")
}

func parseBPMDefinition(root map[string]any) (bpmDefinition, error) {
	header, _ := root["metadata"].(map[string]any)
	if runsAs, _ := header["runs_as"].(string); !strings.EqualFold(runsAs, "BPM") {
		return bpmDefinition{}, fmt.Errorf("BPM header metadata must set runs_as: BPM")
	}
	source, err := json.Marshal(root)
	if err != nil {
		return bpmDefinition{}, fmt.Errorf("serialize parsed BPM definition: %w", err)
	}
	definition := bpmDefinition{Source: string(source)}
	currentBP := ""
	for _, title := range bpmSectionOrder(root) {
		parts := strings.Fields(strings.TrimSpace(title))
		if len(parts) == 0 {
			continue
		}
		code := parts[0]
		if bpmFlowCodePattern.MatchString(code) {
			flow, err := parseBPMFlow(root[title], code, currentBP)
			if err != nil {
				return bpmDefinition{}, err
			}
			definition.Flows = append(definition.Flows, flow)
			continue
		}
		if !bpmNodeCodePattern.MatchString(code) {
			return bpmDefinition{}, fmt.Errorf("unsupported BPM element heading %q", title)
		}
		section, ok := root[title].(map[string]any)
		if !ok {
			return bpmDefinition{}, fmt.Errorf("section %q must contain a YAML mapping", title)
		}
		metadata, ok := section["metadata"].(map[string]any)
		if !ok {
			return bpmDefinition{}, fmt.Errorf("section %q must contain a YAML metadata block", title)
		}
		nodeType := code[:2]
		node := bpmNodeDefinition{Code: code, Type: nodeType, Metadata: metadata}
		if nodeType == "BP" {
			if definition.hasBP(code) {
				return bpmDefinition{}, fmt.Errorf("duplicate BP code %s; define one version of a BP per BPM section", code)
			}
			currentBP = code
		} else if nodeType == "ST" && currentBP == "" {
			node.Global = true
		} else {
			if currentBP == "" {
				return bpmDefinition{}, fmt.Errorf("%s must appear after its owning BP section", code)
			}
			node.ParentCode = currentBP
		}
		name, _ := metadata["name"].(string)
		if name == "" && nodeType != "ST" {
			return bpmDefinition{}, fmt.Errorf("%s requires a name", code)
		}
		if nodeType == "BP" {
			version, hasVersion := metadata["version"].(string)
			if !hasVersion && metadata["version"] != nil {
				return bpmDefinition{}, fmt.Errorf("%s version must be a string", code)
			}
			if version == "" {
				metadata["version"] = "v1.0.0"
			}
		}
		for _, existing := range definition.Nodes {
			if existing.Code == code && existing.ParentCode == node.ParentCode && existing.Global == node.Global {
				return bpmDefinition{}, fmt.Errorf("duplicate BPM element %s in BP %s", code, node.ParentCode)
			}
		}
		definition.Nodes = append(definition.Nodes, node)
	}
	if len(definition.Nodes) == 0 {
		return bpmDefinition{}, fmt.Errorf("document must define at least one BP node")
	}
	for _, node := range definition.Nodes {
		if node.Type == "BP" || node.Global && node.Type == "ST" {
			continue
		}
		if !definition.hasBP(node.ParentCode) {
			return bpmDefinition{}, fmt.Errorf("%s refers to undefined BP %s", node.Code, node.ParentCode)
		}
	}
	for _, flow := range definition.Flows {
		if !definition.hasBP(flow.ParentCode) {
			return bpmDefinition{}, fmt.Errorf("%s refers to undefined BP %s", flow.Code, flow.ParentCode)
		}
		edges, err := parseBPMFlowEdges(flow.Lines)
		if err != nil {
			return bpmDefinition{}, fmt.Errorf("%s: %w", flow.Code, err)
		}
		for _, edge := range edges {
			for _, code := range []string{edge.From, edge.To} {
				if !definition.hasNode(flow.ParentCode, code) {
					return bpmDefinition{}, fmt.Errorf("%s refers to undefined node %s in BP %s", flow.Code, code, flow.ParentCode)
				}
			}
		}
	}
	for _, node := range definition.Nodes {
		if node.Type != "BA" {
			continue
		}
		actorCodes, err := bpmStringList(node.Metadata["actors"])
		if err != nil {
			return bpmDefinition{}, fmt.Errorf("%s actors: %w", node.Code, err)
		}
		for _, actorCode := range actorCodes {
			if !definition.hasNodeType(node.ParentCode, actorCode, "ST") {
				return bpmDefinition{}, fmt.Errorf("%s refers to undefined stakeholder %s", node.Code, actorCode)
			}
		}
		if subprocessCode, _ := node.Metadata["subprocess"].(string); subprocessCode != "" && !definition.hasBP(subprocessCode) {
			return bpmDefinition{}, fmt.Errorf("%s refers to undefined sub-process %s", node.Code, subprocessCode)
		}
	}
	return definition, nil
}

func bpmSectionOrder(root map[string]any) []string {
	order := []string{}
	switch values := root["__order"].(type) {
	case []any:
		for _, value := range values {
			if title, ok := value.(string); ok {
				order = append(order, title)
			}
		}
	case []string:
		order = append(order, values...)
	}
	if len(order) > 0 {
		return order
	}
	for title := range root {
		if title != "metadata" && title != "__order" && title != "order" {
			order = append(order, title)
		}
	}
	sort.Strings(order)
	return order
}

func (definition bpmDefinition) hasBP(code string) bool {
	for _, node := range definition.Nodes {
		if node.Type == "BP" && node.Code == code {
			return true
		}
	}
	return false
}

func (definition bpmDefinition) hasNode(bpCode, code string) bool {
	for _, node := range definition.Nodes {
		if node.Code == code && (node.ParentCode == bpCode || node.Global && node.Type == "ST") {
			return true
		}
	}
	return false
}

func (definition bpmDefinition) hasNodeType(bpCode, code, nodeType string) bool {
	for _, node := range definition.Nodes {
		if node.Code == code && node.Type == nodeType && (node.ParentCode == bpCode || node.Global && node.Type == "ST") {
			return true
		}
	}
	return false
}

func parseBPMFlow(section any, code, parentCode string) (bpmFlowDefinition, error) {
	if parentCode == "" {
		return bpmFlowDefinition{}, fmt.Errorf("%s must appear after its owning BP section", code)
	}
	sectionMap, ok := section.(map[string]any)
	if !ok {
		return bpmFlowDefinition{}, fmt.Errorf("%s must contain a YAML mapping or flow sequence", code)
	}
	metadata, _ := sectionMap["metadata"].(map[string]any)
	expression := metadata["expression"]
	if expression == nil {
		expression = metadata["flow"]
	}
	lines := []string{}
	switch value := expression.(type) {
	case string:
		lines = append(lines, value)
	case []any:
		for _, line := range value {
			text, ok := line.(string)
			if !ok {
				return bpmFlowDefinition{}, fmt.Errorf("%s flow entries must be strings", code)
			}
			lines = append(lines, text)
		}
	default:
		return bpmFlowDefinition{}, fmt.Errorf("%s requires an expression or flow sequence", code)
	}
	if len(lines) == 0 {
		return bpmFlowDefinition{}, fmt.Errorf("%s flow sequence must not be empty", code)
	}
	return bpmFlowDefinition{Code: code, ParentCode: parentCode, Lines: lines}, nil
}

func parseBPMFlowEdges(lines []string) ([]bpmEdgeDefinition, error) {
	edges := []bpmEdgeDefinition{}
	for _, line := range lines {
		tokens := strings.Split(line, "->")
		current := []string{}
		for _, raw := range tokens {
			token := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "-"))
			if token == "" {
				continue
			}
			match := bpmDecisionPattern.FindStringSubmatch(token)
			if match == nil {
				if !bpmNodeCodePattern.MatchString(token) {
					return nil, fmt.Errorf("invalid flow element %q", token)
				}
				for _, from := range current {
					edges = append(edges, bpmEdgeDefinition{From: from, To: token})
				}
				current = []string{token}
				continue
			}
			decisionCode := match[1]
			condition := strings.TrimSpace(match[2])
			trueTarget := strings.TrimSpace(match[3])
			falseTarget := strings.TrimSpace(match[4])
			if !bpmNodeCodePattern.MatchString(decisionCode) || !strings.HasPrefix(decisionCode, "DP") {
				return nil, fmt.Errorf("invalid decision node %q", decisionCode)
			}
			if !bpmNodeCodePattern.MatchString(trueTarget) || !bpmNodeCodePattern.MatchString(falseTarget) {
				return nil, fmt.Errorf("invalid target in decision %q", token)
			}
			for _, from := range current {
				edges = append(edges, bpmEdgeDefinition{From: from, To: decisionCode})
			}
			trueValue, falseValue := true, false
			edges = append(edges,
				bpmEdgeDefinition{From: decisionCode, To: trueTarget, Condition: condition, BranchValue: &trueValue},
				bpmEdgeDefinition{From: decisionCode, To: falseTarget, Condition: condition, BranchValue: &falseValue},
			)
			current = []string{trueTarget, falseTarget}
		}
	}
	if len(edges) == 0 {
		return nil, fmt.Errorf("flow contains no connections")
	}
	return edges, nil
}

type bpmSyncStore struct {
	db      db.DBInterface
	dialect SQLDialect
	now     time.Time
}

func (store *bpmSyncStore) persist(definition bpmDefinition) error {
	bpIDs := map[string]any{}
	nodeIDs := map[string]map[string]any{}
	for _, node := range definition.Nodes {
		if node.Type != "BP" {
			continue
		}
		version, _ := node.Metadata["version"].(string)
		row := map[string]any{
			"node_code":         node.Code,
			"element_type":      node.Type,
			"name":              node.Metadata["name"],
			"description":       bpmString(node.Metadata, "desc", "description"),
			"version":           version,
			"process_status":    bpmDefault(bpmString(node.Metadata, "process_status", "status"), "draft"),
			"steps_orientation": node.Metadata["steps_orientation"],
			"definition_source": definition.Source,
			"active":            true,
			"excluded":          false,
		}
		id, err := store.upsert("process_node", "process_node_id", row, map[string]any{"node_code": node.Code, "version": version})
		if err != nil {
			return bpmSyncFailure("process node "+node.Code, err)
		}
		bpIDs[node.Code] = id
		nodeIDs[node.Code] = map[string]any{node.Code: id}
	}

	for _, node := range definition.Nodes {
		if node.Type == "BP" {
			continue
		}
		if node.Global {
			for _, bp := range definition.Nodes {
				if bp.Type == "BP" {
					if err := store.upsertBPMNode(node, bp.Code, bpIDs, nodeIDs); err != nil {
						return bpmSyncFailure("process node "+node.Code, err)
					}
				}
			}
		} else if err := store.upsertBPMNode(node, node.ParentCode, bpIDs, nodeIDs); err != nil {
			return bpmSyncFailure("process node "+node.Code, err)
		}
	}

	for _, node := range definition.Nodes {
		if node.Type != "BA" {
			continue
		}
		subprocessCode, _ := node.Metadata["subprocess"].(string)
		if subprocessCode == "" {
			continue
		}
		subprocessID, ok := bpIDs[subprocessCode]
		if !ok {
			return fmt.Errorf("%s refers to undefined sub-process %s", node.Code, subprocessCode)
		}
		row := map[string]any{"subprocess_node_id": subprocessID, "excluded": false}
		if _, err := store.upsert("process_node", "process_node_id", row, map[string]any{"process_node_id": nodeIDs[node.ParentCode][node.Code]}); err != nil {
			return bpmSyncFailure("sub-process for "+node.Code, err)
		}
	}

	for _, node := range definition.Nodes {
		if node.Type != "ST" {
			continue
		}
		bpCodes := []string{node.ParentCode}
		if node.Global {
			bpCodes = nil
			for _, bp := range definition.Nodes {
				if bp.Type == "BP" {
					bpCodes = append(bpCodes, bp.Code)
				}
			}
		}
		for _, bpCode := range bpCodes {
			stakeholderID := nodeIDs[bpCode][node.Code]
			points, _ := node.Metadata["focal_points"].([]any)
			for _, item := range points {
				point, ok := item.(map[string]any)
				if !ok {
					return fmt.Errorf("%s focal_points entries must be objects", node.Code)
				}
				email, _ := point["email"].(string)
				name, _ := point["name"].(string)
				if email == "" || name == "" {
					return fmt.Errorf("%s focal points require name and email", node.Code)
				}
				row := map[string]any{
					"stakeholder_node_id": stakeholderID,
					"name":                name,
					"email":               email,
					"role":                point["role"],
					"active":              true,
					"excluded":            false,
				}
				if _, err := store.upsert("stakeholder_focal_point", "stakeholder_focal_point_id", row, map[string]any{"stakeholder_node_id": stakeholderID, "email": email}); err != nil {
					return bpmSyncFailure("focal point "+email, err)
				}
			}
		}
	}

	for _, node := range definition.Nodes {
		if node.Type != "BA" {
			continue
		}
		activityID := nodeIDs[node.ParentCode][node.Code]
		actorCodes, err := bpmStringList(node.Metadata["actors"])
		if err != nil {
			return bpmSyncFailure("actors for "+node.Code, err)
		}
		for _, actorCode := range actorCodes {
			stakeholderID, ok := nodeIDs[node.ParentCode][actorCode]
			if !ok {
				return fmt.Errorf("%s refers to undefined stakeholder %s", node.Code, actorCode)
			}
			row := map[string]any{
				"activity_node_id":    activityID,
				"stakeholder_node_id": stakeholderID,
				"responsibility":      "assignee",
				"active":              true,
				"excluded":            false,
			}
			key := map[string]any{"activity_node_id": activityID, "stakeholder_node_id": stakeholderID, "responsibility": "assignee"}
			if _, err := store.upsert("process_activity_actor", "process_activity_actor_id", row, key); err != nil {
				return bpmSyncFailure("actor "+actorCode+" for "+node.Code, err)
			}
		}
		if err := store.persistActivitySchema(node, activityID); err != nil {
			return bpmSyncFailure("schema for "+node.Code, err)
		}
		if err := store.persistActions(node, activityID); err != nil {
			return bpmSyncFailure("actions for "+node.Code, err)
		}
	}

	for _, flow := range definition.Flows {
		businessProcessID := bpIDs[flow.ParentCode]
		expression := strings.Join(flow.Lines, "\n")
		flowRow := map[string]any{
			"process_node_id": businessProcessID,
			"flow_code":       flow.Code,
			"expression":      expression,
			"active":          true,
			"excluded":        false,
		}
		flowID, err := store.upsert("process_flow", "process_flow_id", flowRow, map[string]any{"process_node_id": businessProcessID, "flow_code": flow.Code})
		if err != nil {
			return bpmSyncFailure("flow "+flow.Code, err)
		}
		edges, err := parseBPMFlowEdges(flow.Lines)
		if err != nil {
			return bpmSyncFailure("flow "+flow.Code, err)
		}
		if err := store.retireFlowEdges(flowID); err != nil {
			return bpmSyncFailure("retire old edges for "+flow.Code, err)
		}
		for i, edge := range edges {
			fromID, fromOK := nodeIDs[flow.ParentCode][edge.From]
			toID, toOK := nodeIDs[flow.ParentCode][edge.To]
			if !fromOK || !toOK {
				return fmt.Errorf("%s refers to undefined node(s) %s -> %s", flow.Code, edge.From, edge.To)
			}
			var branchValue any
			if edge.BranchValue != nil {
				branchValue = *edge.BranchValue
			}
			row := map[string]any{
				"process_flow_id": flowID,
				"from_node_id":    fromID,
				"to_node_id":      toID,
				"edge_order":      i + 1,
				"is_decision":     edge.BranchValue != nil,
				"condition":       edge.Condition,
				"branch_value":    branchValue,
				"excluded":        false,
			}
			key := map[string]any{"process_flow_id": flowID, "from_node_id": fromID, "to_node_id": toID, "branch_value": branchValue}
			if _, err := store.upsert("process_edge", "process_edge_id", row, key); err != nil {
				return bpmSyncFailure("edge "+edge.From+" -> "+edge.To, err)
			}
		}
	}
	return nil
}

func (store *bpmSyncStore) upsertBPMNode(node bpmNodeDefinition, bpCode string, bpIDs map[string]any, nodeIDs map[string]map[string]any) error {
	businessProcessID, ok := bpIDs[bpCode]
	if !ok {
		return fmt.Errorf("%s refers to undefined BP %s", node.Code, bpCode)
	}
	row := map[string]any{
		"parent_node_id": businessProcessID,
		"node_code":      node.Code,
		"element_type":   node.Type,
		"name":           bpmDefault(bpmString(node.Metadata, "name"), node.Code),
		"description":    bpmString(node.Metadata, "desc", "description"),
		"order_index":    node.Metadata["order_index"],
		"active":         true,
		"excluded":       false,
	}
	if node.Type == "BA" {
		row["execution_mode"] = bpmDefault(bpmString(node.Metadata, "execution_mode"), "manual")
		row["schedule"] = node.Metadata["schedule"]
	}
	id, err := store.upsert("process_node", "process_node_id", row, map[string]any{"parent_node_id": businessProcessID, "node_code": node.Code})
	if err != nil {
		return err
	}
	if nodeIDs[bpCode] == nil {
		nodeIDs[bpCode] = map[string]any{}
	}
	nodeIDs[bpCode][node.Code] = id
	return nil
}

func (store *bpmSyncStore) persistActivitySchema(node bpmNodeDefinition, activityID any) error {
	fields, _ := node.Metadata["data_schema"].([]any)
	for _, item := range fields {
		field, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("%s data_schema entries must be objects", node.Code)
		}
		fieldName, _ := field["name"].(string)
		dataType, _ := field["type"].(string)
		if fieldName == "" || dataType == "" {
			return fmt.Errorf("%s schema fields require name and type", node.Code)
		}
		if strings.EqualFold(dataType, "bool") {
			dataType = "boolean"
		}
		label := field["label"]
		if label == nil {
			label = fieldName
		}
		options, err := bpmJSONText(field["options"])
		if err != nil {
			return fmt.Errorf("%s field %s options: %w", node.Code, fieldName, err)
		}
		row := map[string]any{
			"activity_node_id": activityID,
			"field":            fieldName,
			"label":            label,
			"data_type":        strings.ToLower(dataType),
			"description":      bpmString(field, "desc", "description"),
			"nullable":         bpmBoolDefault(field, "nullable", true),
			"default_value":    field["default_value"],
			"validation_rule":  field["validation_rule"],
			"order_index":      field["order_index"],
			"format":           field["format"],
			"size":             field["size"],
			"elipsis":          field["elipsis"],
			"input_type":       field["input_type"],
			"options":          options,
			"active":           true,
			"excluded":         false,
		}
		key := map[string]any{"activity_node_id": activityID, "field": fieldName}
		if _, err := store.upsert("process_activity_schema", "process_activity_schema_id", row, key); err != nil {
			return fmt.Errorf("field %s: %w", fieldName, err)
		}
	}
	return nil
}

func (store *bpmSyncStore) persistActions(node bpmNodeDefinition, activityID any) error {
	buttons, _ := node.Metadata["action_buttons"].([]any)
	for _, item := range buttons {
		button, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("%s action_buttons entries must be objects", node.Code)
		}
		name, _ := button["name"].(string)
		if name == "" {
			return fmt.Errorf("%s action buttons require a name", node.Code)
		}
		actionType, _ := button["type"].(string)
		if actionType == "" {
			return fmt.Errorf("%s action %s requires a type", node.Code, name)
		}
		endpoint := button["api"]
		if endpoint == nil {
			endpoint = button["etl"]
		}
		configuration, err := bpmJSONText(button["configuration"])
		if err != nil {
			return fmt.Errorf("%s action %s configuration: %w", node.Code, name, err)
		}
		row := map[string]any{
			"activity_node_id": activityID,
			"name":             name,
			"label":            bpmDefault(bpmString(button, "label"), name),
			"action_type":      actionType,
			"endpoint":         endpoint,
			"configuration":    configuration,
			"active":           true,
			"excluded":         false,
		}
		if _, err := store.upsert("process_action", "process_action_id", row, map[string]any{"activity_node_id": activityID, "name": name}); err != nil {
			return fmt.Errorf("action %s: %w", name, err)
		}
	}
	return nil
}

func (store *bpmSyncStore) retireFlowEdges(flowID any) error {
	rows, _, err := store.db.QueryMultiRows(
		fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ? AND %s = ?`,
			store.dialect.GetColumnName("process_edge_id"),
			store.dialect.GetTableName("process_edge"),
			store.dialect.GetColumnName("process_flow_id"),
			store.dialect.GetColumnName("excluded"),
		),
		flowID,
		store.dialect.GetBooleanValue(false),
	)
	if err != nil {
		return fmt.Errorf("load existing edges: %w", err)
	}
	for _, row := range *rows {
		id, ok := row["process_edge_id"]
		if !ok {
			return fmt.Errorf("existing edge has no process_edge_id")
		}
		if _, err := store.upsert("process_edge", "process_edge_id", map[string]any{"excluded": true}, map[string]any{"process_edge_id": id}); err != nil {
			return err
		}
	}
	return nil
}

func (store *bpmSyncStore) upsert(table, primaryKey string, row, key map[string]any) (any, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("%s upsert requires a natural key", table)
	}
	if _, ok := row["created_at"]; !ok {
		row["created_at"] = store.now
	}
	row["updated_at"] = store.now
	where, args, err := store.whereClause(key)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s`,
		store.dialect.GetColumnName(primaryKey),
		store.dialect.GetTableName(table),
		where,
	)
	foundRow, found, err := store.db.QuerySingleRow(query, args...)
	if err != nil {
		return nil, fmt.Errorf("find %s by natural key: %w", table, err)
	}
	if found && len(*foundRow) > 0 {
		id, ok := (*foundRow)[primaryKey]
		if !ok || id == nil {
			return nil, fmt.Errorf("find %s by natural key returned no %s", table, primaryKey)
		}
		if err := store.update(table, primaryKey, id, row); err != nil {
			return nil, err
		}
		return id, nil
	}

	fields := make([]string, 0, len(row))
	for field := range row {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	columns, placeholders, params := make([]string, 0, len(fields)), make([]string, 0, len(fields)), map[string]any{}
	for _, field := range fields {
		columns = append(columns, store.dialect.GetColumnName(field))
		placeholders = append(placeholders, ":"+field)
		params[field] = row[field]
	}
	insert := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		store.dialect.GetTableName(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := store.db.ExecuteNamedQuery(insert, store.dialect.DataTypeConversion(params)); err != nil {
		return nil, fmt.Errorf("insert %s: %w", table, err)
	}
	insertedRow, found, err := store.db.QuerySingleRow(query, args...)
	if err != nil {
		return nil, fmt.Errorf("find inserted %s: %w", table, err)
	}
	if !found || len(*insertedRow) == 0 || (*insertedRow)[primaryKey] == nil {
		return nil, fmt.Errorf("inserted %s but could not read %s", table, primaryKey)
	}
	return (*insertedRow)[primaryKey], nil
}

func (store *bpmSyncStore) update(table, primaryKey string, id any, row map[string]any) error {
	fields := make([]string, 0, len(row))
	for field := range row {
		if field != primaryKey && field != "created_at" {
			fields = append(fields, field)
		}
	}
	sort.Strings(fields)
	if len(fields) == 0 {
		return nil
	}
	sets, params := make([]string, 0, len(fields)), map[string]any{"bpm_primary_key": id}
	for _, field := range fields {
		sets = append(sets, fmt.Sprintf("%s = :%s", store.dialect.GetColumnName(field), field))
		params[field] = row[field]
	}
	query := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s = :bpm_primary_key`,
		store.dialect.GetTableName(table),
		strings.Join(sets, ", "),
		store.dialect.GetColumnName(primaryKey),
	)
	if _, err := store.db.ExecuteNamedQuery(query, store.dialect.DataTypeConversion(params)); err != nil {
		return fmt.Errorf("update %s: %w", table, err)
	}
	return nil
}

func (store *bpmSyncStore) whereClause(key map[string]any) (string, []any, error) {
	fields := make([]string, 0, len(key))
	for field := range key {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	clauses, params := make([]string, 0, len(fields)), make([]any, 0, len(fields))
	for _, field := range fields {
		value := key[field]
		if value == nil {
			clauses = append(clauses, store.dialect.GetColumnName(field)+" IS NULL")
			continue
		}
		clauses = append(clauses, store.dialect.GetColumnName(field)+" = ?")
		params = append(params, store.dialect.DataTypeConversion(map[string]any{field: value})[field])
	}
	return strings.Join(clauses, " AND "), params, nil
}

func bpmSyncFailure(item string, err error) error {
	return fmt.Errorf("BPM sync failed for %s: %w", item, err)
}

func bpmString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func bpmStringList(value any) ([]string, error) {
	switch values := value.(type) {
	case nil:
		return nil, nil
	case []any:
		result := make([]string, 0, len(values))
		for _, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("list entries must be strings")
			}
			result = append(result, text)
		}
		return result, nil
	case string:
		if strings.TrimSpace(values) == "" {
			return nil, nil
		}
		return []string{values}, nil
	default:
		return nil, fmt.Errorf("expected a list of strings")
	}
}

func bpmBoolDefault(data map[string]any, key string, fallback bool) bool {
	value, ok := data[key].(bool)
	if !ok {
		return fallback
	}
	return value
}

func bpmDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func bpmJSONText(value any) (any, error) {
	switch value.(type) {
	case map[string]any, []any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode JSON text: %w", err)
		}
		return string(encoded), nil
	}
	return value, nil
}
