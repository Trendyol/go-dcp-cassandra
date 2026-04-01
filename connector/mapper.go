package connector

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

type Mapper func(event couchbase.Event) []cassandra.Model

var (
	collectionTableMappings *[]config.CollectionTableMapping
	mappingCache            = make(map[string]config.CollectionTableMapping)
)

func SetCollectionTableMappings(mappings *[]config.CollectionTableMapping) {
	collectionTableMappings = mappings
	mappingCache = make(map[string]config.CollectionTableMapping)
}

func DefaultMapper(event couchbase.Event) []cassandra.Model {
	if event.IsMutated {
		mapping := findCollectionTableMapping(event.CollectionName)
		model := buildUpsertModel(mapping, event)
		return []cassandra.Model{&model}
	}
	if event.IsDeleted || event.IsExpired {
		mapping := findCollectionTableMapping(event.CollectionName)
		model := buildDeleteModel(mapping, event)
		return []cassandra.Model{&model}
	}
	return nil
}

func findCollectionTableMapping(collectionName string) config.CollectionTableMapping {
	if mapping, exists := mappingCache[collectionName]; exists {
		return mapping
	}

	if collectionTableMappings == nil {
		panic("collectionTableMappings is not initialized. Call SetCollectionTableMappings first.")
	}

	for _, mapping := range *collectionTableMappings {
		if mapping.Collection == collectionName {
			mappingCache[collectionName] = mapping
			return mapping
		}
	}

	for _, mapping := range *collectionTableMappings {
		if mapping.Collection == "" || mapping.Collection == "_default" {
			mappingCache[collectionName] = mapping
			return mapping
		}
	}

	panic(fmt.Sprintf("no mapping found for collection: %s", collectionName))
}

func getNestedField(document map[string]any, fieldPath string) (any, bool) {
	if !strings.Contains(fieldPath, ".") {
		value, exists := document[fieldPath]
		return value, exists
	}

	parts := strings.Split(fieldPath, ".")
	current := document
	for _, part := range parts[:len(parts)-1] {
		value, exists := current[part]
		if !exists {
			return nil, false
		}
		nested, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = nested
	}

	value, exists := current[parts[len(parts)-1]]
	return value, exists
}

func convertFieldValue(fieldPath string, value any) any {
	if fieldPath != "date" {
		return value
	}

	switch v := value.(type) {
	case float64:
		return v
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
		return 0.0
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return value
	}
}

func buildUpsertModel(mapping config.CollectionTableMapping, event couchbase.Event) cassandra.Raw {
	var sourceDocument map[string]any
	if err := json.Unmarshal(event.Value, &sourceDocument); err != nil {
		sourceDocument = make(map[string]any)
	}

	targetDocument := make(map[string]any)

	for cassandraColumn, sourceField := range mapping.FieldMappings {
		switch sourceField {
		case "_key":
			targetDocument[cassandraColumn] = string(event.Key)
		case "documentData":
			targetDocument[cassandraColumn] = string(event.Value)
		default:
			if fieldValue, exists := getNestedField(sourceDocument, sourceField); exists {
				targetDocument[cassandraColumn] = convertFieldValue(sourceField, fieldValue)
			} else {
				targetDocument[cassandraColumn] = nil
			}
		}
	}

	return cassandra.Raw{
		Table:     mapping.TableName,
		Document:  targetDocument,
		Operation: cassandra.Upsert,
	}
}

func buildDeleteModel(mapping config.CollectionTableMapping, event couchbase.Event) cassandra.Raw {
	var sourceDocument map[string]any
	if event.Value != nil {
		err := json.Unmarshal(event.Value, &sourceDocument)
		if err != nil {
			panic(err)
		}
	}
	if sourceDocument == nil {
		sourceDocument = make(map[string]any)
	}

	filter := make(map[string]any)

	for cassandraColumn, sourceField := range mapping.FieldMappings {
		switch sourceField {
		case "_key":
			filter[cassandraColumn] = string(event.Key)
		case "documentData":
		default:
			if fieldValue, exists := getNestedField(sourceDocument, sourceField); exists {
				filter[cassandraColumn] = convertFieldValue(sourceField, fieldValue)
			}
		}
	}

	return cassandra.Raw{
		Table:     mapping.TableName,
		Filter:    filter,
		Operation: cassandra.Delete,
	}
}
