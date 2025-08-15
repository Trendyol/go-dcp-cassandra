package dcpcassandra

import (
	"encoding/json"
	"fmt"
	"go-dcp-cassandra/cassandra"
	"go-dcp-cassandra/couchbase"
	"strconv"
	"strings"

	config "go-dcp-cassandra/configs"
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

func Map(event couchbase.Event) []cassandra.Model {
	if event.IsMutated {
		mapping := findCollectionTableMapping(event.CollectionName)
		model := buildUpsertModel(mapping, event)
		return []cassandra.Model{&model}
	} else if event.IsDeleted || event.IsExpired {
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

	for _, mapping := range *collectionTableMappings {
		if mapping.Collection == collectionName {
			mappingCache[collectionName] = mapping
			return mapping
		}
	}

	panic(fmt.Sprintf("no mapping found for collection: %s", collectionName))
}

func getNestedField(document map[string]interface{}, fieldPath string) (interface{}, bool) {
	// Handle nested fields like "meta.id"
	if strings.Contains(fieldPath, ".") {
		parts := strings.Split(fieldPath, ".")
		current := document

		for i, part := range parts {
			if i == len(parts)-1 {
				// Last part, return the value
				if value, exists := current[part]; exists {
					return value, true
				}
				return nil, false
			} else {
				// Intermediate part, navigate deeper
				if value, exists := current[part]; exists {
					if nested, ok := value.(map[string]interface{}); ok {
						current = nested
					} else {
						return nil, false
					}
				} else {
					return nil, false
				}
			}
		}
	}

	if value, exists := document[fieldPath]; exists {
		return value, true
	}
	return nil, false
}

func convertFieldValue(fieldPath string, value interface{}) interface{} {
	// Handle date field conversion
	if fieldPath == "date" {
		switch v := value.(type) {
		case float64:
			return v
		case string:
			// Try to parse string as float64
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				return parsed
			}
			return 0.0
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}

	return value
}

func buildUpsertModel(mapping config.CollectionTableMapping, event couchbase.Event) cassandra.Raw {
	var sourceDocument map[string]interface{}
	if err := json.Unmarshal(event.Value, &sourceDocument); err != nil {
		sourceDocument = make(map[string]interface{})
	}

	targetDocument := make(map[string]interface{})

	for cassandraColumn, sourceField := range mapping.FieldMappings {
		if sourceField == "documentData" {
			targetDocument[cassandraColumn] = string(event.Value)
		} else if fieldValue, exists := getNestedField(sourceDocument, sourceField); exists {
			targetDocument[cassandraColumn] = convertFieldValue(sourceField, fieldValue)
		} else {
			targetDocument[cassandraColumn] = nil
		}
	}

	if _, hasIdMapping := mapping.FieldMappings["id"]; !hasIdMapping {
		targetDocument["id"] = string(event.Key)
	} else if targetDocument["id"] == nil {
		targetDocument["id"] = string(event.Key)
	}

	return cassandra.Raw{
		Table:     mapping.TableName,
		Document:  targetDocument,
		Operation: cassandra.Upsert,
		ID:        string(event.Key),
	}
}

func buildDeleteModel(mapping config.CollectionTableMapping, event couchbase.Event) cassandra.Raw {
	var sourceDocument map[string]interface{}
	if event.Value != nil {
		err := json.Unmarshal(event.Value, &sourceDocument)
		if err != nil {
			panic(err)
		}
	}
	if sourceDocument == nil {
		sourceDocument = make(map[string]interface{})
	}

	filter := make(map[string]interface{})

	for cassandraColumn, sourceField := range mapping.FieldMappings {
		if sourceField == "id" {
		} else if fieldValue, exists := getNestedField(sourceDocument, sourceField); exists {
			filter[cassandraColumn] = convertFieldValue(sourceField, fieldValue)
		}
	}

	filter["id"] = string(event.Key)

	return cassandra.Raw{
		Table:     mapping.TableName,
		Filter:    filter,
		Operation: cassandra.Delete,
		ID:        string(event.Key),
	}
}
