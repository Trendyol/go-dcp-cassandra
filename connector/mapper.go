package connector

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	"github.com/Trendyol/go-dcp-cassandra/config"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

var (
	collectionTableMappings *[]config.CollectionTableMapping
	mappingCache            = make(map[string]config.CollectionTableMapping)
	mappingCacheMu          sync.RWMutex
)

func SetCollectionTableMappings(mappings *[]config.CollectionTableMapping) {
	mappingCacheMu.Lock()
	defer mappingCacheMu.Unlock()
	collectionTableMappings = mappings
	mappingCache = make(map[string]config.CollectionTableMapping)
}

func DefaultMapper(event couchbase.Event) []cassandra.Model {
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
	mappingCacheMu.RLock()
	if mapping, exists := mappingCache[collectionName]; exists {
		mappingCacheMu.RUnlock()
		return mapping
	}
	mappingCacheMu.RUnlock()

	mappingCacheMu.Lock()
	defer mappingCacheMu.Unlock()

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

func getNestedField(document map[string]interface{}, fieldPath string) (interface{}, bool) {
	if strings.Contains(fieldPath, ".") {
		parts := strings.Split(fieldPath, ".")
		current := document

		for i, part := range parts {
			if i == len(parts)-1 {
				if value, exists := current[part]; exists {
					return value, true
				}
				return nil, false
			} else {
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
	if fieldPath == "date" {
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

	if len(mapping.PrimaryKeyFields) > 0 {
		pkSet := make(map[string]struct{}, len(mapping.PrimaryKeyFields))
		for _, pk := range mapping.PrimaryKeyFields {
			pkSet[pk] = struct{}{}
		}
		for col := range filter {
			if _, ok := pkSet[col]; !ok {
				delete(filter, col)
			}
		}
	}

	return cassandra.Raw{
		Table:     mapping.TableName,
		Filter:    filter,
		Operation: cassandra.Delete,
	}
}
