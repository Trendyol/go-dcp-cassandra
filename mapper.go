package dcpcassandra

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

type Mapper func(event couchbase.Event) []cassandra.Model

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

	for _, mapping := range *collectionTableMappings {
		if mapping.Collection == collectionName {
			mappingCache[collectionName] = mapping
			return mapping
		}
	}

	panic(fmt.Sprintf("no mapping found for collection: %s", collectionName))
}

func getNestedField(document map[string]any, fieldPath string) (any, bool) {
	current := document
	remaining := fieldPath
	for {
		key, rest, hasMore := strings.Cut(remaining, ".")
		if !hasMore {
			value, exists := current[key]
			return value, exists
		}
		nested, ok := current[key].(map[string]any)
		if !ok {
			return nil, false
		}
		current = nested
		remaining = rest
	}
}

func convertFieldValue(fieldPath string, value any) any {
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
