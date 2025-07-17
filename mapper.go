package main

import (
	"encoding/json"
	"fmt"

	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"
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

func buildUpsertModel(mapping config.CollectionTableMapping, event couchbase.Event) cassandra.Raw {
	var sourceDocument map[string]interface{}
	if err := json.Unmarshal(event.Value, &sourceDocument); err != nil {
		sourceDocument = make(map[string]interface{})
	}

	// Create target document for Cassandra
	targetDocument := make(map[string]interface{})

	// Map fields from source document to target columns based on fieldMappings
	for cassandraColumn, sourceField := range mapping.FieldMappings {
		if sourceField == "id" {
			// Special handling for document key
			targetDocument[cassandraColumn] = string(event.Key)
		} else if sourceField == "documentData" {
			// Special handling for full document data
			targetDocument[cassandraColumn] = string(event.Value)
		} else if fieldValue, exists := sourceDocument[sourceField]; exists {
			// Map field from source document
			targetDocument[cassandraColumn] = fieldValue
		} else {
			// Field not found in source document, set to nil or default value
			targetDocument[cassandraColumn] = nil
		}
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
		json.Unmarshal(event.Value, &sourceDocument)
	}
	if sourceDocument == nil {
		sourceDocument = make(map[string]interface{})
	}

	// Create filter for delete operation based on fieldMappings
	filter := make(map[string]interface{})

	// Map fields from source document to target columns based on fieldMappings
	for cassandraColumn, sourceField := range mapping.FieldMappings {
		if sourceField == "id" {
			// Special handling for document key
			filter[cassandraColumn] = string(event.Key)
		} else if fieldValue, exists := sourceDocument[sourceField]; exists {
			// Map field from source document
			filter[cassandraColumn] = fieldValue
		}
		// For delete operations, we only include fields that are available
		// Missing fields are not included in the filter
	}

	return cassandra.Raw{
		Table:     mapping.TableName,
		Filter:    filter,
		Operation: cassandra.Delete,
		ID:        string(event.Key),
	}
}
