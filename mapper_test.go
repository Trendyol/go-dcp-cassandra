package main

import (
	"reflect"
	"testing"
	"time"

	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"
)

func TestDefaultMapper_Mutation(t *testing.T) {
	// Test setup
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "id",
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123", "meta": {"version": "1.0"}}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	if len(result) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(result))
	}

	rawModel, ok := result[0].(*cassandra.Raw)
	if !ok {
		t.Fatal("Expected *cassandra.Raw model")
	}

	if rawModel.Table != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", rawModel.Table)
	}

	if rawModel.Operation != cassandra.Upsert {
		t.Errorf("Expected operation Upsert, got %s", rawModel.Operation)
	}

	if rawModel.Document["id"] != "doc_key" {
		t.Errorf("Expected id 'doc_key', got '%v'", rawModel.Document["id"])
	}

	if rawModel.Document["partition_key"] != "part_123" {
		t.Errorf("Expected partition_key 'part_123', got '%v'", rawModel.Document["partition_key"])
	}

	if rawModel.Document["data"] != jsonData {
		t.Errorf("Expected data '%s', got '%v'", jsonData, rawModel.Document["data"])
	}

	// Check metadata field mapping
	expectedMeta := map[string]interface{}{"version": "1.0"}
	if !reflect.DeepEqual(rawModel.Document["metadata"], expectedMeta) {
		t.Errorf("Expected metadata %v, got %v", expectedMeta, rawModel.Document["metadata"])
	}
}

func TestDefaultMapper_MutationWithMissingFields(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "id",
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
			"status":        "status", // This field is missing in source
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	// Test data with missing field
	jsonData := `{"partitionId": "part_123", "meta": {"version": "1.0"}}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	if len(result) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(result))
	}

	rawModel, ok := result[0].(*cassandra.Raw)
	if !ok {
		t.Fatal("Expected *cassandra.Raw model")
	}

	if rawModel.Document["status"] != nil {
		t.Errorf("Expected status to be nil, got '%v'", rawModel.Document["status"])
	}

	if rawModel.Document["partition_key"] != "part_123" {
		t.Errorf("Expected partition_key 'part_123', got '%v'", rawModel.Document["partition_key"])
	}
}

func TestDefaultMapper_Deletion(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "id",
			"partition_key": "partitionId",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123"}`
	event := couchbase.NewDeleteEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	if len(result) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(result))
	}

	rawModel, ok := result[0].(*cassandra.Raw)
	if !ok {
		t.Fatal("Expected *cassandra.Raw model")
	}

	if rawModel.Operation != cassandra.Delete {
		t.Errorf("Expected operation Delete, got %s", rawModel.Operation)
	}

	if rawModel.Filter["id"] != "doc_key" {
		t.Errorf("Expected filter id 'doc_key', got '%v'", rawModel.Filter["id"])
	}

	if rawModel.Filter["partition_key"] != "part_123" {
		t.Errorf("Expected filter partition_key 'part_123', got '%v'", rawModel.Filter["partition_key"])
	}

	if rawModel.Document != nil {
		t.Error("Expected document to be nil for delete operations")
	}
}

func TestDefaultMapper_Expiration(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	event := couchbase.NewExpireEvent(
		[]byte("doc_key"),
		nil,
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	if len(result) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(result))
	}

	rawModel, ok := result[0].(*cassandra.Raw)
	if !ok {
		t.Fatal("Expected *cassandra.Raw model")
	}

	if rawModel.Operation != cassandra.Delete {
		t.Errorf("Expected operation Delete, got %s", rawModel.Operation)
	}
}

func TestDefaultMapper_UnknownEvent(t *testing.T) {
	// Test setup
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	event := couchbase.Event{
		CollectionName: "test_collection",
		EventTime:      time.Now(),
		Key:            []byte("doc_key"),
		Value:          []byte(`{"data": "test"}`),
		Cas:            123,
		VbID:           1,
		IsDeleted:      false,
		IsExpired:      false,
		IsMutated:      false, // Not any known type
	}

	result := DefaultMapper(event)

	if result != nil {
		t.Errorf("Expected nil result for unknown event type, got %v", result)
	}
}

func TestFindCollectionTableMapping(t *testing.T) {
	mapping1 := config.CollectionTableMapping{
		Collection: "collection1",
		TableName:  "table1",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}
	mapping2 := config.CollectionTableMapping{
		Collection: "collection2",
		TableName:  "table2",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping1, mapping2})

	result := findCollectionTableMapping("collection1")
	if result.Collection != "collection1" {
		t.Errorf("Expected collection 'collection1', got '%s'", result.Collection)
	}
	if result.TableName != "table1" {
		t.Errorf("Expected table 'table1', got '%s'", result.TableName)
	}

	// Test cache functionality
	result2 := findCollectionTableMapping("collection1")
	if result2.Collection != "collection1" {
		t.Errorf("Expected cached collection 'collection1', got '%s'", result2.Collection)
	}
}

func TestFindCollectionTableMapping_NotFound(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "collection1",
		TableName:  "table1",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for non-existent collection mapping")
		}
	}()

	findCollectionTableMapping("non_existent_collection")
}

func TestBuildUpsertModel_WithFieldMapping(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "id",
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
		},
	}

	jsonData := `{"partitionId": "part_123", "meta": {"version": "1.0"}}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := buildUpsertModel(mapping, event)

	if result.Table != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", result.Table)
	}

	if result.Operation != cassandra.Upsert {
		t.Errorf("Expected operation Upsert, got %s", result.Operation)
	}

	if result.Document["id"] != "doc_key" {
		t.Errorf("Expected id 'doc_key', got '%v'", result.Document["id"])
	}

	if result.Document["partition_key"] != "part_123" {
		t.Errorf("Expected partition_key 'part_123', got '%v'", result.Document["partition_key"])
	}

	if result.Document["data"] != jsonData {
		t.Errorf("Expected data '%s', got '%v'", jsonData, result.Document["data"])
	}
}

func TestBuildUpsertModel_InvalidJSON(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":   "id",
			"data": "documentData",
		},
	}

	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte("invalid json"),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := buildUpsertModel(mapping, event)

	if result.Document["id"] != "doc_key" {
		t.Errorf("Expected id 'doc_key', got '%v'", result.Document["id"])
	}

	if result.Document["data"] != "invalid json" {
		t.Errorf("Expected data 'invalid json', got '%v'", result.Document["data"])
	}
}

func TestBuildDeleteModel_WithFieldMapping(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "id",
			"partition_key": "partitionId",
		},
	}

	jsonData := `{"partitionId": "part_123"}`
	event := couchbase.NewDeleteEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := buildDeleteModel(mapping, event)

	if result.Table != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", result.Table)
	}

	if result.Operation != cassandra.Delete {
		t.Errorf("Expected operation Delete, got %s", result.Operation)
	}

	if result.Filter["id"] != "doc_key" {
		t.Errorf("Expected filter id 'doc_key', got '%v'", result.Filter["id"])
	}

	if result.Filter["partition_key"] != "part_123" {
		t.Errorf("Expected filter partition_key 'part_123', got '%v'", result.Filter["partition_key"])
	}
}

func TestSetCollectionTableMappings(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id": "id",
		},
	}

	mappings := []config.CollectionTableMapping{mapping}

	SetCollectionTableMappings(&mappings)

	if collectionTableMappings == nil {
		t.Error("Expected collectionTableMappings to be set")
	}

	if len(*collectionTableMappings) != 1 {
		t.Errorf("Expected 1 mapping, got %d", len(*collectionTableMappings))
	}

	if len(mappingCache) != 0 {
		t.Errorf("Expected cache to be empty, got %d entries", len(mappingCache))
	}
}
