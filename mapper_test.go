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
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "meta.id",
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123", "meta": {"id": "meta_id_123", "version": "1.0"}}`
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

	if rawModel.Document["id"] != "meta_id_123" {
		t.Errorf("Expected id 'meta_id_123', got '%v'", rawModel.Document["id"])
	}

	if rawModel.Document["partition_key"] != "part_123" {
		t.Errorf("Expected partition_key 'part_123', got '%v'", rawModel.Document["partition_key"])
	}

	if rawModel.Document["data"] != jsonData {
		t.Errorf("Expected data '%s', got '%v'", jsonData, rawModel.Document["data"])
	}

	expectedMeta := map[string]interface{}{"id": "meta_id_123", "version": "1.0"}
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
			"status":        "status",
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
		IsMutated:      false,
	}

	result := DefaultMapper(event)

	if result != nil {
		t.Errorf("Expected nil result for unknown event type, got %v", result)
	}
}

func TestDefaultMapper_AutomaticEventKeyId(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123", "meta": {"id": "meta_id_123", "version": "1.0"}}`
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

	if rawModel.Document["id"] != "doc_key" {
		t.Errorf("Expected id from event.Key 'doc_key', got '%v'", rawModel.Document["id"])
	}
}

func TestDefaultMapper_ExplicitIdMapping(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"id":            "customId",
			"partition_key": "partitionId",
			"data":          "documentData",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123", "customId": "custom_123", "meta": {"id": "meta_id_123"}}`
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

	if rawModel.Document["id"] != "custom_123" {
		t.Errorf("Expected id from customId 'custom_123', got '%v'", rawModel.Document["id"])
	}
}

func TestDefaultMapper_FallbackToEventKey(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"partition_key": "partitionId",
			"data":          "documentData",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123"}`
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

	if rawModel.Document["id"] != "doc_key" {
		t.Errorf("Expected id from event.Key 'doc_key', got '%v'", rawModel.Document["id"])
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
			"id":            "meta.id",
			"partition_key": "partitionId",
			"data":          "documentData",
			"metadata":      "meta",
		},
	}

	jsonData := `{"partitionId": "part_123", "meta": {"id": "meta_id_123", "version": "1.0"}}`
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

	if result.Document["id"] != "meta_id_123" {
		t.Errorf("Expected id 'meta_id_123', got '%v'", result.Document["id"])
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

func TestGetNestedField(t *testing.T) {
	document := map[string]interface{}{
		"simple": "value",
		"meta": map[string]interface{}{
			"id":      "nested_id",
			"version": "1.0",
		},
	}

	value, exists := getNestedField(document, "simple")
	if !exists || value != "value" {
		t.Errorf("Expected simple field 'value', got '%v' (exists: %v)", value, exists)
	}

	value, exists = getNestedField(document, "meta.id")
	if !exists || value != "nested_id" {
		t.Errorf("Expected nested field 'nested_id', got '%v' (exists: %v)", value, exists)
	}

	value, exists = getNestedField(document, "nonexistent")
	if exists {
		t.Errorf("Expected non-existent field to return false, got '%v'", value)
	}

	value, exists = getNestedField(document, "meta.nonexistent")
	if exists {
		t.Errorf("Expected non-existent nested field to return false, got '%v'", value)
	}
}

func TestConvertFieldValue(t *testing.T) {
	result := convertFieldValue("date", float64(1576682819416))
	if result != float64(1576682819416) {
		t.Errorf("Expected float64 1576682819416, got %v", result)
	}

	result = convertFieldValue("date", "1576682819416")
	if result != float64(1576682819416) {
		t.Errorf("Expected float64 1576682819416, got %v", result)
	}

	result = convertFieldValue("date", int(1576682819416))
	if result != float64(1576682819416) {
		t.Errorf("Expected float64 1576682819416, got %v", result)
	}

	result = convertFieldValue("date", int64(1576682819416))
	if result != float64(1576682819416) {
		t.Errorf("Expected float64 1576682819416, got %v", result)
	}

	result = convertFieldValue("date", "invalid")
	if result != 0.0 {
		t.Errorf("Expected 0.0 for invalid string, got %v", result)
	}

	result = convertFieldValue("other_field", "test_value")
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got %v", result)
	}
}

func TestDefaultMapper_WithDateConversion(t *testing.T) {
	mapping := config.CollectionTableMapping{
		Collection: "test_collection",
		TableName:  "test_table",
		FieldMappings: map[string]string{
			"partition_key": "partitionId",
			"date_field":    "date",
		},
	}

	SetCollectionTableMappings(&[]config.CollectionTableMapping{mapping})

	jsonData := `{"partitionId": "part_123", "date": "1576682819416"}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)
	rawModel := result[0].(*cassandra.Raw)

	if rawModel.Document["date_field"] != float64(1576682819416) {
		t.Errorf("Expected date_field to be float64 1576682819416, got %v", rawModel.Document["date_field"])
	}
}
