package connector

import (
	"testing"
	"time"

	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMapper_Mutation(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "id",
				"data": "documentData", // Special field that gets the full JSON
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	jsonData := `{"id": "meta_id_123", "partitionId": "part_123", "status": "active", "meta": {"id": "meta_id_123", "version": "1.0"}}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	assert.Len(t, result, 1, "Expected 1 model")

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok, "Expected *cassandra.Raw model")

	assert.Equal(t, "example_table", rawModel.Table, "Expected table 'example_table'")
	assert.Equal(t, cassandra.Upsert, rawModel.Operation, "Expected operation Upsert")
	assert.Equal(t, "doc_key", rawModel.Document["id"], "Expected id 'doc_key'")
	assert.Equal(t, jsonData, rawModel.Document["data"], "Expected data to match input")
}

func TestDefaultMapper_Deletion(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "id",
				"data": "documentData",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

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

	assert.Len(t, result, 1, "Expected 1 model")

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok, "Expected *cassandra.Raw model")

	assert.Equal(t, "example_table", rawModel.Table, "Expected table 'example_table'")
	assert.Equal(t, cassandra.Delete, rawModel.Operation, "Expected operation Delete")
	assert.Equal(t, "doc_key", rawModel.Filter["id"], "Expected id 'doc_key' in filter")
}

func TestDefaultMapper_Expiration(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "id",
				"data": "documentData",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewExpireEvent(
		[]byte("doc_key"),
		nil,
		"test_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	assert.Len(t, result, 1, "Expected 1 model")

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok, "Expected *cassandra.Raw model")

	assert.Equal(t, "example_table", rawModel.Table, "Expected table 'example_table'")
	assert.Equal(t, cassandra.Delete, rawModel.Operation, "Expected operation Delete")
	assert.Equal(t, "doc_key", rawModel.Filter["id"], "Expected id 'doc_key' in filter")
}

func TestDefaultMapper_UnknownEvent(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "id",
				"data": "documentData",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

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

	assert.Len(t, result, 0, "Expected 0 models for unknown event")
}
