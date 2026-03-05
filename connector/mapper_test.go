package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

func TestDefaultMapper_Mutation(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "_key",
				"data": "documentData",
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

	assert.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok)

	assert.Equal(t, "example_table", rawModel.Table)
	assert.Equal(t, cassandra.Upsert, rawModel.Operation)
	assert.Equal(t, "doc_key", rawModel.Document["id"])
	assert.Equal(t, jsonData, rawModel.Document["data"])
}

func TestDefaultMapper_Deletion(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "_key",
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

	assert.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok)

	assert.Equal(t, "example_table", rawModel.Table)
	assert.Equal(t, cassandra.Delete, rawModel.Operation)
	assert.Equal(t, "doc_key", rawModel.Filter["id"])
}

func TestDefaultMapper_Expiration(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "_key",
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

	assert.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok)

	assert.Equal(t, "example_table", rawModel.Table)
	assert.Equal(t, cassandra.Delete, rawModel.Operation)
	assert.Equal(t, "doc_key", rawModel.Filter["id"])
}

func TestDefaultMapper_UnknownEvent(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "test_collection",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":   "_key",
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
