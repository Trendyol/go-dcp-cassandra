package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestDefaultMapper_Deletion_PrimaryKeyFields(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "pk_collection",
			TableName:  "pk_table",
			FieldMappings: map[string]string{
				"id":     "_key",
				"name":   "name",
				"status": "status",
			},
			PrimaryKeyFields: []string{"id"},
		},
	}
	SetCollectionTableMappings(&mappings)

	jsonData := `{"name": "test_name", "status": "active"}`
	event := couchbase.NewDeleteEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"pk_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	assert.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok)

	assert.Equal(t, cassandra.Delete, rawModel.Operation)
	assert.Equal(t, "doc_key", rawModel.Filter["id"])
	assert.NotContains(t, rawModel.Filter, "name", "non-PK field should not be in delete filter")
	assert.NotContains(t, rawModel.Filter, "status", "non-PK field should not be in delete filter")
}

func TestDefaultMapper_Deletion_NoPrimaryKeyFields_FallsBack(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "fallback_collection",
			TableName:  "fallback_table",
			FieldMappings: map[string]string{
				"id":   "_key",
				"name": "name",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	jsonData := `{"name": "test_name"}`
	event := couchbase.NewDeleteEvent(
		[]byte("doc_key"),
		[]byte(jsonData),
		"fallback_collection",
		time.Now(),
		123,
		1,
	)

	result := DefaultMapper(event)

	assert.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	assert.True(t, ok)

	assert.Equal(t, "doc_key", rawModel.Filter["id"])
	assert.Equal(t, "test_name", rawModel.Filter["name"], "without PrimaryKeyFields all mapped fields should be included")
}

func TestDefaultMapper_DefaultCollectionFallback(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "_default",
			TableName:  "catch_all",
			FieldMappings: map[string]string{
				"id":   "_key",
				"data": "documentData",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("fallback_key"),
		[]byte(`{"name":"test"}`),
		"unknown_collection",
		time.Now(),
		1,
		0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	require.True(t, ok)
	assert.Equal(t, "catch_all", rawModel.Table, "_default mapping should be used as fallback")
	assert.Equal(t, "fallback_key", rawModel.Document["id"])
}

func TestDefaultMapper_NestedFieldMissing(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "nested_miss",
			TableName:  "example_table",
			FieldMappings: map[string]string{
				"id":    "_key",
				"email": "contact.email",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("key1"),
		[]byte(`{"contact":{"phone":"555"}}`),
		"nested_miss",
		time.Now(),
		1,
		0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	rawModel, ok := result[0].(*cassandra.Raw)
	require.True(t, ok)
	assert.Equal(t, "key1", rawModel.Document["id"])
	assert.Nil(t, rawModel.Document["email"], "missing nested field should map to nil")
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
