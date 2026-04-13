package connector

import (
	"sync"
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

// Regression: concurrent mapper access must not race on mappingCache.
func TestConcurrentMapperAccess(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "col_a",
			TableName:     "table_a",
			FieldMappings: map[string]string{"id": "_key"},
		},
		{
			Collection:    "col_b",
			TableName:     "table_b",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		col := "col_a"
		if i%2 == 0 {
			col = "col_b"
		}
		wg.Go(func() {
			event := couchbase.NewMutateEvent(
				[]byte("key"), []byte(`{"id":"1"}`), col,
				time.Now(), 1, 1,
			)
			result := DefaultMapper(event)
			assert.NotNil(t, result)
		})
	}
	wg.Wait()
}

// Regression: SetCollectionTableMappings must clear the cache so
// changed mappings take effect.
func TestSetCollectionTableMappings_ClearsCache(t *testing.T) {
	mappings1 := []config.CollectionTableMapping{
		{
			Collection:    "items",
			TableName:     "items_v1",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings1)

	event := couchbase.NewMutateEvent(
		[]byte("key"), []byte(`{}`), "items",
		time.Now(), 1, 0,
	)
	result1 := DefaultMapper(event)
	assert.Equal(t, "items_v1", result1[0].(*cassandra.Raw).Table)

	mappings2 := []config.CollectionTableMapping{
		{
			Collection:    "items",
			TableName:     "items_v2",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings2)

	result2 := DefaultMapper(event)
	assert.Equal(t, "items_v2", result2[0].(*cassandra.Raw).Table,
		"cache must be cleared when mappings are updated")
}

// Regression: connector mapper falls back to default/empty collection
// mapping when no exact match is found.
func TestDefaultMapper_FallbackToDefaultCollection(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "_default",
			TableName:     "fallback_table",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("key"), []byte(`{}`), "any_unknown_collection",
		time.Now(), 1, 0,
	)
	result := DefaultMapper(event)
	assert.Len(t, result, 1)
	assert.Equal(t, "fallback_table", result[0].(*cassandra.Raw).Table)
}

// Regression: concurrent mapping updates + reads must not race.
func TestConcurrentMapperAccess_WithMappingUpdate(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{Collection: "col", TableName: "table", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&mappings)

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Go(func() {
			event := couchbase.NewMutateEvent(
				[]byte("key"), []byte(`{}`), "col",
				time.Now(), 1, 1,
			)
			result := DefaultMapper(event)
			assert.Len(t, result, 1)
		})
	}

	for i := 0; i < 10; i++ {
		wg.Go(func() {
			m := []config.CollectionTableMapping{
				{Collection: "col", TableName: "table", FieldMappings: map[string]string{"id": "_key"}},
			}
			SetCollectionTableMappings(&m)
		})
	}

	wg.Wait()
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

// --- PrimaryKeyFields tests (tombstone reduction) ---

func TestDefaultMapper_PrimaryKeyFields_Delete(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:       "orders",
			TableName:        "orders_table",
			PrimaryKeyFields: []string{"id", "partition_id"},
			FieldMappings: map[string]string{
				"id":           "_key",
				"partition_id": "partitionId",
				"status":       "status",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("order_1"),
		[]byte(`{"partitionId":"p1","status":"active"}`),
		"orders", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Equal(t, "order_1", raw.Filter["id"])
	assert.Equal(t, "p1", raw.Filter["partition_id"])

	_, hasStatus := raw.Filter["status"]
	assert.False(t, hasStatus, "non-PK field must be excluded")
}

func TestDefaultMapper_NoPrimaryKeyFields_Delete(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "items",
			TableName:     "items_table",
			FieldMappings: map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("item_1"),
		[]byte(`{"status":"deleted"}`),
		"items", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, "item_1", raw.Filter["id"])
	assert.Equal(t, "deleted", raw.Filter["status"],
		"without PrimaryKeyFields all fields should be included")
}

func TestDefaultMapper_PrimaryKeyFields_AllPK(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:       "events",
			TableName:        "events_table",
			PrimaryKeyFields: []string{"id", "ts"},
			FieldMappings:    map[string]string{"id": "_key", "ts": "timestamp"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("ev_1"),
		[]byte(`{"timestamp":"2024-01-01"}`),
		"events", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Len(t, raw.Filter, 2, "all fields are PK so all should remain")
}

func TestDefaultMapper_PrimaryKeyFields_Expiration(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:       "orders",
			TableName:        "orders_table",
			PrimaryKeyFields: []string{"id"},
			FieldMappings:    map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewExpireEvent(
		[]byte("order_1"), nil,
		"orders", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Len(t, raw.Filter, 1, "only PK field should remain for expiration")
	assert.Equal(t, "order_1", raw.Filter["id"])
}

func TestDefaultMapper_PrimaryKeyFields_SinglePK(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:       "docs",
			TableName:        "docs_table",
			PrimaryKeyFields: []string{"id"},
			FieldMappings:    map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("doc_1"),
		[]byte(`{"status":"x"}`),
		"docs", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	require.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Len(t, raw.Filter, 1, "only PK field should remain")
	assert.Equal(t, "doc_1", raw.Filter["id"])
}
