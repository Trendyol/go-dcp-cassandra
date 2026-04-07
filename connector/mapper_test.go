package connector

import (
	"sync"
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

func TestSetCollectionTableMappings_ClearsCache(t *testing.T) {
	m1 := []config.CollectionTableMapping{
		{Collection: "items", TableName: "items_v1", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&m1)

	event := couchbase.NewMutateEvent(
		[]byte("key"), []byte(`{}`), "items", time.Now(), 1, 0,
	)
	assert.Equal(t, "items_v1", DefaultMapper(event)[0].(*cassandra.Raw).Table)

	m2 := []config.CollectionTableMapping{
		{Collection: "items", TableName: "items_v2", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&m2)

	assert.Equal(t, "items_v2", DefaultMapper(event)[0].(*cassandra.Raw).Table,
		"cache must be cleared when mappings are updated")
}

func TestDefaultMapper_FallbackToDefaultCollection(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{Collection: "_default", TableName: "fallback_table", FieldMappings: map[string]string{"id": "_key"}},
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

// --- getNestedField tests (strings.Cut refactoring) ---

func TestGetNestedField_SimpleField(t *testing.T) {
	doc := map[string]any{"name": "test"}
	val, ok := getNestedField(doc, "name")
	assert.True(t, ok)
	assert.Equal(t, "test", val)
}

func TestGetNestedField_OneLevelNested(t *testing.T) {
	doc := map[string]any{"meta": map[string]any{"version": "2.0"}}
	val, ok := getNestedField(doc, "meta.version")
	assert.True(t, ok)
	assert.Equal(t, "2.0", val)
}

func TestGetNestedField_DeepNested(t *testing.T) {
	doc := map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": map[string]any{"d": "deep_value"},
			},
		},
	}
	val, ok := getNestedField(doc, "a.b.c.d")
	assert.True(t, ok)
	assert.Equal(t, "deep_value", val)
}

func TestGetNestedField_MissingIntermediateKey(t *testing.T) {
	doc := map[string]any{"a": map[string]any{"x": "y"}}
	val, ok := getNestedField(doc, "a.b.c")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestGetNestedField_NonMapIntermediate(t *testing.T) {
	doc := map[string]any{"a": "not_a_map"}
	val, ok := getNestedField(doc, "a.b")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestGetNestedField_MissingTopLevel(t *testing.T) {
	doc := map[string]any{"x": "y"}
	val, ok := getNestedField(doc, "missing")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestGetNestedField_EmptyDocument(t *testing.T) {
	doc := map[string]any{}
	val, ok := getNestedField(doc, "any.field")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestDefaultMapper_DeepNestedField(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "docs",
			TableName:     "docs_table",
			FieldMappings: map[string]string{"id": "_key", "city": "address.home.city"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("doc_1"), []byte(`{"address":{"home":{"city":"Istanbul"}}}`),
		"docs", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, "Istanbul", raw.Document["city"])
}

func TestDefaultMapper_MissingNestedField(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "docs",
			TableName:     "docs_table",
			FieldMappings: map[string]string{"id": "_key", "missing": "a.b.c"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("doc_1"), []byte(`{"a":{"x":"y"}}`),
		"docs", time.Now(), 1, 0,
	)

	result := DefaultMapper(event)
	raw := result[0].(*cassandra.Raw)
	assert.Nil(t, raw.Document["missing"])
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
