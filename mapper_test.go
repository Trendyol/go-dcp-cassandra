package dcpcassandra

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

func TestMap_Mutation(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "orders",
			TableName:     "orders_table",
			FieldMappings: map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("order_1"), []byte(`{"status":"active"}`),
		"orders", time.Now(), 1, 0,
	)

	result := Map(event)
	assert.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Upsert, raw.Operation)
	assert.Equal(t, "orders_table", raw.Table)
	assert.Equal(t, "order_1", raw.Document["id"])
	assert.Equal(t, "active", raw.Document["status"])
}

func TestMap_Deletion(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "orders",
			TableName:     "orders_table",
			FieldMappings: map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("order_1"), []byte(`{"status":"deleted"}`),
		"orders", time.Now(), 1, 0,
	)

	result := Map(event)
	assert.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Equal(t, "order_1", raw.Filter["id"])
	assert.Equal(t, "deleted", raw.Filter["status"])
}

func TestMap_DeletionWithoutValue(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "orders",
			TableName:     "orders_table",
			FieldMappings: map[string]string{"id": "_key", "status": "status"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("order_1"), nil,
		"orders", time.Now(), 1, 0,
	)

	result := Map(event)
	assert.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Equal(t, "order_1", raw.Filter["id"])
	_, hasStatus := raw.Filter["status"]
	assert.False(t, hasStatus, "missing source field should not appear in filter")
}

func TestMap_Expiration(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "orders",
			TableName:     "orders_table",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewExpireEvent(
		[]byte("order_1"), nil,
		"orders", time.Now(), 1, 0,
	)

	result := Map(event)
	assert.Len(t, result, 1)

	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Equal(t, "order_1", raw.Filter["id"])
}

func TestMap_UnknownEvent(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "orders",
			TableName:     "orders_table",
			FieldMappings: map[string]string{"id": "_key"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.Event{CollectionName: "orders", Key: []byte("key")}
	result := Map(event)
	assert.Nil(t, result)
}

func TestMap_DocumentDataField(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "docs",
			TableName:     "docs_table",
			FieldMappings: map[string]string{"id": "_key", "data": "documentData"},
		},
	}
	SetCollectionTableMappings(&mappings)

	payload := `{"some":"json"}`
	event := couchbase.NewMutateEvent(
		[]byte("doc_1"), []byte(payload),
		"docs", time.Now(), 1, 0,
	)

	result := Map(event)
	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, payload, raw.Document["data"])
}

func TestMap_UnknownCollection_Panics(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{Collection: "known", TableName: "t", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&mappings)

	assert.Panics(t, func() {
		event := couchbase.NewMutateEvent(
			[]byte("key"), []byte(`{}`), "unknown",
			time.Now(), 1, 0,
		)
		Map(event)
	})
}

func TestSetCollectionTableMappings_ClearsCache(t *testing.T) {
	m1 := []config.CollectionTableMapping{
		{Collection: "col", TableName: "v1", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&m1)

	event := couchbase.NewMutateEvent(
		[]byte("key"), []byte(`{}`), "col", time.Now(), 1, 0,
	)
	assert.Equal(t, "v1", Map(event)[0].(*cassandra.Raw).Table)

	m2 := []config.CollectionTableMapping{
		{Collection: "col", TableName: "v2", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&m2)

	assert.Equal(t, "v2", Map(event)[0].(*cassandra.Raw).Table,
		"cache must be cleared when mappings are updated")
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

func TestMap_NestedField(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection:    "docs",
			TableName:     "docs_table",
			FieldMappings: map[string]string{"id": "_key", "version": "meta.version"},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("doc_1"), []byte(`{"meta":{"version":"2.0"}}`),
		"docs", time.Now(), 1, 0,
	)

	result := Map(event)
	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, "2.0", raw.Document["version"])
}

func TestMap_DeepNestedField(t *testing.T) {
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

	result := Map(event)
	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, "Istanbul", raw.Document["city"])
}

// --- convertFieldValue tests ---

func TestConvertFieldValue_DateFloat(t *testing.T) {
	v := convertFieldValue("date", 1234.56)
	assert.Equal(t, 1234.56, v)
}

func TestConvertFieldValue_DateString(t *testing.T) {
	v := convertFieldValue("date", "42.5")
	assert.Equal(t, 42.5, v)
}

func TestConvertFieldValue_DateInvalidString(t *testing.T) {
	v := convertFieldValue("date", "not_a_number")
	assert.Equal(t, 0.0, v)
}

func TestConvertFieldValue_DateInt(t *testing.T) {
	v := convertFieldValue("date", 100)
	assert.Equal(t, float64(100), v)
}

func TestConvertFieldValue_NonDateField(t *testing.T) {
	v := convertFieldValue("name", "hello")
	assert.Equal(t, "hello", v)
}

// --- Concurrency tests ---

func TestConcurrentMapperAccess(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{Collection: "col_a", TableName: "table_a", FieldMappings: map[string]string{"id": "_key"}},
		{Collection: "col_b", TableName: "table_b", FieldMappings: map[string]string{"id": "_key"}},
		{Collection: "col_c", TableName: "table_c", FieldMappings: map[string]string{"id": "_key"}},
	}
	SetCollectionTableMappings(&mappings)

	collections := []string{"col_a", "col_b", "col_c"}
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		col := collections[i%len(collections)]
		wg.Go(func() {
			event := couchbase.NewMutateEvent(
				[]byte("key"), []byte(`{"id":"1"}`), col,
				time.Now(), 1, 1,
			)
			result := Map(event)
			assert.NotNil(t, result)
			assert.Len(t, result, 1)
		})
	}
	wg.Wait()
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
			result := Map(event)
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
