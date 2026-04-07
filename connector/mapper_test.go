package connector

import (
	"fmt"
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

// --- mappingCache race: concurrent vBucket goroutines ---
// Bug: mappingCache was a plain map accessed from 1024 parallel vBucket
// goroutines without synchronization. Under -race, concurrent map reads/writes
// are detected. This test fails with -race on master and passes with RWMutex.

func TestConcurrentMapperAccess(t *testing.T) {
	mappings := make([]config.CollectionTableMapping, 10)
	for i := range mappings {
		mappings[i] = config.CollectionTableMapping{
			Collection: fmt.Sprintf("col_%d", i),
			TableName:  fmt.Sprintf("table_%d", i),
			FieldMappings: map[string]string{
				"id": "_key",
			},
		}
	}
	SetCollectionTableMappings(&mappings)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			col := fmt.Sprintf("col_%d", idx%10)
			event := couchbase.NewMutateEvent(
				[]byte(fmt.Sprintf("key_%d", idx)),
				[]byte(`{"id":"1"}`),
				col,
				time.Now(),
				uint64(idx),
				uint16(idx%1024),
			)
			result := DefaultMapper(event)
			assert.Len(t, result, 1)
		}(i)
	}
	wg.Wait()
}

func TestDefaultMapper_DefaultCollectionFallback(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{
			Collection: "_default",
			TableName:  "fallback_table",
			FieldMappings: map[string]string{
				"id": "_key",
			},
		},
	}
	SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("doc_key"),
		[]byte(`{"id":"1"}`),
		"unknown_collection",
		time.Now(),
		123,
		1,
	)
	result := DefaultMapper(event)
	assert.Len(t, result, 1)
	raw := result[0].(*cassandra.Raw)
	assert.Equal(t, "fallback_table", raw.Table)
}
