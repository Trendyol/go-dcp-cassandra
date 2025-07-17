package cassandra

import (
	"fmt"
	config "go-dcp-cassandra/configs"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/models"
	"github.com/stretchr/testify/assert"
)

func TestBulkOperations(t *testing.T) {
	bulk := &Bulk{
		keyspace:            "test_keyspace",
		dcpCheckpointCommit: func() { t.Log("Checkpoint committed") },
	}

	t.Run("TestInsertOperation", func(t *testing.T) {
		model := &Raw{
			Table: "test_table",
			Document: map[string]interface{}{
				"claimid":     "claim-1",
				"claimitemid": "item-1",
				"date":        time.Now(),
				"name":        "Test Document",
			},
			Operation: Insert,
		}
		if model.Operation != Insert {
			t.Errorf("Expected operation to be Insert, got %v", model.Operation)
		}
	})

	t.Run("TestUpdateOperation", func(t *testing.T) {
		model := &Raw{
			Table: "test_table",
			Document: map[string]interface{}{
				"name": "Updated Document",
			},
			Filter: map[string]interface{}{
				"claimid":     "claim-1",
				"claimitemid": "item-1",
				"date":        time.Now(),
			},
			Operation: Update,
		}
		if model.Operation != Update {
			t.Errorf("Expected operation to be Update, got %v", model.Operation)
		}
	})

	t.Run("TestDeleteOperation", func(t *testing.T) {
		model := &Raw{
			Table: "test_table",
			Filter: map[string]interface{}{
				"claimid":     "claim-1",
				"claimitemid": "item-1",
				"date":        time.Now(),
			},
			Operation: Delete,
		}
		if model.Operation != Delete {
			t.Errorf("Expected operation to be Delete, got %v", model.Operation)
		}
	})

	t.Run("TestBatchDeduplication", func(t *testing.T) {
		key := bulk.getActionKey(&Raw{
			Table: "test",
			Document: map[string]interface{}{
				"claimid":     "claim-1",
				"claimitemid": "item-1",
				"date":        "2024-01-01T00:00:00Z",
			},
		})
		if key == "" {
			t.Errorf("Expected non-empty key")
		}
	})
}

func TestNewBulk_InvalidSession(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:    []string{"invalid-host"},
			Keyspace: "test_keyspace",
			Username: "cassandra",
			Password: "wrong",
			Timeout:  1 * time.Millisecond,
		},
	}
	_, err := NewBulk(cfg, func() {})
	if err == nil {
		t.Error("Expected error for invalid session, got nil")
	}
}

func TestInsert_PrimaryKeyMissing(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:    []string{"127.0.0.1"},
			Keyspace: "test_keyspace",
			Username: "cassandra",
			Password: "cassandra",
			Timeout:  5 * time.Second,
		},
	}
	bulk, _ := NewBulk(cfg, func() {})
	raw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{},
		Operation: Insert,
	}
	err := bulk.insert(raw)
	if err == nil {
		t.Error("Expected error for missing primary key")
	}
}

func TestGetActionKey(t *testing.T) {
	bulk := &Bulk{}
	raw := &Raw{
		Table:    "test",
		Document: map[string]interface{}{"id": "abc"},
	}
	key := bulk.getActionKey(raw)
	expected := "test:id=abc;"
	if key != expected {
		t.Errorf("Expected %s, got %s", expected, key)
	}
}

func TestGetPrimaryKeyFields(t *testing.T) {
	bulk := &Bulk{}
	raw := &Raw{}
	keys := bulk.getPrimaryKeyFields(raw)
	if len(keys) != 1 || keys[0] != "id" {
		t.Errorf("Expected [id], got %v", keys)
	}
}

func TestJoin(t *testing.T) {
	arr := []string{"a", "b", "c"}
	result := join(arr, ",")
	if result != "a,b,c" {
		t.Errorf("Expected a,b,c, got %s", result)
	}
}

type mockSession struct{}

func (m *mockSession) Query(string, ...interface{}) Query         { return &mockQuery{} }
func (m *mockSession) PreparedQuery(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSession) NewBatch(BatchType) Batch                   { return &mockBatch{} }
func (m *mockSession) Close()                                     {}

type mockQuery struct{}

func (m *mockQuery) Exec() error { return nil }

type mockBatch struct {
	size int
}

func (m *mockBatch) Query(string, ...interface{}) {
	m.size++
}

func (m *mockBatch) Size() int {
	return m.size
}

func (m *mockBatch) ExecuteBatch() error {
	return nil
}

type mockSessionErr struct{}

func (m *mockSessionErr) Query(string, ...interface{}) Query         { return &mockQueryErr{} }
func (m *mockSessionErr) PreparedQuery(string, ...interface{}) Query { return &mockQueryErr{} }
func (m *mockSessionErr) NewBatch(BatchType) Batch                   { return &mockBatchErr{} }
func (m *mockSessionErr) Close()                                     {}

type mockQueryErr struct{}

func (m *mockQueryErr) Exec() error { return fmt.Errorf("mock error") }

type mockBatchErr struct {
	size int
}

func (m *mockBatchErr) Query(string, ...interface{}) {
	m.size++
}

func (m *mockBatchErr) Size() int {
	return m.size
}

func (m *mockBatchErr) ExecuteBatch() error {
	return fmt.Errorf("mock batch error")
}

func TestBulk_WorkerProcessesBatch(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		jobCh:               make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
	}
	raw := &Raw{
		Table:    "test_table",
		Document: map[string]interface{}{"id": "1"},
	}
	batch := []BatchItem{{Model: raw, Size: 1, Ctx: nil}}
	bulk.wg.Add(1)
	go bulk.worker()
	bulk.jobCh <- batch
	close(bulk.jobCh)
	bulk.wg.Wait()
}

func TestBulk_Close(t *testing.T) {
	bulk := &Bulk{
		batchTicker: time.NewTicker(10 * time.Millisecond),
		shutdownCh:  make(chan struct{}),
		session:     &mockSession{},
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		bulk.Close()
	}()
}

func TestBulk_FlushMessages_DcpRebalancing(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		keyspace:            "test_keyspace",
		dcpCheckpointCommit: func() {},
		isDcpRebalancing:    1,
		batch:               make([]BatchItem, 0, 10),
		batchKeys:           make(map[string]int, 10),
		jobCh:               make(chan []BatchItem, 1),
		metric:              &Metric{},
	}
	bulk.flushMessages()
	if len(bulk.batch) != 0 {
		t.Error("Batch should remain unchanged when DCP rebalancing")
	}
}

func TestBulk_AddActions_DcpRebalancing(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		keyspace:            "test_keyspace",
		dcpCheckpointCommit: func() {},
		isDcpRebalancing:    1,
		batch:               make([]BatchItem, 0, 10),
		batchKeys:           make(map[string]int, 10),
		jobCh:               make(chan []BatchItem, 1),
		metric:              &Metric{},
	}
	actions := []Model{
		&Raw{Table: "test", Document: map[string]interface{}{"id": "1"}},
	}
	bulk.AddActions(nil, time.Now(), actions)
	if len(bulk.batch) != 0 {
		t.Error("Batch should remain unchanged when DCP rebalancing")
	}
}

func TestBulk_InsertUpdateDelete_Errors(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSession{},
		keyspace:           "ks",
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}
	raw := &Raw{
		Table:     "tbl",
		Document:  map[string]interface{}{},
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Insert,
	}
	if err := bulk.insert(raw); err == nil {
		t.Error("Expected error from insert (primary key missing)")
	}
	raw.Document = map[string]interface{}{"id": "1"}
	raw.Operation = Update
	if err := bulk.update(raw); err != nil {
		t.Errorf("Expected update to succeed, got error: %v", err)
	}
	raw.Operation = Delete
	if err := bulk.delete(raw); err != nil {
		t.Errorf("Expected delete to succeed, got error: %v", err)
	}
}

func TestBulk_FlushMessages_ResetsBatch(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		batch:               []BatchItem{{Model: &Raw{}, Ctx: nil}},
		batchKeys:           map[string]int{"a": 0},
		batchSizeLimit:      10,
		dcpCheckpointCommit: func() {},
		jobCh:               make(chan []BatchItem, 1),
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		metric:              &Metric{},
	}

	bulk.wg.Add(1)
	go bulk.worker()

	bulk.flushMessages()

	close(bulk.jobCh)
	bulk.wg.Wait()

	if len(bulk.batch) != 0 {
		t.Error("Batch should be reset after flush")
	}
}

func TestBulk_WorkerHandlesError(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSessionErr{},
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
		keyspace:           "test",
	}

	raw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"id": "1"},
		Operation: Insert,
	}

	err := bulk.insert(raw)
	assert.Error(t, err, "Expected error from mockSessionErr")
	assert.Contains(t, err.Error(), "mock error")
}

func TestBulk_ErrorHandling_Operations(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSessionErr{},
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
		keyspace:           "test",
	}

	insertRaw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"id": "1"},
		Operation: Insert,
	}
	err := bulk.insert(insertRaw)
	assert.Error(t, err)

	updateRaw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"field": "value"},
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Update,
	}
	err = bulk.update(updateRaw)
	assert.Error(t, err)

	deleteRaw := &Raw{
		Table:     "test_table",
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Delete,
	}
	err = bulk.delete(deleteRaw)
	assert.Error(t, err)
}

func TestBulk_AddActionsWithCtx(t *testing.T) {
	bulk := &Bulk{
		batch:               make([]BatchItem, 0, 10),
		batchKeys:           make(map[string]int, 10),
		batchSizeLimit:      10,
		batchByteSizeLimit:  10485760,
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
	}

	mockCtx := &models.ListenerContext{}
	raw := &Raw{
		Table:    "test_table",
		Document: map[string]interface{}{"id": "1"},
	}

	bulk.AddActions(mockCtx, time.Now(), []Model{raw})

	assert.Len(t, bulk.batch, 1)
	assert.Equal(t, mockCtx, bulk.batch[0].Ctx, "Ctx should be set in BatchItem")
}

func TestBulk_BatchItemCtxField(t *testing.T) {
	item := BatchItem{
		Model: &Raw{},
		Size:  1,
		Ctx:   &models.ListenerContext{},
	}

	assert.NotNil(t, item.Ctx, "Ctx field should not be nil")
	assert.Equal(t, 1, item.Size, "Size should be 1")
	assert.NotNil(t, item.Model, "Model should not be nil")
}

func TestBulk_PreparedStatementCaching(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSession{},
		keyspace:           "ks",
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	raw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"id": "1", "name": "test"},
		Operation: Insert,
	}

	cacheKey1 := fmt.Sprintf("INSERT:%s:%d", raw.Table, len(raw.Document))
	query1 := bulk.getCachedPreparedStatement(cacheKey1, raw, "INSERT")
	assert.NotEmpty(t, query1)

	query2 := bulk.getCachedPreparedStatement(cacheKey1, raw, "INSERT")
	assert.Equal(t, query1, query2)

	bulk.preparedStmtsMutex.RLock()
	_, exists := bulk.preparedStmts[cacheKey1]
	bulk.preparedStmtsMutex.RUnlock()
	assert.True(t, exists)
}

func TestBulk_BatchMode_ProcessBatchWithBatch(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSession{},
		keyspace:           "ks",
		useBatch:           true,
		batchType:          LoggedBatch,
		maxBatchSize:       10,
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	items := []BatchItem{
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"id": "1", "name": "test1"},
				Operation: Insert,
			},
			Ctx: &models.ListenerContext{Ack: func() {}},
		},
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"name": "updated"},
				Filter:    map[string]interface{}{"id": "2"},
				Operation: Update,
			},
			Ctx: &models.ListenerContext{Ack: func() {}},
		},
		{
			Model: &Raw{
				Table:     "test_table",
				Filter:    map[string]interface{}{"id": "3"},
				Operation: Delete,
			},
			Ctx: &models.ListenerContext{Ack: func() {}},
		},
	}

	err := bulk.processBatchWithBatch(items)
	assert.NoError(t, err)
}

func TestBulk_Worker_UseBatchMode(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		keyspace:            "ks",
		useBatch:            true,
		batchType:           UnloggedBatch,
		maxBatchSize:        5,
		jobCh:               make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		metric:              &Metric{},
	}

	items := []BatchItem{
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"id": "1", "name": "test"},
				Operation: Insert,
			},
			Ctx: &models.ListenerContext{Ack: func() {}},
		},
	}

	bulk.wg.Add(1)
	go bulk.worker()
	bulk.jobCh <- items
	close(bulk.jobCh)
	bulk.wg.Wait()

	assert.True(t, bulk.metric.BulkRequestProcessLatencyMs >= 0)
}

func TestBulk_GetBatchType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected BatchType
	}{
		{
			name:     "logged batch",
			input:    "logged",
			expected: LoggedBatch,
		},
		{
			name:     "unlogged batch",
			input:    "unlogged",
			expected: UnloggedBatch,
		},
		{
			name:     "counter batch",
			input:    "counter",
			expected: CounterBatch,
		},
		{
			name:     "empty defaults to logged",
			input:    "",
			expected: LoggedBatch,
		},
		{
			name:     "invalid defaults to logged",
			input:    "invalid",
			expected: LoggedBatch,
		},
		{
			name:     "case insensitive",
			input:    "UNLOGGED",
			expected: UnloggedBatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getBatchType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBulk_NewBulk_WithBatchConfig(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:               []string{"localhost"},
			Keyspace:            "test",
			UseBatch:            true,
			BatchType:           "unlogged",
			MaxBatchSize:        100,
			WorkerCount:         2,
			BatchSizeLimit:      50,
			BatchTickerDuration: 1 * time.Second,
		},
	}

	bulk, err := NewBulk(cfg, func() {})

	assert.Error(t, err)
	assert.Nil(t, bulk)
}

func TestBulk_PreparedStatementCache_ConcurrentAccess(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSession{},
		keyspace:           "test",
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			raw := &Raw{
				Table:     fmt.Sprintf("table%d", id%3),
				Document:  map[string]interface{}{"id": fmt.Sprintf("doc%d", id), "name": fmt.Sprintf("test%d", id)},
				Operation: Insert,
			}
			_ = bulk.insert(raw)
		}(i)
	}
	wg.Wait()

	if len(bulk.preparedStmts) == 0 {
		t.Error("Expected prepared statements to be cached")
	}
}

func TestBulk_BatchErrorHandling_NoCouchbaseCommit(t *testing.T) {
	commitCalled := false
	commitFn := func() {
		commitCalled = true
	}

	bulk := &Bulk{
		session:             &mockSessionErr{},
		keyspace:            "test",
		dcpCheckpointCommit: commitFn,
		metric:              &Metric{},
		useBatch:            true,
		batchType:           LoggedBatch,
		maxBatchSize:        100,
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
	}

	raw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"id": "1", "name": "test"},
		Operation: Insert,
	}

	done := make(chan struct{})
	batch := []BatchItem{
		{
			Model: raw,
			Size:  1,
			Ctx:   nil,
			Done:  done,
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Expected panic occurred: %v", r)
		} else {
			t.Error("Expected panic but none occurred")
		}

		select {
		case <-done:
			t.Log("Done channel was properly closed")
		default:
			t.Error("Done channel was not closed")
		}

		if commitCalled {
			t.Error("dcpCheckpointCommit should not be called on error")
		} else {
			t.Log("dcpCheckpointCommit was correctly not called")
		}
	}()

	bulk.processBatch(batch)
}
