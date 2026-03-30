package cassandra

import (
	"fmt"
	"sync"
	"testing"
	"time"

	dcpmodels "github.com/Trendyol/go-dcp/models"
	"github.com/stretchr/testify/assert"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
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

func TestGetActionKey(t *testing.T) {
	bulk := &Bulk{}

	t.Run("insert uses document fields", func(t *testing.T) {
		raw := &Raw{
			Table:     "test",
			Document:  map[string]interface{}{"id": "abc", "name": "x"},
			Operation: Insert,
		}
		key := bulk.getActionKey(raw)
		assert.Equal(t, "test:id=abc;name=x;", key)
	})

	t.Run("update uses filter fields", func(t *testing.T) {
		raw := &Raw{
			Table:     "test",
			Document:  map[string]interface{}{"name": "x"},
			Filter:    map[string]interface{}{"product_id": 1, "culture": "tr"},
			Operation: Update,
		}
		key := bulk.getActionKey(raw)
		assert.Equal(t, "test:culture=tr;product_id=1;", key)
	})

	t.Run("delete uses filter fields", func(t *testing.T) {
		raw := &Raw{
			Table:     "test",
			Filter:    map[string]interface{}{"product_id": 1},
			Operation: Delete,
		}
		key := bulk.getActionKey(raw)
		assert.Equal(t, "test:product_id=1;", key)
	})

	t.Run("empty source falls back to batch index", func(t *testing.T) {
		raw := &Raw{
			Table:     "test",
			Operation: Insert,
		}
		key := bulk.getActionKey(raw)
		assert.Equal(t, "batch:0", key)
	})
}

func TestBulk_Insert_NilSession(t *testing.T) {
	bulk := &Bulk{}
	raw := &Raw{
		Table:     "tbl",
		Document:  map[string]interface{}{"product_id": 1},
		Operation: Insert,
	}
	err := bulk.insert(raw, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session is nil")
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

func (m *mockBatch) WithTimestamp(int64) {}

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

func (m *mockBatchErr) WithTimestamp(int64) {}

func (m *mockBatchErr) Size() int {
	return m.size
}

func (m *mockBatchErr) ExecuteBatch() error {
	return fmt.Errorf("mock batch error")
}

type captureBatch struct {
	queries      []string
	size         int
	hasTimestamp bool
	timestamp    int64
	onExecute    *int
}

func (c *captureBatch) Query(stmt string, values ...interface{}) {
	c.queries = append(c.queries, stmt)
	c.size++
}

func (c *captureBatch) WithTimestamp(timestamp int64) {
	c.hasTimestamp = true
	c.timestamp = timestamp
}

func (c *captureBatch) Size() int {
	return c.size
}

func (c *captureBatch) ExecuteBatch() error {
	if c.onExecute != nil {
		*c.onExecute++
	}
	return nil
}

type captureSession struct {
	lastBatch             *captureBatch
	executeBatchCallCount int
}

func (c *captureSession) Query(string, ...interface{}) Query         { return &mockQuery{} }
func (c *captureSession) PreparedQuery(string, ...interface{}) Query { return &mockQuery{} }
func (c *captureSession) Close()                                     {}
func (c *captureSession) NewBatch(BatchType) Batch {
	c.lastBatch = &captureBatch{onExecute: &c.executeBatchCallCount}
	return c.lastBatch
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
	batch := []BatchItem{{Model: raw, Size: 1}}
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
	bulk.AddActions(nil, time.Now(), 0, actions)
	if len(bulk.batch) != 0 {
		t.Error("Batch should remain unchanged when DCP rebalancing")
	}
}

func TestBulk_InsertUpdateDelete_Success(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSession{},
		keyspace:           "ks",
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	raw := &Raw{
		Table:     "tbl",
		Document:  map[string]interface{}{"product_id": 1, "culture": "tr"},
		Operation: Insert,
	}
	assert.NoError(t, bulk.insert(raw, nil))

	raw.Operation = Update
	raw.Filter = map[string]interface{}{"product_id": 1}
	assert.NoError(t, bulk.update(raw, nil))

	raw.Operation = Delete
	assert.NoError(t, bulk.delete(raw, nil))
}

func TestBulk_FlushMessages_ResetsBatch(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		batch:               []BatchItem{{Model: &Raw{}}},
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

	err := bulk.insert(raw, nil)
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
	err := bulk.insert(insertRaw, nil)
	assert.Error(t, err)

	updateRaw := &Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"field": "value"},
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Update,
	}
	err = bulk.update(updateRaw, nil)
	assert.Error(t, err)

	deleteRaw := &Raw{
		Table:     "test_table",
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Delete,
	}
	err = bulk.delete(deleteRaw, nil)
	assert.Error(t, err)
}

func TestBulk_BatchItemCtxField(t *testing.T) {
	item := BatchItem{
		Model: &Raw{},
		Size:  1,
	}

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

	cacheKey1 := fmt.Sprintf("INSERT:%s:%d:%t", raw.Table, len(raw.Document), false)
	query1 := bulk.getCachedPreparedStatement(cacheKey1, raw, "INSERT", false)
	assert.NotEmpty(t, query1)

	query2 := bulk.getCachedPreparedStatement(cacheKey1, raw, "INSERT", false)
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
		},
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"name": "updated"},
				Filter:    map[string]interface{}{"id": "2"},
				Operation: Update,
			},
		},
		{
			Model: &Raw{
				Table:     "test_table",
				Filter:    map[string]interface{}{"id": "3"},
				Operation: Delete,
			},
		},
	}

	err := bulk.processWithCqlBatch(items)
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
			_ = bulk.insert(raw, nil)
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

func TestBulk_AddActions_AckAfterWrite_Global(t *testing.T) {
	ackCalled := false
	bulk := &Bulk{
		session:             &mockSession{},
		jobCh:               make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		batch:               make([]BatchItem, 0, 10),
		batchKeys:           make(map[string]int, 10),
		batchSizeLimit:      10,
		batchByteSizeLimit:  1024,
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
	}

	bulk.wg.Add(1)
	go bulk.worker()

	ctx := &dcpmodels.ListenerContext{Ack: func() { ackCalled = true }}
	actions := []Model{
		&Raw{Table: "test_table", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	}

	bulk.AddActions(ctx, time.Now(), 7, actions)
	assert.False(t, ackCalled)

	bulk.flushMessages()
	assert.True(t, ackCalled)

	close(bulk.jobCh)
	bulk.wg.Wait()
}

func TestBulk_AddActions_AckImmediate_Global(t *testing.T) {
	ackCalled := false
	bulk := &Bulk{
		session:            &mockSession{},
		batch:              make([]BatchItem, 0, 10),
		batchKeys:          make(map[string]int, 10),
		batchSizeLimit:     10,
		batchByteSizeLimit: 1024,
		ackMode:            ackModeImmediate,
		metric:             &Metric{},
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	ctx := &dcpmodels.ListenerContext{Ack: func() { ackCalled = true }}
	actions := []Model{
		&Raw{Table: "test_table", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	}

	bulk.AddActions(ctx, time.Now(), 7, actions)
	assert.True(t, ackCalled)
}

func TestBulk_AddActions_AckAfterWrite_EventScope(t *testing.T) {
	ackCalled := false
	bulk := &Bulk{
		session:             &mockSession{},
		jobCh:               make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		batchScope:          batchScopeEvent,
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
	}

	bulk.wg.Add(1)
	go bulk.worker()

	ctx := &dcpmodels.ListenerContext{Ack: func() { ackCalled = true }}
	actions := []Model{
		&Raw{Table: "test_table", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
		&Raw{Table: "test_table", Document: map[string]interface{}{"id": "2"}, Operation: Insert},
	}

	bulk.AddActions(ctx, time.Now(), 7, actions)
	assert.True(t, ackCalled)

	close(bulk.jobCh)
	bulk.wg.Wait()
}

func TestBulk_PreparedStatement_WithTimestamp(t *testing.T) {
	bulk := &Bulk{
		keyspace:           "ks",
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	insert := &Raw{Table: "tbl", Document: map[string]interface{}{"id": "1"}}
	update := &Raw{Table: "tbl", Document: map[string]interface{}{"name": "v"}, Filter: map[string]interface{}{"id": "1"}}
	deleteModel := &Raw{Table: "tbl", Filter: map[string]interface{}{"id": "1"}}

	insertQ := bulk.getCachedPreparedStatement("i", insert, "INSERT", true)
	updateQ := bulk.getCachedPreparedStatement("u", update, "UPDATE", true)
	deleteQ := bulk.getCachedPreparedStatement("d", deleteModel, "DELETE", true)

	assert.Contains(t, insertQ, "USING TIMESTAMP ?")
	assert.Contains(t, updateQ, "USING TIMESTAMP ?")
	assert.Contains(t, deleteQ, "USING TIMESTAMP ?")
}

func TestBulk_PreparedStatementCacheKey_UsesColumnNames(t *testing.T) {
	insertA := buildInsertCacheKey("tbl", map[string]interface{}{"id": 1, "name": "a"}, false)
	insertB := buildInsertCacheKey("tbl", map[string]interface{}{"id": 1, "title": "a"}, false)
	assert.NotEqual(t, insertA, insertB)

	updateA := buildUpdateCacheKey(
		"tbl",
		map[string]interface{}{"name": "a"},
		map[string]interface{}{"id": 1, "lang": "tr"},
		false,
	)
	updateB := buildUpdateCacheKey(
		"tbl",
		map[string]interface{}{"title": "a"},
		map[string]interface{}{"id": 1, "lang": "tr"},
		false,
	)
	assert.NotEqual(t, updateA, updateB)

	deleteA := buildDeleteCacheKey("tbl", map[string]interface{}{"id": 1, "lang": "tr"}, false)
	deleteB := buildDeleteCacheKey("tbl", map[string]interface{}{"id": 1, "region": "eu"}, false)
	assert.NotEqual(t, deleteA, deleteB)
}

func TestBulk_ProcessBatchWithBatch_UsesBatchTimestampInEventScope(t *testing.T) {
	session := &captureSession{}
	ts := int64(123456789)
	bulk := &Bulk{
		session:            session,
		keyspace:           "ks",
		useBatch:           true,
		batchScope:         batchScopeEvent,
		batchType:          LoggedBatch,
		maxBatchSize:       10,
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	items := []BatchItem{{
		Model: &Raw{
			Table:     "test_table",
			Document:  map[string]interface{}{"id": "1", "name": "v"},
			Operation: Insert,
		},
		TimestampMicros: &ts,
	}}

	err := bulk.processWithCqlBatch(items)
	assert.NoError(t, err)
	assert.NotNil(t, session.lastBatch)
	assert.True(t, session.lastBatch.hasTimestamp)
	assert.Equal(t, ts, session.lastBatch.timestamp)
	assert.Len(t, session.lastBatch.queries, 1)
	assert.NotContains(t, session.lastBatch.queries[0], "USING TIMESTAMP ?")
}

func TestBulk_ProcessWithCqlBatch_DoesNotSplitInEventScope(t *testing.T) {
	session := &captureSession{}
	ts := int64(123456789)
	bulk := &Bulk{
		session:            session,
		keyspace:           "ks",
		useBatch:           true,
		batchScope:         batchScopeEvent,
		batchType:          LoggedBatch,
		maxBatchSize:       1,
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
	}

	items := []BatchItem{
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"id": "1", "name": "v1"},
				Operation: Insert,
			},
			TimestampMicros: &ts,
		},
		{
			Model: &Raw{
				Table:     "test_table",
				Document:  map[string]interface{}{"id": "2", "name": "v2"},
				Operation: Insert,
			},
			TimestampMicros: &ts,
		},
	}

	err := bulk.processWithCqlBatch(items)
	assert.NoError(t, err)
	assert.Equal(t, 1, session.executeBatchCallCount)
	assert.Len(t, session.lastBatch.queries, 2)
}
