package cassandra

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/models"
	"github.com/stretchr/testify/assert"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
)

func newListenerContext(ackFn func()) *models.ListenerContext {
	return &models.ListenerContext{Ack: ackFn}
}

func TestBulkOperations(t *testing.T) {
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
		assert.Equal(t, Insert, model.Operation)
	})

	t.Run("TestUpdateOperation", func(t *testing.T) {
		model := &Raw{
			Table:     "test_table",
			Document:  map[string]interface{}{"name": "Updated Document"},
			Filter:    map[string]interface{}{"claimid": "claim-1"},
			Operation: Update,
		}
		assert.Equal(t, Update, model.Operation)
	})

	t.Run("TestDeleteOperation", func(t *testing.T) {
		model := &Raw{
			Table:     "test_table",
			Filter:    map[string]interface{}{"claimid": "claim-1"},
			Operation: Delete,
		}
		assert.Equal(t, Delete, model.Operation)
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
	assert.Error(t, err)
}

func TestBulk_Insert_NilSession(t *testing.T) {
	bulk := &Bulk{}
	raw := &Raw{
		Table:     "tbl",
		Document:  map[string]interface{}{"product_id": 1},
		Operation: Insert,
	}
	err := bulk.insert(raw)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session is nil")
}

func TestJoin(t *testing.T) {
	assert.Equal(t, "a,b,c", join([]string{"a", "b", "c"}, ","))
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

func (m *mockBatch) Query(string, ...interface{}) { m.size++ }
func (m *mockBatch) Size() int                    { return m.size }
func (m *mockBatch) ExecuteBatch() error          { return nil }
func (m *mockBatch) WithTimestamp(int64)          {}

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

func (m *mockBatchErr) Query(string, ...interface{}) { m.size++ }
func (m *mockBatchErr) Size() int                    { return m.size }
func (m *mockBatchErr) ExecuteBatch() error          { return fmt.Errorf("mock batch error") }
func (m *mockBatchErr) WithTimestamp(int64)          {}

func TestBulk_WorkerProcessesBatch(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
	}
	batch := []BatchItem{{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}}}}
	bulk.wg.Add(1)
	go bulk.worker()
	bulk.jobQueue <- batch
	close(bulk.jobQueue)
	bulk.wg.Wait()
}

func TestBulk_AddActions_DcpRebalancing(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		isDcpRebalancing:    1,
		metric:              &Metric{},
	}
	bulk.AddActions(nil, time.Now(), []Model{
		&Raw{Table: "test", Document: map[string]interface{}{"id": "1"}},
	})
	assert.Equal(t, 0, len(bulk.jobQueue))
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
	assert.NoError(t, bulk.insert(raw))

	raw.Operation = Update
	raw.Filter = map[string]interface{}{"product_id": 1}
	assert.NoError(t, bulk.update(raw))

	raw.Operation = Delete
	assert.NoError(t, bulk.delete(raw))
}

func TestBulk_WorkerHandlesError(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSessionErr{},
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
		keyspace:           "test",
	}
	err := bulk.insert(&Raw{
		Table:     "test_table",
		Document:  map[string]interface{}{"id": "1"},
		Operation: Insert,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error")
}

func TestBulk_ErrorHandling_Operations(t *testing.T) {
	bulk := &Bulk{
		session:            &mockSessionErr{},
		preparedStmts:      make(map[string]string),
		preparedStmtsMutex: sync.RWMutex{},
		keyspace:           "test",
	}
	assert.Error(t, bulk.insert(&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}))
	assert.Error(t, bulk.update(&Raw{
		Table:     "t",
		Document:  map[string]interface{}{"f": "v"},
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Update,
	}))
	assert.Error(t, bulk.delete(&Raw{Table: "t", Filter: map[string]interface{}{"id": "1"}, Operation: Delete}))
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
	cacheKey := fmt.Sprintf("INSERT:%s:%d:%v", raw.Table, len(raw.Document), false)
	q1 := bulk.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	q2 := bulk.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	assert.NotEmpty(t, q1)
	assert.Equal(t, q1, q2)
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
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1", "name": "a"}, Operation: Insert}},
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"name": "b"}, Filter: map[string]interface{}{"id": "2"}, Operation: Update}},
		{Model: &Raw{Table: "t", Filter: map[string]interface{}{"id": "3"}, Operation: Delete}},
	}
	assert.NoError(t, bulk.processBatchWithBatch(items))
}

func TestBulk_Worker_UseBatchMode(t *testing.T) {
	bulk := &Bulk{
		session:             &mockSession{},
		keyspace:            "ks",
		useBatch:            true,
		batchType:           UnloggedBatch,
		maxBatchSize:        5,
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		metric:              &Metric{},
	}
	bulk.wg.Add(1)
	go bulk.worker()
	bulk.jobQueue <- []BatchItem{{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1", "name": "test"}, Operation: Insert}}}
	close(bulk.jobQueue)
	bulk.wg.Wait()
	assert.GreaterOrEqual(t, bulk.metric.BulkRequestProcessLatencyMs, int64(0))
}

func TestBulk_GetBatchType(t *testing.T) {
	assert.Equal(t, LoggedBatch, getBatchType("logged"))
	assert.Equal(t, UnloggedBatch, getBatchType("unlogged"))
	assert.Equal(t, CounterBatch, getBatchType("counter"))
	assert.Equal(t, LoggedBatch, getBatchType(""))
	assert.Equal(t, LoggedBatch, getBatchType("invalid"))
	assert.Equal(t, UnloggedBatch, getBatchType("UNLOGGED"))
}

func TestBulk_NewBulk_WithBatchConfig(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:        []string{"localhost"},
			Keyspace:     "test",
			UseBatch:     true,
			BatchType:    "unlogged",
			MaxBatchSize: 100,
			WorkerCount:  1,
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
			_ = bulk.insert(&Raw{
				Table:     fmt.Sprintf("table%d", id%3),
				Document:  map[string]interface{}{"id": fmt.Sprintf("doc%d", id), "name": fmt.Sprintf("test%d", id)},
				Operation: Insert,
			})
		}(i)
	}
	wg.Wait()
	assert.NotEmpty(t, bulk.preparedStmts)
}

func TestBulk_BatchErrorHandling_NoCouchbaseCommit(t *testing.T) {
	commitCalled := false
	bulk := &Bulk{
		session:             &mockSessionErr{},
		keyspace:            "test",
		dcpCheckpointCommit: func() { commitCalled = true },
		metric:              &Metric{},
		useBatch:            true,
		batchType:           LoggedBatch,
		maxBatchSize:        100,
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
	}
	batch := []BatchItem{{Model: &Raw{Table: "test_table", Document: map[string]interface{}{"id": "1", "name": "test"}, Operation: Insert}}}
	defer func() {
		assert.NotNil(t, recover(), "Expected panic")
		assert.False(t, commitCalled, "dcpCheckpointCommit should not be called on error")
	}()
	bulk.processBatch(batch)
}

// --- ackMode tests ---

func TestAddActions_AckMode_Immediate(t *testing.T) {
	ackCalled := false
	writeStarted := make(chan struct{})
	writeProceed := make(chan struct{})

	bulk := &Bulk{
		session: &mockSessionBlocking{
			onQuery: func() {
				close(writeStarted)
				<-writeProceed
			},
		},
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeImmediate,
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		keyspace:            "ks",
	}

	bulk.wg.Add(1)
	go bulk.worker()

	ctx := newListenerContext(func() { ackCalled = true })
	go bulk.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	// Ack must be called before the write completes.
	<-writeStarted
	assert.True(t, ackCalled, "ackMode immediate: ack should be called before write completes")
	close(writeProceed)

	close(bulk.jobQueue)
	bulk.wg.Wait()
}

func TestAddActions_AckMode_AfterWrite(t *testing.T) {
	ackCalled := false
	writeStarted := make(chan struct{})
	writeProceed := make(chan struct{})

	bulk := &Bulk{
		session: &mockSessionBlocking{
			onQuery: func() {
				close(writeStarted)
				<-writeProceed
			},
		},
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeAfterWrite,
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		keyspace:            "ks",
	}

	bulk.wg.Add(1)
	go bulk.worker()

	ctx := newListenerContext(func() { ackCalled = true })
	go bulk.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	// Ack must NOT be called while the write is in progress.
	<-writeStarted
	assert.False(t, ackCalled, "ackMode after_write: ack should not be called before write completes")
	close(writeProceed)

	// Give the worker time to call ack after write.
	time.Sleep(20 * time.Millisecond)
	assert.True(t, ackCalled, "ackMode after_write: ack should be called after write completes")

	close(bulk.jobQueue)
	bulk.wg.Wait()
}

// mockSessionBlocking blocks on PreparedQuery to allow timing assertions.
type mockSessionBlocking struct {
	onQuery func()
}

func (m *mockSessionBlocking) Query(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionBlocking) NewBatch(BatchType) Batch           { return &mockBatch{} }
func (m *mockSessionBlocking) Close()                             {}
func (m *mockSessionBlocking) PreparedQuery(string, ...interface{}) Query {
	if m.onQuery != nil {
		m.onQuery()
	}
	return &mockQuery{}
}

// --- writeTimestamp tests ---

func TestAddActions_WriteTimestamp_None(t *testing.T) {
	bulk := &Bulk{
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeImmediate,
		writeTimestamp:      writeTimestampNone,
	}

	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
	ctx := newListenerContext(func() {})
	bulk.AddActions(ctx, time.Now(), []Model{raw})

	assert.Equal(t, int64(0), raw.Timestamp, "writeTimestamp none: Raw.Timestamp should be untouched")
}

func TestAddActions_WriteTimestamp_EventTime(t *testing.T) {
	bulk := &Bulk{
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeImmediate,
		writeTimestamp:      writeTimestampEventTime,
	}

	eventTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
	ctx := newListenerContext(func() {})
	bulk.AddActions(ctx, eventTime, []Model{raw})

	assert.Equal(t, eventTime.UnixMicro(), raw.Timestamp,
		"writeTimestamp event_time: Raw.Timestamp should equal event time in microseconds")
}

func TestAddActions_WriteTimestamp_Now(t *testing.T) {
	bulk := &Bulk{
		jobQueue:            make(chan []BatchItem, 1),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeImmediate,
		writeTimestamp:      writeTimestampNow,
	}

	before := time.Now().UnixMicro()
	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
	ctx := newListenerContext(func() {})
	bulk.AddActions(ctx, time.Now(), []Model{raw})
	after := time.Now().UnixMicro()

	assert.GreaterOrEqual(t, raw.Timestamp, before,
		"writeTimestamp now: Raw.Timestamp should be >= time before AddActions")
	assert.LessOrEqual(t, raw.Timestamp, after,
		"writeTimestamp now: Raw.Timestamp should be <= time after AddActions")
}

// --- rebalancing tests ---

func TestAddActions_Rebalancing_NoAck(t *testing.T) {
	ackCalled := false
	bulk := &Bulk{
		jobQueue:         make(chan []BatchItem, 1),
		metric:           &Metric{},
		ackMode:          ackModeAfterWrite,
		isDcpRebalancing: 1,
	}

	ctx := newListenerContext(func() { ackCalled = true })
	bulk.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	assert.False(t, ackCalled, "no ack should be called during rebalancing")
	assert.Equal(t, 0, len(bulk.jobQueue), "no event should be enqueued during rebalancing")
}

// --- workerCount concurrency cap test ---

func TestWorkerCount_CapsParallelism(t *testing.T) {
	const workerCount = 3
	var concurrent int64
	var maxConcurrent int64

	var mu sync.Mutex

	bulk := &Bulk{
		session: &mockSessionBlocking{
			onQuery: func() {
				cur := atomic.AddInt64(&concurrent, 1)
				mu.Lock()
				if cur > maxConcurrent {
					maxConcurrent = cur
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&concurrent, -1)
			},
		},
		jobQueue:            make(chan []BatchItem, workerCount),
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		ackMode:             ackModeImmediate,
		writeTimestamp:      writeTimestampNone,
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		keyspace:            "ks",
	}

	for i := 0; i < workerCount; i++ {
		bulk.wg.Add(1)
		go bulk.worker()
	}

	// Send more events than workers to ensure the cap is tested.
	for i := 0; i < workerCount*3; i++ {
		bulk.jobQueue <- []BatchItem{{
			Model: &Raw{Table: "t", Document: map[string]interface{}{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
		}}
	}

	close(bulk.jobQueue)
	bulk.wg.Wait()

	assert.LessOrEqual(t, maxConcurrent, int64(workerCount),
		"concurrent writes should never exceed workerCount")
}

// --- resolveTimestamp tests ---

func TestResolveTimestamp(t *testing.T) {
	eventTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("none returns 0", func(t *testing.T) {
		b := &Bulk{writeTimestamp: writeTimestampNone}
		assert.Equal(t, int64(0), b.resolveTimestamp(eventTime))
	})

	t.Run("event_time returns event UnixMicro", func(t *testing.T) {
		b := &Bulk{writeTimestamp: writeTimestampEventTime}
		assert.Equal(t, eventTime.UnixMicro(), b.resolveTimestamp(eventTime))
	})

	t.Run("now returns current time within reasonable range", func(t *testing.T) {
		b := &Bulk{writeTimestamp: writeTimestampNow}
		before := time.Now().UnixMicro()
		ts := b.resolveTimestamp(eventTime)
		after := time.Now().UnixMicro()
		assert.GreaterOrEqual(t, ts, before)
		assert.LessOrEqual(t, ts, after)
	})
}

// --- processBatch serial path test ---

func TestProcessBatch_SerialWrites(t *testing.T) {
	var callOrder []int
	var mu sync.Mutex

	// Use a session that records call order.
	session := &mockSessionOrdered{
		onQuery: func(id int) {
			mu.Lock()
			callOrder = append(callOrder, id)
			mu.Unlock()
		},
	}

	bulk := &Bulk{
		session:             session,
		keyspace:            "ks",
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
	}

	batch := []BatchItem{
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}},
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "2"}, Operation: Insert}},
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "3"}, Operation: Insert}},
	}

	bulk.processBatch(batch)

	assert.Equal(t, []int{1, 2, 3}, callOrder,
		"items should be written serially in order")
}

type mockSessionOrdered struct {
	callCount int
	onQuery   func(int)
}

func (m *mockSessionOrdered) Query(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionOrdered) NewBatch(BatchType) Batch           { return &mockBatch{} }
func (m *mockSessionOrdered) Close()                             {}
func (m *mockSessionOrdered) PreparedQuery(string, ...interface{}) Query {
	m.callCount++
	if m.onQuery != nil {
		m.onQuery(m.callCount)
	}
	return &mockQuery{}
}
