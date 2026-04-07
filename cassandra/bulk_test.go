package cassandra

import (
	"fmt"
	"strings"
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

func newBulk(session Session) *Bulk {
	done := make(chan struct{})
	close(done)
	shutdownDone := make(chan struct{})
	close(shutdownDone)
	return &Bulk{
		session:             session,
		keyspace:            "ks",
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		batchBuffer:         make([]BatchItem, 0, 100),
		batchSizeLimit:      100,
		batchByteSizeLimit:  10 * 1024 * 1024,
		batchTickerDuration: 10 * time.Second,
		batchTicker:         time.NewTicker(10 * time.Second),
		maxInFlightRequests: 10,
		flushDone:           done,
		shutdownDoneCh:      shutdownDone,
	}
}

// --- Model / operation tests ---

func TestBulkOperations(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		m := &Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}
		assert.Equal(t, Insert, m.Operation)
	})
	t.Run("Update", func(t *testing.T) {
		m := &Raw{Table: "t", Document: map[string]any{"name": "x"}, Filter: map[string]any{"id": "1"}, Operation: Update}
		assert.Equal(t, Update, m.Operation)
	})
	t.Run("Delete", func(t *testing.T) {
		m := &Raw{Table: "t", Filter: map[string]any{"id": "1"}, Operation: Delete}
		assert.Equal(t, Delete, m.Operation)
	})
}

// --- NewBulk ---

func TestNewBulk_InvalidSession(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:    []string{"invalid-host"},
			Keyspace: "test",
			Username: "cassandra",
			Password: "wrong",
			Timeout:  1 * time.Millisecond,
		},
	}
	cfg.ApplyDefaults()
	_, err := NewBulk(cfg, func() {})
	assert.Error(t, err)
}

// --- Session nil checks ---

func TestBulk_Insert_NilSession(t *testing.T) {
	b := &Bulk{}
	err := b.insert(&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session is nil")
}

// --- CRUD success ---

func TestBulk_InsertUpdateDelete_Success(t *testing.T) {
	b := newBulk(&mockSession{})
	assert.NoError(t, b.insert(&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}))
	assert.NoError(t, b.update(&Raw{
		Table: "t", Document: map[string]any{"name": "x"},
		Filter: map[string]any{"id": "1"}, Operation: Update,
	}))
	assert.NoError(t, b.delete(&Raw{Table: "t", Filter: map[string]any{"id": "1"}, Operation: Delete}))
}

// --- Error handling ---

func TestBulk_WorkerHandlesError(t *testing.T) {
	b := newBulk(&mockSessionErr{})
	err := b.insert(&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error")
}

func TestBulk_ErrorHandling_Operations(t *testing.T) {
	b := newBulk(&mockSessionErr{})
	assert.Error(t, b.insert(&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}))
	assert.Error(t, b.update(&Raw{
		Table:     "t",
		Document:  map[string]any{"f": "v"},
		Filter:    map[string]any{"id": "1"},
		Operation: Update,
	}))
	assert.Error(t, b.delete(&Raw{Table: "t", Filter: map[string]any{"id": "1"}, Operation: Delete}))
}

func TestBulk_WriteError_Panics(t *testing.T) {
	b := newBulk(&mockSessionErr{})

	defer func() {
		assert.NotNil(t, recover(), "Expected panic on Cassandra write error")
	}()

	b.requestSync(BatchItem{Model: &Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}})
}

// --- Flush triggered by size ---

func TestFlush_TriggeredBySize(t *testing.T) {
	writeCount := int64(0)
	b := newBulk(&mockSessionCounting{count: &writeCount})
	b.batchSizeLimit = 3
	b.maxInFlightRequests = 10

	ctx := newListenerContext(func() {})
	for i := 0; i < 3; i++ {
		b.AddActions(ctx, time.Now(), []Model{
			&Raw{Table: "t", Document: map[string]any{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
		})
	}

	// Wait for flush goroutine to complete
	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	assert.Equal(t, int64(3), atomic.LoadInt64(&writeCount))
}

// --- Flush triggered by ticker ---

func TestFlush_TriggeredByTicker(t *testing.T) {
	writeCount := int64(0)
	done := make(chan struct{})
	close(done)
	b := &Bulk{
		session:             &mockSessionCounting{count: &writeCount},
		keyspace:            "ks",
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		batchBuffer:         make([]BatchItem, 0, 100),
		batchSizeLimit:      1000,
		batchByteSizeLimit:  10 * 1024 * 1024,
		batchTickerDuration: 20 * time.Millisecond,
		batchTicker:         time.NewTicker(20 * time.Millisecond),
		maxInFlightRequests: 10,
		flushDone:           done,
		shutdownCh:          make(chan struct{}),
		shutdownDoneCh:      make(chan struct{}),
	}

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
	})

	// StartBulk in background, let ticker fire
	go b.StartBulk()
	time.Sleep(60 * time.Millisecond)
	close(b.shutdownCh)
	<-b.shutdownDoneCh

	assert.Equal(t, int64(1), atomic.LoadInt64(&writeCount))
}

// --- Single flush at a time ---

func TestFlush_SingleFlushAtATime(t *testing.T) {
	writeStarted := make(chan struct{})
	writeProceed := make(chan struct{})

	b := newBulk(&mockSessionBlocking{onQuery: func() {
		select {
		case writeStarted <- struct{}{}:
		default:
		}
		<-writeProceed
	}})
	b.batchSizeLimit = 1
	b.maxInFlightRequests = 10

	ctx := newListenerContext(func() {})

	// First event triggers flush 1
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
	})
	<-writeStarted // flush 1 is writing

	// Second event triggers flush 2 — it should be enqueued but not start writing until flush 1 finishes
	written2 := make(chan struct{})
	go func() {
		b.AddActions(ctx, time.Now(), []Model{
			&Raw{Table: "t", Document: map[string]any{"id": "2"}, Operation: Insert},
		})
		close(written2)
	}()

	// Flush 2 should not have started writing yet
	select {
	case <-writeStarted:
		t.Error("flush 2 started before flush 1 completed")
	case <-time.After(30 * time.Millisecond):
		// correct — flush 2 is waiting
	}

	// Unblock flush 1
	close(writeProceed)

	// Now flush 2 should proceed
	select {
	case <-written2:
	case <-time.After(500 * time.Millisecond):
		t.Error("flush 2 did not complete in time")
	}
}

// --- Ack timing ---

func TestFlush_AcksAfterWrite(t *testing.T) {
	writeStarted := make(chan struct{})
	writeProceed := make(chan struct{})
	ackCalled := false

	b := newBulk(&mockSessionBlocking{onQuery: func() {
		close(writeStarted)
		<-writeProceed
	}})
	b.batchSizeLimit = 1
	b.maxInFlightRequests = 1

	ctx := newListenerContext(func() { ackCalled = true })
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
	})

	<-writeStarted
	assert.False(t, ackCalled, "ack should not be called before write completes")

	close(writeProceed)
	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	assert.True(t, ackCalled, "ack should be called after write completes")
}

// --- Commit after all writes ---

func TestFlush_CommitAfterAllWrites(t *testing.T) {
	writeCount := int64(0)
	commitCalled := false
	commitMu := sync.Mutex{}

	done := make(chan struct{})
	close(done)
	shutdownDone := make(chan struct{})
	close(shutdownDone)
	b := &Bulk{
		session:  &mockSessionCounting{count: &writeCount},
		keyspace: "ks",
		dcpCheckpointCommit: func() {
			commitMu.Lock()
			commitCalled = true
			commitMu.Unlock()
		},
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		batchBuffer:         make([]BatchItem, 0, 100),
		batchSizeLimit:      3,
		batchByteSizeLimit:  10 * 1024 * 1024,
		batchTickerDuration: 10 * time.Second,
		batchTicker:         time.NewTicker(10 * time.Second),
		maxInFlightRequests: 10,
		flushDone:           done,
		shutdownDoneCh:      shutdownDone,
	}

	ctx := newListenerContext(func() {})
	for i := 0; i < 3; i++ {
		b.AddActions(ctx, time.Now(), []Model{
			&Raw{Table: "t", Document: map[string]any{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
		})
	}

	b.flushMu.Lock()
	flushDone := b.flushDone
	b.flushMu.Unlock()
	<-flushDone

	assert.Equal(t, int64(3), atomic.LoadInt64(&writeCount))
	commitMu.Lock()
	assert.True(t, commitCalled)
	commitMu.Unlock()
}

// --- Per-event batching ---

func runBatchPerEventTest(t *testing.T, batchPerEvent bool, expectedCount int64) {
	t.Helper()
	count := int64(0)
	b := newBulk(&mockSessionCounting{count: &count})
	b.batchPerEvent = batchPerEvent
	b.batchSizeLimit = 4
	b.maxInFlightRequests = 10

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]any{"id": "2"}, Operation: Insert},
	})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "3"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]any{"id": "4"}, Operation: Insert},
	})

	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	assert.Equal(t, expectedCount, atomic.LoadInt64(&count))
}

func TestBatchPerEvent_Groups(t *testing.T) {
	// batchPerEvent true: 2 events × 1 UNLOGGED BATCH each = 2 batch executions
	runBatchPerEventTest(t, true, 2)
}

func TestBatchPerEvent_Disabled(t *testing.T) {
	// batchPerEvent false: 4 individual prepared statement writes
	runBatchPerEventTest(t, false, 4)
}

// --- maxInFlightRequests caps concurrency ---

func TestMaxInFlightRequests_CapsParallelism(t *testing.T) {
	var concurrent int64
	var maxConcurrent int64
	var mu sync.Mutex

	b := newBulk(&mockSessionBlocking{onQuery: func() {
		cur := atomic.AddInt64(&concurrent, 1)
		mu.Lock()
		if cur > maxConcurrent {
			maxConcurrent = cur
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&concurrent, -1)
	}})
	b.maxInFlightRequests = 3
	b.batchSizeLimit = 10

	items := make([]Model, 9)
	for i := range items {
		items[i] = &Raw{Table: "t", Document: map[string]any{"id": fmt.Sprintf("%d", i)}, Operation: Insert}
	}

	ctx := newListenerContext(func() {})
	for _, item := range items {
		b.AddActions(ctx, time.Now(), []Model{item})
	}

	b.batchMutex.Lock()
	b.flushLocked()
	b.batchMutex.Unlock()

	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	assert.LessOrEqual(t, maxConcurrent, int64(3), "concurrent writes must not exceed maxInFlightRequests")
}

// --- writeTimestamp tests ---

func TestAddActions_WriteTimestamp_None(t *testing.T) {
	b := newBulk(&mockSession{})
	b.writeTimestamp = writeTimestampNone
	raw := &Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}
	b.AddActions(newListenerContext(func() {}), time.Now(), []Model{raw})
	assert.Equal(t, int64(0), raw.Timestamp)
}

func TestAddActions_WriteTimestamp_EventTime(t *testing.T) {
	b := newBulk(&mockSession{})
	b.writeTimestamp = writeTimestampEventTime
	eventTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	raw := &Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}
	b.AddActions(newListenerContext(func() {}), eventTime, []Model{raw})
	assert.Equal(t, eventTime.UnixMicro(), raw.Timestamp)
}

func TestAddActions_WriteTimestamp_Now(t *testing.T) {
	b := newBulk(&mockSession{})
	b.writeTimestamp = writeTimestampNow
	before := time.Now().UnixMicro()
	raw := &Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert}
	b.AddActions(newListenerContext(func() {}), time.Now(), []Model{raw})
	after := time.Now().UnixMicro()
	assert.GreaterOrEqual(t, raw.Timestamp, before)
	assert.LessOrEqual(t, raw.Timestamp, after)
}

// --- Rebalancing ---

func TestAddActions_Rebalancing_NoEnqueue(t *testing.T) {
	b := newBulk(&mockSession{})
	b.isDcpRebalancing = 1
	b.AddActions(newListenerContext(func() {}), time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
	})
	assert.Equal(t, 0, len(b.batchBuffer))
}

// --- resolveTimestamp ---

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
	t.Run("now returns current time", func(t *testing.T) {
		b := &Bulk{writeTimestamp: writeTimestampNow}
		before := time.Now().UnixMicro()
		ts := b.resolveTimestamp(eventTime)
		after := time.Now().UnixMicro()
		assert.GreaterOrEqual(t, ts, before)
		assert.LessOrEqual(t, ts, after)
	})
}

// --- estimateSize ---

func TestEstimateSize(t *testing.T) {
	raw := &Raw{
		Table:     "t",
		Document:  map[string]any{"data": `{"key":"value"}`},
		Operation: Insert,
	}
	size := estimateSize(raw)
	assert.Equal(t, len("data")+len(`{"key":"value"}`), size)
}

// --- strings.Join (was custom join) ---

func TestStringsJoin(t *testing.T) {
	assert.Equal(t, "a,b,c", strings.Join([]string{"a", "b", "c"}, ","))
}

// --- Prepared statement caching ---

func TestBulk_PreparedStatementCaching(t *testing.T) {
	b := newBulk(&mockSession{})
	raw := &Raw{Table: "test_table", Document: map[string]any{"id": "1", "name": "test"}, Operation: Insert}
	columns := sortedKeys(raw.Document)
	cacheKey := fmt.Sprintf("INSERT:%s:%s:%v", raw.Table, strings.Join(columns, ","), false)
	q1 := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	q2 := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	assert.NotEmpty(t, q1)
	assert.Equal(t, q1, q2)
}

// TestBulk_CacheKeyCollision verifies that documents with the same number of
// columns but different column names produce different prepared statements.
// Bug: The old cache key used len(Document) as the discriminator, so
// {a:1, b:2} and {x:1, y:2} would share the same cached query.
// This test fails on master and passes with the fix.
func TestBulk_CacheKeyCollision(t *testing.T) {
	b := newBulk(&mockSession{})
	raw1 := &Raw{Table: "t", Document: map[string]any{"alpha": "1", "beta": "2"}, Operation: Insert}
	raw2 := &Raw{Table: "t", Document: map[string]any{"gamma": "1", "delta": "2"}, Operation: Insert}

	q1, _ := b.buildInsertValues(raw1, false)
	q2, _ := b.buildInsertValues(raw2, false)

	assert.NotEqual(t, q1, q2, "different column sets must produce different queries")
	assert.Contains(t, q1, "alpha")
	assert.Contains(t, q2, "gamma")
}

func TestBulk_ConcurrentInserts(t *testing.T) {
	b := newBulk(&mockSession{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = b.insert(&Raw{
				Table:     fmt.Sprintf("table%d", id%3),
				Document:  map[string]any{"id": fmt.Sprintf("doc%d", id), "name": fmt.Sprintf("test%d", id)},
				Operation: Insert,
			})
		}(i)
	}
	wg.Wait()
}

// --- Shutdown race: Close must wait for StartBulk ---
// Bug: Close() called session.Close() immediately after signalling shutdown,
// but StartBulk's final flush might still be using the session.
// This test fails with -race on master and passes with the fix.

func TestBulk_Close_WaitsForStartBulk(t *testing.T) {
	writeCount := int64(0)
	done := make(chan struct{})
	close(done)
	b := &Bulk{
		session:             &mockSessionCounting{count: &writeCount},
		keyspace:            "ks",
		dcpCheckpointCommit: func() {},
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		batchBuffer:         make([]BatchItem, 0, 100),
		batchSizeLimit:      1000,
		batchByteSizeLimit:  10 * 1024 * 1024,
		batchTickerDuration: 50 * time.Millisecond,
		batchTicker:         time.NewTicker(50 * time.Millisecond),
		maxInFlightRequests: 10,
		flushDone:           done,
		shutdownCh:          make(chan struct{}),
		shutdownDoneCh:      make(chan struct{}),
	}

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
	})

	go b.StartBulk()

	// Let the ticker fire at least once.
	time.Sleep(80 * time.Millisecond)

	b.Close()

	// If Close returned, StartBulk has finished — session is safe to use until this point.
	assert.GreaterOrEqual(t, atomic.LoadInt64(&writeCount), int64(1))
}

// --- Metric data race: concurrent metric reads/writes ---
// Bug: runFlush wrote b.metric.BulkRequestSize directly while GetMetric
// returned the shared pointer. Under -race, concurrent access is detected.
// This test fails with -race on master and passes with atomic operations.

func TestBulk_MetricRace(t *testing.T) {
	writeCount := int64(0)
	b := newBulk(&mockSessionCounting{count: &writeCount})
	b.batchSizeLimit = 1
	b.maxInFlightRequests = 10

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := newListenerContext(func() {})
		for i := 0; i < 10; i++ {
			b.AddActions(ctx, time.Now(), []Model{
				&Raw{Table: "t", Document: map[string]any{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
			})
		}
	}()

	// Reader goroutine — must not race with writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			m := b.GetMetric()
			_ = m.ProcessLatencyMs
			_ = m.BulkRequestSize
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}

// --- Flush metrics update ---

func TestFlush_UpdatesMetrics(t *testing.T) {
	writeCount := int64(0)
	b := newBulk(&mockSessionCounting{count: &writeCount})
	b.batchSizeLimit = 2
	b.maxInFlightRequests = 10

	// Set sentinel values so we can verify they change.
	atomic.StoreInt64(&b.metric.BulkRequestSize, -1)
	atomic.StoreInt64(&b.metric.BulkRequestProcessLatencyMs, -1)

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]any{"id": "1"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]any{"id": "2"}, Operation: Insert},
	})

	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	m := b.GetMetric()
	assert.Equal(t, int64(2), m.BulkRequestSize)
	assert.GreaterOrEqual(t, m.BulkRequestProcessLatencyMs, int64(0))
}

// --- Mock implementations ---

type mockSession struct{}

func (m *mockSession) Query(string, ...any) Query         { return &mockQuery{} }
func (m *mockSession) PreparedQuery(string, ...any) Query { return &mockQuery{} }
func (m *mockSession) NewBatch(BatchType) Batch                   { return &mockBatch{} }
func (m *mockSession) Close()                                     {}

type mockQuery struct{}

func (m *mockQuery) Exec() error { return nil }

type mockBatch struct{ size int }

func (m *mockBatch) Query(string, ...any) { m.size++ }
func (m *mockBatch) Size() int                    { return m.size }
func (m *mockBatch) ExecuteBatch() error          { return nil }
func (m *mockBatch) WithTimestamp(int64)          {}

type mockSessionErr struct{}

func (m *mockSessionErr) Query(string, ...any) Query         { return &mockQueryErr{} }
func (m *mockSessionErr) PreparedQuery(string, ...any) Query { return &mockQueryErr{} }
func (m *mockSessionErr) NewBatch(BatchType) Batch                   { return &mockBatchErr{} }
func (m *mockSessionErr) Close()                                     {}

type mockQueryErr struct{}

func (m *mockQueryErr) Exec() error { return fmt.Errorf("mock error") }

type mockBatchErr struct{ size int }

func (m *mockBatchErr) Query(string, ...any) { m.size++ }
func (m *mockBatchErr) Size() int                    { return m.size }
func (m *mockBatchErr) ExecuteBatch() error          { return fmt.Errorf("mock batch error") }
func (m *mockBatchErr) WithTimestamp(int64)          {}

// mockSessionBlocking blocks on PreparedQuery to allow timing assertions.
type mockSessionBlocking struct{ onQuery func() }

func (m *mockSessionBlocking) Query(string, ...any) Query { return &mockQuery{} }
func (m *mockSessionBlocking) NewBatch(BatchType) Batch           { return &mockBatch{} }
func (m *mockSessionBlocking) Close()                             {}
func (m *mockSessionBlocking) PreparedQuery(string, ...any) Query {
	if m.onQuery != nil {
		m.onQuery()
	}
	return &mockQuery{}
}

// mockSessionCounting counts PreparedQuery calls and ExecuteBatch calls.
type mockSessionCounting struct {
	count *int64
}

func (m *mockSessionCounting) Query(string, ...any) Query { return &mockQuery{} }
func (m *mockSessionCounting) Close()                             {}
func (m *mockSessionCounting) PreparedQuery(string, ...any) Query {
	atomic.AddInt64(m.count, 1)
	return &mockQuery{}
}

func (m *mockSessionCounting) NewBatch(BatchType) Batch {
	return &mockBatchCounting{count: m.count}
}

type mockBatchCounting struct {
	count *int64
	size  int
}

func (m *mockBatchCounting) Query(string, ...any) { m.size++ }
func (m *mockBatchCounting) Size() int                    { return m.size }
func (m *mockBatchCounting) WithTimestamp(int64)          {}
func (m *mockBatchCounting) ExecuteBatch() error {
	atomic.AddInt64(m.count, 1)
	return nil
}
