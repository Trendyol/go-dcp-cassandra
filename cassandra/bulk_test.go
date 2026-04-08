package cassandra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelCodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
)

func newListenerContext(ackFn func()) *models.ListenerContext {
	return &models.ListenerContext{Ack: ackFn}
}

func newBulk(session Session) *Bulk {
	done := make(chan struct{})
	close(done)
	return &Bulk{
		tracer:              otel.Tracer("test"),
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
		shutdownDoneCh:      make(chan struct{}),
	}
}

// --- Model / operation tests ---

func TestBulkOperations(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		m := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
		assert.Equal(t, Insert, m.Operation)
	})
	t.Run("Update", func(t *testing.T) {
		m := &Raw{Table: "t", Document: map[string]interface{}{"name": "x"}, Filter: map[string]interface{}{"id": "1"}, Operation: Update}
		assert.Equal(t, Update, m.Operation)
	})
	t.Run("Delete", func(t *testing.T) {
		m := &Raw{Table: "t", Filter: map[string]interface{}{"id": "1"}, Operation: Delete}
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
	err := b.insert(&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session is nil")
}

// --- CRUD success ---

func TestBulk_InsertUpdateDelete_Success(t *testing.T) {
	b := newBulk(&mockSession{})
	assert.NoError(t, b.insert(&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}))
	assert.NoError(t, b.update(&Raw{
		Table: "t", Document: map[string]interface{}{"name": "x"},
		Filter: map[string]interface{}{"id": "1"}, Operation: Update,
	}))
	assert.NoError(t, b.delete(&Raw{Table: "t", Filter: map[string]interface{}{"id": "1"}, Operation: Delete}))
}

// --- Error handling ---

func TestBulk_WorkerHandlesError(t *testing.T) {
	b := newBulk(&mockSessionErr{})
	err := b.insert(&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mock error")
}

func TestBulk_ErrorHandling_Operations(t *testing.T) {
	b := newBulk(&mockSessionErr{})
	assert.Error(t, b.insert(&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}))
	assert.Error(t, b.update(&Raw{
		Table:     "t",
		Document:  map[string]interface{}{"f": "v"},
		Filter:    map[string]interface{}{"id": "1"},
		Operation: Update,
	}))
	assert.Error(t, b.delete(&Raw{Table: "t", Filter: map[string]interface{}{"id": "1"}, Operation: Delete}))
}

func TestBulk_WriteError_Panics(t *testing.T) {
	b := newBulk(&mockSessionErr{})

	defer func() {
		assert.NotNil(t, recover(), "Expected panic on Cassandra write error")
	}()

	b.requestSync(context.Background(), BatchItem{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}})
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
			&Raw{Table: "t", Document: map[string]interface{}{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
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
		tracer:              otel.Tracer("test"),
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
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	// StartBulk in background, let ticker fire
	go b.StartBulk()
	time.Sleep(60 * time.Millisecond)
	close(b.shutdownCh)

	b.flushMu.Lock()
	flushDone := b.flushDone
	b.flushMu.Unlock()
	<-flushDone

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
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})
	<-writeStarted // flush 1 is writing

	// Second event triggers flush 2 — it should be enqueued but not start writing until flush 1 finishes
	written2 := make(chan struct{})
	go func() {
		b.AddActions(ctx, time.Now(), []Model{
			&Raw{Table: "t", Document: map[string]interface{}{"id": "2"}, Operation: Insert},
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
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
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
	b := &Bulk{
		tracer:   otel.Tracer("test"),
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
		shutdownDoneCh:      make(chan struct{}),
	}

	ctx := newListenerContext(func() {})
	for i := 0; i < 3; i++ {
		b.AddActions(ctx, time.Now(), []Model{
			&Raw{Table: "t", Document: map[string]interface{}{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
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
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]interface{}{"id": "2"}, Operation: Insert},
	})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "3"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]interface{}{"id": "4"}, Operation: Insert},
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
		items[i] = &Raw{Table: "t", Document: map[string]interface{}{"id": fmt.Sprintf("%d", i)}, Operation: Insert}
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
	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
	b.AddActions(newListenerContext(func() {}), time.Now(), []Model{raw})
	assert.Equal(t, int64(0), raw.Timestamp)
}

func TestAddActions_WriteTimestamp_EventTime(t *testing.T) {
	b := newBulk(&mockSession{})
	b.writeTimestamp = writeTimestampEventTime
	eventTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
	b.AddActions(newListenerContext(func() {}), eventTime, []Model{raw})
	assert.Equal(t, eventTime.UnixMicro(), raw.Timestamp)
}

func TestAddActions_WriteTimestamp_Now(t *testing.T) {
	b := newBulk(&mockSession{})
	b.writeTimestamp = writeTimestampNow
	before := time.Now().UnixMicro()
	raw := &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}
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
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
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
		Document:  map[string]interface{}{"data": `{"key":"value"}`},
		Operation: Insert,
	}
	size := estimateSize(raw)
	assert.Equal(t, len("data")+len(`{"key":"value"}`), size)
}

// --- join ---

func TestJoin(t *testing.T) {
	assert.Equal(t, "a,b,c", join([]string{"a", "b", "c"}, ","))
}

// --- Prepared statement caching ---

func TestBulk_PreparedStatementCaching(t *testing.T) {
	b := newBulk(&mockSession{})
	raw := &Raw{Table: "test_table", Document: map[string]interface{}{"id": "1", "name": "test"}, Operation: Insert}
	cacheKey := "INSERT:test_table:id,name:false"
	q1 := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	q2 := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	assert.NotEmpty(t, q1)
	assert.Equal(t, q1, q2)
}

func TestBulk_ConcurrentInserts(t *testing.T) {
	b := newBulk(&mockSession{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Go(func() {
			_ = b.insert(&Raw{
				Table:     fmt.Sprintf("table%d", i%3),
				Document:  map[string]interface{}{"id": fmt.Sprintf("doc%d", i), "name": fmt.Sprintf("test%d", i)},
				Operation: Insert,
			})
		})
	}
	wg.Wait()
}

// --- Regression: high-concurrency vBucket simulation ---
// Simulates the production scenario: many goroutines adding actions
// while flushes happen and metrics are read by Prometheus.

func TestBulk_HighConcurrency_VBucketSimulation(t *testing.T) {
	writeCount := int64(0)
	b := newBulk(&mockSessionCounting{count: &writeCount})
	b.batchSizeLimit = 50
	b.maxInFlightRequests = 10
	b.shutdownCh = make(chan struct{})
	b.shutdownDoneCh = make(chan struct{})
	b.batchTickerDuration = 5 * time.Millisecond
	b.batchTicker.Reset(5 * time.Millisecond)

	go b.StartBulk()

	var wg sync.WaitGroup

	// 100 goroutines simulating vBucket processors adding actions concurrently.
	for i := 0; i < 100; i++ {
		wg.Go(func() {
			ctx := newListenerContext(func() {})
			for j := 0; j < 10; j++ {
				b.AddActions(ctx, time.Now(), []Model{
					&Raw{
						Table:     fmt.Sprintf("t%d", j%5),
						Document:  map[string]interface{}{"id": fmt.Sprintf("%d-%d", i, j)},
						Operation: Insert,
					},
				})
			}
		})
	}

	// Concurrent metric reads (Prometheus collector).
	wg.Go(func() {
		for k := 0; k < 200; k++ {
			m := b.GetMetric()
			_ = m.ProcessLatencyMs
			_ = m.BulkRequestSize
		}
	})

	wg.Wait()

	// Allow remaining flushes to complete.
	b.Close()

	total := atomic.LoadInt64(&writeCount)
	assert.Greater(t, total, int64(0), "expected some writes to have completed")
}

// --- Regression: shutdown race (Close must wait for StartBulk) ---

func TestBulk_Close_WaitsForStartBulk(t *testing.T) {
	b := newBulk(&mockSession{})
	b.shutdownCh = make(chan struct{})
	b.shutdownDoneCh = make(chan struct{})

	started := make(chan struct{})
	go func() {
		close(started)
		b.StartBulk()
	}()
	<-started
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		b.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return; likely not waiting for StartBulk")
	}
}

// --- Regression: metric data race (must use atomic) ---

func TestBulk_MetricRace(t *testing.T) {
	b := newBulk(&mockSession{})
	b.batchSizeLimit = 1
	b.maxInFlightRequests = 10
	b.shutdownCh = make(chan struct{})
	b.shutdownDoneCh = make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 50; i++ {
			_ = b.GetMetric()
		}
	})
	wg.Go(func() {
		ctx := newListenerContext(func() {})
		for i := 0; i < 50; i++ {
			b.AddActions(ctx, time.Now(), []Model{
				&Raw{Table: "t", Document: map[string]interface{}{"id": fmt.Sprintf("%d", i)}, Operation: Insert},
			})
		}
	})
	wg.Wait()
}

// --- Regression: cache key collision (INSERT) ---
// Before fix: cache key used column count, so {a,b} and {x,y} produced the same key.
// After fix: cache key includes sorted column names, so they produce distinct keys.

func TestBulk_CacheKeyCollision_Insert(t *testing.T) {
	b := newBulk(&mockSession{})
	raw1 := &Raw{Table: "t", Document: map[string]interface{}{"a": "1", "b": "2"}, Operation: Insert}
	raw2 := &Raw{Table: "t", Document: map[string]interface{}{"x": "1", "y": "2"}, Operation: Insert}

	q1, _ := b.buildInsertValues(raw1, false)
	q2, _ := b.buildInsertValues(raw2, false)

	assert.NotEqual(t, q1, q2, "different columns must produce different INSERT queries")
}

// --- Regression: cache key collision (UPDATE) ---

func TestBulk_CacheKeyCollision_Update(t *testing.T) {
	b := newBulk(&mockSession{})
	raw1 := &Raw{
		Table: "t", Document: map[string]interface{}{"a": "v"},
		Filter: map[string]interface{}{"pk1": "1"}, Operation: Update,
	}
	raw2 := &Raw{
		Table: "t", Document: map[string]interface{}{"b": "v"},
		Filter: map[string]interface{}{"pk2": "1"}, Operation: Update,
	}

	q1, _ := b.buildUpdateValues(raw1, false)
	q2, _ := b.buildUpdateValues(raw2, false)

	assert.NotEqual(t, q1, q2, "different columns must produce different UPDATE queries")
}

// --- Regression: cache key collision (DELETE) ---

func TestBulk_CacheKeyCollision_Delete(t *testing.T) {
	b := newBulk(&mockSession{})
	raw1 := &Raw{Table: "t", Filter: map[string]interface{}{"a": "1", "b": "2"}, Operation: Delete}
	raw2 := &Raw{Table: "t", Filter: map[string]interface{}{"x": "1", "y": "2"}, Operation: Delete}

	q1, _ := b.buildDeleteValues(raw1, false)
	q2, _ := b.buildDeleteValues(raw2, false)

	assert.NotEqual(t, q1, q2, "different filter columns must produce different DELETE queries")
}

// --- Cache key correctness: same columns produce the same cached query ---

func TestBulk_SameColumns_ProduceSameCacheKey(t *testing.T) {
	b := newBulk(&mockSession{})
	raw1 := &Raw{Table: "t", Document: map[string]interface{}{"id": "1", "name": "a"}, Operation: Insert}
	raw2 := &Raw{Table: "t", Document: map[string]interface{}{"id": "2", "name": "b"}, Operation: Insert}

	q1, _ := b.buildInsertValues(raw1, false)
	q2, _ := b.buildInsertValues(raw2, false)

	assert.Equal(t, q1, q2, "same column names must produce identical cached queries")
}

// --- Regression: flush updates metrics atomically ---

func TestFlush_UpdatesMetrics(t *testing.T) {
	writeCount := int64(0)
	b := newBulk(&mockSessionCounting{count: &writeCount})
	b.batchSizeLimit = 2
	b.maxInFlightRequests = 10

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
		&Raw{Table: "t", Document: map[string]interface{}{"id": "2"}, Operation: Insert},
	})

	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	m := b.GetMetric()
	assert.Equal(t, int64(2), m.BulkRequestSize)
	assert.GreaterOrEqual(t, m.BulkRequestProcessLatencyMs, int64(0))
}

// --- Regression: GetMetric returns a snapshot, not a live pointer ---
// Before fix: GetMetric returned the same pointer, so values could change.
// After fix: GetMetric returns a new struct with atomic loads.

func TestBulk_GetMetric_ReturnsSnapshot(t *testing.T) {
	b := newBulk(&mockSession{})
	atomic.StoreInt64(&b.metric.ProcessLatencyMs, 100)
	atomic.StoreInt64(&b.metric.BulkRequestSize, 42)

	snapshot := b.GetMetric()
	assert.Equal(t, int64(100), snapshot.ProcessLatencyMs)
	assert.Equal(t, int64(42), snapshot.BulkRequestSize)

	atomic.StoreInt64(&b.metric.ProcessLatencyMs, 999)
	assert.Equal(t, int64(100), snapshot.ProcessLatencyMs,
		"snapshot must not change after source metric is updated")
}

// --- Regression: Close waits for in-flight flush, then closes session ---

func TestBulk_Close_SessionClosedAfterFlush(t *testing.T) {
	var events []string
	var mu sync.Mutex
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}

	s := &mockSessionOrdered{
		onPreparedQuery: func() {
			time.Sleep(30 * time.Millisecond)
			record("write_done")
		},
		onClose: func() {
			record("session_closed")
		},
	}

	b := newBulk(s)
	b.shutdownCh = make(chan struct{})
	b.shutdownDoneCh = make(chan struct{})
	b.batchSizeLimit = 1
	b.maxInFlightRequests = 1

	go b.StartBulk()
	time.Sleep(5 * time.Millisecond)

	ctx := newListenerContext(func() {})
	b.AddActions(ctx, time.Now(), []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	b.flushMu.Lock()
	done := b.flushDone
	b.flushMu.Unlock()
	<-done

	b.Close()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"write_done", "session_closed"}, events,
		"session.Close must happen after all writes complete")
}

// --- Regression: AddActions sets ProcessLatencyMs atomically ---

func TestAddActions_SetsProcessLatencyMs(t *testing.T) {
	b := newBulk(&mockSession{})
	past := time.Now().Add(-50 * time.Millisecond)

	b.AddActions(newListenerContext(func() {}), past, []Model{
		&Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert},
	})

	m := b.GetMetric()
	assert.GreaterOrEqual(t, m.ProcessLatencyMs, int64(50),
		"ProcessLatencyMs must reflect time since event")
}

// --- OpenTelemetry tracing tests ---

func TestRequestSync_CreatesOTelSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	b := newBulk(&mockSession{})
	b.tracer = tp.Tracer("test")
	ctx := context.Background()

	b.requestSync(ctx, BatchItem{
		Model: &Raw{
			Table:     "orders",
			Document:  map[string]interface{}{"id": "1", "status": "active"},
			Operation: Insert,
		},
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "cassandra.write", spans[0].Name)

	attrs := spans[0].Attributes
	found := map[string]string{}
	for _, a := range attrs {
		found[string(a.Key)] = a.Value.AsString()
	}
	assert.Equal(t, "orders", found["db.cassandra.table"])
	assert.Equal(t, string(Insert), found["db.operation"])
}

func TestRequestSync_RecordsErrorOnSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	b := newBulk(&mockSessionErr{})
	b.tracer = tp.Tracer("test")
	ctx := context.Background()

	assert.Panics(t, func() {
		b.requestSync(ctx, BatchItem{
			Model: &Raw{
				Table:     "t",
				Document:  map[string]interface{}{"id": "1"},
				Operation: Insert,
			},
		})
	})

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, otelCodes.Error, spans[0].Status.Code)
	assert.NotEmpty(t, spans[0].Events, "span should have recorded error event")
}

func TestRunFlush_CreatesFlushSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	b := newBulk(&mockSession{})
	b.tracer = tp.Tracer("test")
	done := make(chan struct{})

	batch := []BatchItem{
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}, Ack: func() {}},
	}
	b.runFlush(context.Background(), batch, done)

	spans := exporter.GetSpans()
	var flushSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cassandra.flush" {
			flushSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, flushSpan, "expected cassandra.flush span")

	var batchSizeAttr *attribute.KeyValue
	for _, a := range flushSpan.Attributes {
		if a.Key == "batch.size" {
			batchSizeAttr = &a
			break
		}
	}
	require.NotNil(t, batchSizeAttr)
	assert.Equal(t, int64(1), batchSizeAttr.Value.AsInt64())
}

func TestWriteUnloggedBatch_CreatesBatchSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	b := newBulk(&mockSession{})
	b.tracer = tp.Tracer("test")
	items := []BatchItem{
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}},
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "2"}, Operation: Insert}},
	}
	b.writeUnloggedBatch(context.Background(), items)

	spans := exporter.GetSpans()
	var batchSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cassandra.batch" {
			batchSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, batchSpan, "expected cassandra.batch span")

	var itemsAttr *attribute.KeyValue
	for _, a := range batchSpan.Attributes {
		if a.Key == "batch.items" {
			itemsAttr = &a
			break
		}
	}
	require.NotNil(t, itemsAttr)
	assert.Equal(t, int64(2), itemsAttr.Value.AsInt64())
}

func TestWriteUnloggedBatch_RecordsErrorOnSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	b := newBulk(&mockSessionBatchErr{})
	b.tracer = tp.Tracer("test")
	items := []BatchItem{
		{Model: &Raw{Table: "t", Document: map[string]interface{}{"id": "1"}, Operation: Insert}},
	}

	assert.Panics(t, func() {
		b.writeUnloggedBatch(context.Background(), items)
	})

	spans := exporter.GetSpans()
	var batchSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cassandra.batch" {
			batchSpan = &spans[i]
			break
		}
	}
	require.NotNil(t, batchSpan, "expected cassandra.batch span")
	assert.Equal(t, otelCodes.Error, batchSpan.Status.Code)
	assert.NotEmpty(t, batchSpan.Events, "span should have recorded error event")
}

// --- Mock implementations ---

type mockSession struct{}

func (m *mockSession) Query(string, ...interface{}) Query         { return &mockQuery{} }
func (m *mockSession) PreparedQuery(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSession) NewBatch(BatchType) Batch                   { return &mockBatch{} }
func (m *mockSession) Close()                                     {}

type mockQuery struct{}

func (m *mockQuery) Exec() error { return nil }

type mockBatch struct{ size int }

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

type mockBatchErr struct{ size int }

func (m *mockBatchErr) Query(string, ...interface{}) { m.size++ }
func (m *mockBatchErr) Size() int                    { return m.size }
func (m *mockBatchErr) ExecuteBatch() error          { return fmt.Errorf("mock batch error") }
func (m *mockBatchErr) WithTimestamp(int64)          {}

// mockSessionOrdered tracks the order of PreparedQuery and Close calls.
type mockSessionOrdered struct {
	onPreparedQuery func()
	onClose         func()
}

func (m *mockSessionOrdered) Query(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionOrdered) NewBatch(BatchType) Batch           { return &mockBatch{} }

func (m *mockSessionOrdered) PreparedQuery(string, ...interface{}) Query {
	if m.onPreparedQuery != nil {
		m.onPreparedQuery()
	}
	return &mockQuery{}
}

func (m *mockSessionOrdered) Close() {
	if m.onClose != nil {
		m.onClose()
	}
}

// mockSessionBlocking blocks on PreparedQuery to allow timing assertions.
type mockSessionBlocking struct{ onQuery func() }

func (m *mockSessionBlocking) Query(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionBlocking) NewBatch(BatchType) Batch           { return &mockBatch{} }
func (m *mockSessionBlocking) Close()                             {}
func (m *mockSessionBlocking) PreparedQuery(string, ...interface{}) Query {
	if m.onQuery != nil {
		m.onQuery()
	}
	return &mockQuery{}
}

// mockSessionCounting counts PreparedQuery calls and ExecuteBatch calls.
type mockSessionCounting struct {
	count *int64
}

func (m *mockSessionCounting) Query(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionCounting) Close()                             {}
func (m *mockSessionCounting) PreparedQuery(string, ...interface{}) Query {
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

func (m *mockBatchCounting) Query(string, ...interface{}) { m.size++ }
func (m *mockBatchCounting) Size() int                    { return m.size }
func (m *mockBatchCounting) WithTimestamp(int64)          {}
func (m *mockBatchCounting) ExecuteBatch() error {
	atomic.AddInt64(m.count, 1)
	return nil
}

// mockSessionBatchErr returns a session whose batch ExecuteBatch always fails.
type mockSessionBatchErr struct{}

func (m *mockSessionBatchErr) Query(string, ...interface{}) Query         { return &mockQuery{} }
func (m *mockSessionBatchErr) PreparedQuery(string, ...interface{}) Query { return &mockQuery{} }
func (m *mockSessionBatchErr) Close()                                     {}
func (m *mockSessionBatchErr) NewBatch(BatchType) Batch                   { return &mockBatchErr{} }
