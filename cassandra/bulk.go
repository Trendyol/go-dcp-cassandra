package cassandra

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Trendyol/go-dcp/models"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
)

// BatchItem represents a single action to be written to Cassandra,
// along with its ack function and the DCP event ID it belongs to.
// EventID is used to group actions from the same DCP event when
// batchPerEvent is enabled.
type BatchItem struct {
	Model   Model
	Ack     func()
	EventID int64
}

type Bulk struct {
	preparedStmtsMutex  sync.RWMutex
	batchBuffer         []BatchItem
	session             Session
	keyspace            string
	writeTimestamp      string
	dcpCheckpointCommit func()
	preparedStmts       map[string]string
	metric              *Metric
	shutdownCh          chan struct{}
	batchMutex          sync.Mutex
	// flushDone is closed when the current in-flight flush completes.
	// A new channel is created for each flush. Enforces single flush at a time.
	flushDone           chan struct{}
	flushMu             sync.Mutex
	shutdownDoneCh      chan struct{}
	batchTicker         *time.Ticker
	batchTickerDuration time.Duration
	eventCounter        int64
	maxInFlightRequests int
	batchSizeLimit      int
	batchByteSizeLimit  int
	currentBatchSize    int
	currentByteSize     int
	isDcpRebalancing    int32
	batchPerEvent       bool
}

const (
	writeTimestampNone      = "none"
	writeTimestampEventTime = "event_time"
	writeTimestampNow       = "now"
)

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
	BulkRequestSize             int64
	BulkRequestByteSize         int64
}

func NewBulk(cfg *config.Connector, dcpCheckpointCommit func()) (*Bulk, error) {
	realSession, err := NewCassandraSession(cfg.Cassandra)
	if err != nil {
		return nil, err
	}

	// flushDone starts already closed — no previous flush to wait for.
	initialDone := make(chan struct{})
	close(initialDone)

	b := &Bulk{
		session:             realSession,
		keyspace:            cfg.Cassandra.Keyspace,
		dcpCheckpointCommit: dcpCheckpointCommit,
		shutdownCh:          make(chan struct{}),
		shutdownDoneCh:      make(chan struct{}),
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		batchBuffer:         make([]BatchItem, 0, cfg.Cassandra.BatchSizeLimit),
		batchSizeLimit:      cfg.Cassandra.BatchSizeLimit,
		batchByteSizeLimit:  cfg.Cassandra.BatchByteSizeLimit,
		batchTickerDuration: cfg.Cassandra.BatchTickerDuration,
		batchTicker:         time.NewTicker(cfg.Cassandra.BatchTickerDuration),
		maxInFlightRequests: cfg.Cassandra.MaxInFlightRequests,
		batchPerEvent:       cfg.Cassandra.BatchPerEvent,
		writeTimestamp:      cfg.Cassandra.WriteTimestamp,
		flushDone:           initialDone,
	}

	return b, nil
}

// StartBulk runs the ticker-driven flush loop. Blocks until Close is called.
func (b *Bulk) StartBulk() {
	defer close(b.shutdownDoneCh)
	for {
		select {
		case <-b.batchTicker.C:
			b.batchMutex.Lock()
			b.flushLocked()
			b.batchMutex.Unlock()

		case <-b.shutdownCh:
			b.batchTicker.Stop()
			b.batchMutex.Lock()
			b.flushLocked()
			b.batchMutex.Unlock()
			// Wait for the last flush to complete before returning.
			b.flushMu.Lock()
			done := b.flushDone
			b.flushMu.Unlock()
			<-done
			return
		}
	}
}

func (b *Bulk) Close() {
	close(b.shutdownCh)
	<-b.shutdownDoneCh
	b.session.Close()
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []Model) {
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 {
		slog.Warn("could not add new message to batch while rebalancing")
		return
	}

	atomic.StoreInt64(&b.metric.ProcessLatencyMs, time.Since(eventTime).Milliseconds())

	if len(actions) == 0 {
		return
	}

	var once sync.Once
	ackFn := func() { once.Do(ctx.Ack) }

	timestamp := b.resolveTimestamp(eventTime)
	if timestamp != 0 {
		for _, action := range actions {
			if raw, ok := action.(*Raw); ok {
				raw.Timestamp = timestamp
			}
		}
	}

	// Assign a unique event ID so multi-row events can be grouped for batchPerEvent.
	eventID := atomic.AddInt64(&b.eventCounter, 1)

	totalSize := 0
	items := make([]BatchItem, 0, len(actions))
	for _, action := range actions {
		if action == nil {
			continue
		}
		items = append(items, BatchItem{Model: action, Ack: ackFn, EventID: eventID})
		totalSize += estimateSize(action)
	}

	if len(items) == 0 {
		return
	}

	b.batchMutex.Lock()

	// Flush first if adding these items would breach limits.
	if len(b.batchBuffer) > 0 &&
		(b.currentBatchSize+len(items) > b.batchSizeLimit ||
			b.currentByteSize+totalSize > b.batchByteSizeLimit) {
		b.flushLocked()
	}

	b.batchBuffer = append(b.batchBuffer, items...)
	b.currentBatchSize += len(items)
	b.currentByteSize += totalSize

	// Flush if limits are now reached.
	if b.currentBatchSize >= b.batchSizeLimit || b.currentByteSize >= b.batchByteSizeLimit {
		b.flushLocked()
	}

	b.batchMutex.Unlock()
}

// flushLocked swaps the buffer and dispatches a flush goroutine.
// Enforces a maximum of 2 buffers in the system (one active, one in-flight):
// if a flush is already in progress, it waits for it to complete before
// swapping. This bounds memory usage and provides backpressure to the DCP
// goroutine when Cassandra cannot keep up.
// Must be called with batchMutex held.
func (b *Bulk) flushLocked() {
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 || len(b.batchBuffer) == 0 {
		return
	}

	// Wait for any in-flight flush to complete before swapping.
	// This is the backpressure point: if Cassandra is slow, AddActions blocks here.
	b.flushMu.Lock()
	prevDone := b.flushDone
	b.flushMu.Unlock()

	// Release batchMutex while waiting so ticker goroutine is not starved.
	b.batchMutex.Unlock()
	<-prevDone
	b.batchMutex.Lock()

	// Re-check after re-acquiring the lock — buffer may have been flushed
	// by ticker while we were waiting.
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 || len(b.batchBuffer) == 0 {
		return
	}

	// Swap the buffer — AddActions can now write to a fresh buffer.
	batch := b.batchBuffer
	b.batchBuffer = make([]BatchItem, 0, b.batchSizeLimit)
	b.currentBatchSize = 0
	b.currentByteSize = 0

	// Reset the ticker so it doesn't fire immediately after a size-triggered flush.
	b.batchTicker.Reset(b.batchTickerDuration)

	// Create a new done channel for this flush.
	thisDone := make(chan struct{})
	b.flushMu.Lock()
	b.flushDone = thisDone
	b.flushMu.Unlock()

	go b.runFlush(batch, thisDone)
}

// runFlush writes all items from batch to Cassandra concurrently
// (bounded by maxInFlightRequests), acks them, and calls dcpCheckpointCommit.
// The caller (flushLocked) guarantees no other flush is in progress.
func (b *Bulk) runFlush(batch []BatchItem, thisDone chan struct{}) {
	defer close(thisDone)

	ctx, span := tracer.Start(context.Background(), "cassandra.flush",
		trace.WithAttributes(
			attribute.String("db.system", "cassandra"),
			attribute.String("db.name", b.keyspace),
			attribute.Int("batch.size", len(batch)),
			attribute.Bool("batch.per_event", b.batchPerEvent),
		))
	defer span.End()

	startedTime := time.Now()

	if b.batchPerEvent {
		b.writeByEvent(ctx, batch)
	} else {
		b.writeConcurrently(ctx, batch)
	}

	for _, item := range batch {
		if item.Ack != nil {
			item.Ack()
		}
	}

	b.dcpCheckpointCommit()

	latency := time.Since(startedTime).Milliseconds()
	span.SetAttributes(attribute.Int64("flush.latency_ms", latency))
	atomic.StoreInt64(&b.metric.BulkRequestSize, int64(len(batch)))
	atomic.StoreInt64(&b.metric.BulkRequestProcessLatencyMs, latency)
}

// writeConcurrently writes all items independently with a semaphore bounding
// the number of concurrent Cassandra requests.
func (b *Bulk) writeConcurrently(ctx context.Context, batch []BatchItem) {
	semaphore := make(chan struct{}, b.maxInFlightRequests)
	var wg sync.WaitGroup

	for _, item := range batch {
		if item.Model == nil {
			continue
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func(it BatchItem) {
			defer wg.Done()
			defer func() { <-semaphore }()
			b.requestSync(ctx, it)
		}(item)
	}

	wg.Wait()
}

// writeByEvent groups items by EventID and writes each group as a single
// CQL UNLOGGED BATCH, bounded by the semaphore.
func (b *Bulk) writeByEvent(ctx context.Context, batch []BatchItem) {
	type group struct {
		eventID int64
		items   []BatchItem
	}
	seen := make(map[int64]int, len(batch))
	groups := make([]group, 0, len(batch))

	for _, item := range batch {
		if idx, ok := seen[item.EventID]; ok {
			groups[idx].items = append(groups[idx].items, item)
		} else {
			seen[item.EventID] = len(groups)
			groups = append(groups, group{eventID: item.EventID, items: []BatchItem{item}})
		}
	}

	semaphore := make(chan struct{}, b.maxInFlightRequests)
	var wg sync.WaitGroup

	for _, g := range groups {
		if len(g.items) == 0 {
			continue
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func(items []BatchItem) {
			defer wg.Done()
			defer func() { <-semaphore }()
			if len(items) == 1 {
				b.requestSync(ctx, items[0])
			} else {
				b.writeUnloggedBatch(ctx, items)
			}
		}(g.items)
	}

	wg.Wait()
}

// writeUnloggedBatch writes multiple items from the same DCP event as a
// single CQL UNLOGGED BATCH.
func (b *Bulk) writeUnloggedBatch(ctx context.Context, items []BatchItem) {
	_, span := tracer.Start(ctx, "cassandra.batch",
		trace.WithAttributes(
			attribute.Int("batch.items", len(items)),
		))
	defer span.End()

	batch := b.session.NewBatch(UnloggedBatch)

	for _, item := range items {
		if item.Model == nil {
			continue
		}
		rawModel, ok := item.Model.(*Raw)
		if !ok {
			continue
		}
		query, values := b.buildQueryAndValues(rawModel)
		if rawModel.Timestamp > 0 {
			batch.WithTimestamp(rawModel.Timestamp)
		}
		batch.Query(query, values...)
	}

	if batch.Size() == 0 {
		return
	}

	if err := batch.ExecuteBatch(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		panic(fmt.Sprintf("Cassandra unlogged batch write failed: %v", err))
	}
}

func (b *Bulk) requestSync(ctx context.Context, item BatchItem) {
	if item.Model == nil {
		return
	}

	rawModel, ok := item.Model.(*Raw)
	if !ok {
		return
	}

	_, span := tracer.Start(ctx, "cassandra.write",
		trace.WithAttributes(
			attribute.String("db.cassandra.table", rawModel.Table),
			attribute.String("db.operation", string(rawModel.Operation)),
		))
	defer span.End()

	var err error
	switch rawModel.Operation {
	case Insert, Upsert:
		err = b.insert(rawModel)
	case Update:
		err = b.update(rawModel)
	case Delete:
		err = b.delete(rawModel)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		panic(fmt.Sprintf("Cassandra %s failed on table %s: %v", rawModel.Operation, rawModel.Table, err))
	}
}

func (b *Bulk) insert(raw *Raw) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}
	query, values := b.buildInsertValues(raw, raw.Timestamp > 0)
	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) update(raw *Raw) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}
	query, values := b.buildUpdateValues(raw, raw.Timestamp > 0)
	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) delete(raw *Raw) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}
	query, values := b.buildDeleteValues(raw, raw.Timestamp > 0)
	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) resolveTimestamp(eventTime time.Time) int64 {
	switch b.writeTimestamp {
	case writeTimestampEventTime:
		return eventTime.UnixMicro()
	case writeTimestampNow:
		return time.Now().UnixMicro()
	default:
		return 0
	}
}

func estimateSize(model Model) int {
	raw, ok := model.(*Raw)
	if !ok {
		return 0
	}
	size := 0
	for k, v := range raw.Document {
		size += len(k)
		size += estimateValueSize(v)
	}
	for k, v := range raw.Filter {
		size += len(k)
		size += estimateValueSize(v)
	}
	return size
}

func estimateValueSize(v any) int {
	switch val := v.(type) {
	case string:
		return len(val)
	case []byte:
		return len(val)
	default:
		return 8
	}
}

func (b *Bulk) PrepareStartRebalancing() {
	atomic.StoreInt32(&b.isDcpRebalancing, 1)
	// Acquire batchMutex to ensure any in-progress AddActions completes.
	b.batchMutex.Lock()
	b.batchMutex.Unlock() //nolint:staticcheck
}

func (b *Bulk) PrepareEndRebalancing() {
	atomic.StoreInt32(&b.isDcpRebalancing, 0)
}

func (b *Bulk) GetMetric() *Metric {
	if b.metric == nil {
		return &Metric{}
	}
	return &Metric{
		ProcessLatencyMs:            atomic.LoadInt64(&b.metric.ProcessLatencyMs),
		BulkRequestProcessLatencyMs: atomic.LoadInt64(&b.metric.BulkRequestProcessLatencyMs),
		BulkRequestSize:             atomic.LoadInt64(&b.metric.BulkRequestSize),
		BulkRequestByteSize:         atomic.LoadInt64(&b.metric.BulkRequestByteSize),
	}
}

//nolint:funlen
func (b *Bulk) getCachedPreparedStatement(cacheKey string, raw *Raw, operation string) string {
	b.preparedStmtsMutex.RLock()
	if query, exists := b.preparedStmts[cacheKey]; exists {
		b.preparedStmtsMutex.RUnlock()
		return query
	}
	b.preparedStmtsMutex.RUnlock()

	b.preparedStmtsMutex.Lock()
	defer b.preparedStmtsMutex.Unlock()

	if query, exists := b.preparedStmts[cacheKey]; exists {
		return query
	}

	hasTS := raw.Timestamp > 0
	var query string

	switch operation {
	case "INSERT":
		columns := sortedKeys(raw.Document)
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			b.keyspace, raw.Table, strings.Join(columns, ","), strings.Join(placeholders, ","))
		if hasTS {
			query += " USING TIMESTAMP ?"
		}
	case "UPDATE":
		docColumns := sortedKeys(raw.Document)
		setParts := make([]string, len(docColumns))
		for i, k := range docColumns {
			setParts[i] = fmt.Sprintf("%s = ?", k)
		}
		filterColumns := sortedKeys(raw.Filter)
		whereParts := make([]string, len(filterColumns))
		for i, k := range filterColumns {
			whereParts[i] = fmt.Sprintf("%s = ?", k)
		}
		if hasTS {
			query = fmt.Sprintf("UPDATE %s.%s USING TIMESTAMP ? SET %s WHERE %s",
				b.keyspace, raw.Table, strings.Join(setParts, ","), strings.Join(whereParts, " AND "))
		} else {
			query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
				b.keyspace, raw.Table, strings.Join(setParts, ","), strings.Join(whereParts, " AND "))
		}
	case "DELETE":
		filterColumns := sortedKeys(raw.Filter)
		whereParts := make([]string, len(filterColumns))
		for i, k := range filterColumns {
			whereParts[i] = fmt.Sprintf("%s = ?", k)
		}
		if hasTS {
			query = fmt.Sprintf("DELETE FROM %s.%s USING TIMESTAMP ? WHERE %s",
				b.keyspace, raw.Table, strings.Join(whereParts, " AND "))
		} else {
			query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
				b.keyspace, raw.Table, strings.Join(whereParts, " AND "))
		}
	}

	b.preparedStmts[cacheKey] = query
	return query
}

func (b *Bulk) buildQueryAndValues(raw *Raw) (string, []any) {
	hasTS := raw.Timestamp > 0
	switch raw.Operation {
	case Insert, Upsert:
		return b.buildInsertValues(raw, hasTS)
	case Update:
		return b.buildUpdateValues(raw, hasTS)
	case Delete:
		return b.buildDeleteValues(raw, hasTS)
	default:
		return b.buildInsertValues(raw, hasTS)
	}
}

func (b *Bulk) buildInsertValues(raw *Raw, hasTS bool) (string, []any) {
	columns := sortedKeys(raw.Document)
	cacheKey := fmt.Sprintf("INSERT:%s:%s:%v", raw.Table, strings.Join(columns, ","), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")
	values := make([]any, 0, len(columns)+1)
	for _, col := range columns {
		values = append(values, raw.Document[col])
	}
	if hasTS {
		values = append(values, raw.Timestamp)
	}
	return query, values
}

func (b *Bulk) buildUpdateValues(raw *Raw, hasTS bool) (string, []any) {
	docColumns := sortedKeys(raw.Document)
	filterColumns := sortedKeys(raw.Filter)
	cacheKey := fmt.Sprintf("UPDATE:%s:%s:%s:%v", raw.Table, strings.Join(docColumns, ","), strings.Join(filterColumns, ","), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "UPDATE")
	values := make([]any, 0, len(docColumns)+len(filterColumns)+1)

	if hasTS {
		values = append(values, raw.Timestamp)
	}
	for _, col := range docColumns {
		values = append(values, raw.Document[col])
	}
	for _, col := range filterColumns {
		values = append(values, raw.Filter[col])
	}
	return query, values
}

func (b *Bulk) buildDeleteValues(raw *Raw, hasTS bool) (string, []any) {
	filterColumns := sortedKeys(raw.Filter)
	cacheKey := fmt.Sprintf("DELETE:%s:%s:%v", raw.Table, strings.Join(filterColumns, ","), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "DELETE")
	values := make([]any, 0, len(filterColumns)+1)

	if hasTS {
		values = append(values, raw.Timestamp)
	}
	for _, col := range filterColumns {
		values = append(values, raw.Filter[col])
	}
	return query, values
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
