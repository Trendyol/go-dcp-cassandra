package cassandra

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-dcp/models"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
)

type Bulk struct {
	session             Session
	jobQueue            chan []BatchItem
	dcpCheckpointCommit func()
	preparedStmts       map[string]string
	metric              *Metric
	shutdownCh          chan struct{}
	keyspace            string
	wg                  sync.WaitGroup
	preparedStmtsMutex  sync.RWMutex
	batchType           BatchType
	maxBatchSize        int
	isDcpRebalancing    int32
	useBatch            bool
	ackMode             string
	writeTimestamp      string
}

type BatchItem struct {
	Model Model
	Ack   func()
}

const (
	ackModeImmediate        = "immediate"
	ackModeAfterWrite       = "after_write"
	writeTimestampNone      = "none"
	writeTimestampEventTime = "event_time"
	writeTimestampNow       = "now"
)

type Mapper func(event interface{}) []Model

type BulkBuilder struct{}

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

	workerCount := cfg.Cassandra.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}

	b := &Bulk{
		session:             realSession,
		keyspace:            cfg.Cassandra.Keyspace,
		dcpCheckpointCommit: dcpCheckpointCommit,
		jobQueue:            make(chan []BatchItem, cfg.Cassandra.WorkerQueueSize),
		shutdownCh:          make(chan struct{}),
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		useBatch:            cfg.Cassandra.UseBatch,
		batchType:           getBatchType(cfg.Cassandra.BatchType),
		maxBatchSize:        cfg.Cassandra.MaxBatchSize,
		ackMode:             cfg.Cassandra.AckMode,
		writeTimestamp:      cfg.Cassandra.WriteTimestamp,
	}

	for i := 0; i < workerCount; i++ {
		b.wg.Add(1)
		go b.worker()
	}

	return b, nil
}

func (b *Bulk) worker() {
	defer b.wg.Done()
	for batch := range b.jobQueue {
		if batch != nil {
			b.processBatch(batch)
		}
	}
}

// StartBulk blocks until Close is called. With no buffer or ticker,
// this simply waits for the shutdown signal and drains the worker pool.
func (b *Bulk) StartBulk() {
	<-b.shutdownCh
	close(b.jobQueue)
	b.wg.Wait()
}

func (b *Bulk) Close() {
	close(b.shutdownCh)
	b.session.Close()
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []Model) {
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 {
		log.Printf("could not add new message to batch while rebalancing")
		return
	}

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()

	if len(actions) == 0 {
		if b.ackMode == ackModeImmediate {
			ctx.Ack()
		}
		return
	}

	var ackFn func()
	if b.ackMode == ackModeAfterWrite {
		var once sync.Once
		ackFn = func() { once.Do(ctx.Ack) }
	} else {
		ctx.Ack()
	}

	timestamp := b.resolveTimestamp(eventTime)
	if timestamp != 0 {
		for _, action := range actions {
			if raw, ok := action.(*Raw); ok {
				raw.Timestamp = timestamp
			}
		}
	}

	batch := make([]BatchItem, 0, len(actions))
	for _, action := range actions {
		if action == nil {
			continue
		}
		batch = append(batch, BatchItem{Model: action, Ack: ackFn})
	}

	if len(batch) == 0 {
		if ackFn != nil {
			ackFn()
		}
		return
	}

	// Blocks when jobQueue is full, backpressuring the DCP dispatch goroutine.
	b.jobQueue <- batch
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

func (b *Bulk) processBatch(batch []BatchItem) {
	defer func() {
		if r := recover(); r != nil {
			panic(r)
		}
	}()

	startedTime := time.Now()

	if b.useBatch {
		err := b.processBatchWithBatch(batch)
		if err != nil {
			log.Printf("Cassandra batch write error: %v", err)
			panic(fmt.Sprintf("Cassandra batch write failed: %v", err))
		}
	} else {
		for _, item := range batch {
			b.requestSync(item)
		}
	}

	for _, item := range batch {
		if item.Ack != nil {
			item.Ack()
		}
	}

	b.dcpCheckpointCommit()
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
}

func (b *Bulk) requestSync(item BatchItem) {
	if item.Model == nil {
		return
	}

	rawModel, ok := item.Model.(*Raw)
	if !ok {
		return
	}

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

func join(arr []string, sep string) string {
	result := ""
	for i, s := range arr {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

func (b *Bulk) PrepareStartRebalancing() {
	atomic.StoreInt32(&b.isDcpRebalancing, 1)
}

func (b *Bulk) PrepareEndRebalancing() {
	atomic.StoreInt32(&b.isDcpRebalancing, 0)
}

func (b *Bulk) GetMetric() *Metric {
	if b.metric == nil {
		return &Metric{}
	}
	return b.metric
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
		columns := make([]string, 0, len(raw.Document))
		for k := range raw.Document {
			columns = append(columns, k)
		}
		sort.Strings(columns)

		placeholders := make([]string, 0, len(raw.Document))
		for range columns {
			placeholders = append(placeholders, "?")
		}
		query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", b.keyspace, raw.Table, join(columns, ","), join(placeholders, ","))
		if hasTS {
			query += " USING TIMESTAMP ?"
		}
	case "UPDATE":
		docColumns := make([]string, 0, len(raw.Document))
		for k := range raw.Document {
			docColumns = append(docColumns, k)
		}
		sort.Strings(docColumns)

		setParts := make([]string, 0, len(raw.Document))
		for _, k := range docColumns {
			setParts = append(setParts, fmt.Sprintf("%s = ?", k))
		}

		filterColumns := make([]string, 0, len(raw.Filter))
		for k := range raw.Filter {
			filterColumns = append(filterColumns, k)
		}
		sort.Strings(filterColumns)

		whereParts := make([]string, 0, len(raw.Filter))
		for _, k := range filterColumns {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", k))
		}

		setClause := join(setParts, ",")
		whereClause := join(whereParts, " AND ")
		if hasTS {
			query = fmt.Sprintf("UPDATE %s.%s USING TIMESTAMP ? SET %s WHERE %s",
				b.keyspace, raw.Table, setClause, whereClause)
		} else {
			query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
				b.keyspace, raw.Table, setClause, whereClause)
		}
	case "DELETE":
		filterColumns := make([]string, 0, len(raw.Filter))
		for k := range raw.Filter {
			filterColumns = append(filterColumns, k)
		}
		sort.Strings(filterColumns)

		whereParts := make([]string, 0, len(raw.Filter))
		for _, k := range filterColumns {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", k))
		}

		if hasTS {
			query = fmt.Sprintf("DELETE FROM %s.%s USING TIMESTAMP ? WHERE %s", b.keyspace, raw.Table, join(whereParts, " AND "))
		} else {
			query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", b.keyspace, raw.Table, join(whereParts, " AND "))
		}
	}

	b.preparedStmts[cacheKey] = query
	return query
}

func getBatchType(batchTypeStr string) BatchType {
	switch strings.ToLower(batchTypeStr) {
	case "unlogged":
		return UnloggedBatch
	case "counter":
		return CounterBatch
	case "logged":
		fallthrough
	default:
		return LoggedBatch
	}
}

func (b *Bulk) processBatchWithBatch(items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	batch := b.session.NewBatch(b.batchType)

	for _, item := range items {
		if item.Model == nil {
			continue
		}

		rawModel, ok := item.Model.(*Raw)
		if !ok {
			continue
		}

		query, values := b.buildQueryAndValues(rawModel)
		batch.Query(query, values...)

		if batch.Size() >= b.maxBatchSize {
			if err := batch.ExecuteBatch(); err != nil {
				return err
			}
			batch = b.session.NewBatch(b.batchType)
		}
	}

	if batch.Size() > 0 {
		if err := batch.ExecuteBatch(); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bulk) buildQueryAndValues(raw *Raw) (string, []interface{}) {
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

func (b *Bulk) buildInsertValues(raw *Raw, hasTS bool) (string, []interface{}) {
	cacheKey := fmt.Sprintf("INSERT:%s:%d:%v", raw.Table, len(raw.Document), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")

	columns := sortedKeys(raw.Document)
	values := make([]interface{}, 0, len(raw.Document)+1)
	for _, col := range columns {
		values = append(values, raw.Document[col])
	}
	if hasTS {
		values = append(values, raw.Timestamp)
	}
	return query, values
}

func (b *Bulk) buildUpdateValues(raw *Raw, hasTS bool) (string, []interface{}) {
	cacheKey := fmt.Sprintf("UPDATE:%s:%d:%d:%v", raw.Table, len(raw.Document), len(raw.Filter), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "UPDATE")

	docColumns := sortedKeys(raw.Document)
	filterColumns := sortedKeys(raw.Filter)
	values := make([]interface{}, 0, len(raw.Document)+len(raw.Filter)+1)

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

func (b *Bulk) buildDeleteValues(raw *Raw, hasTS bool) (string, []interface{}) {
	cacheKey := fmt.Sprintf("DELETE:%s:%d:%v", raw.Table, len(raw.Filter), hasTS)
	query := b.getCachedPreparedStatement(cacheKey, raw, "DELETE")

	filterColumns := sortedKeys(raw.Filter)
	values := make([]interface{}, 0, len(raw.Filter)+1)

	if hasTS {
		values = append(values, raw.Timestamp)
	}
	for _, col := range filterColumns {
		values = append(values, raw.Filter[col])
	}
	return query, values
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
