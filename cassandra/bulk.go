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

// lane holds all mutable state that is local to a single worker lane.
// Because every vbucket is pinned to exactly one lane (vbID % len(lanes)),
// the lane's flushLock only ever has to serialize events within that lane,
// not across the whole connector.  Different lanes run entirely independently
// and in parallel.
type lane struct {
	queue         chan []BatchItem
	flushLock     sync.Mutex
	batch         []BatchItem
	batchKeys     map[string]int
	batchTicker   *time.Ticker
	shutdownCh    chan struct{}
	batchIndex    int
	batchSize     int
	batchByteSize int
}

type Bulk struct {
	session             Session
	dcpCheckpointCommit func()
	lanes               []*lane
	preparedStmts       map[string]string
	metric              *Metric
	shutdownCh          chan struct{}
	keyspace            string
	wg                  sync.WaitGroup
	batchByteSizeLimit  int
	batchSizeLimit      int
	batchType           BatchType
	maxBatchSize        int
	batchScope          string
	ackMode             string
	writeTimestamp      string
	preparedStmtsMutex  sync.RWMutex
	isDcpRebalancing    int32
	useBatch            bool
}

type BatchItem struct {
	Model           Model
	Acks            []func()
	Size            int
	TimestampMicros *int64
	VbID            uint16
	IsEventScoped   bool
}

const (
	batchScopeGlobal    = "global"
	batchScopeEvent     = "event"
	ackModeImmediate    = "immediate"
	ackModeAfterWrite   = "after_write"
	writeTimestampNone  = "none"
	writeTimestampEvent = "event_time"
	writeTimestampNow   = "now"
)

type Mapper func(event interface{}) []Model

type BulkBuilder struct{}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
	BulkRequestSize             int64
	BulkRequestByteSize         int64
}

func newLane(batchSizeLimit int, tickerDuration time.Duration, eventQueueSize int) *lane {
	return &lane{
		queue:       make(chan []BatchItem, eventQueueSize),
		batch:       make([]BatchItem, 0, batchSizeLimit),
		batchKeys:   make(map[string]int, batchSizeLimit),
		batchTicker: time.NewTicker(tickerDuration),
		shutdownCh:  make(chan struct{}),
	}
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
	batchSizeLimit := cfg.Cassandra.BatchSizeLimit
	if batchSizeLimit <= 0 {
		batchSizeLimit = 1000
	}
	batchByteSizeLimit := cfg.Cassandra.BatchByteSizeLimit
	if batchByteSizeLimit <= 0 {
		batchByteSizeLimit = 10485760
	}

	lanes := make([]*lane, workerCount)
	for i := range lanes {
		lanes[i] = newLane(batchSizeLimit, cfg.Cassandra.BatchTickerDuration, cfg.Cassandra.EventQueueSize)
	}

	b := &Bulk{
		session:             realSession,
		keyspace:            cfg.Cassandra.Keyspace,
		dcpCheckpointCommit: dcpCheckpointCommit,
		lanes:               lanes,
		batchSizeLimit:      batchSizeLimit,
		batchByteSizeLimit:  batchByteSizeLimit,
		shutdownCh:          make(chan struct{}),
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		useBatch:            cfg.Cassandra.UseBatch,
		batchType:           getBatchType(cfg.Cassandra.BatchType),
		maxBatchSize:        cfg.Cassandra.MaxBatchSize,
		batchScope:          cfg.Cassandra.BatchScope,
		ackMode:             cfg.Cassandra.AckMode,
		writeTimestamp:      cfg.Cassandra.WriteTimestamp,
	}
	return b, nil
}

// laneFor returns the lane responsible for the given vbucket.
func (b *Bulk) laneFor(vbID uint16) *lane {
	return b.lanes[int(vbID)%len(b.lanes)]
}

func (b *Bulk) StartBulk() {
	for _, l := range b.lanes {
		b.wg.Add(1)
		go b.runLane(l)
	}
	<-b.shutdownCh
	b.wg.Wait()
}

// runLane owns the ticker-driven flush loop and the worker loop for one lane.
// Both are multiplexed in a single goroutine so the lane's batch buffer is only
// ever touched by this goroutine (or by AddActions under the lane's flushLock).
func (b *Bulk) runLane(l *lane) {
	defer b.wg.Done()
	for {
		select {
		case <-l.batchTicker.C:
			l.flushLock.Lock()
			b.flushLaneLocked(l)
			l.flushLock.Unlock()

		case batch, ok := <-l.queue:
			if !ok {
				// queue closed — drain remaining ticker ticks then return
				l.batchTicker.Stop()
				return
			}
			if batch != nil {
				b.processBatch(batch)
			}

		case <-l.shutdownCh:
			l.batchTicker.Stop()
			// Flush any buffered global-scope items before closing
			l.flushLock.Lock()
			b.flushLaneLocked(l)
			l.flushLock.Unlock()
			close(l.queue)
			// Drain remaining queued batches
			for batch := range l.queue {
				if batch != nil {
					b.processBatch(batch)
				}
			}
			return
		}
	}
}

func (b *Bulk) Close() {
	close(b.shutdownCh)
	for _, l := range b.lanes {
		close(l.shutdownCh)
	}
	b.wg.Wait()
	b.session.Close()
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, vbID uint16, actions []Model) {
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

	ackFn := b.createAckFunc(ctx)
	timestampMicros := b.resolveTimestampMicros(eventTime)
	l := b.laneFor(vbID)

	if b.batchScope == batchScopeEvent {
		// Event scope: enqueue directly to the lane's worker queue and block
		// until the write completes.  No lane lock is held during the wait,
		// so other vbuckets routed to different lanes proceed concurrently.
		b.processEventScopedActions(l, actions, ackFn, timestampMicros, vbID)
		return
	}

	// Global scope: buffer within the lane; only the lane's own lock is held.
	l.flushLock.Lock()
	b.ensureLaneBatchCapacity(l, len(actions))
	b.addLaneActions(l, actions, ackFn, timestampMicros, vbID)
	if l.batchSize >= b.batchSizeLimit || l.batchByteSize >= b.batchByteSizeLimit {
		b.flushLaneLocked(l)
	}
	l.flushLock.Unlock()
}

func (b *Bulk) createAckFunc(ctx *models.ListenerContext) func() {
	if b.ackMode == ackModeImmediate {
		ctx.Ack()
		return nil
	}

	var once sync.Once
	return func() {
		once.Do(ctx.Ack)
	}
}

func (b *Bulk) resolveTimestampMicros(eventTime time.Time) *int64 {
	if b.writeTimestamp == writeTimestampNone {
		return nil
	}

	var ts int64
	if b.writeTimestamp == writeTimestampNow {
		ts = time.Now().UnixMicro()
	} else {
		ts = eventTime.UnixMicro()
	}
	return &ts
}

// processEventScopedActions enqueues one batch per DCP event into the lane's
// queue and returns immediately (fire and forget).  The lane's worker goroutine
// will call ack and dcpCheckpointCommit after the write completes.
// Backpressure is provided by the bounded lane queue: when it is full this
// call blocks until the worker drains an entry, naturally throttling the
// upstream DCP dispatch goroutine.
func (b *Bulk) processEventScopedActions(l *lane, actions []Model, ackFn func(), timestampMicros *int64, vbID uint16) {
	eventBatch := make([]BatchItem, 0, len(actions))
	for _, action := range actions {
		if action == nil {
			continue
		}
		eventBatch = append(eventBatch, BatchItem{
			Model:           action,
			Size:            1,
			Acks:            []func(){ackFn},
			TimestampMicros: timestampMicros,
			VbID:            vbID,
			IsEventScoped:   true,
		})
	}

	if len(eventBatch) == 0 {
		if ackFn != nil {
			ackFn()
		}
		return
	}

	l.queue <- eventBatch
}

func (b *Bulk) ensureLaneBatchCapacity(l *lane, actionCount int) {
	if len(l.batch)+actionCount <= cap(l.batch) {
		return
	}

	newCapacity := cap(l.batch) * 2
	if newCapacity < len(l.batch)+actionCount {
		newCapacity = len(l.batch) + actionCount
	}
	newBatch := make([]BatchItem, len(l.batch), newCapacity)
	copy(newBatch, l.batch)
	l.batch = newBatch
}

func (b *Bulk) addLaneActions(l *lane, actions []Model, ackFn func(), timestampMicros *int64, vbID uint16) {
	for _, action := range actions {
		if action == nil {
			continue
		}
		key := b.getActionKey(l, action)
		itemSize := calcItemSize(action)

		if batchIndex, ok := l.batchKeys[key]; ok {
			current := l.batch[batchIndex]
			current.Model = action
			current.Size = 1
			if ackFn != nil {
				current.Acks = append(current.Acks, ackFn)
			}
			current.TimestampMicros = timestampMicros
			current.VbID = vbID
			l.batch[batchIndex] = current
			continue
		}

		l.batch = append(l.batch, BatchItem{
			Model:           action,
			Size:            1,
			Acks:            []func(){ackFn},
			TimestampMicros: timestampMicros,
			VbID:            vbID,
		})
		l.batchKeys[key] = l.batchIndex
		l.batchIndex++
		l.batchSize++
		l.batchByteSize += itemSize
	}
}

// flushLaneLocked drains the lane's buffer, groups items by vbucket, and
// processes each vbucket group concurrently via goroutines — bypassing the
// lane queue entirely to avoid deadlocking runLane with itself.
// Must be called with l.flushLock held.
func (b *Bulk) flushLaneLocked(l *lane) {
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 {
		return
	}
	if len(l.batch) == 0 {
		return
	}

	batchCopy := make([]BatchItem, len(l.batch))
	copy(batchCopy, l.batch)
	l.batch = l.batch[:0]
	l.batchKeys = make(map[string]int, b.batchSizeLimit)
	l.batchIndex = 0
	l.batchSize = 0
	l.batchByteSize = 0

	grouped := make(map[uint16][]BatchItem)
	order := make([]uint16, 0)
	for _, item := range batchCopy {
		if _, exists := grouped[item.VbID]; !exists {
			order = append(order, item.VbID)
		}
		grouped[item.VbID] = append(grouped[item.VbID], item)
	}

	var wg sync.WaitGroup
	wg.Add(len(order))
	for _, vbID := range order {
		group := grouped[vbID]
		go func(batch []BatchItem) {
			defer wg.Done()
			b.processBatch(batch)
		}(group)
	}
	wg.Wait()

	b.dcpCheckpointCommit()
}

func (b *Bulk) request(item BatchItem, wg *sync.WaitGroup) {
	defer wg.Done()

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
		err = b.insert(rawModel, item.TimestampMicros)
	case Update:
		err = b.update(rawModel, item.TimestampMicros)
	case Delete:
		err = b.delete(rawModel, item.TimestampMicros)
	}
	if err != nil {
		panic(fmt.Sprintf("Cassandra %s failed on table %s: %v", rawModel.Operation, rawModel.Table, err))
	}
}

//nolint:funlen
func (b *Bulk) processBatch(batch []BatchItem) {
	defer func() {
		if r := recover(); r != nil {
			panic(r)
		}
	}()

	startedTime := time.Now()

	if b.useBatch {
		err := b.processWithCqlBatch(batch)
		if err != nil {
			log.Printf("Cassandra batch write error: %v", err)
			panic(fmt.Sprintf("Cassandra batch write failed: %v", err))
		}
	} else {
		var wg sync.WaitGroup
		wg.Add(len(batch))

		for _, item := range batch {
			go b.request(item, &wg)
		}

		wg.Wait()
	}

	isEventScoped := len(batch) > 0 && batch[0].IsEventScoped

	for _, item := range batch {
		for _, ack := range item.Acks {
			if ack != nil {
				ack()
			}
		}
	}

	if isEventScoped {
		b.dcpCheckpointCommit()
	}

	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
}

func (b *Bulk) insert(raw *Raw, timestampMicros *int64) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	withTimestamp := timestampMicros != nil
	columns := sortedMapKeys(raw.Document)
	cacheKey := buildInsertCacheKeyFromColumns(raw.Table, columns, withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "INSERT", withTimestamp)

	values := make([]interface{}, 0, len(raw.Document)+1)
	for _, column := range columns {
		values = append(values, raw.Document[column])
	}
	if withTimestamp {
		values = append(values, *timestampMicros)
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) update(raw *Raw, timestampMicros *int64) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	withTimestamp := timestampMicros != nil
	docColumns := sortedMapKeys(raw.Document)
	filterColumns := sortedMapKeys(raw.Filter)
	cacheKey := buildUpdateCacheKeyFromColumns(raw.Table, docColumns, filterColumns, withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "UPDATE", withTimestamp)

	values := make([]interface{}, 0, len(raw.Document)+len(raw.Filter)+1)

	if withTimestamp {
		values = append(values, *timestampMicros)
	}

	for _, column := range docColumns {
		values = append(values, raw.Document[column])
	}

	for _, column := range filterColumns {
		values = append(values, raw.Filter[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) delete(raw *Raw, timestampMicros *int64) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	withTimestamp := timestampMicros != nil
	filterColumns := sortedMapKeys(raw.Filter)
	cacheKey := buildDeleteCacheKeyFromColumns(raw.Table, filterColumns, withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "DELETE", withTimestamp)

	values := make([]interface{}, 0, len(raw.Filter)+1)
	if withTimestamp {
		values = append(values, *timestampMicros)
	}
	for _, column := range filterColumns {
		values = append(values, raw.Filter[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) getActionKey(l *lane, model Model) string {
	rawModel, ok := model.(*Raw)
	if !ok {
		return fmt.Sprintf("batch:%d", l.batchIndex)
	}

	var source map[string]interface{}
	switch rawModel.Operation {
	case Update, Delete:
		source = rawModel.Filter
	case Insert, Upsert:
		source = rawModel.Document
	}

	if len(source) == 0 {
		return fmt.Sprintf("batch:%d", l.batchIndex)
	}

	keys := make([]string, 0, len(source))
	for k := range source {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	key := rawModel.Table + ":"
	for _, k := range keys {
		key += fmt.Sprintf("%s=%v;", k, source[k])
	}
	return key
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

func sortedMapKeys(values map[string]interface{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func buildInsertCacheKey(table string, document map[string]interface{}, withTimestamp bool) string {
	return buildInsertCacheKeyFromColumns(table, sortedMapKeys(document), withTimestamp)
}

func buildUpdateCacheKey(table string, document map[string]interface{}, filter map[string]interface{}, withTimestamp bool) string {
	return buildUpdateCacheKeyFromColumns(table, sortedMapKeys(document), sortedMapKeys(filter), withTimestamp)
}

func buildDeleteCacheKey(table string, filter map[string]interface{}, withTimestamp bool) string {
	return buildDeleteCacheKeyFromColumns(table, sortedMapKeys(filter), withTimestamp)
}

func buildInsertCacheKeyFromColumns(table string, columns []string, withTimestamp bool) string {
	return fmt.Sprintf("INSERT:%s:%s:%t", table, strings.Join(columns, ","), withTimestamp)
}

func buildUpdateCacheKeyFromColumns(table string, docColumns []string, filterColumns []string, withTimestamp bool) string {
	return fmt.Sprintf(
		"UPDATE:%s:%s:%s:%t",
		table,
		strings.Join(docColumns, ","),
		strings.Join(filterColumns, ","),
		withTimestamp,
	)
}

func buildDeleteCacheKeyFromColumns(table string, filterColumns []string, withTimestamp bool) string {
	return fmt.Sprintf("DELETE:%s:%s:%t", table, strings.Join(filterColumns, ","), withTimestamp)
}

func calcItemSize(action Model) int {
	raw, ok := action.(*Raw)
	if !ok {
		return 0
	}

	total := 0
	if raw.Document != nil {
		for k, v := range raw.Document {
			total += len(k)
			if s, ok := v.(string); ok {
				total += len(s)
			} else {
				total += 8
			}
		}
	}

	if raw.Filter != nil {
		for k, v := range raw.Filter {
			total += len(k)
			if s, ok := v.(string); ok {
				total += len(s)
			} else {
				total += 8
			}
		}
	}

	return total
}

func (b *Bulk) PrepareStartRebalancing() {
	atomic.StoreInt32(&b.isDcpRebalancing, 1)
	// Acquire all lane locks to ensure any in-progress flush completes before
	// we return, and to drain any pending buffered batches.
	for _, l := range b.lanes {
		l.flushLock.Lock()
	}
	for _, l := range b.lanes {
		l.flushLock.Unlock()
	}
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
func (b *Bulk) getCachedPreparedStatement(cacheKey string, raw *Raw, operation string, withTimestamp bool) string {
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

	var query string
	switch operation {
	case "INSERT":
		columns := make([]string, 0, len(raw.Document))
		for k := range raw.Document {
			columns = append(columns, k)
		}
		// Sort columns to ensure consistent ordering
		sort.Strings(columns)

		placeholders := make([]string, 0, len(raw.Document))
		for range columns {
			placeholders = append(placeholders, "?")
		}
		query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", b.keyspace, raw.Table, join(columns, ","), join(placeholders, ","))
		if withTimestamp {
			query += " USING TIMESTAMP ?"
		}
	case "UPDATE":
		docColumns := make([]string, 0, len(raw.Document))
		for k := range raw.Document {
			docColumns = append(docColumns, k)
		}
		// Sort columns to ensure consistent ordering
		sort.Strings(docColumns)

		setParts := make([]string, 0, len(raw.Document))
		for _, k := range docColumns {
			setParts = append(setParts, fmt.Sprintf("%s = ?", k))
		}

		filterColumns := make([]string, 0, len(raw.Filter))
		for k := range raw.Filter {
			filterColumns = append(filterColumns, k)
		}
		// Sort filter columns to ensure consistent ordering
		sort.Strings(filterColumns)

		whereParts := make([]string, 0, len(raw.Filter))
		for _, k := range filterColumns {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", k))
		}
		if withTimestamp {
			query = fmt.Sprintf(
				"UPDATE %s.%s USING TIMESTAMP ? SET %s WHERE %s",
				b.keyspace,
				raw.Table,
				join(setParts, ","),
				join(whereParts, " AND "),
			)
		} else {
			query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", b.keyspace, raw.Table, join(setParts, ","), join(whereParts, " AND "))
		}
	case "DELETE":
		filterColumns := make([]string, 0, len(raw.Filter))
		for k := range raw.Filter {
			filterColumns = append(filterColumns, k)
		}
		// Sort filter columns to ensure consistent ordering
		sort.Strings(filterColumns)

		whereParts := make([]string, 0, len(raw.Filter))
		for _, k := range filterColumns {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", k))
		}
		if withTimestamp {
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

//nolint:funlen
func (b *Bulk) processWithCqlBatch(items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	batch := b.session.NewBatch(b.batchType)
	useBatchTimestamp := b.batchScope == batchScopeEvent && items[0].TimestampMicros != nil
	allowBatchSplitting := b.batchScope != batchScopeEvent
	if useBatchTimestamp {
		batch.WithTimestamp(*items[0].TimestampMicros)
	}

	for _, item := range items {
		if item.Model == nil {
			continue
		}

		rawModel, ok := item.Model.(*Raw)
		if !ok {
			continue
		}

		var query string
		var values []interface{}
		queryWithTimestamp := item.TimestampMicros != nil && !useBatchTimestamp

		switch rawModel.Operation {
		case Insert, Upsert:
			columns := sortedMapKeys(rawModel.Document)
			cacheKey := buildInsertCacheKeyFromColumns(rawModel.Table, columns, queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "INSERT", queryWithTimestamp)

			values = make([]interface{}, 0, len(rawModel.Document)+1)
			for _, column := range columns {
				values = append(values, rawModel.Document[column])
			}
			if queryWithTimestamp {
				values = append(values, *item.TimestampMicros)
			}

		case Update:
			docColumns := sortedMapKeys(rawModel.Document)
			filterColumns := sortedMapKeys(rawModel.Filter)
			cacheKey := buildUpdateCacheKeyFromColumns(rawModel.Table, docColumns, filterColumns, queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "UPDATE", queryWithTimestamp)

			values = make([]interface{}, 0, len(rawModel.Document)+len(rawModel.Filter)+1)

			if queryWithTimestamp {
				values = append(values, *item.TimestampMicros)
			}

			for _, column := range docColumns {
				values = append(values, rawModel.Document[column])
			}

			for _, column := range filterColumns {
				values = append(values, rawModel.Filter[column])
			}

		case Delete:
			filterColumns := sortedMapKeys(rawModel.Filter)
			cacheKey := buildDeleteCacheKeyFromColumns(rawModel.Table, filterColumns, queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "DELETE", queryWithTimestamp)

			values = make([]interface{}, 0, len(rawModel.Filter)+1)
			if queryWithTimestamp {
				values = append(values, *item.TimestampMicros)
			}
			for _, column := range filterColumns {
				values = append(values, rawModel.Filter[column])
			}
		}

		batch.Query(query, values...)

		if allowBatchSplitting && batch.Size() >= b.maxBatchSize {
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
