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
	jobCh               chan []BatchItem
	workerQueues        []chan []BatchItem
	dcpCheckpointCommit func()
	batchTicker         *time.Ticker
	preparedStmts       map[string]string
	batchKeys           map[string]int
	metric              *Metric
	shutdownCh          chan struct{}
	keyspace            string
	batch               []BatchItem
	wg                  sync.WaitGroup
	batchByteSizeLimit  int
	workerCount         int
	batchByteSize       int
	batchSize           int
	batchIndex          int
	batchSizeLimit      int
	batchType           BatchType
	maxBatchSize        int
	batchScope          string
	ackMode             string
	writeTimestamp      string
	preparedStmtsMutex  sync.RWMutex
	flushLock           sync.Mutex
	isDcpRebalancing    int32
	useBatch            bool
}

type BatchItem struct {
	Model           Model
	Done            chan struct{}
	Acks            []func()
	Size            int
	TimestampMicros *int64
	VbID            uint16
}

const (
	batchScopeGlobal    = "global"
	batchScopeEvent     = "event"
	ackModeImmediate    = "immediate"
	ackModeAfterWrite   = "after_write"
	writeTimestampNone  = "none"
	writeTimestampEvent = "event_time"
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
	batchSizeLimit := cfg.Cassandra.BatchSizeLimit
	if batchSizeLimit <= 0 {
		batchSizeLimit = 1000
	}
	batchByteSizeLimit := cfg.Cassandra.BatchByteSizeLimit
	if batchByteSizeLimit <= 0 {
		batchByteSizeLimit = 10485760
	}

	channelBufferSize := workerCount
	workerQueues := make([]chan []BatchItem, workerCount)
	for i := range workerQueues {
		workerQueues[i] = make(chan []BatchItem, channelBufferSize)
	}

	b := &Bulk{
		session:             realSession,
		keyspace:            cfg.Cassandra.Keyspace,
		dcpCheckpointCommit: dcpCheckpointCommit,
		batchTicker:         time.NewTicker(cfg.Cassandra.BatchTickerDuration),
		batchSizeLimit:      batchSizeLimit,
		batchByteSizeLimit:  batchByteSizeLimit,
		batch:               make([]BatchItem, 0, batchSizeLimit),
		batchKeys:           make(map[string]int, batchSizeLimit),
		workerCount:         workerCount,
		workerQueues:        workerQueues,
		jobCh:               workerQueues[0],
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

func (b *Bulk) StartBulk() {
	for i := 0; i < len(b.workerQueues); i++ {
		b.wg.Add(1)
		go b.workerOnQueue(b.workerQueues[i])
	}

	for {
		select {
		case <-b.batchTicker.C:
			b.flushMessages()
		case <-b.shutdownCh:
			b.flushMessages()
			for _, q := range b.workerQueues {
				close(q)
			}
			b.wg.Wait()
			return
		}
	}
}

func (b *Bulk) worker() {
	b.workerOnQueue(b.jobCh)
}

func (b *Bulk) workerOnQueue(queue <-chan []BatchItem) {
	defer b.wg.Done()
	for batch := range queue {
		if batch == nil {
			continue
		}
		b.processBatch(batch)
	}
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
	var doneChannel chan struct{}
	if len(batch) > 0 {
		doneChannel = batch[0].Done
	}

	defer func() {
		if r := recover(); r != nil {
			if doneChannel != nil {
				select {
				case <-doneChannel:
				default:
					close(doneChannel)
				}
			}
			panic(r)
		} else if doneChannel != nil {
			close(doneChannel)
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

	for _, item := range batch {
		for _, ack := range item.Acks {
			if ack != nil {
				ack()
			}
		}
	}

	b.metric.BulkRequestSize = int64(b.batchSize)
	b.metric.BulkRequestByteSize = int64(b.batchByteSize)
	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
}

func (b *Bulk) Close() {
	close(b.shutdownCh)
	b.session.Close()
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, vbID uint16, actions []Model) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

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

	if b.batchScope == batchScopeEvent {
		b.processEventScopedActions(actions, ackFn, timestampMicros, vbID)
		return
	}

	b.ensureBatchCapacity(len(actions))
	b.addGlobalActions(actions, ackFn, timestampMicros, vbID)

	if b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit {
		b.flushMessagesLocked()
	}
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
	if b.writeTimestamp != writeTimestampEvent {
		return nil
	}
	ts := eventTime.UnixMicro()
	return &ts
}

func (b *Bulk) processEventScopedActions(actions []Model, ackFn func(), timestampMicros *int64, vbID uint16) {
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
		})
	}

	if len(eventBatch) == 0 {
		if ackFn != nil {
			ackFn()
		}
		return
	}

	done := make(chan struct{})
	for i := range eventBatch {
		eventBatch[i].Done = done
	}

	b.enqueueBatch(vbID, eventBatch)
	<-done
	b.dcpCheckpointCommit()
}

func (b *Bulk) ensureBatchCapacity(actionCount int) {
	if len(b.batch)+actionCount <= cap(b.batch) {
		return
	}

	newCapacity := cap(b.batch) * 2
	if newCapacity < len(b.batch)+actionCount {
		newCapacity = len(b.batch) + actionCount
	}
	newBatch := make([]BatchItem, len(b.batch), newCapacity)
	copy(newBatch, b.batch)
	b.batch = newBatch
}

func (b *Bulk) addGlobalActions(actions []Model, ackFn func(), timestampMicros *int64, vbID uint16) {
	for _, action := range actions {
		if action == nil {
			continue
		}
		key := b.getActionKey(action)
		itemSize := calcItemSize(action)

		if batchIndex, ok := b.batchKeys[key]; ok {
			current := b.batch[batchIndex]
			current.Model = action
			current.Size = 1
			if ackFn != nil {
				current.Acks = append(current.Acks, ackFn)
			}
			current.TimestampMicros = timestampMicros
			current.VbID = vbID
			b.batch[batchIndex] = current
			continue
		}

		b.batch = append(b.batch, BatchItem{
			Model:           action,
			Size:            1,
			Acks:            []func(){ackFn},
			TimestampMicros: timestampMicros,
			VbID:            vbID,
		})
		b.batchKeys[key] = b.batchIndex
		b.batchIndex++
		b.batchSize++
		b.batchByteSize += itemSize
	}
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	b.flushMessagesLocked()
}

func (b *Bulk) flushMessagesLocked() {
	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 {
		return
	}
	if len(b.batch) > 0 {
		batchCopy := make([]BatchItem, len(b.batch))
		copy(batchCopy, b.batch)
		b.dispatchBatchByVb(batchCopy)
		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
		b.dcpCheckpointCommit()
	}
}

func (b *Bulk) dispatchBatchByVb(items []BatchItem) {
	if len(items) == 0 {
		return
	}

	groupped := make(map[uint16][]BatchItem)
	order := make([]uint16, 0)
	for _, item := range items {
		if _, exists := groupped[item.VbID]; !exists {
			order = append(order, item.VbID)
		}
		groupped[item.VbID] = append(groupped[item.VbID], item)
	}

	dones := make([]chan struct{}, 0, len(order))
	for _, vbID := range order {
		group := make([]BatchItem, len(groupped[vbID]))
		copy(group, groupped[vbID])
		done := make(chan struct{})
		for i := range group {
			group[i].Done = done
		}
		dones = append(dones, done)
		b.enqueueBatch(vbID, group)
	}

	for _, done := range dones {
		<-done
	}
}

func (b *Bulk) enqueueBatch(vbID uint16, batch []BatchItem) {
	if len(b.workerQueues) <= 1 {
		b.jobCh <- batch
		return
	}

	index := int(vbID) % len(b.workerQueues)
	b.workerQueues[index] <- batch
}

func (b *Bulk) insert(raw *Raw, timestampMicros *int64) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	withTimestamp := timestampMicros != nil
	cacheKey := fmt.Sprintf("INSERT:%s:%d:%t", raw.Table, len(raw.Document), withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "INSERT", withTimestamp)

	// Sort columns to match the query preparation order
	columns := make([]string, 0, len(raw.Document))
	for k := range raw.Document {
		columns = append(columns, k)
	}
	sort.Strings(columns)

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
	cacheKey := fmt.Sprintf("UPDATE:%s:%d:%d:%t", raw.Table, len(raw.Document), len(raw.Filter), withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "UPDATE", withTimestamp)

	// Sort columns to match the query preparation order
	docColumns := make([]string, 0, len(raw.Document))
	for k := range raw.Document {
		docColumns = append(docColumns, k)
	}
	sort.Strings(docColumns)

	filterColumns := make([]string, 0, len(raw.Filter))
	for k := range raw.Filter {
		filterColumns = append(filterColumns, k)
	}
	sort.Strings(filterColumns)

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
	cacheKey := fmt.Sprintf("DELETE:%s:%d:%t", raw.Table, len(raw.Filter), withTimestamp)
	query := b.getCachedPreparedStatement(cacheKey, raw, "DELETE", withTimestamp)

	// Sort columns to match the query preparation order
	filterColumns := make([]string, 0, len(raw.Filter))
	for k := range raw.Filter {
		filterColumns = append(filterColumns, k)
	}
	sort.Strings(filterColumns)

	values := make([]interface{}, 0, len(raw.Filter)+1)
	if withTimestamp {
		values = append(values, *timestampMicros)
	}
	for _, column := range filterColumns {
		values = append(values, raw.Filter[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) getActionKey(model Model) string {
	rawModel, ok := model.(*Raw)
	if !ok {
		return fmt.Sprintf("batch:%d", b.batchIndex)
	}

	var source map[string]interface{}
	switch rawModel.Operation {
	case Update, Delete:
		source = rawModel.Filter
	case Insert, Upsert:
		source = rawModel.Document
	}

	if len(source) == 0 {
		return fmt.Sprintf("batch:%d", b.batchIndex)
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
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	atomic.StoreInt32(&b.isDcpRebalancing, 1)
}

func (b *Bulk) PrepareEndRebalancing() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

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
			cacheKey := fmt.Sprintf("INSERT:%s:%d:%t", rawModel.Table, len(rawModel.Document), queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "INSERT", queryWithTimestamp)

			// Sort columns to match the query preparation order
			columns := make([]string, 0, len(rawModel.Document))
			for k := range rawModel.Document {
				columns = append(columns, k)
			}
			sort.Strings(columns)

			values = make([]interface{}, 0, len(rawModel.Document)+1)
			for _, column := range columns {
				values = append(values, rawModel.Document[column])
			}
			if queryWithTimestamp {
				values = append(values, *item.TimestampMicros)
			}

		case Update:
			cacheKey := fmt.Sprintf("UPDATE:%s:%d:%d:%t", rawModel.Table, len(rawModel.Document), len(rawModel.Filter), queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "UPDATE", queryWithTimestamp)

			// Sort columns to match the query preparation order
			docColumns := make([]string, 0, len(rawModel.Document))
			for k := range rawModel.Document {
				docColumns = append(docColumns, k)
			}
			sort.Strings(docColumns)

			filterColumns := make([]string, 0, len(rawModel.Filter))
			for k := range rawModel.Filter {
				filterColumns = append(filterColumns, k)
			}
			sort.Strings(filterColumns)

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
			cacheKey := fmt.Sprintf("DELETE:%s:%d:%t", rawModel.Table, len(rawModel.Filter), queryWithTimestamp)
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "DELETE", queryWithTimestamp)

			// Sort columns to match the query preparation order
			filterColumns := make([]string, 0, len(rawModel.Filter))
			for k := range rawModel.Filter {
				filterColumns = append(filterColumns, k)
			}
			sort.Strings(filterColumns)

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
