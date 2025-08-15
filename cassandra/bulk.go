package cassandra

import (
	"fmt"
	config "go-dcp-cassandra/configs"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-dcp/models"
)

type Bulk struct {
	session             Session
	jobCh               chan []BatchItem
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
	preparedStmtsMutex  sync.RWMutex
	flushLock           sync.Mutex
	isDcpRebalancing    int32
	useBatch            bool
}

type BatchItem struct {
	Model Model
	Ctx   *models.ListenerContext
	Done  chan struct{}
	Size  int
}

type Mapper func(event interface{}) []Model

type BulkBuilder struct{}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
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
	if channelBufferSize > 10 {
		channelBufferSize = 10
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
		jobCh:               make(chan []BatchItem, channelBufferSize),
		shutdownCh:          make(chan struct{}),
		metric:              &Metric{},
		preparedStmts:       make(map[string]string),
		preparedStmtsMutex:  sync.RWMutex{},
		useBatch:            cfg.Cassandra.UseBatch,
		batchType:           getBatchType(cfg.Cassandra.BatchType),
		maxBatchSize:        cfg.Cassandra.MaxBatchSize,
	}
	return b, nil
}

func (b *Bulk) StartBulk() {
	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.worker()
	}

	for {
		select {
		case <-b.batchTicker.C:
			b.flushMessages()
		case <-b.shutdownCh:
			b.flushMessages()
			close(b.jobCh)
			b.wg.Wait()
			return
		}
	}
}

func (b *Bulk) worker() {
	defer b.wg.Done()
	for batch := range b.jobCh {
		if batch == nil {
			continue
		}
		b.processBatch(batch)
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
		err := b.processBatchWithBatch(batch)
		if err != nil {
			log.Printf("Cassandra batch write error: %v", err)
			panic(fmt.Sprintf("Cassandra batch write failed: %v", err))
		}
	} else {
		for _, item := range batch {
			if item.Model == nil {
				continue
			}

			rawModel, ok := item.Model.(*Raw)
			if !ok {
				continue
			}

			var err error
			docID := ""
			if rawModel.Document != nil {
				if id, ok := rawModel.Document["id"]; ok {
					if strID, ok := id.(string); ok {
						docID = strID
					}
				}
			} else if rawModel.Filter != nil {
				if id, ok := rawModel.Filter["id"]; ok {
					if strID, ok := id.(string); ok {
						docID = strID
					}
				}
			}

			switch rawModel.Operation {
			case Insert, Upsert:
				err = b.insert(rawModel)
			case Update:
				err = b.update(rawModel)
			case Delete:
				err = b.delete(rawModel)
			}
			if err != nil {
				panic(fmt.Sprintf("Cassandra write failed for doc id %s: %v", docID, err))
			}

			if item.Ctx != nil && item.Ctx.Ack != nil {
				item.Ctx.Ack()
			}
		}
	}

	b.metric.BulkRequestProcessLatencyMs = time.Since(startedTime).Milliseconds()
}

func (b *Bulk) Close() {
	close(b.shutdownCh)
	b.session.Close()
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []Model) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if atomic.LoadInt32(&b.isDcpRebalancing) != 0 {
		log.Printf("could not add new message to batch while rebalancing")
		return
	}

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()

	if len(b.batch)+len(actions) > cap(b.batch) {
		newCapacity := cap(b.batch) * 2
		if newCapacity < len(b.batch)+len(actions) {
			newCapacity = len(b.batch) + len(actions)
		}
		newBatch := make([]BatchItem, len(b.batch), newCapacity)
		copy(newBatch, b.batch)
		b.batch = newBatch
	}

	for _, action := range actions {
		if action == nil {
			continue
		}
		size := 1
		key := b.getActionKey(action)
		var itemSize int
		if raw, ok := action.(*Raw); ok && raw.Document != nil {
			for k, v := range raw.Document {
				itemSize += len(k)
				if s, ok := v.(string); ok {
					itemSize += len(s)
				} else {
					itemSize += 8
				}
			}
		}

		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batch[batchIndex] = BatchItem{
				Model: action,
				Size:  size,
				Ctx:   ctx,
			}
		} else {
			b.batch = append(b.batch, BatchItem{
				Model: action,
				Size:  size,
				Ctx:   ctx,
			})
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
			b.batchSize++
			b.batchByteSize += itemSize
		}
	}

	if b.batchSize >= b.batchSizeLimit || b.batchByteSize >= b.batchByteSizeLimit {
		b.flushMessagesLocked()
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
		done := make(chan struct{})
		batchCopy := make([]BatchItem, len(b.batch))
		copy(batchCopy, b.batch)
		for i := range batchCopy {
			batchCopy[i].Done = done
		}
		b.jobCh <- batchCopy
		<-done
		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0
		b.batchByteSize = 0
		b.dcpCheckpointCommit()
	}
}

func (b *Bulk) insert(raw *Raw) error {
	pkFields := b.getPrimaryKeyFields(raw)
	for _, field := range pkFields {
		if _, ok := raw.Document[field]; !ok {
			return fmt.Errorf("primary key field '%s' is required for idempotency", field)
		}
	}
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	cacheKey := fmt.Sprintf("INSERT:%s:%d", raw.Table, len(raw.Document))
	query := b.getCachedPreparedStatement(cacheKey, raw, "INSERT")

	// Sort columns to match the query preparation order
	columns := make([]string, 0, len(raw.Document))
	for k := range raw.Document {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	values := make([]interface{}, 0, len(raw.Document))
	for _, column := range columns {
		values = append(values, raw.Document[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) update(raw *Raw) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	cacheKey := fmt.Sprintf("UPDATE:%s:%d:%d", raw.Table, len(raw.Document), len(raw.Filter))
	query := b.getCachedPreparedStatement(cacheKey, raw, "UPDATE")

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

	values := make([]interface{}, 0, len(raw.Document)+len(raw.Filter))

	for _, column := range docColumns {
		values = append(values, raw.Document[column])
	}

	for _, column := range filterColumns {
		values = append(values, raw.Filter[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) delete(raw *Raw) error {
	if b.session == nil {
		return fmt.Errorf("cassandra session is nil")
	}

	cacheKey := fmt.Sprintf("DELETE:%s:%d", raw.Table, len(raw.Filter))
	query := b.getCachedPreparedStatement(cacheKey, raw, "DELETE")

	// Sort columns to match the query preparation order
	filterColumns := make([]string, 0, len(raw.Filter))
	for k := range raw.Filter {
		filterColumns = append(filterColumns, k)
	}
	sort.Strings(filterColumns)

	values := make([]interface{}, 0, len(raw.Filter))
	for _, column := range filterColumns {
		values = append(values, raw.Filter[column])
	}

	return b.session.PreparedQuery(query, values...).Exec()
}

func (b *Bulk) getActionKey(model Model) string {
	if rawModel, ok := model.(*Raw); ok && rawModel.Document != nil {
		pkFields := b.getPrimaryKeyFields(rawModel)
		key := rawModel.Table + ":"
		for _, field := range pkFields {
			if val, ok := rawModel.Document[field]; ok {
				key += fmt.Sprintf("%s=%v;", field, val)
			}
		}
		if key != rawModel.Table+":" {
			return key
		}
	}
	return fmt.Sprintf("batch:%d", b.batchIndex)
}

func (b *Bulk) getPrimaryKeyFields(raw *Raw) []string {
	return []string{"id"}
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
		query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", b.keyspace, raw.Table, join(setParts, ","), join(whereParts, " AND "))
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
		query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", b.keyspace, raw.Table, join(whereParts, " AND "))
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

		var query string
		var values []interface{}

		switch rawModel.Operation {
		case Insert, Upsert:
			cacheKey := fmt.Sprintf("INSERT:%s:%d", rawModel.Table, len(rawModel.Document))
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "INSERT")

			// Sort columns to match the query preparation order
			columns := make([]string, 0, len(rawModel.Document))
			for k := range rawModel.Document {
				columns = append(columns, k)
			}
			sort.Strings(columns)

			values = make([]interface{}, 0, len(rawModel.Document))
			for _, column := range columns {
				values = append(values, rawModel.Document[column])
			}

		case Update:
			cacheKey := fmt.Sprintf("UPDATE:%s:%d:%d", rawModel.Table, len(rawModel.Document), len(rawModel.Filter))
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "UPDATE")

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

			values = make([]interface{}, 0, len(rawModel.Document)+len(rawModel.Filter))

			for _, column := range docColumns {
				values = append(values, rawModel.Document[column])
			}

			for _, column := range filterColumns {
				values = append(values, rawModel.Filter[column])
			}

		case Delete:
			cacheKey := fmt.Sprintf("DELETE:%s:%d", rawModel.Table, len(rawModel.Filter))
			query = b.getCachedPreparedStatement(cacheKey, rawModel, "DELETE")

			// Sort columns to match the query preparation order
			filterColumns := make([]string, 0, len(rawModel.Filter))
			for k := range rawModel.Filter {
				filterColumns = append(filterColumns, k)
			}
			sort.Strings(filterColumns)

			values = make([]interface{}, 0, len(rawModel.Filter))
			for _, column := range filterColumns {
				values = append(values, rawModel.Filter[column])
			}
		}

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

	for _, item := range items {
		if item.Ctx != nil && item.Ctx.Ack != nil {
			item.Ctx.Ack()
		}
	}

	return nil
}
