package cassandra

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type BatchType int

const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

type Session interface {
	Query(string, ...interface{}) Query
	PreparedQuery(string, ...interface{}) Query
	NewBatch(BatchType) Batch
	Close()
}

type Query interface {
	Exec() error
}

type Batch interface {
	Query(string, ...interface{})
	WithTimestamp(int64)
	Size() int
	ExecuteBatch() error
}

type GocqlSessionAdapter struct {
	*gocql.Session
}

func NewGocqlSessionAdapter(session *gocql.Session) *GocqlSessionAdapter {
	return &GocqlSessionAdapter{Session: session}
}

func (s *GocqlSessionAdapter) Query(stmt string, values ...interface{}) Query {
	return &GocqlQueryAdapter{q: s.Session.Query(stmt, values...)}
}

// PreparedQuery delegates to gocql's internal prepared statement cache,
// which is already thread-safe. No custom cache needed.
func (s *GocqlSessionAdapter) PreparedQuery(stmt string, values ...interface{}) Query {
	return &GocqlQueryAdapter{q: s.Session.Query(stmt, values...)}
}

func (s *GocqlSessionAdapter) NewBatch(batchType BatchType) Batch {
	var gocqlBatchType gocql.BatchType
	switch batchType {
	case LoggedBatch:
		gocqlBatchType = gocql.LoggedBatch
	case UnloggedBatch:
		gocqlBatchType = gocql.UnloggedBatch
	case CounterBatch:
		gocqlBatchType = gocql.CounterBatch
	default:
		gocqlBatchType = gocql.LoggedBatch
	}

	return &GocqlBatchAdapter{
		batch: s.Batch(gocqlBatchType),
	}
}

type GocqlQueryAdapter struct {
	q *gocql.Query
}

func (q *GocqlQueryAdapter) Exec() error {
	return q.q.Exec()
}

type GocqlBatchAdapter struct {
	batch *gocql.Batch
}

func (b *GocqlBatchAdapter) Query(stmt string, values ...interface{}) {
	b.batch.Query(stmt, values...)
}

func (b *GocqlBatchAdapter) WithTimestamp(timestamp int64) {
	b.batch.WithTimestamp(timestamp)
}

func (b *GocqlBatchAdapter) Size() int {
	return b.batch.Size()
}

func (b *GocqlBatchAdapter) ExecuteBatch() error {
	return b.batch.Exec()
}
