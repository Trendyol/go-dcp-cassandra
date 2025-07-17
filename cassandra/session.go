package cassandra

import (
	"sync"

	"github.com/gocql/gocql"
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
	Size() int
	ExecuteBatch() error
}

type GocqlSessionAdapter struct {
	*gocql.Session
	preparedStmts map[string]*gocql.Query
	mutex         sync.RWMutex
}

func NewGocqlSessionAdapter(session *gocql.Session) *GocqlSessionAdapter {
	return &GocqlSessionAdapter{
		Session:       session,
		preparedStmts: make(map[string]*gocql.Query),
		mutex:         sync.RWMutex{},
	}
}

func (s *GocqlSessionAdapter) Query(stmt string, values ...interface{}) Query {
	return &GocqlQueryAdapter{q: s.Session.Query(stmt, values...)}
}

func (s *GocqlSessionAdapter) PreparedQuery(stmt string, values ...interface{}) Query {
	s.mutex.RLock()
	if preparedQuery, exists := s.preparedStmts[stmt]; exists {
		s.mutex.RUnlock()
		return &GocqlQueryAdapter{q: preparedQuery.Bind(values...)}
	}
	s.mutex.RUnlock()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if preparedQuery, exists := s.preparedStmts[stmt]; exists {
		return &GocqlQueryAdapter{q: preparedQuery.Bind(values...)}
	}

	preparedQuery := s.Session.Query(stmt)
	s.preparedStmts[stmt] = preparedQuery
	return &GocqlQueryAdapter{q: preparedQuery.Bind(values...)}
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
		batch:   s.Session.NewBatch(gocqlBatchType),
		session: s.Session,
	}
}

type GocqlQueryAdapter struct {
	q *gocql.Query
}

func (q *GocqlQueryAdapter) Exec() error {
	return q.q.Exec()
}

type GocqlBatchAdapter struct {
	batch   *gocql.Batch
	session *gocql.Session
}

func (b *GocqlBatchAdapter) Query(stmt string, values ...interface{}) {
	b.batch.Query(stmt, values...)
}

func (b *GocqlBatchAdapter) Size() int {
	return b.batch.Size()
}

func (b *GocqlBatchAdapter) ExecuteBatch() error {
	return b.session.ExecuteBatch(b.batch)
}
