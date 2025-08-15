package cassandra

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type enhancedMockSession struct {
	queries                []string
	preparedQueries        []string
	queryCallCount         int
	preparedQueryCallCount int
	newBatchCallCount      int
}

func (m *enhancedMockSession) Query(stmt string, values ...interface{}) Query {
	m.queryCallCount++
	m.queries = append(m.queries, stmt)
	return &enhancedMockQuery{}
}

func (m *enhancedMockSession) PreparedQuery(stmt string, values ...interface{}) Query {
	m.preparedQueryCallCount++
	m.preparedQueries = append(m.preparedQueries, stmt)
	return &enhancedMockQuery{}
}

func (m *enhancedMockSession) NewBatch(batchType BatchType) Batch {
	m.newBatchCallCount++
	return &enhancedMockBatch{batchType: batchType}
}

func (m *enhancedMockSession) Close() {}

type enhancedMockQuery struct {
	execCalled bool
}

func (m *enhancedMockQuery) Exec() error {
	m.execCalled = true
	return nil
}

type enhancedMockBatch struct {
	queries   []string
	batchType BatchType
	size      int
}

func (m *enhancedMockBatch) Query(stmt string, values ...interface{}) {
	m.queries = append(m.queries, stmt)
	m.size++
}

func (m *enhancedMockBatch) Size() int {
	return m.size
}

func (m *enhancedMockBatch) ExecuteBatch() error {
	return nil
}

func TestSessionInterfaceImplementation(t *testing.T) {
	var _ Session = &GocqlSessionAdapter{}
}

func TestQueryInterfaceImplementation(t *testing.T) {
	var _ Query = &GocqlQueryAdapter{}
}

func TestBatchInterfaceImplementation(t *testing.T) {
	var _ Batch = &enhancedMockBatch{}
}

func TestEnhancedMockSession_PreparedQuery(t *testing.T) {
	// Test mock session with PreparedQuery
	session := &enhancedMockSession{}

	query := session.PreparedQuery("SELECT * FROM test WHERE id = ?", "value1")
	assert.NotNil(t, query)
	assert.Equal(t, 1, session.preparedQueryCallCount)
	assert.Contains(t, session.preparedQueries, "SELECT * FROM test WHERE id = ?")
}

func TestEnhancedMockSession_NewBatch(t *testing.T) {
	session := &enhancedMockSession{}

	tests := []struct {
		name      string
		batchType BatchType
	}{
		{
			name:      "LoggedBatch",
			batchType: LoggedBatch,
		},
		{
			name:      "UnloggedBatch",
			batchType: UnloggedBatch,
		},
		{
			name:      "CounterBatch",
			batchType: CounterBatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := session.NewBatch(tt.batchType)
			assert.NotNil(t, batch)

			mockBatch, ok := batch.(*enhancedMockBatch)
			assert.True(t, ok)
			assert.Equal(t, tt.batchType, mockBatch.batchType)

			// Test that batch can execute operations
			batch.Query("INSERT INTO test VALUES (?)", "value")
			assert.Equal(t, 1, batch.Size())

			err := batch.ExecuteBatch()
			assert.NoError(t, err)
		})
	}

	assert.Equal(t, len(tests), session.newBatchCallCount)
}

func TestEnhancedMockBatch_Operations(t *testing.T) {
	batch := &enhancedMockBatch{batchType: LoggedBatch}

	// Test initial state
	assert.Equal(t, 0, batch.Size())

	// Add queries
	batch.Query("INSERT INTO test VALUES (?)", "value1")
	batch.Query("UPDATE test SET name = ? WHERE id = ?", "newname", "id1")
	batch.Query("DELETE FROM test WHERE id = ?", "id2")

	assert.Equal(t, 3, batch.Size())
	assert.Len(t, batch.queries, 3)

	// Execute batch
	err := batch.ExecuteBatch()
	assert.NoError(t, err)
}
