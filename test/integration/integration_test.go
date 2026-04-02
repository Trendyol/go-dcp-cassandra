//go:build integration

package integration

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	connectorpkg "github.com/Trendyol/go-dcp-cassandra/connector"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

const cassandraHost = "localhost:9042"

var directSession *gocql.Session

// ---------------------------------------------------------------------------
// Setup / Teardown
// ---------------------------------------------------------------------------

func TestMain(m *testing.M) {
	if err := waitForCassandra(); err != nil {
		fmt.Fprintf(os.Stderr, "Cassandra not available: %v\n", err)
		os.Exit(1)
	}
	if err := setupSchema(); err != nil {
		fmt.Fprintf(os.Stderr, "Schema setup failed: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	if directSession != nil {
		directSession.Close()
	}
	os.Exit(code)
}

func waitForCassandra() error {
	cluster := gocql.NewCluster(cassandraHost)
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second

	for i := range 60 {
		session, err := cluster.CreateSession()
		if err == nil {
			session.Close()
			return nil
		}
		if i%10 == 0 {
			fmt.Printf("Waiting for Cassandra… attempt %d/60\n", i+1)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for Cassandra at %s", cassandraHost)
}

func setupSchema() error {
	cluster := gocql.NewCluster(cassandraHost)
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	err = session.Query(`CREATE KEYSPACE IF NOT EXISTS integration_test
		WITH replication = {'class':'SimpleStrategy','replication_factor':1}`).Exec()
	session.Close()
	if err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}

	cluster.Keyspace = "integration_test"
	directSession, err = cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect keyspace: %w", err)
	}

	ddls := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id text PRIMARY KEY, name text, email text, status text
		)`,
		`CREATE TABLE IF NOT EXISTS orders (
			order_id text, partition_id text, product text, status text,
			PRIMARY KEY (order_id, partition_id)
		)`,
	}
	for _, ddl := range ddls {
		if err := directSession.Query(ddl).Exec(); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}
	return nil
}

func truncate(t *testing.T, table string) {
	t.Helper()
	require.NoError(t, directSession.Query(fmt.Sprintf("TRUNCATE integration_test.%s", table)).Exec())
}

// newTestBulk creates a real Bulk connected to Cassandra with short flush
// intervals so tests complete quickly. The Bulk is closed via t.Cleanup.
func newTestBulk(t *testing.T, opts ...func(*config.Connector)) *cassandra.Bulk {
	t.Helper()
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:               []string{cassandraHost},
			Keyspace:            "integration_test",
			BatchSizeLimit:      1,
			BatchTickerDuration: 50 * time.Millisecond,
			MaxInFlightRequests: 10,
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.ApplyDefaults()

	bulk, err := cassandra.NewBulk(cfg, func() {})
	require.NoError(t, err)
	go bulk.StartBulk()
	t.Cleanup(func() { bulk.Close() })
	return bulk
}

func ctx() *models.ListenerContext {
	return &models.ListenerContext{Ack: func() {}}
}

// waitFlush gives the async flush goroutine time to complete.
func waitFlush() { time.Sleep(500 * time.Millisecond) }

// ---------------------------------------------------------------------------
// 1. Session creation
// ---------------------------------------------------------------------------

func TestCassandraSession(t *testing.T) {
	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:    []string{cassandraHost},
			Keyspace: "integration_test",
		},
	}
	cfg.ApplyDefaults()

	session, err := cassandra.NewCassandraSession(cfg.Cassandra)
	require.NoError(t, err)
	session.Close()
}

// ---------------------------------------------------------------------------
// 2. CRUD through Bulk
// ---------------------------------------------------------------------------

func TestBulkInsert(t *testing.T) {
	truncate(t, "users")
	bulk := newTestBulk(t)

	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{
			Table:     "users",
			Document:  map[string]any{"id": "u1", "name": "Alice", "email": "alice@test.com", "status": "active"},
			Operation: cassandra.Insert,
		},
	})
	waitFlush()

	var name, email, status string
	require.NoError(t, directSession.Query("SELECT name, email, status FROM users WHERE id = ?", "u1").Scan(&name, &email, &status))
	assert.Equal(t, "Alice", name)
	assert.Equal(t, "alice@test.com", email)
	assert.Equal(t, "active", status)
}

func TestBulkUpsert(t *testing.T) {
	truncate(t, "users")
	bulk := newTestBulk(t)

	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{Table: "users", Document: map[string]any{"id": "u2", "name": "Bob"}, Operation: cassandra.Upsert},
	})
	waitFlush()

	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{Table: "users", Document: map[string]any{"id": "u2", "name": "Bob Updated"}, Operation: cassandra.Upsert},
	})
	waitFlush()

	var name string
	require.NoError(t, directSession.Query("SELECT name FROM users WHERE id = ?", "u2").Scan(&name))
	assert.Equal(t, "Bob Updated", name)
}

func TestBulkUpdate(t *testing.T) {
	truncate(t, "users")
	require.NoError(t, directSession.Query("INSERT INTO users (id, name, status) VALUES (?, ?, ?)", "u3", "Charlie", "inactive").Exec())

	bulk := newTestBulk(t)
	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{
			Table:     "users",
			Document:  map[string]any{"name": "Charlie Updated", "status": "active"},
			Filter:    map[string]any{"id": "u3"},
			Operation: cassandra.Update,
		},
	})
	waitFlush()

	var name, status string
	require.NoError(t, directSession.Query("SELECT name, status FROM users WHERE id = ?", "u3").Scan(&name, &status))
	assert.Equal(t, "Charlie Updated", name)
	assert.Equal(t, "active", status)
}

func TestBulkDelete(t *testing.T) {
	truncate(t, "users")
	require.NoError(t, directSession.Query("INSERT INTO users (id, name) VALUES (?, ?)", "u4", "Dave").Exec())

	bulk := newTestBulk(t)
	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{Table: "users", Filter: map[string]any{"id": "u4"}, Operation: cassandra.Delete},
	})
	waitFlush()

	var count int
	require.NoError(t, directSession.Query("SELECT count(*) FROM users WHERE id = ?", "u4").Scan(&count))
	assert.Equal(t, 0, count)
}

// ---------------------------------------------------------------------------
// 3. Concurrent writes — race safety with 100 parallel goroutines
// ---------------------------------------------------------------------------

func TestBulkConcurrentWrites(t *testing.T) {
	truncate(t, "users")
	bulk := newTestBulk(t, func(cfg *config.Connector) {
		cfg.Cassandra.BatchSizeLimit = 50
		cfg.Cassandra.BatchTickerDuration = 100 * time.Millisecond
	})

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
				&cassandra.Raw{
					Table:     "users",
					Document:  map[string]any{"id": fmt.Sprintf("c_%d", idx), "name": fmt.Sprintf("User %d", idx)},
					Operation: cassandra.Insert,
				},
			})
		}(i)
	}
	wg.Wait()
	time.Sleep(2 * time.Second)

	var count int
	require.NoError(t, directSession.Query("SELECT count(*) FROM users").Scan(&count))
	assert.Equal(t, 100, count, "all 100 concurrent writes should succeed")
}

// ---------------------------------------------------------------------------
// 4. Batch-per-event → UNLOGGED BATCH
// ---------------------------------------------------------------------------

func TestBulkBatchPerEvent(t *testing.T) {
	truncate(t, "orders")
	bulk := newTestBulk(t, func(cfg *config.Connector) {
		cfg.Cassandra.BatchPerEvent = true
		cfg.Cassandra.BatchSizeLimit = 10
	})

	bulk.AddActions(ctx(), time.Now(), []cassandra.Model{
		&cassandra.Raw{
			Table:     "orders",
			Document:  map[string]any{"order_id": "o1", "partition_id": "p1", "product": "Widget", "status": "new"},
			Operation: cassandra.Insert,
		},
		&cassandra.Raw{
			Table:     "orders",
			Document:  map[string]any{"order_id": "o1", "partition_id": "p2", "product": "Gadget", "status": "new"},
			Operation: cassandra.Insert,
		},
	})
	waitFlush()

	var count int
	require.NoError(t, directSession.Query("SELECT count(*) FROM orders WHERE order_id = ?", "o1").Scan(&count))
	assert.Equal(t, 2, count, "both rows from the UNLOGGED BATCH should exist")
}

// ---------------------------------------------------------------------------
// 5. Write timestamp propagation (event_time)
// ---------------------------------------------------------------------------

func TestBulkWriteTimestamp(t *testing.T) {
	truncate(t, "users")
	bulk := newTestBulk(t, func(cfg *config.Connector) {
		cfg.Cassandra.WriteTimestamp = "event_time"
	})

	eventTime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	bulk.AddActions(ctx(), eventTime, []cassandra.Model{
		&cassandra.Raw{Table: "users", Document: map[string]any{"id": "ts1", "name": "Timestamp"}, Operation: cassandra.Insert},
	})
	waitFlush()

	var writetime int64
	require.NoError(t, directSession.Query("SELECT WRITETIME(name) FROM users WHERE id = ?", "ts1").Scan(&writetime))
	assert.Equal(t, eventTime.UnixMicro(), writetime, "WRITETIME should match event_time")
}

// ---------------------------------------------------------------------------
// 6. Metric snapshot
// ---------------------------------------------------------------------------

func TestBulkMetrics(t *testing.T) {
	truncate(t, "users")
	bulk := newTestBulk(t)

	bulk.AddActions(ctx(), time.Now().Add(-100*time.Millisecond), []cassandra.Model{
		&cassandra.Raw{Table: "users", Document: map[string]any{"id": "m1", "name": "Metrics"}, Operation: cassandra.Insert},
	})
	waitFlush()

	m := bulk.GetMetric()
	assert.Greater(t, m.ProcessLatencyMs, int64(0), "latency should reflect event age")
	assert.Equal(t, int64(1), m.BulkRequestSize, "should report 1 item flushed")
}

// ---------------------------------------------------------------------------
// 7. Concurrent mapper access — RWMutex under 200 goroutines
// ---------------------------------------------------------------------------

func TestMapperConcurrentAccess(t *testing.T) {
	mappings := []config.CollectionTableMapping{
		{Collection: "col_a", TableName: "users", FieldMappings: map[string]string{"id": "_key", "name": "name"}},
		{Collection: "col_b", TableName: "users", FieldMappings: map[string]string{"id": "_key", "email": "email"}},
	}
	connectorpkg.SetCollectionTableMappings(&mappings)

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			col := "col_a"
			if idx%2 == 0 {
				col = "col_b"
			}
			event := couchbase.NewMutateEvent(
				[]byte(fmt.Sprintf("key_%d", idx)),
				[]byte(fmt.Sprintf(`{"name":"n%d","email":"e%d@t.com"}`, idx, idx)),
				col, time.Now(), uint64(idx), uint16(idx%1024), //nolint:gosec // test-only, idx is bounded [0,200)
			)
			result := connectorpkg.DefaultMapper(event)
			assert.Len(t, result, 1)
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 8. PrimaryKeyFields — tombstone reduction end-to-end
// ---------------------------------------------------------------------------

func TestPrimaryKeyFieldsDelete(t *testing.T) {
	truncate(t, "orders")

	require.NoError(t, directSession.Query(
		"INSERT INTO orders (order_id, partition_id, product, status) VALUES (?, ?, ?, ?)",
		"del_o1", "del_p1", "Widget", "shipped").Exec())

	mappings := []config.CollectionTableMapping{
		{
			Collection: "pk_delete",
			TableName:  "orders",
			FieldMappings: map[string]string{
				"order_id":     "_key",
				"partition_id": "partitionId",
				"product":      "product",
				"status":       "status",
			},
			PrimaryKeyFields: []string{"order_id", "partition_id"},
		},
	}
	connectorpkg.SetCollectionTableMappings(&mappings)

	event := couchbase.NewDeleteEvent(
		[]byte("del_o1"),
		[]byte(`{"partitionId":"del_p1","product":"Widget","status":"shipped"}`),
		"pk_delete", time.Now(), 1, 0,
	)

	result := connectorpkg.DefaultMapper(event)
	require.Len(t, result, 1)

	raw, ok := result[0].(*cassandra.Raw)
	require.True(t, ok)
	assert.Equal(t, cassandra.Delete, raw.Operation)
	assert.Equal(t, "del_o1", raw.Filter["order_id"])
	assert.Equal(t, "del_p1", raw.Filter["partition_id"])
	assert.NotContains(t, raw.Filter, "product", "non-PK field excluded from filter")
	assert.NotContains(t, raw.Filter, "status", "non-PK field excluded from filter")

	bulk := newTestBulk(t)
	bulk.AddActions(ctx(), time.Now(), result)
	waitFlush()

	var count int
	require.NoError(t, directSession.Query(
		"SELECT count(*) FROM orders WHERE order_id = ? AND partition_id = ?",
		"del_o1", "del_p1").Scan(&count))
	assert.Equal(t, 0, count, "row should be deleted via PK-only filter")
}

// ---------------------------------------------------------------------------
// 9. Nested field mapping — strings.Cut end-to-end
// ---------------------------------------------------------------------------

func TestMapperNestedFields(t *testing.T) {
	truncate(t, "users")

	mappings := []config.CollectionTableMapping{
		{
			Collection: "nested",
			TableName:  "users",
			FieldMappings: map[string]string{
				"id":    "_key",
				"name":  "profile.name",
				"email": "profile.contact.email",
			},
		},
	}
	connectorpkg.SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("nested_u1"),
		[]byte(`{"profile":{"name":"Nested User","contact":{"email":"nested@test.com"}}}`),
		"nested", time.Now(), 1, 0,
	)

	result := connectorpkg.DefaultMapper(event)
	require.Len(t, result, 1)

	raw, ok := result[0].(*cassandra.Raw)
	require.True(t, ok)
	assert.Equal(t, "nested_u1", raw.Document["id"])
	assert.Equal(t, "Nested User", raw.Document["name"])
	assert.Equal(t, "nested@test.com", raw.Document["email"])

	bulk := newTestBulk(t)
	bulk.AddActions(ctx(), time.Now(), result)
	waitFlush()

	var name, email string
	require.NoError(t, directSession.Query("SELECT name, email FROM users WHERE id = ?", "nested_u1").Scan(&name, &email))
	assert.Equal(t, "Nested User", name)
	assert.Equal(t, "nested@test.com", email)
}
