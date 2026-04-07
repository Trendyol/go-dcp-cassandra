//go:build integration

package integration

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/connector"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"
)

const (
	cassandraHost    = "localhost:9042"
	testKeyspace     = "test_ks"
	waitTimeout      = 2 * time.Minute
	waitPollInterval = 2 * time.Second
)

func waitForCassandra(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	attempt := 0
	for time.Now().Before(deadline) {
		attempt++
		conn, err := net.DialTimeout("tcp", cassandraHost, time.Second)
		if err == nil {
			conn.Close()
			t.Logf("Cassandra available after %d attempts", attempt)
			return
		}
		if attempt%10 == 1 {
			t.Logf("Waiting for Cassandra… attempt %d/60", attempt)
		}
		time.Sleep(waitPollInterval)
	}
	t.Fatalf("Cassandra not available: timeout waiting for Cassandra at %s", cassandraHost)
}

func newTestConfig() config.Cassandra {
	return config.Cassandra{
		Hosts:               []string{cassandraHost},
		Keyspace:            testKeyspace,
		Consistency:         "LOCAL_ONE",
		BatchSizeLimit:      1000,
		BatchByteSizeLimit:  10 * 1024 * 1024,
		BatchTickerDuration: 1 * time.Second,
		MaxInFlightRequests: 10,
		Timeout:             10 * time.Second,
		ConnectTimeout:      10 * time.Second,
	}
}

func setupKeyspaceAndTable(t *testing.T, session cassandra.Session) {
	t.Helper()
	require.NoError(t, session.Query(fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s "+
			"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		testKeyspace,
	)).Exec())

	require.NoError(t, session.Query(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.orders ("+
			"order_id text, partition_id text, product text, status text, "+
			"PRIMARY KEY (order_id, partition_id))",
		testKeyspace,
	)).Exec())
}

func truncateTable(t *testing.T, session cassandra.Session) {
	t.Helper()
	require.NoError(t, session.Query(
		fmt.Sprintf("TRUNCATE %s.orders", testKeyspace),
	).Exec())
}

func TestIntegration_SessionCreation(t *testing.T) {
	waitForCassandra(t)
	cfg := newTestConfig()
	session, err := cassandra.NewCassandraSession(cfg)
	require.NoError(t, err)
	defer session.Close()
	setupKeyspaceAndTable(t, session)
}

func TestIntegration_DirectCRUD(t *testing.T) {
	waitForCassandra(t)
	cfg := newTestConfig()
	session, err := cassandra.NewCassandraSession(cfg)
	require.NoError(t, err)
	defer session.Close()
	setupKeyspaceAndTable(t, session)
	truncateTable(t, session)

	require.NoError(t, session.Query(fmt.Sprintf(
		"INSERT INTO %s.orders (order_id, partition_id, product, status) VALUES (?, ?, ?, ?)",
		testKeyspace,
	), "o1", "p1", "Widget", "new").Exec())

	require.NoError(t, session.Query(fmt.Sprintf(
		"UPDATE %s.orders SET status = ? WHERE order_id = ? AND partition_id = ?",
		testKeyspace,
	), "shipped", "o1", "p1").Exec())

	require.NoError(t, session.Query(fmt.Sprintf(
		"DELETE FROM %s.orders WHERE order_id = ? AND partition_id = ?",
		testKeyspace,
	), "o1", "p1").Exec())
}

func TestIntegration_PreparedQueryConcurrency(t *testing.T) {
	waitForCassandra(t)
	cfg := newTestConfig()
	session, err := cassandra.NewCassandraSession(cfg)
	require.NoError(t, err)
	defer session.Close()
	setupKeyspaceAndTable(t, session)
	truncateTable(t, session)

	stmt := fmt.Sprintf(
		"INSERT INTO %s.orders (order_id, partition_id, product, status) VALUES (?, ?, ?, ?)",
		testKeyspace,
	)

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := session.PreparedQuery(
				stmt,
				fmt.Sprintf("concurrent_%d", idx), "p1", "Item", "new",
			).Exec()
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestIntegration_Metrics(t *testing.T) {
	waitForCassandra(t)
	cfg := newTestConfig()
	connCfg := &config.Connector{Cassandra: cfg}
	connCfg.ApplyDefaults()
	bulk, err := cassandra.NewBulk(connCfg, func() {})
	require.NoError(t, err)

	m := bulk.GetMetric()
	assert.NotNil(t, m)
	assert.GreaterOrEqual(t, m.ProcessLatencyMs, int64(0))
}

func TestIntegration_MapperConcurrency(t *testing.T) {
	mappings := make([]config.CollectionTableMapping, 5)
	for i := range 5 {
		mappings[i] = config.CollectionTableMapping{
			Collection: fmt.Sprintf("col_%d", i),
			TableName:  fmt.Sprintf("table_%d", i),
			FieldMappings: map[string]string{
				"id": "_key",
			},
		}
	}
	connector.SetCollectionTableMappings(&mappings)

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			col := fmt.Sprintf("col_%d", idx%5)
			event := couchbase.NewMutateEvent(
				[]byte(fmt.Sprintf("key_%d", idx)),
				[]byte(`{"id":"1"}`),
				col,
				time.Now(),
				uint64(idx), //nolint:gosec // test-only, idx is bounded
				uint16(idx%1024),
			)
			result := connector.DefaultMapper(event)
			assert.Len(t, result, 1)
		}(i)
	}
	wg.Wait()
}
