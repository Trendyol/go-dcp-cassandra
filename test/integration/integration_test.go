//go:build integration

package integration

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
	config "github.com/Trendyol/go-dcp-cassandra/configs"
	"github.com/Trendyol/go-dcp-cassandra/couchbase"

	connectorpkg "github.com/Trendyol/go-dcp-cassandra/connector"
)

const (
	testHost     = "localhost:9042"
	testKeyspace = "test_ks"
)

func waitForCassandra(t *testing.T) {
	t.Helper()
	for i := range 60 {
		conn, err := net.DialTimeout("tcp", testHost, time.Second)
		if err == nil {
			conn.Close()
			return
		}
		if i%10 == 0 {
			t.Logf("Waiting for Cassandra... attempt %d/60", i+1)
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("Cassandra not available: timeout waiting for", testHost)
}

func setupKeyspace(t *testing.T) *gocql.Session {
	t.Helper()
	cluster := gocql.NewCluster("localhost")
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	session, err := cluster.CreateSession()
	require.NoError(t, err)

	err = session.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s
		 WITH replication = {'class':'SimpleStrategy','replication_factor':1}`,
		testKeyspace,
	)).Exec()
	require.NoError(t, err)

	session.Close()

	cluster.Keyspace = testKeyspace
	session, err = cluster.CreateSession()
	require.NoError(t, err)

	return session
}

func TestIntegration_SessionCreation(t *testing.T) {
	waitForCassandra(t)
	ks := setupKeyspace(t)
	ks.Close()

	cfg := config.Cassandra{
		Hosts:    []string{"localhost"},
		Keyspace: testKeyspace,
		Timeout:  10 * time.Second,
	}
	cfg.SetTestDefaults()

	session, err := cassandra.NewCassandraSession(cfg)
	require.NoError(t, err)
	defer session.Close()
}

func TestIntegration_CRUD(t *testing.T) {
	waitForCassandra(t)
	session := setupKeyspace(t)
	defer session.Close()

	err := session.Query(`CREATE TABLE IF NOT EXISTS test_crud (
		id text PRIMARY KEY,
		name text,
		value text
	)`).Exec()
	require.NoError(t, err)

	cfg := &config.Connector{
		Cassandra: config.Cassandra{
			Hosts:    []string{"localhost"},
			Keyspace: testKeyspace,
			Timeout:  10 * time.Second,
		},
	}
	cfg.ApplyDefaults()

	bulk, err := cassandra.NewBulk(cfg, func() {})
	require.NoError(t, err)

	t.Run("Insert", func(t *testing.T) {
		err := bulk.TestInsert(&cassandra.Raw{
			Table:     "test_crud",
			Document:  map[string]any{"id": "1", "name": "test", "value": "v1"},
			Operation: cassandra.Insert,
		})
		require.NoError(t, err)

		var name, value string
		err = session.Query("SELECT name, value FROM test_crud WHERE id = ?", "1").Scan(&name, &value)
		require.NoError(t, err)
		assert.Equal(t, "test", name)
		assert.Equal(t, "v1", value)
	})

	t.Run("Update", func(t *testing.T) {
		err := bulk.TestUpdate(&cassandra.Raw{
			Table:     "test_crud",
			Document:  map[string]any{"name": "updated"},
			Filter:    map[string]any{"id": "1"},
			Operation: cassandra.Update,
		})
		require.NoError(t, err)

		var name string
		err = session.Query("SELECT name FROM test_crud WHERE id = ?", "1").Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "updated", name)
	})

	t.Run("Delete", func(t *testing.T) {
		err := bulk.TestDelete(&cassandra.Raw{
			Table:     "test_crud",
			Filter:    map[string]any{"id": "1"},
			Operation: cassandra.Delete,
		})
		require.NoError(t, err)

		var name string
		err = session.Query("SELECT name FROM test_crud WHERE id = ?", "1").Scan(&name)
		assert.Error(t, err, "row should be deleted")
	})
}

func TestIntegration_DefaultMapper(t *testing.T) {
	waitForCassandra(t)
	session := setupKeyspace(t)
	defer session.Close()

	err := session.Query(`CREATE TABLE IF NOT EXISTS mapper_test (
		id text PRIMARY KEY,
		status text
	)`).Exec()
	require.NoError(t, err)

	mappings := []config.CollectionTableMapping{
		{
			Collection: "my_col",
			TableName:  "mapper_test",
			FieldMappings: map[string]string{
				"id":     "_key",
				"status": "status",
			},
		},
	}
	connectorpkg.SetCollectionTableMappings(&mappings)

	event := couchbase.NewMutateEvent(
		[]byte("doc_1"),
		[]byte(`{"status":"active"}`),
		"my_col",
		time.Now(), 1, 0,
	)

	result := connectorpkg.DefaultMapper(event)
	require.Len(t, result, 1)

	raw, ok := result[0].(*cassandra.Raw)
	require.True(t, ok)
	assert.Equal(t, "mapper_test", raw.Table)
	assert.Equal(t, "doc_1", raw.Document["id"])
	assert.Equal(t, "active", raw.Document["status"])
}

func TestIntegration_ConcurrentPreparedQuery(t *testing.T) {
	waitForCassandra(t)
	session := setupKeyspace(t)
	defer session.Close()

	err := session.Query(`CREATE TABLE IF NOT EXISTS concurrent_test (
		id text PRIMARY KEY,
		data text
	)`).Exec()
	require.NoError(t, err)

	cfg := config.Cassandra{
		Hosts:    []string{"localhost"},
		Keyspace: testKeyspace,
		Timeout:  10 * time.Second,
	}
	cfg.SetTestDefaults()

	cassSession, err := cassandra.NewCassandraSession(cfg)
	require.NoError(t, err)
	defer cassSession.Close()

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			stmt := "INSERT INTO concurrent_test (id, data) VALUES (?, ?)"
			err := cassSession.PreparedQuery(stmt, fmt.Sprintf("id_%d", i), "data").Exec()
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	var count int
	err = session.Query("SELECT count(*) FROM concurrent_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 20, count)
}
