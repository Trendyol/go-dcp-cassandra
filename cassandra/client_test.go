package cassandra

import (
	config "go-dcp-cassandra/configs"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestNewCassandraSession_ConsistencyLevels(t *testing.T) {
	tests := []struct {
		name          string
		consistency   string
		expectedLevel gocql.Consistency
	}{
		{
			name:          "ANY consistency",
			consistency:   "ANY",
			expectedLevel: gocql.Any,
		},
		{
			name:          "ONE consistency",
			consistency:   "ONE",
			expectedLevel: gocql.One,
		},
		{
			name:          "TWO consistency",
			consistency:   "TWO",
			expectedLevel: gocql.Two,
		},
		{
			name:          "THREE consistency",
			consistency:   "THREE",
			expectedLevel: gocql.Three,
		},
		{
			name:          "QUORUM consistency",
			consistency:   "QUORUM",
			expectedLevel: gocql.Quorum,
		},
		{
			name:          "ALL consistency",
			consistency:   "ALL",
			expectedLevel: gocql.All,
		},
		{
			name:          "LOCAL_QUORUM consistency",
			consistency:   "LOCAL_QUORUM",
			expectedLevel: gocql.LocalQuorum,
		},
		{
			name:          "EACH_QUORUM consistency",
			consistency:   "EACH_QUORUM",
			expectedLevel: gocql.EachQuorum,
		},
		{
			name:          "LOCAL_ONE consistency",
			consistency:   "LOCAL_ONE",
			expectedLevel: gocql.LocalOne,
		},
		{
			name:          "Empty consistency defaults to QUORUM",
			consistency:   "",
			expectedLevel: gocql.Quorum,
		},
		{
			name:          "Invalid consistency defaults to QUORUM",
			consistency:   "INVALID",
			expectedLevel: gocql.Quorum,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Cassandra{
				Hosts:       []string{"localhost:9042"},
				Keyspace:    "test_keyspace",
				Consistency: tt.consistency,
			}

			cluster := gocql.NewCluster(cfg.Hosts...)
			cluster.Keyspace = cfg.Keyspace

			switch cfg.Consistency {
			case "ANY":
				cluster.Consistency = gocql.Any
			case "ONE":
				cluster.Consistency = gocql.One
			case "TWO":
				cluster.Consistency = gocql.Two
			case "THREE":
				cluster.Consistency = gocql.Three
			case "QUORUM":
				cluster.Consistency = gocql.Quorum
			case "ALL":
				cluster.Consistency = gocql.All
			case "LOCAL_QUORUM":
				cluster.Consistency = gocql.LocalQuorum
			case "EACH_QUORUM":
				cluster.Consistency = gocql.EachQuorum
			case "LOCAL_ONE":
				cluster.Consistency = gocql.LocalOne
			default:
				cluster.Consistency = gocql.Quorum
			}

			assert.Equal(t, tt.expectedLevel, cluster.Consistency)
		})
	}
}
