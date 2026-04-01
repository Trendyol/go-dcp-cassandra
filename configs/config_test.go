package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCassandra_SetDefaults(t *testing.T) {
	tests := []struct {
		name           string
		consistency    string
		expectedResult string
	}{
		{
			name:           "Empty consistency should default to QUORUM",
			consistency:    "",
			expectedResult: "QUORUM",
		},
		{
			name:           "Whitespace consistency should default to QUORUM",
			consistency:    "   ",
			expectedResult: "QUORUM",
		},
		{
			name:           "Valid consistency should remain unchanged",
			consistency:    "ONE",
			expectedResult: "ONE",
		},
		{
			name:           "Case insensitive consistency should work",
			consistency:    "quorum",
			expectedResult: "QUORUM",
		},
		{
			name:           "Invalid consistency should default to QUORUM",
			consistency:    "INVALID",
			expectedResult: "QUORUM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cassandra := Cassandra{
				Consistency: tt.consistency,
			}
			cassandra.setDefaults()
			assert.Equal(t, tt.expectedResult, cassandra.Consistency)
		})
	}
}

func TestCassandra_SetDefaults_BatchConfig(t *testing.T) {
	cassandra := Cassandra{}
	cassandra.setDefaults()

	assert.Equal(t, 2000, cassandra.BatchSizeLimit)
	assert.Equal(t, 10*1024*1024, cassandra.BatchByteSizeLimit)
	assert.Equal(t, 10*time.Second, cassandra.BatchTickerDuration)
	assert.Equal(t, 100, cassandra.MaxInFlightRequests)
	assert.False(t, cassandra.BatchPerEvent)
}

func TestCassandra_SetDefaults_WriteTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "none"},
		{"invalid", "none"},
		{"none", "none"},
		{"event_time", "event_time"},
		{"now", "now"},
		{"  NOW  ", "now"},
	}
	for _, tt := range tests {
		c := Cassandra{WriteTimestamp: tt.input}
		c.setDefaults()
		assert.Equal(t, tt.expected, c.WriteTimestamp, "input: %q", tt.input)
	}
}

func TestCassandra_SetDefaults_HostSelectionPolicy(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "token_aware"},
		{"invalid", "token_aware"},
		{"token_aware", "token_aware"},
		{"round_robin", "round_robin"},
		{"  ROUND_ROBIN  ", "round_robin"},
	}
	for _, tt := range tests {
		c := Cassandra{HostSelectionPolicy: tt.input}
		c.setDefaults()
		assert.Equal(t, tt.expected, c.HostSelectionPolicy, "input: %q", tt.input)
	}
}

func TestCassandra_SetDefaults_ConnectionPooling(t *testing.T) {
	cassandra := Cassandra{}
	cassandra.setDefaults()

	assert.Equal(t, 2, cassandra.NumConns)
	assert.Equal(t, 10*time.Second, cassandra.Timeout)
	assert.Equal(t, 5*time.Second, cassandra.ConnectTimeout)
	assert.Equal(t, 30*time.Second, cassandra.KeepAlive)
	assert.Equal(t, 1000, cassandra.MaxPreparedStmts)
	assert.Equal(t, 5000, cassandra.PageSize)
}

func TestCassandra_SetDefaults_RetryPolicy(t *testing.T) {
	cassandra := Cassandra{}
	cassandra.setDefaults()

	// Test retry policy defaults
	assert.Equal(t, 3, cassandra.RetryPolicy.NumRetries)
	assert.Equal(t, 100*time.Millisecond, cassandra.RetryPolicy.MinRetryDelay)
	assert.Equal(t, 1*time.Second, cassandra.RetryPolicy.MaxRetryDelay)
}

func TestCassandra_SetDefaults_CustomValues(t *testing.T) {
	cassandra := Cassandra{
		NumConns:         5,
		ConnectTimeout:   10 * time.Second,
		MaxPreparedStmts: 2000,
		RetryPolicy: struct {
			NumRetries    int           `yaml:"numRetries"`
			MinRetryDelay time.Duration `yaml:"minRetryDelay"`
			MaxRetryDelay time.Duration `yaml:"maxRetryDelay"`
		}{
			NumRetries:    5,
			MinRetryDelay: 50 * time.Millisecond,
			MaxRetryDelay: 2 * time.Second,
		},
	}

	cassandra.setDefaults()

	assert.Equal(t, 5, cassandra.NumConns)
	assert.Equal(t, 10*time.Second, cassandra.ConnectTimeout)
	assert.Equal(t, 2000, cassandra.MaxPreparedStmts)
	assert.Equal(t, 5, cassandra.RetryPolicy.NumRetries)
	assert.Equal(t, 50*time.Millisecond, cassandra.RetryPolicy.MinRetryDelay)
	assert.Equal(t, 2*time.Second, cassandra.RetryPolicy.MaxRetryDelay)
}

func TestConnector_ApplyDefaults(t *testing.T) {
	config := Connector{
		Cassandra: Cassandra{
			Hosts:       []string{"localhost:9042"},
			Keyspace:    "test_keyspace",
			Consistency: "INVALID_CONSISTENCY",
		},
	}

	config.ApplyDefaults()
	assert.Equal(t, "QUORUM", config.Cassandra.Consistency, "Invalid consistency should default to QUORUM")

	config.Cassandra.Consistency = "ONE"
	config.ApplyDefaults()
	assert.Equal(t, "ONE", config.Cassandra.Consistency, "Valid consistency should remain unchanged")

	config.Cassandra.Consistency = ""
	config.ApplyDefaults()
	assert.Equal(t, "QUORUM", config.Cassandra.Consistency, "Empty consistency should default to QUORUM")
}

func TestConnector_Validation(t *testing.T) {
	config := Connector{
		Cassandra: Cassandra{
			Hosts:       []string{"localhost:9042"},
			Keyspace:    "test_keyspace",
			Consistency: "INVALID_CONSISTENCY",
		},
	}

	config.Cassandra.setDefaults()
	assert.Equal(t, "QUORUM", config.Cassandra.Consistency, "Invalid consistency should default to QUORUM")

	config.Cassandra.Consistency = "ONE"
	config.Cassandra.setDefaults()
	assert.Equal(t, "ONE", config.Cassandra.Consistency, "Valid consistency should remain unchanged")

	config.Cassandra.Consistency = ""
	config.Cassandra.setDefaults()
	assert.Equal(t, "QUORUM", config.Cassandra.Consistency, "Empty consistency should default to QUORUM")
}
