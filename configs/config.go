package config

import (
	"strings"
	"time"

	"github.com/Trendyol/go-dcp/config"
)

type CollectionTableMapping struct {
	FieldMappings map[string]string `yaml:"fieldMappings"`
	Collection    string            `yaml:"collection"`
	TableName     string            `yaml:"tableName"`
}

type Cassandra struct {
	Username            string        `yaml:"username"`
	Password            string        `yaml:"password"`
	Keyspace            string        `yaml:"keyspace"`
	Hosts               []string      `yaml:"hosts"`
	Compressor          string        `yaml:"compressor"`
	SerialConsistency   string        `yaml:"serialConsistency"`
	Consistency         string        `yaml:"consistency"`
	TableName           string        `yaml:"tableName"`
	Timeout             time.Duration `yaml:"timeout"`
	ConnectTimeout      time.Duration `yaml:"connectTimeout"`
	KeepAlive           time.Duration `yaml:"keepAlive"`
	NumConns            int           `yaml:"numConns"`
	PageSize            int           `yaml:"pageSize"`
	MaxPreparedStmts    int           `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo   int           `yaml:"maxRoutingKeyInfo"`
	WorkerCount         int           `yaml:"workerCount"`
	UseBatch            bool          `yaml:"useBatch"`
	BatchType           string        `yaml:"batchType"`
	BatchScope          string        `yaml:"batchScope"`
	AckMode             string        `yaml:"ackMode"`
	WriteTimestamp      string        `yaml:"writeTimestamp"`
	BatchSize           int           `yaml:"batchSize"`
	BatchSizeLimit      int           `yaml:"batchSizeLimit"`
	BatchByteSizeLimit  int           `yaml:"batchByteSizeLimit"`
	BatchTimeout        time.Duration `yaml:"batchTimeout"`
	BatchTickerDuration time.Duration `yaml:"batchTickerDuration"`
	MaxBatchSize        int           `yaml:"maxBatchSize"`
	SSL                 struct {
		CertPath           string `yaml:"certPath"`
		KeyPath            string `yaml:"keyPath"`
		CaPath             string `yaml:"caPath"`
		Enable             bool   `yaml:"enable"`
		InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	} `yaml:"ssl"`
	CollectionTableMapping []CollectionTableMapping `yaml:"collectionTableMapping,omitempty"`
	RetryPolicy            struct {
		NumRetries    int           `yaml:"numRetries"`
		MinRetryDelay time.Duration `yaml:"minRetryDelay"`
		MaxRetryDelay time.Duration `yaml:"maxRetryDelay"`
	} `yaml:"retryPolicy"`
}

type Connector struct {
	Dcp       config.Dcp `yaml:",inline" mapstructure:",squash"`
	Cassandra Cassandra  `yaml:"cassandra" mapstructure:"cassandra"`
}

func (c *Cassandra) setDefaults() {
	validConsistencies := map[string]bool{
		"ANY":          true,
		"ONE":          true,
		"TWO":          true,
		"THREE":        true,
		"QUORUM":       true,
		"ALL":          true,
		"LOCAL_QUORUM": true,
		"EACH_QUORUM":  true,
		"LOCAL_ONE":    true,
	}

	consistency := strings.TrimSpace(strings.ToUpper(c.Consistency))
	if consistency == "" || !validConsistencies[consistency] {
		c.Consistency = "QUORUM"
	} else {
		c.Consistency = consistency
	}

	c.setBatchDefaults()

	if c.NumConns <= 0 {
		c.NumConns = 2
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = 30 * time.Second
	}
	if c.MaxPreparedStmts <= 0 {
		c.MaxPreparedStmts = 1000
	}
	if c.PageSize <= 0 {
		c.PageSize = 5000
	}
	if c.RetryPolicy.NumRetries <= 0 {
		c.RetryPolicy.NumRetries = 3
	}
	if c.RetryPolicy.MinRetryDelay <= 0 {
		c.RetryPolicy.MinRetryDelay = 100 * time.Millisecond
	}
	if c.RetryPolicy.MaxRetryDelay <= 0 {
		c.RetryPolicy.MaxRetryDelay = 1 * time.Second
	}
}

func (c *Cassandra) setBatchDefaults() {
	if c.BatchType == "" {
		c.BatchType = "logged"
	}

	batchScope := strings.TrimSpace(strings.ToLower(c.BatchScope))
	if batchScope != "event" && batchScope != "global" {
		c.BatchScope = "global"
	} else {
		c.BatchScope = batchScope
	}

	ackMode := strings.TrimSpace(strings.ToLower(c.AckMode))
	if ackMode != "immediate" && ackMode != "after_write" {
		c.AckMode = "after_write"
	} else {
		c.AckMode = ackMode
	}

	writeTimestamp := strings.TrimSpace(strings.ToLower(c.WriteTimestamp))
	if writeTimestamp != "none" && writeTimestamp != "event_time" {
		c.WriteTimestamp = "none"
	} else {
		c.WriteTimestamp = writeTimestamp
	}

	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 65536
	}
}

func (c *Connector) ApplyDefaults() {
	c.Cassandra.setDefaults()
}
