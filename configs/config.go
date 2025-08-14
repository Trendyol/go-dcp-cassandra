package config

import (
	"strings"
	"time"

	"github.com/Trendyol/go-dcp/config"
)

type CollectionTableMapping struct {
	Collection    string            `yaml:"collection"`
	TableName     string            `yaml:"tableName"`
	FieldMappings map[string]string `yaml:"fieldMappings"`
}

type Cassandra struct {
	Hosts                  []string                 `yaml:"hosts"`
	Username               string                   `yaml:"username"`
	Password               string                   `yaml:"password"`
	Keyspace               string                   `yaml:"keyspace"`
	Timeout                time.Duration            `yaml:"timeout"`
	BatchSize              int                      `yaml:"batchSize"`
	BatchTimeout           time.Duration            `yaml:"batchTimeout"`
	BatchSizeLimit         int                      `yaml:"batchSizeLimit"`
	BatchTickerDuration    time.Duration            `yaml:"batchTickerDuration"`
	BatchByteSizeLimit     int                      `yaml:"batchByteSizeLimit"`
	WorkerCount            int                      `yaml:"workerCount"`
	TableName              string                   `yaml:"tableName"`
	CollectionTableMapping []CollectionTableMapping `yaml:"collectionTableMapping,omitempty"`
	Consistency            string                   `yaml:"consistency"`
	BatchType              string                   `yaml:"batchType"`
	UseBatch               bool                     `yaml:"useBatch"`
	MaxBatchSize           int                      `yaml:"maxBatchSize"`
	NumConns               int                      `yaml:"numConns"`
	ConnectTimeout         time.Duration            `yaml:"connectTimeout"`
	KeepAlive              time.Duration            `yaml:"keepAlive"`
	MaxPreparedStmts       int                      `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo      int                      `yaml:"maxRoutingKeyInfo"`
	PageSize               int                      `yaml:"pageSize"`
	SerialConsistency      string                   `yaml:"serialConsistency"`
	RetryPolicy            struct {
		NumRetries    int           `yaml:"numRetries"`
		MinRetryDelay time.Duration `yaml:"minRetryDelay"`
		MaxRetryDelay time.Duration `yaml:"maxRetryDelay"`
	} `yaml:"retryPolicy"`
	Compressor string `yaml:"compressor"`
	SSL        struct {
		Enable             bool   `yaml:"enable"`
		CertPath           string `yaml:"certPath"`
		KeyPath            string `yaml:"keyPath"`
		CaPath             string `yaml:"caPath"`
		InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	} `yaml:"ssl"`
}

type Connector struct {
	Cassandra Cassandra  `yaml:"cassandra" mapstructure:"cassandra"`
	Dcp       config.Dcp `yaml:",inline" mapstructure:",squash"`
	AppPort   string     `yaml:"appPort"`
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

	if c.BatchType == "" {
		c.BatchType = "logged"
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 65536
	}

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

func (c *Connector) ApplyDefaults() {
	c.Cassandra.setDefaults()
}
