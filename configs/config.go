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
	Username          string `yaml:"username"`
	Password          string `yaml:"password"`
	Keyspace          string `yaml:"keyspace"`
	Compressor        string `yaml:"compressor"`
	SerialConsistency string `yaml:"serialConsistency"`
	BatchType         string `yaml:"batchType"`
	Consistency       string `yaml:"consistency"`
	TableName         string `yaml:"tableName"`
	SSL               struct {
		CertPath           string `yaml:"certPath"`
		KeyPath            string `yaml:"keyPath"`
		CaPath             string `yaml:"caPath"`
		Enable             bool   `yaml:"enable"`
		InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
	} `yaml:"ssl"`
	CollectionTableMapping []CollectionTableMapping `yaml:"collectionTableMapping,omitempty"`
	Hosts                  []string                 `yaml:"hosts"`
	RetryPolicy            struct {
		NumRetries    int           `yaml:"numRetries"`
		MinRetryDelay time.Duration `yaml:"minRetryDelay"`
		MaxRetryDelay time.Duration `yaml:"maxRetryDelay"`
	} `yaml:"retryPolicy"`
	BatchTimeout        time.Duration `yaml:"batchTimeout"`
	KeepAlive           time.Duration `yaml:"keepAlive"`
	BatchByteSizeLimit  int           `yaml:"batchByteSizeLimit"`
	Timeout             time.Duration `yaml:"timeout"`
	MaxBatchSize        int           `yaml:"maxBatchSize"`
	NumConns            int           `yaml:"numConns"`
	ConnectTimeout      time.Duration `yaml:"connectTimeout"`
	WorkerCount         int           `yaml:"workerCount"`
	MaxPreparedStmts    int           `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo   int           `yaml:"maxRoutingKeyInfo"`
	PageSize            int           `yaml:"pageSize"`
	BatchTickerDuration time.Duration `yaml:"batchTickerDuration"`
	BatchSizeLimit      int           `yaml:"batchSizeLimit"`
	BatchSize           int           `yaml:"batchSize"`
	UseBatch            bool          `yaml:"useBatch"`
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
