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
	KeepAlive           time.Duration `yaml:"keepAlive"`
	Timeout             time.Duration `yaml:"timeout"`
	ConnectTimeout      time.Duration `yaml:"connectTimeout"`
	BatchTickerDuration time.Duration `yaml:"batchTickerDuration"`
	NumConns            int           `yaml:"numConns"`
	MaxPreparedStmts    int           `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo   int           `yaml:"maxRoutingKeyInfo"`
	PageSize            int           `yaml:"pageSize"`
	BatchSizeLimit      int           `yaml:"batchSizeLimit"`
	BatchByteSizeLimit  int           `yaml:"batchByteSizeLimit"`
	MaxInFlightRequests int           `yaml:"maxInFlightRequests"`
	BatchPerEvent       bool          `yaml:"batchPerEvent"`
	HostSelectionPolicy string        `yaml:"hostSelectionPolicy"`
	WriteTimestamp      string        `yaml:"writeTimestamp"`
}

type Connector struct {
	Dcp       config.Dcp `yaml:",inline" mapstructure:",squash"`
	Cassandra Cassandra  `yaml:"cassandra" mapstructure:"cassandra"`
}

func (c *Cassandra) setDefaults() {
	c.setConsistencyDefault()
	c.setBatchDefaults()
	c.setConnectionDefaults()
	c.setRetryDefaults()
}

func (c *Cassandra) setConsistencyDefault() {
	validConsistencies := map[string]bool{
		"ANY": true, "ONE": true, "TWO": true, "THREE": true,
		"QUORUM": true, "ALL": true, "LOCAL_QUORUM": true,
		"EACH_QUORUM": true, "LOCAL_ONE": true,
	}
	consistency := strings.TrimSpace(strings.ToUpper(c.Consistency))
	if consistency == "" || !validConsistencies[consistency] {
		c.Consistency = "QUORUM"
	} else {
		c.Consistency = consistency
	}
}

func (c *Cassandra) setBatchDefaults() {
	if c.BatchSizeLimit <= 0 {
		c.BatchSizeLimit = 2000
	}
	if c.BatchByteSizeLimit <= 0 {
		c.BatchByteSizeLimit = 10 * 1024 * 1024 // 10MB
	}
	if c.BatchTickerDuration <= 0 {
		c.BatchTickerDuration = 10 * time.Second
	}
	if c.MaxInFlightRequests <= 0 {
		c.MaxInFlightRequests = 100
	}

	hostSelectionPolicy := strings.TrimSpace(strings.ToLower(c.HostSelectionPolicy))
	if hostSelectionPolicy != "round_robin" && hostSelectionPolicy != "token_aware" {
		c.HostSelectionPolicy = "token_aware"
	} else {
		c.HostSelectionPolicy = hostSelectionPolicy
	}

	writeTimestamp := strings.TrimSpace(strings.ToLower(c.WriteTimestamp))
	if writeTimestamp != "none" && writeTimestamp != "event_time" && writeTimestamp != "now" {
		c.WriteTimestamp = "none"
	} else {
		c.WriteTimestamp = writeTimestamp
	}
}

func (c *Cassandra) setConnectionDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
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
}

func (c *Cassandra) setRetryDefaults() {
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

// SetTestDefaults applies defaults for integration tests.
func (c *Cassandra) SetTestDefaults() {
	c.setDefaults()
}
