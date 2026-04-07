package cassandra

import (
	"crypto/tls"
	"log/slog"

	"github.com/gocql/gocql"

	config "github.com/Trendyol/go-dcp-cassandra/configs"
)

//nolint:funlen
func NewCassandraSession(cfg config.Cassandra) (Session, error) {
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

	if cfg.SerialConsistency != "" {
		switch cfg.SerialConsistency {
		case "SERIAL":
			cluster.SerialConsistency = gocql.Serial
		case "LOCAL_SERIAL":
			cluster.SerialConsistency = gocql.LocalSerial
		default:
			cluster.SerialConsistency = gocql.Serial
		}
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	if cfg.KeepAlive > 0 {
		cluster.SocketKeepalive = cfg.KeepAlive
	}

	if cfg.NumConns > 0 {
		cluster.NumConns = cfg.NumConns
	}

	if cfg.MaxPreparedStmts > 0 {
		cluster.MaxPreparedStmts = cfg.MaxPreparedStmts
	}

	if cfg.MaxRoutingKeyInfo > 0 {
		cluster.MaxRoutingKeyInfo = cfg.MaxRoutingKeyInfo
	}

	if cfg.PageSize > 0 {
		cluster.PageSize = cfg.PageSize
	}

	if cfg.RetryPolicy.NumRetries > 0 {
		cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
			NumRetries: cfg.RetryPolicy.NumRetries,
			Min:        cfg.RetryPolicy.MinRetryDelay,
			Max:        cfg.RetryPolicy.MaxRetryDelay,
		}
	}

	switch cfg.Compressor {
	case "snappy":
		cluster.Compressor = gocql.SnappyCompressor{}
	case "lz4":
		slog.Warn("LZ4 compression is not available in standard gocql, using snappy instead")
		cluster.Compressor = gocql.SnappyCompressor{}
	default:
	}

	if cfg.SSL.Enable {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.SSL.InsecureSkipVerify, //nolint:gosec // user-configurable option
		}

		if cfg.SSL.CertPath != "" || cfg.SSL.KeyPath != "" || cfg.SSL.CaPath != "" {
			slog.Warn("custom SSL certificate configuration is not fully implemented yet")
		}

		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
		}
	}

	switch cfg.HostSelectionPolicy {
	case "round_robin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	default:
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}

	session, err := cluster.CreateSession()
	if err != nil {
		slog.Error("failed to create cassandra session", "error", err)
		return nil, err
	}
	return NewGocqlSessionAdapter(session), nil
}
