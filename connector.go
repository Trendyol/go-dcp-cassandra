package dcpcassandra

import (
	"context"
	"errors"
	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	connectorpkg "go-dcp-cassandra/connector"
	"go-dcp-cassandra/couchbase"
	"os"
	"os/signal"
	"syscall"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
	"gopkg.in/yaml.v3"
)

type Connector interface {
	Start()
	Close()
	GetBulk() *cassandra.Bulk
}

type connector struct {
	dcp    dcp.Dcp
	bulk   *cassandra.Bulk
	mapper Mapper
	config *config.Connector
}

type ConnectorBuilder struct {
	config any
	mapper Mapper
}

func newConnectorConfigFromPath(path string) (*config.Connector, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c config.Connector
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func newConfig(cf any) (*config.Connector, error) {
	switch v := cf.(type) {
	case *config.Connector:
		return v, nil
	case config.Connector:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnector(cf any, mapper Mapper) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	var finalMapper Mapper
	if len(cfg.Cassandra.CollectionTableMapping) > 0 {
		finalMapper = connectorpkg.DefaultMapper
	} else {
		finalMapper = mapper
	}

	conn := &connector{
		mapper: finalMapper,
		config: cfg,
	}

	bulk, err := cassandra.NewBulk(cfg, func() {})
	if err != nil {
		return nil, err
	}
	conn.bulk = bulk

	dcpClient, err := dcp.NewDcp(&cfg.Dcp, conn.listener)
	if err != nil {
		return nil, err
	}
	conn.dcp = dcpClient

	connectorpkg.SetCollectionTableMappings(&cfg.Cassandra.CollectionTableMapping)

	return conn, nil
}

func NewConnectorBuilder(config any) ConnectorBuilder {
	return ConnectorBuilder{
		config: config,
		mapper: SimpleDefaultMapper,
	}
}

func SimpleDefaultMapper(event couchbase.Event) []cassandra.Model {
	var raw = cassandra.Raw{
		Table: "example_table",
		Document: map[string]interface{}{
			"id":   string(event.Key),
			"data": string(event.Value),
		},
		Operation: cassandra.Upsert,
		ID:        string(event.Key),
	}

	return []cassandra.Model{&raw}
}

func (c ConnectorBuilder) SetMapper(mapper Mapper) ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper)
}

func (c *connector) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		c.Close()
		cancel()
	}()
	go func() {
		<-c.dcp.WaitUntilReady()
		c.bulk.StartBulk()
	}()
	c.dcp.Start()
	<-ctx.Done()
}

func (c *connector) Close() {
	if c.dcp != nil {
		c.dcp.Close()
	}
	if c.bulk != nil {
		c.bulk.Close()
	}
}

func (c *connector) listener(ctx *models.ListenerContext) {
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(event.Key, event.Value, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
	default:
		ctx.Ack()
		return
	}

	actions := c.mapper(e)
	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	c.bulk.AddActions(ctx, e.EventTime, actions)
}

func (c *connector) GetBulk() *cassandra.Bulk {
	return c.bulk
}
