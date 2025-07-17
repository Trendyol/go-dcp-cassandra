package connector

import (
	"context"
	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"
	"os"
	"os/signal"
	"syscall"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp/models"
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
	cfg    *config.Connector
	mapper Mapper
}

func NewConnectorBuilder(cfg *config.Connector) ConnectorBuilder {
	return ConnectorBuilder{
		cfg:    cfg,
		mapper: DefaultMapper,
	}
}

func (c ConnectorBuilder) SetMapper(mapper Mapper) ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c ConnectorBuilder) Build() (Connector, error) {
	conn := &connector{
		mapper: c.mapper,
		config: c.cfg,
	}

	bulk, err := cassandra.NewBulk(c.cfg, func() {})
	if err != nil {
		return nil, err
	}
	conn.bulk = bulk

	dcpClient, err := dcp.NewDcp(&c.cfg.Dcp, conn.listener)
	if err != nil {
		return nil, err
	}
	conn.dcp = dcpClient

	SetCollectionTableMappings(&c.cfg.Cassandra.CollectionTableMapping)

	return conn, nil
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
