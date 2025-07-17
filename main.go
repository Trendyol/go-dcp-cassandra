package main

import (
	"context"
	"encoding/json"
	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"
	"go-dcp-cassandra/interface/rest"
	"go-dcp-cassandra/metrics"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func customCassandraMapper(event couchbase.Event, table string) []cassandra.Model {
	var document map[string]interface{}
	if err := json.Unmarshal(event.Value, &document); err != nil {
		log.Printf("Failed to unmarshal event.Value - Key: %s, Error: %v", string(event.Key), err)
		return nil
	}

	document["id"] = string(event.Key)

	document["timestamp"] = time.Now()

	operation := determineOperation(event)
	model := &cassandra.Raw{
		Table:     table,
		Document:  document,
		Operation: operation,
	}

	if operation == cassandra.Delete {
		handleDeleteOperation(model, document)
	}

	return []cassandra.Model{model}
}

func handleDeleteOperation(model *cassandra.Raw, document map[string]interface{}) {
	model.Document = nil
	model.Filter = map[string]interface{}{
		"id": document["id"],
	}
}

func determineOperation(event couchbase.Event) cassandra.OperationType {
	switch {
	case event.IsDeleted || event.IsExpired:
		return cassandra.Delete
	case event.IsMutated:
		return cassandra.Upsert
	default:
		return cassandra.Insert
	}
}

func main() {
	cfg := config.NewAppConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConf := rest.NewServer()
	router := serverConf.SetupRouter()

	srv := &http.Server{
		Addr:    cfg.AppPort,
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	useDefaultMapper := len(cfg.Cassandra.CollectionTableMapping) > 0

	var connector Connector
	var err error

	if useDefaultMapper {
		log.Println("Using default mapper with collection-table mapping")
		connector, err = NewConnectorBuilder(&cfg).Build()
	} else {
		log.Println("Using custom mapper")
		connector, err = NewConnectorBuilder(&cfg).
			SetMapper(func(event couchbase.Event) []cassandra.Model {
				return customCassandraMapper(event, cfg.Cassandra.TableName)
			}).
			Build()
	}

	if err != nil {
		log.Fatalf("Failed to build connector: %v", err)
	}

	metricCollector := metrics.NewMetricCollector(connector.GetBulk())
	prometheus.MustRegister(metricCollector)

	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		metricCollector.Unregister()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}

		connector.Close()
	}()

	go connector.Start()
	<-ctx.Done()
}
