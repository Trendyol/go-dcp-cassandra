package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go-dcp-cassandra/cassandra"
	config "go-dcp-cassandra/configs"
	"go-dcp-cassandra/couchbase"
	"go-dcp-cassandra/interface/rest"
	"go-dcp-cassandra/metrics"
)

func customCassandraMapper(event couchbase.Event) []cassandra.Model {
	if event.IsMutated {
		// Use the default mapper logic
		return Map(event)
	} else if event.IsDeleted || event.IsExpired {
		// Handle delete operations
		return Map(event)
	}
	return nil
}

func DefaultMapper(event couchbase.Event) []cassandra.Model {
	if event.IsMutated {
		mapping := findCollectionTableMapping(event.CollectionName)
		model := buildUpsertModel(mapping, event)
		return []cassandra.Model{&model}
	} else if event.IsDeleted || event.IsExpired {
		mapping := findCollectionTableMapping(event.CollectionName)
		model := buildDeleteModel(mapping, event)
		return []cassandra.Model{&model}
	}

	return nil
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

	log.Printf("CollectionTableMapping length: %d", len(cfg.Cassandra.CollectionTableMapping))
	log.Printf("useDefaultMapper: %v", useDefaultMapper)

	if useDefaultMapper {
		log.Println("Using default mapper with collection-table mapping")
		connector, err = NewConnectorBuilder(&cfg).Build()
	} else {
		log.Println("Using custom mapper")
		connector, err = NewConnectorBuilder(&cfg).
			SetMapper(customCassandraMapper).
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
