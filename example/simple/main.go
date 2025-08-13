package main

import (
	dcpcassandra "go-dcp-cassandra"
	"go-dcp-cassandra/cassandra"
	"go-dcp-cassandra/couchbase"
)

func mapper(event couchbase.Event) []cassandra.Model {
	var raw = cassandra.Raw{
		Table: "example_table",
		Document: map[string]interface{}{
			"id":    string(event.Key),
			"value": string(event.Value),
		},
		Operation: cassandra.Upsert,
		ID:        string(event.Key),
	}

	return []cassandra.Model{&raw}
}

func main() {
	connector, err := dcpcassandra.NewConnectorBuilder("config.yml").
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
