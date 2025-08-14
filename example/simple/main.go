package main

import (
	dcpcassandra "go-dcp-cassandra"
)

func main() {
	connector, err := dcpcassandra.NewConnectorBuilder("config.yml").
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
