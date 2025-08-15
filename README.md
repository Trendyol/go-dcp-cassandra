# Go DCP Cassandra [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-cassandra.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-cassandra) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-cassandra)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-cassandra) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp-cassandra/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp-cassandra)

**Go DCP Cassandra** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Cassandra tables in near real-time.

## Features

* Custom Cassandra queries **per** DCP event.
* **Update multiple documents** for a DCP event(see [Example](#example)).
* Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Managing batch configurations** such as maximum batch size, batch ticker durations.
* **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp#examples)).
* **Easily manageable configurations**.
* **Flexible mapping** with support for both default mapper (configuration-based) and custom mapper (code-based).
* **Parallel processing** with configurable worker count.
* **Graceful shutdown** with safe batch writing to Cassandra.

## Example

**Note:** If you prefer to use the default mapper by entering the configuration instead of creating a custom mapper, please refer to [this](#collection-table-mapping-configuration) topic.
Otherwise, you can refer to the example provided below:

```go
package main

import (
  dcpcassandra "go-dcp-cassandra"
)


func CustomMapper(event couchbase.Event) []cassandra.Model {
  //
}

func main() {
  connector, err := dcpcassandra.NewConnectorBuilder("config.yml").
    SetMapper(CustomMapper). // NOT NEEDED IF YOU'RE USING DEFAULT MAPPER. JUST CALL Build() FUNCTION
    Build()
  if err != nil {
    panic(err)
  }

  defer connector.Close()
  connector.Start()
}
```

## Configuration

### Example Configuration

```yaml
hosts:
  - http://localhost:8091
bucketName: example_bucket
logging:
  level: info
dcp:
  group:
    name: example_group
metadata:
  config:
    bucket: metadata
cassandra:
  hosts:
    - localhost:9042
  keyspace: example_keyspace
  timeout: 10s
  batchSizeLimit: 1000
  batchByteSizeLimit: 10485760
  workerCount: 4
  batchTickerDuration: 5s
  collectionTableMapping:
    - collection: example_collection
      tableName: example_table
      fieldMappings:
        id: "id"                    # Document key → id column
        partition_key: "partitionId" # JSON field partitionId → partition_key column
        data: "documentData"         # Full JSON document → data column
        metadata: "meta"            # JSON field meta → metadata column
        status: "status"            # JSON field status → status column
```

### Dcp Configuration

Check out on [go-dcp](https://github.com/Trendyol/go-dcp#configuration)

### Cassandra Specific Configuration

| Variable                                                | Type                     | Required | Default | Description                                                                                        |                                                           
|---------------------------------------------------------|--------------------------|----------|---------|----------------------------------------------------------------------------------------------------|
| `cassandra.hosts`                                       | []string                 | yes      |         | Cassandra connection hosts                                                                         |
| `cassandra.username`                                    | string                   | yes      |         | Cassandra username                                                                                 |
| `cassandra.password`                                    | string                   | yes      |         | Cassandra password                                                                                 |
| `cassandra.keyspace`                                    | string                   | yes      |         | Cassandra keyspace name                                                                            |
| `cassandra.timeout`                                     | time.Duration            | no       | 10s     | Cassandra query timeout                                                                            |
| `cassandra.batchSize`                                   | int                      | no       | 100     | Batch size for bulk operations                                                                     |
| `cassandra.batchTimeout`                                | time.Duration            | no       | 2s      | Batch timeout duration                                                                             |
| `cassandra.batchSizeLimit`                              | int                      | no       | 1000    | Maximum number of records in a batch                                                              |
| `cassandra.batchTickerDuration`                         | time.Duration            | no       | 5s      | Batch is being flushed automatically at specific time intervals for long waiting messages in batch |
| `cassandra.batchByteSizeLimit`                          | int                      | no       | 10485760| Maximum byte size of a batch                                                                       |
| `cassandra.workerCount`                                 | int                      | no       | 4       | Number of parallel workers                                                                         |
| `cassandra.tableName`                                   | string                   | no       |         | Target table name (used when no collection mapping is configured)                                  |
| `cassandra.consistency`                                 | string                   | no       | QUORUM  | Cassandra consistency level                                                                        |
| `cassandra.collectionTableMapping`                      | []CollectionTableMapping | no       |         | Will be used for default mapper. Please read the next topic.                                       |

### Collection Table Mapping Configuration

Collection table mapping configuration is optional. This configuration should only be provided if you are using the default mapper. If you are implementing your own custom mapper function, this configuration is not needed.

| Variable                                                 | Type    | Required | Default | Description                                                                  |                                                           
|----------------------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------|
| `cassandra.collectionTableMapping[].collection`          | string  | yes      |         | Couchbase collection name                                                    |
| `cassandra.collectionTableMapping[].tableName`           | string  | yes      |         | Target Cassandra table name                                                  |
| `cassandra.collectionTableMapping[].fieldMappings`       | map     | yes      |         | Mapping between Cassandra columns and JSON document fields. Key is Cassandra column name, value is source field name. Special values: "id" for document key, "documentData" for full JSON document |

#### Field Mappings Example

Given a Couchbase document:
```json
{
  "userId": "user123",
  "name": "John Doe",
  "email": "john@example.com",
  "metadata": {
    "createdAt": "2024-01-01T00:00:00Z",
    "version": 1
  }
}
```

And field mappings:
```yaml
fieldMappings:
  id: "id"              # Document key → id column
  user_id: "userId"     # JSON field userId → user_id column
  full_name: "name"     # JSON field name → full_name column
  email_address: "email" # JSON field email → email_address column
  raw_data: "documentData" # Full JSON document → raw_data column
  meta_info: "metadata"  # JSON field metadata → meta_info column
```

The resulting Cassandra row will have:
- `id`: Document key (e.g., "doc123")
- `user_id`: "user123"
- `full_name`: "John Doe"
- `email_address`: "john@example.com"
- `raw_data`: Full JSON string
- `meta_info`: {"createdAt": "2024-01-01T00:00:00Z", "version": 1}

## Exposed metrics

| Metric Name                                   | Description                   | Labels | Value Type |
|-----------------------------------------------|-------------------------------|--------|------------|
| go_dcp_cassandra_connector_latency_ms_current | Time to adding to the batch.  | N/A    | Gauge      |
| go_dcp_cassandra_connector_bulk_request_process_latency_ms_current | Time to process bulk request. | N/A    | Gauge      |

You can also use all DCP-related metrics explained [here](https://github.com/Trendyol/go-dcp#exposed-metrics).
All DCP-related metrics are automatically injected. It means you don't need to do anything.

## Error Handling

- **Cassandra write errors**: Application panics and does not commit to Couchbase to ensure data consistency
- Cassandra connection errors
- Couchbase connection errors
- Document parsing errors
- Timeout errors
- Network connection issues

## Contributing

Go DCP Cassandra is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).