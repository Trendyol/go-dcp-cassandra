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
  dcpcassandra "github.com/Trendyol/go-dcp-cassandra"
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

## How It Works

### Write Pipeline

```
Couchbase DCP
     │
     ▼
  mapper()      ← your code transforms a DCP event into []cassandra.Model
     │
     ▼
  jobQueue      ← bounded channel (workerQueueSize slots, default workerCount×4)
     │
     ▼
  worker pool   ← workerCount goroutines, each writing serially to Cassandra
     │
     ▼
  Cassandra
```

### Worker Pool

Each DCP event is passed directly to the job queue and picked up by the next available worker. Workers process items serially — one Cassandra write at a time — so `workerCount` is a hard cap on concurrent Cassandra connections. Set it to match the number of Cassandra nodes for optimal throughput.

The job queue (`workerQueueSize`, default `workerCount × 4`) allows workers to pick up the next batch immediately after finishing a write without waiting for the next `AddActions` call. When the queue is full, `AddActions` blocks, providing natural backpressure from Cassandra back to the DCP stream.

### Acknowledgement Modes

**`ackMode: after_write`** (default): the DCP event is acknowledged only after the Cassandra write completes. On a crash, unwritten events will be replayed. Stronger durability guarantee, slightly lower throughput.

**`ackMode: immediate`**: the DCP event is acknowledged as soon as it enters the buffer, before the write. Higher throughput. On a crash, events that were buffered but not yet written will not be replayed. Safe only if writes are idempotent (upserts are; deletes are).

### Write Timestamp

Set `writeTimestamp` to attach a `USING TIMESTAMP` clause to every write:

- `none` (default): no timestamp clause; Cassandra uses the server-side write time
- `event_time`: uses the DCP event time in microseconds; useful for last-write-wins conflict resolution across concurrent writers
- `now`: uses the wall clock at write time in microseconds

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
  workerCount: 20        # set to number of Cassandra nodes
  useBatch: false        # individual prepared statements for token-aware routing
  ackMode: after_write   # or immediate for higher throughput if writes are idempotent
  writeTimestamp: none
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

| Variable                          | Type                     | Required | Default      | Description                                                                                                                              |
|-----------------------------------|--------------------------|----------|--------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `cassandra.hosts`                 | []string                 | yes      |              | Cassandra connection hosts                                                                                                               |
| `cassandra.username`              | string                   | yes      |              | Cassandra username                                                                                                                       |
| `cassandra.password`              | string                   | yes      |              | Cassandra password                                                                                                                       |
| `cassandra.keyspace`              | string                   | yes      |              | Cassandra keyspace name                                                                                                                  |
| `cassandra.timeout`               | time.Duration            | no       | 10s          | Cassandra query timeout                                                                                                                  |
| `cassandra.workerCount`           | int                      | no       | 1            | Number of concurrent workers writing to Cassandra. Set to the number of Cassandra nodes for best throughput                              |
| `cassandra.workerQueueSize`       | int                      | no       | workerCount×4 | Job queue depth. A larger value keeps workers busy during write latency spikes. Defaults to 4× workerCount                              |
| `cassandra.useBatch`              | bool                     | no       | false        | When the mapper returns multiple rows per event, group them into a single CQL batch statement. Has no effect when the mapper returns one row per event. Batches lose token-aware routing — all rows go to one coordinator |
| `cassandra.batchType`             | string                   | no       | logged       | CQL batch type when `useBatch` is true: `logged`, `unlogged`, or `counter`. `unlogged` is faster but not atomic across partitions       |
| `cassandra.ackMode`               | string                   | no       | after_write  | When to acknowledge DCP events: `immediate` (before write, higher throughput) or `after_write` (after write, stronger durability)        |
| `cassandra.writeTimestamp`        | string                   | no       | none         | Populate `USING TIMESTAMP` on writes: `none`, `event_time` (DCP event time in µs), or `now` (wall clock at write time in µs)            |
| `cassandra.hostSelectionPolicy`   | string                   | no       | token_aware  | Host selection policy: `token_aware` (default) or `round_robin`                                                                         |
| `cassandra.consistency`           | string                   | no       | QUORUM       | Cassandra consistency level                                                                                                              |
| `cassandra.tableName`             | string                   | no       |              | Target table name (used when no collection mapping is configured)                                                                        |
| `cassandra.collectionTableMapping`| []CollectionTableMapping | no       |              | Used by the default mapper. See next section                                                                                             |

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