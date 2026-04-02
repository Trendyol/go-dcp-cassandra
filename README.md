# Go DCP Cassandra [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-cassandra.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-cassandra) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-cassandra)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-cassandra) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp-cassandra/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp-cassandra)

**Go DCP Cassandra** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Cassandra tables in near real-time.

## Features

* Custom Cassandra queries **per** DCP event.
* **Update multiple documents** for a DCP event(see [Example](#example)).
* Handling different DCP events such as **expiration, deletion and mutation**(see [Example](#example)).
* **Buffer accumulation** with configurable size and time limits for high-throughput batching.
* **Concurrent writes** with configurable in-flight request cap.
* **Per-event unlogged batches** for mappers that emit multiple rows per DCP event.
* **Scale up and down** by custom membership algorithms(Couchbase, KubernetesHa, Kubernetes StatefulSet or
  Static, see [examples](https://github.com/Trendyol/go-dcp#examples)).
* **Easily manageable configurations**.
* **Flexible mapping** with support for both default mapper (configuration-based) and custom mapper (code-based).
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
  mapper()          ← your code transforms a DCP event into []cassandra.Model
     │
     ▼
  active buffer     ← shared in-memory buffer, mutex protected
     │
     ├── flush trigger: batchSizeLimit, batchByteSizeLimit, or batchTickerDuration
     │
     ▼
   swap buffer       ← active buffer swapped out; DCP immediately resumes writing to fresh buffer
     │
     ├── wait for previous flush to complete (max 2 buffers: one active, one in-flight)
     │   if Cassandra is slow, AddActions blocks here providing backpressure
     │
     ▼
  concurrent writes ← up to maxInFlightRequests goroutines write to Cassandra simultaneously
     │               if batchPerEvent: multi-row events grouped into a single UNLOGGED BATCH
     │
     ▼
  all writes done → ack all items in order → dcpClient.Commit()
     │
     ▼
  Cassandra
```

### Buffering and Flushing

DCP events are accumulated in a shared in-memory buffer. A flush is triggered when any of the following conditions are met:

- The number of items reaches `batchSizeLimit`
- The estimated byte size of the buffer reaches `batchByteSizeLimit`
- The `batchTickerDuration` interval elapses

On flush, the buffer is **swapped atomically** — the active buffer is replaced with a fresh empty one and returned immediately, so `AddActions` is never blocked waiting for Cassandra writes.

Only one flush is in flight at a time. If a new flush is triggered while the previous one is still writing, it waits until the previous flush's workers finish before starting its own. This preserves correct checkpoint ordering.

### Concurrent Writes

During a flush, up to `maxInFlightRequests` goroutines write to Cassandra concurrently. This is the primary throughput knob — set it to match the number of Cassandra nodes × connections per node.

With `writeTimestamp: event_time` or `writeTimestamp: now`, concurrent writes for the same key are safe — the `USING TIMESTAMP` clause ensures the most recent event always wins in Cassandra regardless of write completion order.

### Per-Event Batching

When `batchPerEvent: true`, actions returned by the mapper for a **single DCP event** are grouped into one CQL `UNLOGGED BATCH` statement. This reduces round trips for mappers that emit multiple rows per event (e.g. fan-out to multiple tables).

When `batchPerEvent: false` (default), every action is an individual prepared statement routed directly to the owning Cassandra replica via token-aware routing.

### Checkpointing

`dcpClient.Commit()` is called once per flush, after all writes in the flush complete. This is much cheaper than committing per-event — at high throughput, commits happen at the flush boundary (controlled by `batchSizeLimit` and `batchTickerDuration`) rather than on every DCP event.

All items in a flush are acked after writes complete, so the checkpoint always reflects a consistent written state. On a crash, the entire last flush window is replayed.

### Write Timestamp

Set `writeTimestamp` to attach a `USING TIMESTAMP` clause to every write:

- `none` (default): no timestamp clause; Cassandra uses the server-side write time
- `event_time`: uses the DCP event time in microseconds; correct ordering even with concurrent writes
- `now`: uses the wall clock at ingestion time in microseconds; same correctness guarantee as `event_time`

Recommended when using concurrent writes (`maxInFlightRequests > 1`) to ensure last-write-wins correctness.

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
  checkpoint:
    type: auto       # go-dcp persists checkpoints automatically on an interval
    interval: 10s    # tune based on acceptable replay window on crash
metadata:
  config:
    bucket: metadata
cassandra:
  hosts:
    - localhost:9042
  keyspace: example_keyspace
  timeout: 10s
  batchSizeLimit: 2000
  batchByteSizeLimit: 10485760
  batchTickerDuration: 10s
  maxInFlightRequests: 100
  batchPerEvent: false
  writeTimestamp: event_time
  hostSelectionPolicy: token_aware
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

| Variable                            | Type                     | Required | Default      | Description                                                                                                                                          |
|-------------------------------------|--------------------------|----------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cassandra.hosts`                   | []string                 | yes      |              | Cassandra connection hosts                                                                                                                           |
| `cassandra.username`                | string                   | yes      |              | Cassandra username                                                                                                                                   |
| `cassandra.password`                | string                   | yes      |              | Cassandra password                                                                                                                                   |
| `cassandra.keyspace`                | string                   | yes      |              | Cassandra keyspace name                                                                                                                              |
| `cassandra.timeout`                 | time.Duration            | no       | 10s          | Cassandra query timeout                                                                                                                              |
| `cassandra.batchSizeLimit`          | int                      | no       | 2000         | Flush the buffer when this many items have accumulated                                                                                               |
| `cassandra.batchByteSizeLimit`      | int                      | no       | 10485760     | Flush the buffer when its estimated byte size exceeds this limit                                                                                     |
| `cassandra.batchTickerDuration`     | time.Duration            | no       | 10s          | Flush the buffer at this interval even if size/byte limits are not reached                                                                           |
| `cassandra.maxInFlightRequests`     | int                      | no       | 100          | Maximum concurrent Cassandra writes during a flush. Set to Cassandra node count × connections per node                                               |
| `cassandra.batchPerEvent`           | bool                     | no       | false        | Group multiple rows from the same DCP event into a single CQL UNLOGGED BATCH. Useful when the mapper emits multiple rows per event                   |
| `cassandra.writeTimestamp`          | string                   | no       | none         | `none`, `event_time` (DCP event time in µs), or `now` (ingestion wall clock in µs). Recommended when maxInFlightRequests > 1                        |
| `cassandra.hostSelectionPolicy`     | string                   | no       | token_aware  | `token_aware` (default) or `round_robin`                                                                                                             |
| `cassandra.consistency`             | string                   | no       | QUORUM       | Cassandra consistency level                                                                                                                          |
| `cassandra.tableName`               | string                   | no       |              | Target table name (used when no collection mapping is configured)                                                                                    |
| `cassandra.numConns`                | int                      | no       | 2            | Number of connections per Cassandra host                                                                                                             |
| `cassandra.connectTimeout`          | time.Duration            | no       | 5s           | Cassandra initial connection timeout                                                                                                                 |
| `cassandra.keepAlive`               | time.Duration            | no       | 30s          | TCP keep-alive interval                                                                                                                              |
| `cassandra.maxPreparedStmts`        | int                      | no       | 1000         | Maximum number of prepared statements cached by the driver                                                                                           |
| `cassandra.pageSize`                | int                      | no       | 5000         | Default page size for queries                                                                                                                        |
| `cassandra.serialConsistency`       | string                   | no       |              | Serial consistency level for LWT (e.g. `SERIAL`, `LOCAL_SERIAL`)                                                                                    |
| `cassandra.compressor`              | string                   | no       |              | Compression algorithm (`snappy` or `lz4`)                                                                                                            |
| `cassandra.retryPolicy.numRetries`  | int                      | no       | 3            | Number of retries on Cassandra write failure                                                                                                         |
| `cassandra.retryPolicy.minRetryDelay` | time.Duration          | no       | 100ms        | Minimum delay between retries                                                                                                                        |
| `cassandra.retryPolicy.maxRetryDelay` | time.Duration          | no       | 1s           | Maximum delay between retries                                                                                                                        |
| `cassandra.ssl.enable`              | bool                     | no       | false        | Enable TLS/SSL connection to Cassandra                                                                                                               |
| `cassandra.ssl.certPath`            | string                   | no       |              | Path to client certificate file                                                                                                                      |
| `cassandra.ssl.keyPath`             | string                   | no       |              | Path to client private key file                                                                                                                      |
| `cassandra.ssl.caPath`              | string                   | no       |              | Path to CA certificate file                                                                                                                          |
| `cassandra.ssl.insecureSkipVerify`  | bool                     | no       | false        | Skip server certificate verification (not recommended for production)                                                                                |
| `cassandra.collectionTableMapping`  | []CollectionTableMapping | no       |              | Used by the default mapper. See next section                                                                                                         |

### Collection Table Mapping Configuration

Collection table mapping configuration is optional. This configuration should only be provided if you are using the default mapper. If you are implementing your own custom mapper function, this configuration is not needed.

| Variable                                                 | Type     | Required | Default | Description                                                                  |
|----------------------------------------------------------|----------|----------|---------|------------------------------------------------------------------------------|
| `cassandra.collectionTableMapping[].collection`          | string   | yes      |         | Couchbase collection name. Use `_default` as a catch-all fallback            |
| `cassandra.collectionTableMapping[].tableName`           | string   | yes      |         | Target Cassandra table name                                                  |
| `cassandra.collectionTableMapping[].fieldMappings`       | map      | yes      |         | Mapping between Cassandra columns and JSON document fields. Key is Cassandra column name, value is source field name. Special values: `_key` for document key, `documentData` for full JSON document. Supports nested fields with dot notation (e.g. `profile.contact.email`) |
| `cassandra.collectionTableMapping[].primaryKeyFields`    | []string | no       |         | Cassandra column names that form the primary key. When set, DELETE operations only include these columns in the WHERE clause — reducing tombstone accumulation |

#### Field Mappings Example

Given a Couchbase document:
```json
{
  "userId": "user123",
  "name": "John Doe",
  "email": "john@example.com",
  "profile": {
    "contact": {
      "phone": "+1234567890"
    }
  }
}
```

And configuration:
```yaml
collectionTableMapping:
  - collection: users
    tableName: users
    primaryKeyFields:
      - id
      - user_id
    fieldMappings:
      id: "_key"                      # Document key → id column
      user_id: "userId"               # JSON field userId → user_id column
      full_name: "name"               # JSON field name → full_name column
      email_address: "email"          # JSON field email → email_address column
      phone: "profile.contact.phone"  # Nested field → phone column
      raw_data: "documentData"        # Full JSON document → raw_data column
```

The resulting Cassandra row will have:
- `id`: Document key (e.g., "doc123")
- `user_id`: "user123"
- `full_name`: "John Doe"
- `email_address`: "john@example.com"
- `phone`: "+1234567890" (extracted from nested path)
- `raw_data`: Full JSON string

On **deletion**, only `id` and `user_id` (the `primaryKeyFields`) are included in the DELETE WHERE clause — preventing tombstone accumulation from nulled non-key columns.

## Observability

### Tracing (OpenTelemetry)

The connector instruments key operations with [OpenTelemetry](https://opentelemetry.io/) spans:

| Span Name | Description |
|-----------|-------------|
| `cassandra.connect` | Session creation including cluster discovery |
| `cassandra.flush` | Full flush cycle (all writes + ack + commit) |
| `cassandra.write` | Individual prepared statement execution |
| `cassandra.batch` | CQL UNLOGGED BATCH execution (when `batchPerEvent: true`) |

Spans are **no-op by default** — there is zero overhead unless you configure a `TracerProvider` in your application. To enable tracing, configure an OTel exporter (Jaeger, OTLP, etc.) before starting the connector:

```go
import "go.opentelemetry.io/otel"

// Set up your TracerProvider (e.g. OTLP exporter)
otel.SetTracerProvider(yourProvider)

// Start the connector — spans are now exported automatically
connector.Start()
```

### Metrics (Prometheus)

| Metric Name                                   | Description                   | Labels | Value Type |
|-----------------------------------------------|-------------------------------|--------|------------|
| go_dcp_cassandra_connector_latency_ms_current | Time to adding to the batch.  | N/A    | Gauge      |
| go_dcp_cassandra_connector_bulk_request_process_latency_ms_current | Time to process bulk request. | N/A    | Gauge      |
| go_dcp_cassandra_connector_bulk_request_size_current | Number of items in the last flush. | N/A | Gauge |
| go_dcp_cassandra_connector_bulk_request_byte_size_current | Byte size of the last flush. | N/A | Gauge |

You can also use all DCP-related metrics explained [here](https://github.com/Trendyol/go-dcp#exposed-metrics).
All DCP-related metrics are automatically injected. It means you don't need to do anything.

## Error Handling

- **Cassandra write errors**: Application panics and does not commit to Couchbase to ensure data consistency
- Cassandra connection errors
- Couchbase connection errors
- Document parsing errors
- Timeout errors
- Network connection issues

## Running Integration Tests

Integration tests verify end-to-end behavior against a real Cassandra instance:

```bash
make test-integration
```

This command starts a Cassandra container via Docker Compose, runs the test suite with race detection, and tears down the container when done. No manual setup required.

If you prefer to run the steps manually:

```bash
docker compose -f test/integration/docker-compose.yml up -d --wait
go test -race -v -timeout 5m -tags integration ./test/integration/...
docker compose -f test/integration/docker-compose.yml down
```

## Contributing

Go DCP Cassandra is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
