package cassandra

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("github.com/Trendyol/go-dcp-cassandra")
