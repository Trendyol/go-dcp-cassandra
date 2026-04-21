package metric

import (
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Trendyol/go-dcp-cassandra/cassandra"
)

type Collector struct {
	bulk *cassandra.Bulk

	processLatency            *prometheus.Desc
	bulkRequestProcessLatency *prometheus.Desc
	bulkRequestSize           *prometheus.Desc
	bulkRequestByteSize       *prometheus.Desc
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	bulkMetric := c.bulk.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		c.processLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.ProcessLatencyMs),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		c.bulkRequestProcessLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.BulkRequestProcessLatencyMs),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		c.bulkRequestSize,
		prometheus.GaugeValue,
		float64(bulkMetric.BulkRequestSize),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		c.bulkRequestByteSize,
		prometheus.GaugeValue,
		float64(bulkMetric.BulkRequestByteSize),
		[]string{}...,
	)
}

func NewMetricCollector(bulk *cassandra.Bulk) *Collector {
	return &Collector{
		bulk: bulk,

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "cassandra_connector_latency_ms", "current"),
			"Cassandra connector latency ms",
			[]string{},
			nil,
		),

		bulkRequestProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "cassandra_connector_bulk_request_process_latency_ms", "current"),
			"Cassandra connector bulk request process latency ms",
			[]string{},
			nil,
		),

		bulkRequestSize: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "cassandra_connector_bulk_request_size", "current"),
			"Cassandra connector bulk request size",
			[]string{},
			nil,
		),

		bulkRequestByteSize: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "cassandra_connector_bulk_request_byte_size", "current"),
			"Cassandra connector bulk request byte size",
			[]string{},
			nil,
		),
	}
}

func (c *Collector) Unregister() {
	prometheus.Unregister(c)
}
