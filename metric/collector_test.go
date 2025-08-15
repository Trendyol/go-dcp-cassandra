package metric

import (
	"go-dcp-cassandra/cassandra"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestNewMetricCollector(t *testing.T) {
	bulk := &cassandra.Bulk{}
	collector := NewMetricCollector(bulk)

	assert.NotNil(t, collector)
	assert.Equal(t, bulk, collector.bulk)
	assert.NotNil(t, collector.processLatency)
	assert.NotNil(t, collector.bulkRequestProcessLatency)
}

func TestCollector_Describe(t *testing.T) {
	bulk := &cassandra.Bulk{}
	collector := NewMetricCollector(bulk)

	ch := make(chan *prometheus.Desc, 10)
	collector.Describe(ch)
	close(ch)

	descriptions := make([]*prometheus.Desc, 0)
	for desc := range ch {
		descriptions = append(descriptions, desc)
	}

	assert.Len(t, descriptions, 2)
}

func TestCollector_Collect(t *testing.T) {
	bulk := &cassandra.Bulk{}
	collector := NewMetricCollector(bulk)

	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	assert.Len(t, metrics, 2)
}

func TestCollector_Unregister(t *testing.T) {
	bulk := &cassandra.Bulk{}
	collector := NewMetricCollector(bulk)

	collector.Unregister()
}

func TestCollector_Integration(t *testing.T) {
	bulk := &cassandra.Bulk{}
	collector := NewMetricCollector(bulk)

	descCh := make(chan *prometheus.Desc, 10)
	collector.Describe(descCh)
	close(descCh)

	descriptions := make([]*prometheus.Desc, 0)
	for desc := range descCh {
		descriptions = append(descriptions, desc)
	}

	assert.Len(t, descriptions, 2, "Should have 2 metric descriptions")

	metricCh := make(chan prometheus.Metric, 10)
	collector.Collect(metricCh)
	close(metricCh)

	metrics := make([]prometheus.Metric, 0)
	for metric := range metricCh {
		metrics = append(metrics, metric)
	}

	assert.Len(t, metrics, 2, "Should have 2 metric")
}

func TestNewMetricCollector_WithNilBulk(t *testing.T) {
	collector := NewMetricCollector(nil)

	assert.NotNil(t, collector)
	assert.Nil(t, collector.bulk)
	assert.NotNil(t, collector.processLatency)
	assert.NotNil(t, collector.bulkRequestProcessLatency)
}
