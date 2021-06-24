package elastic_exporter

import (
	"fmt"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"iss.digital/mt/elastic_exporter/config"
	"iss.digital/mt/elastic_exporter/errors"
)

// MetricDesc is a descriptor for a family of metrics, sharing the same name, help, labes, type.
type MetricDesc interface {
	Name() string
	Help() string
	ValueType() prometheus.ValueType
	ConstLabels() []*dto.LabelPair
	LogContext() string
}

type labelPair struct {
	key   string
	value string
}

//
// MetricFamily
//

// MetricFamily implements MetricDesc for ElasticSearch metrics, with logic for populating its labels and values from query result.
type MetricFamily struct {
	config      *config.MetricConfig
	constLabels []*dto.LabelPair
	logContext  string
}

// NewMetricFamily creates a new MetricFamily with the given metric config and const labels (e.g. job and instance).
func NewMetricFamily(logContext string, mc *config.MetricConfig, constLabels []*dto.LabelPair) (*MetricFamily, errors.WithContext) {
	logContext = fmt.Sprintf("%s, metric=%q", logContext, mc.Name)

	// Create a copy of original slice to avoid modifying constLabels
	sortedLabels := append(constLabels[:0:0], constLabels...)

	for k, v := range mc.StaticLabels {
		sortedLabels = append(sortedLabels, &dto.LabelPair{
			Name:  proto.String(k),
			Value: proto.String(v),
		})
	}
	sort.Sort(labelPairSorter(sortedLabels))

	return &MetricFamily{
		config:      mc,
		constLabels: sortedLabels,
		logContext:  logContext,
	}, nil
}

func (mf MetricFamily) Collect(metrics []resultMetric, ch chan<- Metric) {
	for _, m := range metrics {
		labels := make([]labelPair, 0, 1)
		if m.labelPair.key != "" && m.labelPair.value != "" {
			labels = append(labels, m.labelPair)
		}
		ch <- NewMetric(&mf, m.value, labels...)
	}
}

// Name implements MetricDesc.
func (mf MetricFamily) Name() string {
	return mf.config.Name
}

// Help implements MetricDesc.
func (mf MetricFamily) Help() string {
	return mf.config.Help
}

// ValueType implements MetricDesc.
func (mf MetricFamily) ValueType() prometheus.ValueType {
	return mf.config.ValueType()
}

// ConstLabels implements MetricDesc.
func (mf MetricFamily) ConstLabels() []*dto.LabelPair {
	return mf.constLabels
}

// LogContext implements MetricDesc.
func (mf MetricFamily) LogContext() string {
	return mf.logContext
}

//
// automaticMetricDesc
//

// automaticMetric is a MetricDesc for automatically generated metrics (e.g. `up` and `scrape_duration`).
type automaticMetricDesc struct {
	name        string
	help        string
	valueType   prometheus.ValueType
	labels      []string
	constLabels []*dto.LabelPair
	logContext  string
}

// NewAutomaticMetricDesc creates a MetricDesc for automatically generated metrics.
func NewAutomaticMetricDesc(
	logContext, name, help string, valueType prometheus.ValueType, constLabels []*dto.LabelPair, labels ...string) MetricDesc {
	return &automaticMetricDesc{
		name:        name,
		help:        help,
		valueType:   valueType,
		constLabels: constLabels,
		labels:      labels,
		logContext:  logContext,
	}
}

// Name implements MetricDesc.
func (a automaticMetricDesc) Name() string {
	return a.name
}

// Help implements MetricDesc.
func (a automaticMetricDesc) Help() string {
	return a.help
}

// ValueType implements MetricDesc.
func (a automaticMetricDesc) ValueType() prometheus.ValueType {
	return a.valueType
}

// ConstLabels implements MetricDesc.
func (a automaticMetricDesc) ConstLabels() []*dto.LabelPair {
	return a.constLabels
}

// Labels implements MetricDesc.
func (a automaticMetricDesc) Labels() []string {
	return a.labels
}

// LogContext implements MetricDesc.
func (a automaticMetricDesc) LogContext() string {
	return a.logContext
}

//
// Metric
//

// A Metric models a single sample value with its meta data being exported to Prometheus.
type Metric interface {
	Desc() MetricDesc
	Write(out *dto.Metric) errors.WithContext
}

// NewMetric returns a metric with one fixed value that cannot be changed.
//
func NewMetric(desc MetricDesc, value float64, labelValues ...labelPair) Metric {
	return &constMetric{
		desc:       desc,
		val:        value,
		labelPairs: makeLabelPairs(desc, labelValues),
	}
}

// constMetric is a metric with one fixed value that cannot be changed.
type constMetric struct {
	desc       MetricDesc
	val        float64
	labelPairs []*dto.LabelPair
}

// Desc implements Metric.
func (m *constMetric) Desc() MetricDesc {
	return m.desc
}

// Write implements Metric.
func (m *constMetric) Write(out *dto.Metric) errors.WithContext {
	out.Label = m.labelPairs
	switch t := m.desc.ValueType(); t {
	case prometheus.CounterValue:
		out.Counter = &dto.Counter{Value: proto.Float64(m.val)}
	case prometheus.GaugeValue:
		out.Gauge = &dto.Gauge{Value: proto.Float64(m.val)}
	default:
		return errors.Errorf(m.desc.LogContext(), "encountered unknown type %v", t)
	}
	return nil
}

func makeLabelPairs(desc MetricDesc, labelValues []labelPair) []*dto.LabelPair {
	constLabels := desc.ConstLabels()

	totalLen := len(labelValues) + len(constLabels)
	if totalLen == 0 {
		// Super fast path.
		return nil
	}
	if len(labelValues) == 0 {
		// Moderately fast path.
		return constLabels
	}
	labelPairs := make([]*dto.LabelPair, 0, totalLen)
	for _, label := range labelValues {
		labelPairs = append(labelPairs, &dto.LabelPair{
			Name:  proto.String(label.key),
			Value: proto.String(label.value),
		})
	}
	labelPairs = append(labelPairs, constLabels...)
	sort.Sort(labelPairSorter(labelPairs))

	return labelPairs
}

// labelPairSorter implements sort.Interface.
// It provides a sortable version of a slice of dto.LabelPair pointers.

type labelPairSorter []*dto.LabelPair

func (s labelPairSorter) Len() int {
	return len(s)
}

func (s labelPairSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s labelPairSorter) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}

type invalidMetric struct {
	err errors.WithContext
}

// NewInvalidMetric returns a metric whose Write method always returns the provided error.
func NewInvalidMetric(err errors.WithContext) Metric {
	return invalidMetric{err}
}

func (m invalidMetric) Desc() MetricDesc { return nil }

func (m invalidMetric) Write(*dto.Metric) errors.WithContext { return m.err }
