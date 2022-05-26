package elastic_exporter

import (
	"fmt"
	"github.com/tidwall/gjson"
	"iss.digital/mt/elastic_exporter/config"
)

type AggregationHandler interface {
	Handle(result gjson.Result, metricsData []metricData) []metricData
}

func NewForType(aggType config.AggregationType, name string) (AggregationHandler, error) {
	var handler AggregationHandler
	switch aggType {
	case config.AggregationTypeTerms:
		handler = &TermsAggregationHandler{name: name}
	case config.AggregationTypeStats:
		handler = &StatsAggregationHandler{}
	case config.AggregationTypeMax:
	case config.AggregationTypeMin:
	case config.AggregationTypeSum:
	case config.AggregationTypeAvg:
	case config.AggregationTypeCardinality:
		handler = &SingleValueAggregationHandler{}
	default:
		return nil, fmt.Errorf("handler for %s not implemented", string(aggType))
	}

	return handler, nil
}

type TermsAggregationHandler struct {
	name string
}

func (t TermsAggregationHandler) Handle(result gjson.Result, metricsData []metricData) []metricData {
	buckets := result.Get("buckets")
	for _, data := range buckets.Array() {
		key := data.Get("key")
		value := data.Get("doc_count").Float()

		metricsData = append(metricsData, newLabeledMetricData(value, t.name, key.String()))
	}

	return metricsData
}

type SingleValueAggregationHandler struct {
}

func (m SingleValueAggregationHandler) Handle(result gjson.Result, metricsData []metricData) []metricData {
	value := result.Get("value").Float()

	return append(metricsData, newMetricData(value))
}

type StatsAggregationHandler struct {
}

func (s StatsAggregationHandler) Handle(result gjson.Result, metricsData []metricData) []metricData {
	metricsData = append(metricsData, newLabeledMetricData(result.Get("count").Float(), "stat", "count"))
	metricsData = append(metricsData, newLabeledMetricData(result.Get("min").Float(), "stat", "min"))
	metricsData = append(metricsData, newLabeledMetricData(result.Get("max").Float(), "stat", "max"))
	metricsData = append(metricsData, newLabeledMetricData(result.Get("avg").Float(), "stat", "avg"))

	return append(metricsData, newLabeledMetricData(result.Get("sum").Float(), "stat", "sum"))
}
