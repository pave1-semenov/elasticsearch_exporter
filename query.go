package elastic_exporter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	log "github.com/golang/glog"
	"github.com/tidwall/gjson"
	"io"
	"iss.digital/mt/elastic_exporter/config"
	"iss.digital/mt/elastic_exporter/errors"
)

// Query wraps a elasticsearch query and all the metrics populated from it. It helps extract keys and values from result.
type Query struct {
	config              *config.QueryConfig
	metricFamilies      []*MetricFamily
	aggregationHandlers map[string]AggregationHandler
	logContext          string

	client *elasticsearch.Client
}

type searchRequest struct {
	Query searchQuery            `json:"query"`
	Aggs  map[string]interface{} `json:"aggs,omitempty"`
}

type searchQuery struct {
	QueryString queryString `json:"query_string"`
}

type queryString struct {
	Query string `json:"query"`
}

type metricData struct {
	*labelPair
	value float64
}

func (d metricData) hasLabels() bool {
	return d.labelPair != nil
}

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(logContext string, qc *config.QueryConfig, metricFamilies ...*MetricFamily) (*Query, errors.WithContext) {
	logContext = fmt.Sprintf("%s, query=%q", logContext, qc.Name)
	handlers := make(map[string]AggregationHandler, len(qc.Aggregations))
	for _, agg := range qc.Aggregations {
		if handler, err := NewForType(agg.Type(), agg.Name); err != nil {
			return nil, errors.Wrap(logContext, err)
		} else {
			handlers[agg.Name] = handler
		}
	}

	q := Query{
		config:              qc,
		metricFamilies:      metricFamilies,
		aggregationHandlers: handlers,
		logContext:          logContext,
	}
	return &q, nil
}

func (q *Query) Collect(ctx context.Context, client *elasticsearch.Client, ch chan<- Metric) {
	if ctx.Err() != nil {
		ch <- NewInvalidMetric(errors.Wrap(q.logContext, ctx.Err()))
		return
	}
	resp, err := q.run(ctx, client)
	if err != nil {
		ch <- NewInvalidMetric(err)
		return
	}

	aggregations := gjson.Get(resp, "aggregations").Map()

	metricsData := make([]metricData, 0, len(aggregations))
	total := gjson.Get(resp, "hits.total.value").Float()

	for name, aggregation := range aggregations {
		if handler, ok := q.aggregationHandlers[name]; !ok {
			log.Infof("handler for aggregation %s not found in query %s", name, q.config.Name)
		} else {
			metricsData = handler.Handle(aggregation, metricsData)
		}
	}

	for _, mf := range q.metricFamilies {
		mf.Collect(metricsData, total, ch)
	}
}

func newLabeledMetricData(value float64, labelKey string, labelValue string) metricData {
	return metricData{
		value: value,
		labelPair: &labelPair{
			key:   labelKey,
			value: labelValue,
		},
	}
}

func newMetricData(value float64) metricData {
	return metricData{
		value: value,
	}
}

// run executes the query on the provided database, in the provided context.
func (q *Query) run(ctx context.Context, client *elasticsearch.Client) (string, errors.WithContext) {
	req := searchRequest{
		Query: searchQuery{queryString{Query: q.config.Query}},
	}
	if len(q.config.Aggregations) > 0 {
		req.Aggs = make(map[string]interface{})
		for _, agg := range q.config.Aggregations {
			req.Aggs[agg.Name] = agg.ParsedBody
		}
	}
	query := esutil.NewJSONReader(req)
	search := client.Search
	var response string

	result, err := search(search.WithBody(query), search.WithContext(ctx), search.WithTrackTotalHits(true), search.WithSize(0))
	if result != nil && result.Body != nil {
		defer result.Body.Close()

		if result.IsError() {
			err = errors.Wrapf(q.logContext, err, "Request failed with status code %d", result.StatusCode)
		} else {
			response = q.read(result.Body)
		}
	}

	return response, errors.Wrap(q.logContext, err)
}

func (q *Query) read(r io.Reader) string {
	var b bytes.Buffer
	_, err := b.ReadFrom(r)
	if err != nil {
		return ""
	}
	return b.String()
}
