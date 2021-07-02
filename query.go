package elastic_exporter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/tidwall/gjson"
	"io"
	"iss.digital/mt/elastic_exporter/config"
	"iss.digital/mt/elastic_exporter/errors"
)

// Query wraps a elasticsearch query and all the metrics populated from it. It helps extract keys and values from result.
type Query struct {
	config         *config.QueryConfig
	metricFamilies []*MetricFamily
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap
	logContext  string

	client *elasticsearch.Client
}

type columnType int
type columnTypeMap map[string]columnType

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

type resultMetric struct {
	labelPair
	value float64
}

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(logContext string, qc *config.QueryConfig, metricFamilies ...*MetricFamily) (*Query, errors.WithContext) {
	logContext = fmt.Sprintf("%s, query=%q", logContext, qc.Name)

	columnTypes := make(columnTypeMap)

	q := Query{
		config:         qc,
		metricFamilies: metricFamilies,
		columnTypes:    columnTypes,
		logContext:     logContext,
	}
	return &q, nil
}

// Collect is the equivalent of prometheus.Collector.Collect() but takes a context to run in and a database to run on.
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
	metrics := make([]resultMetric, 0, len(aggregations)+1)
	total := gjson.Get(resp, "hits.total.value").Float()
	metrics = append(metrics, resultMetric{
		value: total,
	})

	for name, aggregation := range aggregations {
		buckets := aggregation.Get("buckets")
		for _, data := range buckets.Array() {
			key := data.Get("key")
			value := data.Get("doc_count").Float()

			metrics = append(metrics, resultMetric{
				value: q.calculateMetricValue(total, value),
				labelPair: labelPair{
					key:   name,
					value: key.String(),
				},
			})
		}
	}

	for _, mf := range q.metricFamilies {
		mf.Collect(metrics, ch)
	}
}

func (q *Query) calculateMetricValue(total float64, value float64) float64 {
	var result float64
	switch q.config.Aggregation.Type {
	case config.AGGREGATION_TYPE_PERCENT:
		result = (value * 100) / total
	case config.AGGREGATION_TYPE_ABSOLUTE:
	default:
		result = value
	}

	return result
}

// run executes the query on the provided database, in the provided context.
func (q *Query) run(ctx context.Context, client *elasticsearch.Client) (string, errors.WithContext) {
	req := searchRequest{
		Query: searchQuery{queryString{Query: q.config.Query}},
	}
	if q.config.Aggregation != nil {
		req.Aggs = map[string]interface{}{
			q.config.Aggregation.Name: q.config.Aggregation.ParsedBody,
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
			response = read(result.Body)
		}
	}

	return response, errors.Wrap(q.logContext, err)
}

func read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
