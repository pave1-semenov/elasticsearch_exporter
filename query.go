package elastic_exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
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
	body   io.Reader
}

type columnType int
type columnTypeMap map[string]columnType

const (
	columnTypeKey   = 1
	columnTypeValue = 2
)

type elasticSearchQueryResponse struct {
	Hits responseHits `json:"hits"`
}

type responseHits struct {
	Total hitsTotal `json:"total"`
}

type hitsTotal struct {
	Value float64 `json:"value"`
}

type searchRequest struct {
	Query          searchQuery `json:"query"`
	TrackTotalHits bool        `json:"track_total_hits"`
}

type searchQuery struct {
	QueryString queryString `json:"query_string"`
}

type queryString struct {
	Query string `json:"query"`
}

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(logContext string, qc *config.QueryConfig, metricFamilies ...*MetricFamily) (*Query, errors.WithContext) {
	logContext = fmt.Sprintf("%s, query=%q", logContext, qc.Name)

	columnTypes := make(columnTypeMap)

	for _, mf := range metricFamilies {
		for _, kcol := range mf.config.KeyLabels {
			if err := setColumnType(logContext, kcol, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}
		for _, vcol := range mf.config.Values {
			if err := setColumnType(logContext, vcol, columnTypeValue, columnTypes); err != nil {
				return nil, err
			}
		}
	}

	q := Query{
		config:         qc,
		metricFamilies: metricFamilies,
		columnTypes:    columnTypes,
		logContext:     logContext,
	}
	return &q, nil
}

// setColumnType stores the provided type for a given column, checking for conflicts in the process.
func setColumnType(logContext, columnName string, ctype columnType, columnTypes columnTypeMap) errors.WithContext {
	previousType, found := columnTypes[columnName]
	if found {
		if previousType != ctype {
			return errors.Errorf(logContext, "column %q used both as key and value", columnName)
		}
	} else {
		columnTypes[columnName] = ctype
	}
	return nil
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
	mapped := make(map[string]interface{}, 1)
	mapped["hits"] = resp.Hits.Total.Value

	for _, mf := range q.metricFamilies {
		mf.Collect(mapped, ch)
	}
}

// run executes the query on the provided database, in the provided context.
func (q *Query) run(ctx context.Context, client *elasticsearch.Client) (*elasticSearchQueryResponse, errors.WithContext) {
	if q.body == nil {
		q.body = esutil.NewJSONReader(searchRequest{
			Query:          searchQuery{queryString{Query: q.config.Query}},
			TrackTotalHits: true,
		})
	}
	search := client.Search
	var response = elasticSearchQueryResponse{}

	result, err := search(search.WithBody(q.body), search.WithContext(ctx), search.WithTrackTotalHits(true))

	if result != nil && result.IsError() {
		err = errors.Wrapf(q.logContext, err, "Request failed with status code %d", result.StatusCode)
	}

	if err == nil && result != nil {
		defer result.Body.Close()
		err = json.NewDecoder(result.Body).Decode(&response)
	}

	return &response, errors.Wrap(q.logContext, err)
}
