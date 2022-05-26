package elastic_exporter

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"iss.digital/mt/elastic_exporter/config"
	"iss.digital/mt/elastic_exporter/errors"
)

const (
	// Capacity for the channel to collect metrics.
	capMetricChan = 1000

	upMetricName       = "up"
	upMetricHelp       = "1 if the target is reachable, or 0 if the scrape failed"
	scrapeDurationName = "scrape_duration_seconds"
	scrapeDurationHelp = "How long it took to scrape the target in seconds"
)

// Target collects ElasticSearch metrics from a single target. It aggregates one or more Collectors and it looks much
// like a prometheus.Collector, except its Collect() method takes a Context to run in.
type Target interface {
	// Collect is the equivalent of prometheus.Collector.Collect(), but takes a context to run in.
	Collect(ctx context.Context, ch chan<- Metric)
}

// target implements Target. It wraps a elasticsearch.Client, which is initially nil but never changes once instantianted.
type target struct {
	name               string
	dsn                string
	username           string
	password           string
	collectors         []Collector
	constLabels        prometheus.Labels
	globalConfig       *config.GlobalConfig
	upDesc             MetricDesc
	scrapeDurationDesc MetricDesc
	logContext         string

	client *elasticsearch.Client
}

// NewTarget returns a new Target with the given instance name, data source name, collectors and constant filters.
// An empty target name means the exporter is running in single target mode: no synthetic metrics will be exported.
func NewTarget(
	logContext, name, dsn, username, password string, ccs []*config.CollectorConfig, constLabels prometheus.Labels, gc *config.GlobalConfig) (
	Target, errors.WithContext) {

	if name != "" {
		logContext = fmt.Sprintf("%s, target=%q", logContext, name)
	}

	constLabelPairs := make([]*dto.LabelPair, 0, len(constLabels))
	for n, v := range constLabels {
		constLabelPairs = append(constLabelPairs, &dto.LabelPair{
			Name:  proto.String(n),
			Value: proto.String(v),
		})
	}
	sort.Sort(labelPairSorter(constLabelPairs))

	collectors := make([]Collector, 0, len(ccs))
	for _, cc := range ccs {
		c, err := NewCollector(logContext, cc, constLabelPairs)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, c)
	}

	upDesc := NewAutomaticMetricDesc(logContext, upMetricName, upMetricHelp, prometheus.GaugeValue, constLabelPairs)
	scrapeDurationDesc :=
		NewAutomaticMetricDesc(logContext, scrapeDurationName, scrapeDurationHelp, prometheus.GaugeValue, constLabelPairs)

	t := target{
		name:               name,
		dsn:                dsn,
		username:           username,
		password:           password,
		collectors:         collectors,
		constLabels:        constLabels,
		globalConfig:       gc,
		upDesc:             upDesc,
		scrapeDurationDesc: scrapeDurationDesc,
		logContext:         logContext,
	}

	return &t, nil
}

// Collect implements Target.
func (t *target) Collect(ctx context.Context, ch chan<- Metric) {
	var (
		scrapeStart = time.Now()
		targetUp    = true
	)

	err := t.ensureUp(ctx)
	if err != nil {
		ch <- NewInvalidMetric(errors.Wrap(t.logContext, err))
		targetUp = false
	}
	if t.name != "" {
		// Export the target's `up` metric as early as we know what it should be.
		ch <- NewMetric(t.upDesc, boolToFloat64(targetUp))
	}

	var wg sync.WaitGroup
	// Don't bother with the collectors if target is down.
	if targetUp {
		wg.Add(len(t.collectors))
		for _, c := range t.collectors {
			go func(collector Collector) {
				defer wg.Done()
				collector.Collect(ctx, t.client, ch)
			}(c)
		}
	}
	// Wait for all collectors (if any) to complete.
	wg.Wait()

	if t.name != "" {
		// And export a `scrape duration` metric once we're done scraping.
		ch <- NewMetric(t.scrapeDurationDesc, float64(time.Since(scrapeStart))*1e-9)
	}
}

func (t *target) ensureUp(ctx context.Context) errors.WithContext {
	if t.client == nil {
		cfg := elasticsearch.Config{
			Addresses: []string{
				t.dsn,
			},
			Username: t.username,
			Password: t.password,
		}
		client, err := elasticsearch.NewClient(cfg)
		if err != nil {
			if err != ctx.Err() {
				return errors.Wrap(t.logContext, err)
			}
			// if err == ctx.Err() fall through
		} else {
			t.client = client
		}
	}

	// If we have a handle and the context is not closed, test whether the cluster is up.
	if t.client != nil && ctx.Err() == nil {
		var err error

		health, err := t.client.Cluster.Health()

		if err != nil || health.IsError() {
			return errors.Wrap(t.logContext, err)
		}
	}

	if ctx.Err() != nil {
		return errors.Wrap(t.logContext, ctx.Err())
	}
	return nil
}

// boolToFloat64 converts a boolean flag to a float64 value (0.0 or 1.0).
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}
