# ElasticSearch Exporter

[Prometheus](https://prometheus.io) exporter for ElasticSearch based on Lucene queries.

Forked from and inspired by [sql_exporter](https://github.com/free/sql_exporter)

# Overview

ElasticSearch Exporter serves kinda the same purposes as [sql_exporter](https://github.com/free/sql_exporter) - feel free to read its documentation beforehand. 

Primary use case of this exporter is to collect metrics over your logs stored in ElasticSearch ([ELK stack](https://www.elastic.co/what-is/elk-stack)). Apart from well-known SQL queries used by [sql_exporter](https://github.com/free/sql_exporter) this exporter uses [Lucene queries](https://www.elastic.co/guide/en/kibana/current/lucene-query.html) and [Aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html) to gather your label-value pairs. This approach is kinda similar to [Kibana Query Language](https://www.elastic.co/guide/en/kibana/master/kuery-query.html) you might be using already to browse your logs.

# Why

Sometimes it can be tricky or even impossible to effectively expose metrics from your application's runtime. For example, you're using PHP as a programming language and your application's state is reset after every request, or you have a big distributed system without proper service discovery, so it's hard to gather metrics from all the nodes. 

This tool can cone in handy if you're collecting some structured logs with [ELK stack](https://www.elastic.co/what-is/elk-stack) (or EFK, w/e as soon as your data storage is ElasticSearch).

# Usage

Coming soon

# Configuration

Kinda similar to [sql_exporter](https://github.com/free/sql_exporter) apart from defining data sources and queries. Examples section covers those differences.

# Examples

Global configurations and data sources should be defined in ``elastic_exporter.yml`` like this:
```yaml
# Global settings and defaults.
global:
  # Subtracted from Prometheus' scrape_timeout to give us some headroom and prevent Prometheus from
  # timing out first.
  scrape_timeout_offset: 500ms
  # Minimum interval between collector runs: by default (0s) collectors are executed on every scrape.
  min_interval: 0s

# The target to monitor and the list of collectors to execute on it.
target:
  data_source_name:
    url: 'https://es-host.com:9200'
    username: elastic
    password: password

  # Collectors (referenced by name) to execute on the target.
  collectors: [http_calls_collector]

# Collector definition files.
collector_files: 
  - "*.collector.yml"
```

Consider you have logging enabled for all http calls made by your app to some external system:
```json
{"message": "some text", "service": "example", "@timestamp": 1653655853, "duration": 0.0005, "http_code": 200, "host": "https://example.com", "path": "endpoint", "method": "GET"}
```

Now you want to gather some fine metrics like total request count, average request duration, percentage of 5xx responses, etc. We define the rules in ``http_calls_collector.yaml``:

```yaml
# This collector will be referenced in the exporter configuration as `http_calls_collector`.
collector_name: http_calls_collector

# A Prometheus metric with (optional) additional labels, value and labels populated from one query.
metrics:
  - metric_name: codes
    help: pepeClap
    type: gauge
    query_ref: requests
    aggregation_ref: http_url
    track_total: true

  - metric_name: test
    help: accumulates request count by http codes
    type: gauge
    query: "service: example AND @timestamp:[now-5m TO now]"
    aggregation:
      name: 'http_code'
      type: terms
      field: 'http_code.keyword'
```

More coming soon

