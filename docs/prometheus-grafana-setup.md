# Prometheus & Grafana Setup Guide

This guide explains how to set up Prometheus monitoring and Grafana dashboards for TripleStore.

## Prerequisites

- Prometheus server (v2.0+)
- Grafana (v8.0+)
- TripleStore application with Prometheus module enabled

## Exposing Metrics

### 1. Add Prometheus to Your Supervision Tree

```elixir
# In your application.ex or supervisor
children = [
  # ... other children
  {TripleStore.Prometheus, []}
]
```

### 2. Create a Metrics Endpoint

Using Plug:

```elixir
defmodule MyApp.MetricsPlug do
  import Plug.Conn

  def init(opts), do: opts

  def call(%{request_path: "/metrics"} = conn, _opts) do
    metrics = TripleStore.Prometheus.format()
    conn
    |> put_resp_content_type("text/plain; version=0.0.4")
    |> send_resp(200, metrics)
  end

  def call(conn, _opts), do: conn
end
```

Using Phoenix:

```elixir
# In your router.ex
get "/metrics", MetricsController, :index

# In your controller
defmodule MyAppWeb.MetricsController do
  use MyAppWeb, :controller

  def index(conn, _params) do
    metrics = TripleStore.Prometheus.format()
    conn
    |> put_resp_content_type("text/plain; version=0.0.4")
    |> text(metrics)
  end
end
```

### 3. Update Gauge Metrics Periodically

```elixir
# In a GenServer or periodic task
def handle_info(:update_gauges, state) do
  TripleStore.Prometheus.update_gauges(state.store)
  Process.send_after(self(), :update_gauges, 30_000)  # Every 30 seconds
  {:noreply, state}
end
```

## Prometheus Configuration

Add the TripleStore target to your `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'triple_store'
    static_configs:
      - targets: ['localhost:4000']  # Your app's metrics endpoint
    metrics_path: '/metrics'
    scrape_interval: 10s
```

## Available Metrics

### Counters

| Metric | Description |
|--------|-------------|
| `triple_store_query_total` | Total queries executed |
| `triple_store_query_errors_total` | Total query errors |
| `triple_store_insert_total` | Total insert operations |
| `triple_store_insert_triples_total` | Total triples inserted |
| `triple_store_delete_total` | Total delete operations |
| `triple_store_delete_triples_total` | Total triples deleted |
| `triple_store_load_total` | Total load operations |
| `triple_store_load_triples_total` | Total triples loaded |
| `triple_store_cache_hits_total{cache_type="..."}` | Cache hits by type |
| `triple_store_cache_misses_total{cache_type="..."}` | Cache misses by type |
| `triple_store_reasoning_total` | Total materialization operations |
| `triple_store_reasoning_iterations_total` | Total reasoning iterations |
| `triple_store_reasoning_derived_total` | Total derived facts |

### Histograms

| Metric | Description | Buckets (seconds) |
|--------|-------------|-------------------|
| `triple_store_query_duration_seconds` | Query duration | 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0 |
| `triple_store_reasoning_duration_seconds` | Reasoning duration | Same as above |

### Gauges

| Metric | Description |
|--------|-------------|
| `triple_store_triples` | Current triple count |
| `triple_store_memory_bytes` | Estimated memory usage |
| `triple_store_index_entries{index="..."}` | Entries per index |

## Grafana Dashboard

### Import Dashboard

1. Go to Grafana → Dashboards → Import
2. Paste the JSON from [grafana-dashboard.json](./grafana-dashboard.json)
3. Select your Prometheus data source
4. Click Import

### Dashboard Panels

The dashboard includes:

#### Overview Row
- **Triple Count**: Current number of triples
- **Memory Usage**: BEAM memory usage
- **Query Rate**: Queries per second
- **Cache Hit Rate**: Overall cache effectiveness

#### Query Performance Row
- **Query Latency (p50, p95, p99)**: Percentile latencies
- **Query Duration Distribution**: Histogram heatmap
- **Query Error Rate**: Errors per second

#### Throughput Row
- **Insert Rate**: Triples inserted per second
- **Delete Rate**: Triples deleted per second
- **Load Rate**: Bulk load throughput

#### Reasoning Row
- **Materialization Operations**: Count over time
- **Derived Facts Rate**: New facts per second
- **Reasoning Duration**: Time per materialization

#### Cache Row
- **Cache Hit Rate by Type**: Per-cache effectiveness
- **Cache Operations**: Hits vs misses over time

## Sample PromQL Queries

### Query Performance

```promql
# Query rate (queries per second)
rate(triple_store_query_total[5m])

# P95 query latency
histogram_quantile(0.95, rate(triple_store_query_duration_seconds_bucket[5m]))

# P99 query latency
histogram_quantile(0.99, rate(triple_store_query_duration_seconds_bucket[5m]))

# Query error rate
rate(triple_store_query_errors_total[5m])
```

### Cache Performance

```promql
# Overall cache hit rate
sum(rate(triple_store_cache_hits_total[5m])) /
(sum(rate(triple_store_cache_hits_total[5m])) + sum(rate(triple_store_cache_misses_total[5m])))

# Cache hit rate by type
rate(triple_store_cache_hits_total[5m]) /
(rate(triple_store_cache_hits_total[5m]) + rate(triple_store_cache_misses_total[5m]))
```

### Throughput

```promql
# Insert throughput (triples per second)
rate(triple_store_insert_triples_total[5m])

# Load throughput
rate(triple_store_load_triples_total[5m])

# Total write throughput
rate(triple_store_insert_triples_total[5m]) + rate(triple_store_load_triples_total[5m])
```

### Reasoning

```promql
# Reasoning duration average
rate(triple_store_reasoning_duration_seconds_sum[5m]) /
rate(triple_store_reasoning_duration_seconds_count[5m])

# Derived facts per second
rate(triple_store_reasoning_derived_total[5m])

# Average iterations per materialization
rate(triple_store_reasoning_iterations_total[5m]) /
rate(triple_store_reasoning_total[5m])
```

## Grafana Dashboard JSON

```json
{
  "annotations": {
    "list": []
  },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "liveNow": false,
  "panels": [
    {
      "collapsed": false,
      "gridPos": { "h": 1, "w": 24, "x": 0, "y": 0 },
      "id": 1,
      "panels": [],
      "title": "Overview",
      "type": "row"
    },
    {
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "thresholds" },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "green", "value": null }
            ]
          },
          "unit": "short"
        }
      },
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 1 },
      "id": 2,
      "options": {
        "colorMode": "value",
        "graphMode": "area",
        "justifyMode": "auto",
        "orientation": "auto",
        "reduceOptions": {
          "calcs": ["lastNotNull"],
          "fields": "",
          "values": false
        },
        "textMode": "auto"
      },
      "targets": [
        {
          "expr": "triple_store_triples",
          "legendFormat": "Triples"
        }
      ],
      "title": "Triple Count",
      "type": "stat"
    },
    {
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "thresholds" },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "green", "value": null },
              { "color": "yellow", "value": 1073741824 },
              { "color": "red", "value": 4294967296 }
            ]
          },
          "unit": "bytes"
        }
      },
      "gridPos": { "h": 4, "w": 6, "x": 6, "y": 1 },
      "id": 3,
      "options": {
        "colorMode": "value",
        "graphMode": "area",
        "justifyMode": "auto",
        "orientation": "auto",
        "reduceOptions": {
          "calcs": ["lastNotNull"],
          "fields": "",
          "values": false
        },
        "textMode": "auto"
      },
      "targets": [
        {
          "expr": "triple_store_memory_bytes",
          "legendFormat": "Memory"
        }
      ],
      "title": "Memory Usage",
      "type": "stat"
    },
    {
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": { "legend": false, "tooltip": false, "viz": false },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": { "type": "linear" },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": { "group": "A", "mode": "none" },
            "thresholdsStyle": { "mode": "off" }
          },
          "mappings": [],
          "unit": "ops"
        }
      },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 1 },
      "id": 4,
      "options": {
        "legend": { "calcs": ["mean", "max"], "displayMode": "table", "placement": "bottom" },
        "tooltip": { "mode": "multi", "sort": "none" }
      },
      "targets": [
        {
          "expr": "rate(triple_store_query_total[5m])",
          "legendFormat": "Query Rate"
        }
      ],
      "title": "Query Rate",
      "type": "timeseries"
    },
    {
      "collapsed": false,
      "gridPos": { "h": 1, "w": 24, "x": 0, "y": 9 },
      "id": 5,
      "panels": [],
      "title": "Query Performance",
      "type": "row"
    },
    {
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": { "legend": false, "tooltip": false, "viz": false },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": { "type": "linear" },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": { "group": "A", "mode": "none" },
            "thresholdsStyle": { "mode": "off" }
          },
          "mappings": [],
          "unit": "s"
        }
      },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 10 },
      "id": 6,
      "options": {
        "legend": { "calcs": ["mean", "max"], "displayMode": "table", "placement": "bottom" },
        "tooltip": { "mode": "multi", "sort": "none" }
      },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, rate(triple_store_query_duration_seconds_bucket[5m]))",
          "legendFormat": "p50"
        },
        {
          "expr": "histogram_quantile(0.95, rate(triple_store_query_duration_seconds_bucket[5m]))",
          "legendFormat": "p95"
        },
        {
          "expr": "histogram_quantile(0.99, rate(triple_store_query_duration_seconds_bucket[5m]))",
          "legendFormat": "p99"
        }
      ],
      "title": "Query Latency Percentiles",
      "type": "timeseries"
    },
    {
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": {
          "color": { "mode": "palette-classic" },
          "custom": {
            "axisCenteredZero": false,
            "axisColorMode": "text",
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": { "legend": false, "tooltip": false, "viz": false },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": { "type": "linear" },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": { "group": "A", "mode": "none" },
            "thresholdsStyle": { "mode": "off" }
          },
          "mappings": [],
          "unit": "percentunit"
        }
      },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 10 },
      "id": 7,
      "options": {
        "legend": { "calcs": [], "displayMode": "list", "placement": "bottom" },
        "tooltip": { "mode": "multi", "sort": "none" }
      },
      "targets": [
        {
          "expr": "sum(rate(triple_store_cache_hits_total[5m])) / (sum(rate(triple_store_cache_hits_total[5m])) + sum(rate(triple_store_cache_misses_total[5m])))",
          "legendFormat": "Hit Rate"
        }
      ],
      "title": "Cache Hit Rate",
      "type": "timeseries"
    }
  ],
  "refresh": "10s",
  "schemaVersion": 38,
  "style": "dark",
  "tags": ["triple-store", "rdf"],
  "templating": {
    "list": []
  },
  "time": { "from": "now-1h", "to": "now" },
  "timepicker": {},
  "timezone": "",
  "title": "TripleStore Dashboard",
  "uid": "triple-store",
  "version": 1,
  "weekStart": ""
}
```

## Alerting Rules

Create a file `alert_rules.yml` for Prometheus alerting:

```yaml
groups:
  - name: triple_store_alerts
    rules:
      # High query latency
      - alert: TripleStoreHighQueryLatency
        expr: histogram_quantile(0.95, rate(triple_store_query_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High query latency detected"
          description: "P95 query latency is {{ $value | printf \"%.2f\" }}s (threshold: 1s)"

      # Critical query latency
      - alert: TripleStoreCriticalQueryLatency
        expr: histogram_quantile(0.99, rate(triple_store_query_duration_seconds_bucket[5m])) > 5
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Critical query latency"
          description: "P99 query latency is {{ $value | printf \"%.2f\" }}s (threshold: 5s)"

      # High error rate
      - alert: TripleStoreHighErrorRate
        expr: rate(triple_store_query_errors_total[5m]) / rate(triple_store_query_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High query error rate"
          description: "Query error rate is {{ $value | printf \"%.2f\" }}% (threshold: 5%)"

      # Low cache hit rate
      - alert: TripleStoreLowCacheHitRate
        expr: |
          sum(rate(triple_store_cache_hits_total[5m])) /
          (sum(rate(triple_store_cache_hits_total[5m])) + sum(rate(triple_store_cache_misses_total[5m]))) < 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Low cache hit rate"
          description: "Cache hit rate is {{ $value | printf \"%.2f\" }}% (threshold: 50%)"

      # High memory usage
      - alert: TripleStoreHighMemory
        expr: triple_store_memory_bytes > 4294967296  # 4 GB
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
          description: "Memory usage is {{ $value | humanize1024 }}B (threshold: 4GB)"

      # Reasoning taking too long
      - alert: TripleStoreSlowReasoning
        expr: |
          rate(triple_store_reasoning_duration_seconds_sum[5m]) /
          rate(triple_store_reasoning_duration_seconds_count[5m]) > 30
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Slow reasoning operations"
          description: "Average reasoning duration is {{ $value | printf \"%.2f\" }}s (threshold: 30s)"

      # No queries (possible service issue)
      - alert: TripleStoreNoQueries
        expr: rate(triple_store_query_total[5m]) == 0
        for: 15m
        labels:
          severity: info
        annotations:
          summary: "No queries detected"
          description: "No queries have been executed in the last 15 minutes"

      # Rapid triple growth (possible data issue)
      - alert: TripleStoreRapidGrowth
        expr: deriv(triple_store_triples[1h]) > 100000
        for: 30m
        labels:
          severity: info
        annotations:
          summary: "Rapid triple count growth"
          description: "Triple count is growing at {{ $value | printf \"%.0f\" }} per hour"
```

### Adding Rules to Prometheus

Reference the rules file in `prometheus.yml`:

```yaml
rule_files:
  - "alert_rules.yml"
```

### Alertmanager Configuration

Example `alertmanager.yml` for Slack notifications:

```yaml
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

route:
  group_by: ['alertname']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 1h
  receiver: 'slack-notifications'

receivers:
  - name: 'slack-notifications'
    slack_configs:
      - channel: '#alerts'
        send_resolved: true
        title: '{{ .Status | toUpper }}: {{ .CommonLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'
```

## Next Steps

1. Import the dashboard JSON into Grafana
2. Configure alerting rules using the examples above
3. Set up notification channels (Slack, PagerDuty, etc.)
