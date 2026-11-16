# PostHog Metrics Capture Service

Receives OTLP metrics (`/i/v1/metrics`) and Prometheus remote-write (`/i/v1/prometheus/write`) and writes them to Kafka.

The remote-write route accepts two body encodings. `Content-Encoding: snappy` (or no header) is the Prometheus remote-write v1 protocol. `Content-Encoding: zstd` is the VictoriaMetrics remote-write protocol, which `vmagent` sends by default. Both carry the same protobuf and produce the same rows.

This binary is the metrics half of [`capture-logs`](../capture-logs/README.md). It links the `capture-logs` library for parsing, authentication, and the Kafka sink, so metrics traffic can scale and deploy on its own. The base configuration, authentication, and response codes are the ones documented for `capture-logs`. The settings below are specific to this service.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| REMOTE_WRITE_MAX_EXPANDED_BYTES | 167772160 | Largest estimated row set (160 MiB) one remote-write request may expand to. A larger request gets a 400. Separate from `MAX_REQUEST_BODY_SIZE_BYTES` |
| REMOTE_WRITE_K8S_RESOURCE_ATTRIBUTE_TOKENS | (empty) | Comma-separated project tokens. Their remote-write series get `k8s.namespace.name`, `k8s.pod.name`, `k8s.node.name`, `k8s.container.name`, `k8s.deployment.name` and `k8s.daemonset.name` resource attributes copied from the matching Prometheus labels, the keys the OTel collector sets. The labels stay. This changes series fingerprints, so it is opt-in per project |
| REDIS_URL | (none) | Redis that shares the set of recently labelled metric series between pods. Unset means local cache only |
| PROMETHEUS_KEEP_STALE_MARKERS | false | Keep Prometheus stale markers (the NaN with bits `0x7ff0000000000002`) from remote-write as rows, so readers end the series when its target goes away. A stale `_sum` or `+Inf` bucket of a classic histogram becomes one stale native row. Every other NaN or infinity is still dropped. Turn it on only once every metrics reader skips the markers |
| METRICS_SERIES_LABEL_GATE_ENABLED | false | Strip label maps from repeat metric rows (see below). When false the gate only reports metrics |
| METRICS_SERIES_LABEL_INTERVAL_SECS | 1800 | Longest gap between two labelled rows of one metric series. Each series uses a window up to a quarter shorter, derived from its key |
| METRICS_SERIES_REDIS_TIMEOUT_MS | 1000 | Budget for one Redis write batch. A batch past it still runs in Redis; the budget is what slows the writer when Redis is slow |
| METRICS_SERIES_REDIS_SEED_TIMEOUT_MS | 15000 | Longest the service waits at startup for the cache seed. After that it starts with a partial cache |
| METRICS_SERIES_REDIS_PULL_TIMEOUT_MS | 5000 | Budget for one periodic pull from Redis |
| METRICS_SERIES_REDIS_PULL_INTERVAL_SECS | 60 | How often the local cache pulls new series from Redis |
| METRICS_SERIES_CACHE_MAX_ENTRIES | 20000000 | Most series one pod remembers, across all tokens |
| METRICS_SERIES_CACHE_MAX_ENTRIES_PER_TOKEN | 2000000 | Most series one pod remembers for one token |
| METRICS_LABEL_CARDINALITY_MODE | shadow | `off`, `shadow` or `enforce` (see below). Shadow finds and shares drop rules but changes no row |
| METRICS_LABEL_CARDINALITY_VALUE_LIMIT | 10000 | Distinct values one label key of one metric may have in a window before it is dropped |
| METRICS_LABEL_CARDINALITY_MIN_SHARE | 0.5 | Smallest ratio of a key's distinct values to the metric's distinct series for the key to be dropped |
| METRICS_LABEL_CARDINALITY_CANDIDATE_MIN_NEW_SERIES | 500 | New series one pod must see for a metric in a window before it measures that metric's labels |
| METRICS_LABEL_CARDINALITY_MAX_CANDIDATES | 2000 | Most metrics one pod measures at once |
| METRICS_LABEL_CARDINALITY_MAX_KEYS_PER_METRIC | 32 | Most label keys one pod measures for one metric |
| METRICS_LABEL_CARDINALITY_RULE_TTL_SECS | 7200 | How long a drop rule lasts after detection last saw its key over half the limit |
| METRICS_LABEL_CARDINALITY_PROTECTED_KEYS | le,quantile | Comma-separated label keys that are never dropped |

### Metric series label gate

Every metric row carries `series_fingerprint`. The label maps (`attributes`, `resource_attributes`) of a series go out on one row at least every `METRICS_SERIES_LABEL_INTERVAL_SECS`, with `has_labels = true`. Each series shortens that window by a key-derived offset of up to a quarter, so series first seen together do not go out again together, window after window, in one burst. All other rows of that series in the window have empty label maps and `has_labels = false`. The ClickHouse rollups build the series and attribute tables from labelled rows only.

The decision uses a local in-memory cache and never waits on Redis. Redis holds the set of series any pod has labelled: new series are pushed from a background task, and the cache pulls from Redis at startup and every `METRICS_SERIES_REDIS_PULL_INTERVAL_SECS`. Pulls read the set in pages of 50,000 members and merge each page as it arrives; the startup seed stops at `METRICS_SERIES_REDIS_SEED_TIMEOUT_MS` and keeps what it has. A periodic pull reads members scored since the previous pull started, less a minute for clock skew, and the next pull starts from there even when this one failed or ran out of its budget, so a slow pull never leaves later pulls re-reading old members. A pulled series this pod does not know is recorded as seen now. A pulled series this pod already knows moves forward to the start of the pulled range when its own timestamp is older, so pods share one labelling window per series instead of each relabelling every series once per window. A Redis failure only makes labels go out more often.

Pull metrics, all with a `kind` label of `seed` or `periodic`: `capture_metrics_series_redis_pulls` (with `outcome` = `ok`, `timeout`, or `error`), `capture_metrics_series_redis_pull_duration_seconds` (same labels), `capture_metrics_series_redis_pull_pages`, `capture_metrics_series_redis_pull_members_read`, `capture_metrics_series_redis_pull_bytes` (member payload, without protocol framing), `capture_metrics_series_redis_pulled` (members new to this pod), and `capture_metrics_series_redis_pull_refreshed` (members this pod knew, moved forward).

Push metrics: `capture_metrics_series_redis_pushed`, `capture_metrics_series_redis_push_failed` (with `outcome` = `timeout` or `error`), and `capture_metrics_series_redis_push_duration_seconds` (with `outcome` = `ok`, `timeout`, or `error`). `capture_metrics_series_redis_queue_full` counts series dropped because the writer's queue of 100,000 was full. The writer uses its own Redis connection, so a large pull page does not hold its batches back. After three flushes or pulls in a row time out, the writer or puller rebuilds its connection, because a half-open socket times out on every command until the kernel gives up on it.

The cache is bounded. When the global or the per-token cap is full, a new series keeps its labels on every row and is not cached or pushed to Redis, until pruning frees a slot. `capture_metrics_series_cache_full` counts those rows, with `cap` = `global` or `token` for the cap that was full. The `token` series also carries the project token that hit the cap, so the project is known without a log lookup. A `token` count with `capture_metrics_series_cache_size` well under the global cap means that token has more live series than `METRICS_SERIES_CACHE_MAX_ENTRIES_PER_TOKEN` allows.

### High cardinality label drop

The service finds label keys that make one metric grow into many series, and removes them before the fingerprint is final. All series that differ only by a dropped key become one series. Detection is per project token and per metric family; the `_bucket`, `_count` and `_sum` names of a classic histogram are one family, so a rule drops the key from all of them.

Detection has two stages. First, the series label gate reports the rows that are new to its cache, which is one row per series per window, and the service counts them per metric family. A family that passes `METRICS_LABEL_CARDINALITY_CANDIDATE_MIN_NEW_SERIES` in one window is measured: every later row of it goes into a HyperLogLog sketch of its series fingerprints and one sketch per label key (attributes and resource attributes separately). A key is flagged when its distinct values pass `METRICS_LABEL_CARDINALITY_VALUE_LIMIT` and are at least `METRICS_LABEL_CARDINALITY_MIN_SHARE` of the distinct series. The share test flags the key that causes the growth, such as `user_id`, and not every key of a large metric. The sketches restart each `METRICS_SERIES_LABEL_INTERVAL_SECS` window and have about 6.5% error.

A flagged key becomes a drop rule. Rules go to Redis (`capture-logs:metric-label-drop-rules:<hour>`, keyed by a hash of token and family, so no token is stored) and every pod pulls them each `METRICS_SERIES_REDIS_PULL_INTERVAL_SECS`. Detection reads rows before the drop, so it keeps measuring a dropped key. A rule is extended while its key stays over half the limit and expires `METRICS_LABEL_CARDINALITY_RULE_TTL_SECS` after that stops. The `posthog.original_timestamp` attributes and the protected keys are never flagged.

In `enforce` mode the service removes the key from the row and computes the fingerprint again from the remaining labels, with the same recipe as the row builders, so a collapsed row joins the series that a row without the key gets. Remote-write applies the drop after the classic histogram fold, so native histograms stay whole. Each changed row gets a `posthog.dropped_labels` attribute, with the sorted, comma-joined dropped keys as its value, so the metrics UI can show which series were combined. The fingerprint leaves the marker out. Shadow mode adds no marker. The billed bytes count the labels left after the drop and do not count the marker.

A collapsed series holds the samples of every original series. That is correct for delta sums and histograms. Cumulative counters and gauges of the original series are interleaved in one series, so a rate or last value over a collapsed cumulative series is not meaningful. The dropped key exists to stop a cardinality explosion, not to keep those series exact.

Metrics: `capture_metrics_label_cardinality_rules_created` (with `token` and `mode`), `capture_metrics_label_cardinality_rows_dropped` (with `mode`; in shadow mode the rows that would change), `capture_metrics_label_cardinality_candidates` and `capture_metrics_label_cardinality_rules` (gauges), `capture_metrics_label_cardinality_candidates_full`, `capture_metrics_label_cardinality_keys_full`, `capture_metrics_label_cardinality_redis_pushed` and `capture_metrics_label_cardinality_redis_pulls` (with `outcome`). Each new rule is also logged at warn level with the token, family, key and estimates.

## Classic Prometheus histograms on remote write

Prometheus sends a classic histogram as one `_bucket` series for each bucket. Each bucket has an `le` label. Prometheus also sends `_count` and `_sum` series.

The service converts these component samples to one native histogram row when a request contains `_sum` and the `+Inf` bucket for the same label set and timestamp. The native row has `histogram_bounds` and `histogram_counts`. Its series fingerprint is the label fingerprint of an equivalent OTLP histogram, combined with the bound set. The storage keeps one bound set for each series and hour, so a partial bucket set and the complete bucket set of one histogram get separate series. Queries combine them by label set.

Conversion requires cumulative bucket values that are non-negative integers and do not decrease. If `_count` is present, it must equal the `+Inf` bucket. The service does not convert a family that metadata defines as a summary, counter, or gauge. Other component samples stay as normal rows. This preserves data when a sender divides a histogram across requests. Queries must combine native and normal rows by label set.

The service converts `le` and `quantile` label values to Go's shortest float format on every row. For example, `1.0` becomes `1` and `1000000` becomes `1e+06`. The series fingerprint uses the converted label value.

Metrics: `capture_metrics_remote_write_histograms_assembled` counts native rows. `capture_metrics_remote_write_histogram_samples_folded` counts converted component samples. `capture_metrics_remote_write_histogram_components_passed_through` counts component samples that stay as normal rows.

## Running the service

```bash
cargo run --bin capture-apm-metrics
```

Local dev: `bin/start-rust-service capture-apm-metrics` (HTTP on `4321`, management on `3312`).
