# Metrics and health

For default warn-level diagnostic logs and optional OTLP export, see the
[logging reference](logging.md). Logs do not depend on Prometheus being enabled.

Gateway has its own forwarding metrics and process readiness. Engine keeps the
existing execution metrics; Worker keeps its Kafka metrics. See the
[role-specific scaling and health contract](store-isolation.md#readiness-metrics-and-scaling).

Every role serves `/livez` and `/readyz` on its dedicated health listener,
configured by `health.address` (default `:8081`). Prometheus is independent: set
`prometheus.enabled: true` to start `/metrics` at `prometheus.address` (default
`:9090`). When disabled, no Prometheus listener starts. Neither HTTP health nor
Gateway/Engine gRPC health depends on this flag. The metrics port does not serve
health endpoints, and the health port does not serve metrics.

```yaml
health:
  address: ":8081"
prometheus:
  enabled: false
  address: ":9090"
```

The endpoint includes the standard Go runtime and process collectors plus these
Sink metrics:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `sink_build_info` | gauge | `version` | Build identity for the running Sink binary. |
| `sink_grpc_server_requests_total` | counter | `store`, `method`, `code` | Completed Sink gRPC requests by method and canonical gRPC status code. |
| `sink_grpc_server_request_duration_seconds` | histogram | `store`, `method` | End-to-end Sink gRPC request latency. |
| `sink_grpc_server_operation_results_total` | counter | `store`, `method`, `status` | Per-operation batch results and native Execute `succeeded`/`failed` outcomes, including database errors delivered over gRPC OK. |
| `sink_batcher_batches_total` | counter | `store`, `method`, `reason` | Synchronous batches dispatched by flush reason. |
| `sink_batcher_operations` | histogram | `store`, `method` | Operations represented by each dispatched batch. |
| `sink_batcher_bytes` | histogram | `store`, `method` | Original encoded request bytes represented by each dispatched batch. |
| `sink_batcher_queue_duration_seconds` | histogram | `store`, `method` | Time the oldest request waited before its batch started. |
| `sink_batcher_request_queue_duration_seconds` | histogram | `store`, `method` | Each queued RPC's residence until dispatch, cancellation, or shutdown; seven finite buckets through 30 seconds. |
| `sink_batcher_request_queue_exits_total` | counter | `store`, `method`, `outcome` | RPCs leaving the queue via execution, cancellation, or shutdown. |
| `sink_batcher_execution_duration_seconds` | histogram | `store`, `method` | Core service execution time for a dispatched batch. |
| `sink_batcher_queued_operations` | gauge | `store`, `method` | Operations currently waiting for dispatch. |
| `sink_batcher_queued_bytes` | gauge | `store`, `method` | Encoded request bytes currently waiting for dispatch. |
| `sink_batcher_rejected_total` | counter | `store`, `method`, `reason` | Requests rejected before dispatch, including queue exhaustion. |
| `sink_write_phase_duration_seconds` | histogram | `store`, `phase` | Synchronous core write admission, parsing, storage read, Lua, and storage write latency by store; writes distinguish applied/visible. |
| `sink_write_execution_rounds` | histogram | `store`, `phase` | Actual storage read/write calls per synchronous core write, including conflict retries, by store. |
| `sink_write_slow_phases_total` | counter | `store`, `phase` | Phase observations exceeding 5 seconds, by store and phase. |
| `sink_merge_conflicts_total` | counter | `store` | Revision conflicts retried by Lua merge operations. |
| `sink_merge_folded_chains_total` | counter | `store` | Multi-operation merge runs planned for one conditional commit, excluding retries. |
| `sink_merge_folded_operations_total` | counter | `store` | Logical operations in those runs, excluding retries; not a successful commit count. |
| `sink_merge_exhausted_total` | counter | `store` | Lua merges that exhausted the configured revision-conflict attempt budget. |
| `sink_kafka_publisher_records_total` | counter | `store`, `status` | Mutation records accepted or rejected by Kafka. |
| `sink_kafka_publisher_duration_seconds` | histogram | `store` | Synchronous Kafka publish batch latency. |
| `sink_kafka_worker_mutations_total` | counter | `store`, `status` | Mutations applied or failed by workers. |
| `sink_kafka_worker_retries_total` | counter | `store` | Retried Kafka mutations. |
| `sink_kafka_worker_dead_letters_total` | counter | `store` | Mutations copied to the dead-letter topic. |
| `sink_in_flight_requests` | gauge | none | Executing core calls in this process. |
| `sink_in_flight_bytes` | gauge | none | Request/output reservations; not RSS. |
| `sink_admission_rejected_total` | counter | none | Process execution admission rejections. |
| `sink_admission_pool_requests` | gauge | `store`, `pool` | Executing requests in the independent `execution` or `publish` pool. |
| `sink_admission_pool_bytes` | gauge | `store`, `pool` | Bytes reserved in each independent pool. The legacy in-flight gauges report their sum. |
| `sink_admission_pool_rejected_total` | counter | `store`, `pool`, `reason` | Rejections from request/scan slots (`requests`), global bytes (`bytes`), an older byte waiter (`fairness`), a full direct/Scan waiting queue (`queue`), admission wait expiry (`wait_timeout`), or write reservation growth (`resize`). |
| `sink_execution_queued_requests` | gauge | `store` | Direct synchronous RPCs waiting for admission, separate from batching and Scan queues. |
| `sink_execution_queued_bytes` | gauge | `store` | Input and bookkeeping bytes charged to the direct admission queue. |
| `sink_execution_admission_wait_duration_seconds` | histogram | `store` | Direct RPC queue time until admission, rejection or cancellation. |
| `sink_scan_queued_requests` | gauge | `store` | Scan pages waiting for execution admission. |
| `sink_scan_queued_bytes` | gauge | `store` | Conservative reservation bytes charged to the separate Scan waiting queue. |
| `sink_scan_admission_wait_duration_seconds` | histogram | `store` | Time queued Scan pages waited before admission, rejection or cancellation. |
| `sink_execution_store_bytes` | gauge | `store` | Execution bytes reserved by this process, attributed to its bound Store. |
| `sink_kafka_worker_last_poll_timestamp_seconds` | gauge | `store` | Last completed poll, not an idle-worker heartbeat. |
| `sink_kafka_worker_last_commit_timestamp_seconds` | gauge | `store` | Last successful offset commit. |
| `sink_kafka_worker_pending_records` | gauge | `store` | Unresolved records from the last fetch; excludes unpolled backlog. |
| `sink_kafka_worker_oldest_pending_timestamp_seconds` | gauge | `store` | Oldest timestamp in that pending fetch, zero after full settlement. |
| `sink_kafka_worker_recoveries_total` | counter | `store` | Processing rounds with retained source records. |
| `sink_kafka_worker_offset_gap` | gauge | `store` | Committed source offset expired; remains 1 until explicit recovery and restart. |
| `sink_kafka_worker_fetch_errors_total` | counter | `store` | Fetch failures including offset retention gaps. |
| `sink_kafka_worker_delivery_seconds` | histogram | `store` | Oldest fetched-record age at source commit, including quarantined outcomes. |
| `sink_kafka_worker_quarantined_total` | counter | `store` | Acknowledged DLQ publications, including replayed quarantine attempts. |

Engine and Worker Store labels use the single Store file `name` captured at startup.
Unknown or malformed request identities use bounded fallback labels instead of
creating arbitrary time series. Gateway forwarding metrics use the configured
route Store names; the Gateway splits cross-Store RPCs before Engine execution.
Kafka observations use the bound Store, including malformed or misrouted messages
sent to that Worker's DLQ. Admission pool metrics attribute reservations to the
bound Store. The `sink_in_flight_*` and `sink_admission_rejected_total` metrics
remain process totals across pools. Build info and standard Go/process collectors
also remain process-wide.

Labels exclude namespaces, datasets, record keys, and error messages to keep
metric cardinality bounded. The endpoint has no application-level authentication;
bind it to a private interface or protect it with the deployment network policy.

For example, operation throughput and Write request P99 by store across Pods:

```promql
sum by (store, method, status) (rate(sink_grpc_server_operation_results_total[5m]))

histogram_quantile(0.99,
  sum by (store, le) (rate(sink_grpc_server_request_duration_seconds_bucket{method="Write"}[5m]))
)
```

Filter any store-labelled metric with `{store="your-configured-store"}`. Existing
queries using `sum(...)` can still aggregate all stores; dashboard queries and
recording rules that should retain store attribution need `by (store, ...)`.
New label sets create new time series after an upgrade; rate windows need enough
new samples before they show results.

For slow-write diagnosis, the per-RPC queue histogram includes every queue exit,
including canceled and shutdown RPCs. It uses latency boundaries of 1 ms, 10 ms,
100 ms, 1 s, 5 s, 10 s, and 30 s. For example, the number of Write RPCs that left
the queue after more than ten seconds during a five-minute window:

```promql
sum by (store) (increase(sink_batcher_request_queue_duration_seconds_count{method="Write"}[5m]))
-
sum by (store) (increase(sink_batcher_request_queue_duration_seconds_bucket{method="Write",le="10"}[5m]))
```

Use `sink_batcher_request_queue_exits_total` separately for execution/cancellation/
shutdown counts. The compact histogram cannot split the latency distribution by
outcome or completion mode; it supports per-store distributions. The legacy
queue histogram still observes only the oldest request in each dispatched batch.

The write phase histogram uses the same seven finite latency buckets and only
six phase values: `admission`, `parse`, `storage_read`, `lua`,
`storage_write_applied`, and `storage_write_visible`. Non-write phases combine
applied and visible modes. Async acceptance uses the existing Kafka metrics and
is excluded from these synchronous write diagnostics. Overall latency remains
available from the existing RPC and batch-execution histograms.

Storage phase histograms observe each backend call; the round histogram counts
those calls per completed core execution, using boundaries 0, 1, 2, 4, 8, and 16.
Larger counts fall into `+Inf`; `_sum` still retains their full values. These
measurements describe core executions/calls, not individual original RPCs or
commits. Histograms and folding counters retain store attribution. Folding
counters count logical chains/operations; there is no extra chain-size histogram.
For example, slow phases by store:

```promql
sum by (store, phase) (increase(sink_write_slow_phases_total[5m]))
```

A counter increment represents one phase observation strictly longer than five
seconds, not one slow RPC; a core call can contribute several observations.
`storage_write_visible` includes `refresh=wait_for` inside the backend request
and does not isolate refresh from storage processing. Compare applied/visible
writes, round counts, conflict counters, and backend statistics before
attributing a slow batch to refresh. Several short phases or retries can also
produce a slow overall RPC without incrementing the slow-phase counter.

The series budget for these write diagnostics is **123 × S + 168 per Pod**, where
S is the configured store count (one per Engine or Worker). This includes both fixed fallback labels for write phases/rounds,
all possible phase/outcome combinations, `+Inf`, `_sum`, and `_count`. The
breakdown is 30 queue-histogram series and 9 queue-exit counters per configured
store, plus 60 phase-histogram series, 18 round-histogram series, and 6 slow-phase
counters per store/fallback. Queues exist only for configured stores. For six single-Store Engine Pods, the upper bound is **1,746 series** for these diagnostics.
Series are created on observation. Other metrics and historical Pod churn are
outside this budget. A regression test exports both Prometheus text and
OpenMetrics with 1, 3, and 16 label values to enforce the collector budget; it also checks that
unknown store/phase/method/outcome values cannot create unbounded dimensions.

The default standard gRPC health service and `/readyz` report process readiness
for Gateway and Engine. They stay ready during a downstream dependency failure.
The Gateway does not proxy dependency health. On each Engine, dependency status is
available under `sink.storage.<store>` and, when enabled, `sink.kafka.<store>`.
Dependency-specific gRPC health begins as `NOT_SERVING` and is refreshed every
five seconds with a three-second timeout. HTTP
`/readyz?service=sink.storage.<store>` (or the Kafka service name) checks that
specific capability. Worker `/readyz` checks its database, Kafka, and consumer;
`/readyz?service=sink.worker.<store>` selects consumer readiness.
`/livez` reports process liveness independently of dependencies. Keep liveness
independent from dependency readiness to avoid restart loops during an outage.

See the [configuration reference](configuration.md) for listener settings.

## Memory capacity and KEDA

The CLI's shared allocator replaces speculative execution/forwarding admission.
Use these metrics for the new policy; legacy execution/Scan/publishing and
Gateway reservation gauges describe the old allocator and must not drive new
capacity decisions. Existing RPC, backend, Kafka and batch queue metrics remain
useful. Every memory series has bounded `role` and `store` labels (Gateway's
store is empty).

| Metric | Type | Additional labels / meaning |
| --- | --- | --- |
| `sink_memory_capacity_bytes` | gauge | `pool=normal,burst`; sum is configured or detected managed capacity |
| `sink_memory_used_bytes` | gauge | `pool=normal,burst`; held input, result, copies and driver capacity, not RSS |
| `sink_memory_opaque_reserved_bytes` | gauge | Temporary driver allowance included in used bytes; do not add it again |
| `sink_memory_waiting_bytes` | gauge | `phase=request,response`; additional requested allocation bytes |
| `sink_memory_waiting_requests` | gauge | `phase`; waiting acquisitions, not a concurrency limit |
| `sink_memory_oldest_wait_seconds` | gauge | `phase`; zero when idle |
| `sink_memory_burst_borrowers` | gauge | Zero or one completion owner |
| `sink_memory_admitted_total` | counter | `phase`; successful acquisitions |
| `sink_memory_rejected_total` | counter | `phase`, `reason=busy,oversize,wait_timeout` |
| `sink_memory_wait_seconds` | histogram | `phase`, `outcome=admitted,canceled`; timed-out waits count as canceled |
| `sink_memory_capacity_source_info` | gauge | `source=configured,gomemlimit,cgroup,host,fallback` |

Waiting and rejection series include idle zeros. Request admission fails
immediately if full, so request wait gauges normally stay zero. Response wait
bytes represent outstanding demand; retries can contribute repeated acquisition
attempts to counters. Oversized allocations are permanent capacity failures and
should not trigger autoscaling through a rejection-rate metric.

The [KEDA example](../examples/autoscaling/memory-keda.yaml) uses two signals:
held plus pending bytes divided by ordinary fleet capacity, and the recent
fraction of temporary request refusals. Both use `metricType: Value` because they
are fleet ratios. Scope every selector to exactly one Deployment, role and, for
Engines/Workers, Store. Do not combine Gateway and Engine capacity or use raw
monotonic counters as scaling values. Keep Worker Kafka lag triggers as well.

Replace example job/namespace names with the actual scrape labels. The capacity
query rejects incomplete scrapes rather than interpreting missing capacity as
idle; `ignoreNullValues: "false"` makes missing data a scaler error. Maintain
nonzero synchronous replica floors and monitor Prometheus target health.
See KEDA's [Prometheus scaler](https://keda.sh/docs/2.18/scalers/prometheus/) and
[metric types](https://keda.sh/docs/2.18/reference/scaledobject-spec/).

Monitor `process_resident_memory_bytes` and Go heap metrics alongside these
series. Managed ownership is not GC reclamation or RSS: ingress decoding,
Kafka buffering, Lua and runtime objects require the automatic sizing headroom.
A high opaque-reservation share identifies MongoDB wire protection rather than
large logical response quotas.
