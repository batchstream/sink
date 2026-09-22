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
| `sink_write_phase_duration_seconds` | histogram | `store`, `phase` | Synchronous core write parsing, storage read, Lua, and storage write latency by store; writes distinguish applied/visible. |
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
| `sink_in_flight_requests` | gauge | none | Admitted Engine RPCs still queued or executing. |
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
five phase values: `parse`, `storage_read`, `lua`,
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

The series budget for these write diagnostics is **112 × S + 146 per Pod**, where
S is the configured store count (one per Engine or Worker). This includes both fixed fallback labels for write phases/rounds,
all possible phase/outcome combinations, `+Inf`, `_sum`, and `_count`. The
breakdown is 30 queue-histogram series and 9 queue-exit counters per configured
store, plus 50 phase-histogram series, 18 round-histogram series, and 5 slow-phase
counters per store/fallback. Queues exist only for configured stores. For six single-Store Engine Pods, the upper bound is **1,548 series** for these diagnostics.
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

## Store backpressure

Engine and Worker expose their independent Store controller with bounded `role`
and configured `store` labels. Gateway has no Store controller. Health probes
and memory measurements do not feed these metrics or the congestion algorithm.

| Metric | Type | Additional labels / meaning |
| --- | --- | --- |
| `sink_store_concurrency_limit` | gauge | Current local execution window; zero pauses new dispatch |
| `sink_store_executions_in_flight` | gauge | Admitted sequential executions, including existing retries and cursor sends |
| `sink_store_cooldown_seconds` | gauge | Remaining pause before real traffic may probe recovery |
| `sink_store_window_changes_total` | counter | `reason=increase,latency,overload` |
| `sink_store_admissions_total` | counter | `outcome=admitted,rejected`; rejection counts direct admission, not queue polling |
| `sink_store_feedback_total` | counter | `method`, `signal=healthy,congested,ignored`; one sample per Store call |
| `sink_store_backend_duration_seconds_total` | counter | `method`; elapsed Store time excluding admission and downstream Emit waiting |
| `sink_store_latency_baseline_seconds` | gauge | `method`, `batch_size=1,2_32,33_128,129_plus`; zero before learning |

Methods are the fixed set `read`, `write`, `write_visible`, `delete`,
`delete_visible`, `execute`, `query`, `count`, `scan`. Opaque Execute commands
use success/error feedback only, so their latency baseline remains zero.
The collector creates **80 series per process**, independent of request paths,
document identities, query text, and error messages. Deployment/target labels
and historical Pod churn remain outside that count.

Compare backend execution time with `sink_batcher_request_queue_duration_seconds` to
distinguish backend slowdown from admission waiting. A mean Store duration is
`rate(sink_store_backend_duration_seconds_total[5m])` divided by
`sum without (signal) (rate(sink_store_feedback_total[5m]))`. These count interface
calls, not database commands or records. In-flight executions may temporarily
exceed the lowered window because already admitted work is never revoked.

Scaling application CPU, memory or consumer capacity does not raise the Store
window. Queue growth or Kafka lag alongside repeated window decreases indicates
database pressure, which additional replicas cannot resolve. Do not use replica
counts or an HPA recommendation to set database concurrency. See the
[design and verification](design/store-backpressure.md).

## Memory capacity and KEDA

Each process exports its watermark guard with bounded `role` and `store` labels
(Gateway's Store label is empty). Allocation/lease, burst reserve, opaque driver
allowance and waiting-allocation metrics have been removed.

| Metric | Type | Additional labels / meaning |
| --- | --- | --- |
| `sink_memory_limit_bytes` | gauge | Effective process ceiling |
| `sink_memory_used_bytes` | gauge | `source=rss,go_runtime`; observed process usage |
| `sink_memory_watermark_bytes` | gauge | `watermark=high,low`; reject and recovery thresholds |
| `sink_memory_pressure` | gauge | 1 while new work is blocked; otherwise 0 |
| `sink_memory_admitted_total` | counter | Requests admitted at Gateway/Engine entry |
| `sink_memory_rejected_total` | counter | Requests rejected before execution |
| `sink_memory_limit_source_info` | gauge | `source=configured,gomemlimit,cgroup,host,fallback` |

Worker uses the pressure gauge and Kafka lag; its poll pause is not a rejected RPC.
Usage is sampled at most once per 100 ms. It includes resident memory retained by
the runtime and does not return to zero when requests finish. The fallback Go
runtime measurement excludes some native allocations. See [memory policy](design/process-memory-admission.md).

The [KEDA example](../examples/autoscaling/memory-keda.yaml) uses observed usage
divided by the fleet's high watermarks, plus the recent fraction of refused RPCs.
Both use `metricType: Value` for fleet ratios. Scope selectors to one Deployment,
role and Store. Keep Worker Kafka lag triggers. Missing scrapes are scaler errors,
not zero pressure; retain nonzero replica floors and monitor target health.

Monitor `process_resident_memory_bytes` and Go heap metrics alongside these signals.
Watermarks reduce overload but do not reserve memory for already admitted work.
