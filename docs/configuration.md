# Server configuration

Sink reads all server runtime parameters from one YAML file. Pass its path when
starting the binary:

```shell
sink --config /etc/sink/config.yaml
```

The `--config` option is required for every server and worker runtime mode.
`sink version` does not load a configuration file. `sink lua test` does not
require one, but accepts `--config` to apply the same `service.lua` resource
limits without opening any configured backend. Environment variables such as
`SINK_MODE` and `SINK_MONGODB_URI` are not read by the server.

Configuration is loaded once during startup. Unknown fields, malformed YAML,
multiple YAML documents, duplicate storage names, invalid positive-integer
values, and incompatible option combinations prevent the process from
starting. Backend connections are lazy and dependency recovery is independent
per store. A Kafka store remains unavailable for publication and consumption
until its Topic policy has been reconciled and verified. This does not prevent
healthy stores from starting. Restart Sink after changing the file.

## Multiple storage instances

The required `storages` list can contain any combination of MongoDB,
Elasticsearch, and OpenSearch instances. Every entry has a unique `name`. A
request's `address.store` must exactly match that name:

```yaml
mode: server

grpc:
  address: ":8080"
  max_receive_message_bytes: 67108864
  max_send_message_bytes: 67108864

prometheus:
  address: ":9090"

storages:
  - name: mongo-main
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-main:27017
      metadata_field: __sink
      max_concurrent_writes: 64
      max_concurrent_groups: 16
    kafka:
      enabled: true
      brokers:
        - catalog-kafka-1:9092
        - catalog-kafka-2:9092
      topic: catalog-mutations
      group_id: catalog-sink-workers
      dead_letter_topic: catalog-mutations.dlq
      topic_partitions: 4
      topic_replication_factor: 2
      topic_retention_hours: 72
      max_poll_records: 500
      max_retry_attempts: 10
      retry_backoff_milliseconds: 100
      max_retry_backoff_milliseconds: 10000

  - name: mongo-archive
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-archive:27017

  - name: search-main
    driver: elasticsearch
    search:
      endpoints:
        - https://search-1:9200
        - https://search-2:9200
      api_key: replace-with-api-key
    kafka:
      enabled: true
      brokers:
        - search-kafka:9092
      topic: search-mutations
      group_id: search-sink-workers

service:
  max_operations: 1000
  max_merge_attempts: 3
  batching:
    max_wait_milliseconds: 2
    max_operations: 1000
    max_bytes: 16777216
    max_queued_operations: 10000
    max_queued_bytes: 134217728
  lua:
    timeout_milliseconds: 100
    max_source_bytes: 65536
    max_result_bytes: 16777216
    max_cached_programs: 256
    max_instructions: 1000000

shutdown_timeout_seconds: 15
```

Kafka is optional and disabled by default per store. In this example
`mongo-main` and `search-main` explicitly enable independent asynchronous paths
and may use unrelated Kafka clusters. `mongo-archive` has no `kafka` object;
setting `kafka.enabled: false` has the same runtime effect. Synchronous requests
still work while `RETURN_AFTER_ACCEPTED` operations targeting that store return
a retryable per-operation `UNAVAILABLE` failure. A mixed asynchronous batch can
therefore accept operations for enabled stores and reject only disabled stores
without losing the original result order.

The repository's [`config.example.yaml`](../config.example.yaml) is a smaller,
ready-to-edit configuration containing one MongoDB instance.

## Address routing

Sink does not use a separate bindings configuration. The client-provided record
address selects both the configured storage instance and the location inside
that instance:

| Address field | MongoDB | Elasticsearch and OpenSearch |
| --- | --- | --- |
| `store` | Exact `storages[].name` to use | Exact `storages[].name` to use |
| `namespace` | Database name | Logical business namespace; not used to construct the index name |
| `dataset` | Collection name | Complete existing index or alias name |
| `key` | MongoDB `_id` | Document `_id` |

For example, this address selects the `mongo-main` configuration and stores the
document in MongoDB database `catalog`, collection `products`:

```text
store = mongo-main
namespace = catalog
dataset = products
key = product-123
```

With a search driver, set `dataset` to the full name already used by the service.
For example, `namespace = catalog` and `dataset = legacy-products-v2` access the
index or alias `legacy-products-v2`; Sink does not prepend the namespace. Sink
can route operations in one batch to different storage instances and returns
results in the original operation order. An address whose `store` is not
configured receives a per-operation failure.

For search drivers, namespace is not part of the physical or ordering identity:
two addresses with the same store, dataset and typed key reach the same document
even when their namespaces differ. Do not configure multiple index aliases that
can address the same document under different dataset names when ordering matters.

## Synchronous request batching

In `server` and `all` modes, Sink coalesces concurrent one-operation RPCs into
bounded, process-local batches for each configured store. This batching layer
is always active; every single-store `Read` uses this path. `Write` and
`Delete` use it for `WAIT_UNTIL_APPLIED` and `WAIT_UNTIL_VISIBLE`;
`RETURN_AFTER_ACCEPTED` bypasses it because Kafka already batches asynchronous
mutations. Read, write, and delete have independent queues within each store.
A slow batch therefore does not block another method or another store.

`service.batching` configures batch and queue limits; batching cannot be disabled.
Remove the former `service.batching.enabled` field from existing configurations.
The strict configuration parser rejects it as an unknown field for either value.

The first queued request starts `service.batching.max_wait_milliseconds`.
Collection stops when that timer expires or adding another request would cross
the operation or encoded-byte target. A single valid RPC larger than a batch
target still runs alone. Automatic mutation batches combine only RPCs sharing
namespace, dataset, and completion mode. An explicit RPC spanning datasets
executes alone and keeps its original result boundary. This prevents an index's
refresh wait from entering an unrelated index's storage bulk through automatic
batching. `WAIT_UNTIL_APPLIED` is never promoted to `WAIT_UNTIL_VISIBLE`. A change of
completion mode for the same full record address creates an ordering barrier;
requests touching multiple records wait for all of their predecessors. Same-mode
Puts and Merges fold within a group; repeated Reads and Deletes execute once per
full address. Executions use their live callers' deadlines and cancellation signals.

Write/Delete dispatchers can collect and execute later batches while an earlier
batch waits for refresh. Each store/method has at most
`min(max_in_flight_requests, max_store_requests)` active batches. Record
dependencies cover both active and queued RPCs: an RPC touching several records
waits for every predecessor, while unrelated RPCs may pass it. Ordering does not
extend across methods, bypass requests, or server replicas. Queue budgets and
explicit RPC boundaries remain in force. Lua program declarations stay scoped to their original RPC.
Write results become available as each document chain finishes: an original RPC
returns once all of its own operations have final results. A committed Put need
not wait for an unrelated Merge read, nor a successful Merge for another
document's conflict retries. Conditional/Lua outcomes derived from speculative
state stay private until that chain commits or fails definitively. A later
execution error affects only unfinished RPCs; it cannot replace a returned
success. This does not make a multi-operation RPC transactional.

A document becomes eligible for subsequent queued writes once every selected
caller touching it has finished its remaining operations for that address,
even if their RPCs still contain other unfinished documents. Caller cancellation
alone does not release an executing document. Backend bulk calls still return
together; Sink cannot acknowledge an item whose backend result is not yet known.
Execution slots and byte reservations remain held until the owning execution
ends, so early completion cannot bypass admission or memory limits.

An explicit request containing operations for multiple stores bypasses the
micro-batch queues and goes directly to the storage router, which already
executes store groups concurrently. This avoids splitting one RPC into partial
queue admissions with ambiguous failure semantics.

Queue operation and byte limits apply separately to every configured store and
bound memory during a storage slowdown. One store cannot consume another
store's queue allowance; the process-wide maximum is the per-store limit
multiplied by the fixed number of configured stores and the three methods. A
new single-store request that would cross its queue's limit fails with gRPC
`RESOURCE_EXHAUSTED` and is not applied. Requests canceled before dispatch are
omitted. Once a batch is dispatched, other live callers in that batch continue
even if one caller cancels. Once all callers cancel, execution is cancelled too.
Execution is capped by the server request timeout even without caller deadlines.
Dispatched micro-batches wait for shared execution capacity within their
existing deadlines; bypass requests retain immediate admission rejection.
Asynchronous Write and Delete use an independent publishing pool with
`max_publish_requests` and `max_publish_bytes`. Synchronous snapshot reservations,
store saturation, and fair byte waiters cannot block Kafka publishing. A full
publishing pool rejects before enqueueing, and acceptance still requires the
publisher's durable acknowledgement. Store limits apply independently in each
pool. The total execution reservation bound is the sum of both byte limits;
producer buffers, batching queues, and VM/driver overhead remain additional.
Each coalesced RPC has its own read, conditional snapshot, and output budgets.
A shared snapshot is fetched if any interested RPC has room, and every response
copy is charged to its original RPC. Conditional chains evaluate each RPC's
budget before incorporating its state into the next caller's operations.
Batches are split at RPC boundaries when worst-case byte reservations would
exceed `max_in_flight_bytes`. Reads reserve both snapshot and response space;
coalesced conditional writes stream through a shared bounded working set while
retaining each original RPC's snapshot and output quotas across chunks. A full
read chunk defers records without charging their caller quotas or consuming a
conflict attempt. Output chunks commit independently, and only actual conflicts
are retried. With the default 32 MiB read budget, the conditional working-set
reservation is at most 96 MiB per execution: one read chunk, one output batch,
and one candidate prepared before flushing that batch. A single retained record
needs 64 MiB. Encoded inputs, expanded Lua sources, and requested response
documents are reserved separately. VM and driver overhead are additional.

The default 256 MiB execution cap therefore permits larger micro-batches of
small conditional writes without reducing the maximum valid document size.
Read reservations scale with original RPC count. Returned-write response space
is reserved only for the original RPCs requesting documents, even in mixed
batches. Direct calls keep their existing snapshot/output reservation and quotas.
Core admission limits also cover requests that bypass batching. Graceful shutdown first drains active gRPC calls,
then stops every store's batch dispatchers.

Batching happens only among requests for the same store reaching the same Sink
process. More pods increase aggregate queue and storage concurrency, but they
do not share a batcher. Explicit client-side batches still remove gRPC framing,
scheduling, and serialization overhead and are therefore more efficient when
the caller already has several records available.

## Prometheus metrics

Set `prometheus.address` to open a separate HTTP listener. Prometheus metrics
are served at the fixed `/metrics` path in `server`, `worker`, and `all` modes.
Omit the address or set it to an empty string to disable the listener.

```yaml
prometheus:
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
| `sink_in_flight_requests` | gauge | none | Executing core calls across all routes. |
| `sink_in_flight_bytes` | gauge | none | Request/output reservations; not RSS. |
| `sink_admission_rejected_total` | counter | none | Global/per-store execution admission rejections. |
| `sink_admission_pool_requests` | gauge | `store`, `pool` | Executing requests in the independent `execution` or `publish` pool. |
| `sink_admission_pool_bytes` | gauge | `store`, `pool` | Bytes reserved in each independent pool. The legacy in-flight gauges report their sum. |
| `sink_admission_pool_rejected_total` | counter | `store`, `pool`, `reason` | Rejections from request/store/scan slots (`requests`), byte limits (`bytes`), an older byte waiter (`fairness`), a full Scan waiting queue (`queue`), or Scan admission wait expiry (`wait_timeout`). |
| `sink_scan_queued_requests` | gauge | `store` | Scan pages waiting for execution admission. |
| `sink_scan_queued_bytes` | gauge | `store` | Conservative reservation bytes charged to the separate Scan waiting queue. |
| `sink_scan_admission_wait_duration_seconds` | histogram | `store` | Time queued Scan pages waited before admission, rejection or cancellation. |
| `sink_execution_store_bytes` | gauge | `store` | Current execution bytes charged to each configured store. Cross-store requests charge each touched store, so this sum can exceed the global pool gauge. |
| `sink_kafka_worker_last_poll_timestamp_seconds` | gauge | `store` | Last completed poll, not an idle-worker heartbeat. |
| `sink_kafka_worker_last_commit_timestamp_seconds` | gauge | `store` | Last successful offset commit. |
| `sink_kafka_worker_pending_records` | gauge | `store` | Unresolved records from the last fetch; excludes unpolled backlog. |
| `sink_kafka_worker_oldest_pending_timestamp_seconds` | gauge | `store` | Oldest timestamp in that pending fetch, zero after full settlement. |
| `sink_kafka_worker_recoveries_total` | counter | `store` | Processing rounds with retained source records. |
| `sink_kafka_worker_offset_gap` | gauge | `store` | Committed source offset expired; remains 1 until explicit recovery and restart. |
| `sink_kafka_worker_fetch_errors_total` | counter | `store` | Fetch failures including offset retention gaps. |
| `sink_kafka_worker_delivery_seconds` | histogram | `store` | Oldest fetched-record age at source commit, including quarantined outcomes. |
| `sink_kafka_worker_quarantined_total` | counter | `store` | Acknowledged DLQ publications, including replayed quarantine attempts. |

Store labels use `storages[].name` captured at startup. Each request counter and
latency observation is attributed once: requests targeting one configured store
use its name, mixed-store requests use `_multiple`, and empty or unknown stores
use `_unconfigured`. Per-operation result counters use each original operation's
store, including failures in mixed-store batches. Native Execute, Query, Count,
and Scan use `command.store`. Kafka publisher and worker observations use their
configured store, including malformed or misrouted messages sent to a worker's
DLQ. Batch queues use the store owning the queue. Merge counters use the record's
store; write phases and execution rounds use the core request's store or fallback.

Admission pool metrics include `store`; mixed-store requests and their byte
reservations count once under `_multiple`. The global `sink_in_flight_*` and
`sink_admission_rejected_total` metrics remain totals across stores and pools.
Build info and standard Go/process collectors remain process-wide.

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
S is the configured store count. This includes both fixed fallback labels for write phases/rounds,
all possible phase/outcome combinations, `+Inf`, `_sum`, and `_count`. The
breakdown is 30 queue-histogram series and 9 queue-exit counters per configured
store, plus 60 phase-histogram series, 18 round-histogram series, and 6 slow-phase
counters per store/fallback. Queues exist only for configured stores. With three
stores and six Pods, the upper bound is **3,222 series** for these diagnostics.
Series are created on observation. Other metrics and historical Pod churn are
outside this budget. A regression test exports both Prometheus text and
OpenMetrics with 1, 3, and 16 stores to enforce the budget; it also checks that
unknown store/phase/method/outcome values cannot create unbounded dimensions.

The default standard gRPC health service is the process readiness signal for
`server` and `all` modes. It remains `SERVING` during a runtime failure of one
store dependency, allowing unrelated stores to continue serving traffic. Sink
checks dependencies every five seconds with a three-second timeout and exposes
their status under `sink.storage.<store>` and, when Kafka is enabled,
`sink.kafka.<store>`. A dependency-specific service reports `NOT_SERVING` until
that dependency recovers. Dependency-specific health begins as `NOT_SERVING`.
When Prometheus is enabled, `/livez` reports process liveness and `/readyz` checks
all configured dependencies, including workers. Use
`/readyz?service=sink.worker.<store>` for one worker or the existing storage/Kafka
service name for one dependency. Keep liveness independent from dependency
readiness to avoid restart loops during an outage.

### Migrating a single-storage configuration

The former top-level `storage` object is replaced by the `storages` list. Move
the former `storage.mongodb.store` or `storage.search.store` value to the
entry's required `name`, keep the driver-specific connection fields under that
entry, rename `storage.mongodb.hidden_field` to
`storages[].mongodb.metadata_field`, and remove `bindings`. Update client
addresses so their namespace and dataset contain the direct MongoDB
database/collection names. For search drivers, keep the logical namespace and
put the complete existing index or alias name in `dataset`.

## Configuration reference

“Conditionally required” means a field is mandatory only in the modes or with
the drivers stated in its description. Enum values are case-sensitive and must
use the lowercase spelling shown below. Storage names are also case-sensitive.

| Field | Type | Required | Default | Allowed values | Function |
| --- | --- | --- | --- | --- | --- |
| `mode` | enum string | No | `server` | `server`, `worker`, `all` | Process role. See [Mode values](#mode-values). |
| `grpc.address` | string | No | `:8080` | Any valid TCP listen address | TCP listen address for the gRPC and gRPC health services. Used in `server` and `all` modes. |
| `grpc.max_receive_message_bytes` | positive integer | No | `67108864` | Integer greater than `0` | Maximum encoded gRPC request size accepted by the server. |
| `grpc.max_send_message_bytes` | positive integer | No | `67108864` | Integer greater than `0` | Maximum encoded gRPC response size sent by the server. |
| `prometheus.address` | string | No | empty (disabled) | Empty or any valid TCP listen address | HTTP listen address for Prometheus `/metrics`. Available in every runtime mode. |
| `storages` | list | Yes | none | One or more storage objects | Storage instances available for address routing. |
| `storages[].name` | string | Yes | none | Any unique, non-empty name | Exact value selected by `address.store`. |
| `storages[].driver` | enum string | Yes | none | `mongodb`, `elasticsearch`, `opensearch` | Adapter used by this storage instance. See [Storage driver values](#storage-driver-values). |
| `storages[].mongodb.uri` | string | Conditionally | none | Valid MongoDB connection string | Required when the entry's driver is `mongodb`. |
| `storages[].mongodb.metadata_field` | string | No | `__sink` | Any valid MongoDB field except `_id`; cannot contain `.`, `$`, or a null byte | Reserved top-level field where Sink stores internal metadata such as the record revision; removed from documents returned to clients. |
| `storages[].mongodb.max_concurrent_writes` | positive integer | No | `64` | Integer greater than `0` | Maximum concurrent MongoDB conditional writes. |
| `storages[].mongodb.max_concurrent_groups` | positive integer | No | `16` | Integer greater than `0` | Maximum collection groups executed concurrently across all calls to one store. |
| `storages[].search.endpoints` | list of strings | Conditionally | none | One or more HTTP(S) endpoints | Required for `elasticsearch` and `opensearch`. |
| `storages[].search.username` | string | Conditionally | empty | Any username accepted by the search service | Basic-auth username. Must be configured together with `password`. |
| `storages[].search.password` | string | Conditionally | empty | Any password accepted by the search service | Basic-auth password. Must be configured together with `username`. |
| `storages[].search.api_key` | string | No | empty | Any API key accepted by the search service | API key used instead of basic authentication. |
| `service.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `service.max_merge_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum revision-conflict attempts for Merge and folded conditional Put chains. |
| `service.batching.max_wait_milliseconds` | positive integer | No | `2` | Integer greater than `0` | Maximum collection delay measured from the first request in a batch. |
| `service.batching.max_operations` | positive integer | No | `service.max_operations` | Integer from `1` through `service.max_operations` | Operation target for one automatically formed batch. |
| `service.batching.max_bytes` | positive integer | No | `16777216` | Integer greater than `0` | Encoded-byte target for one automatically formed batch; one larger valid RPC still runs alone. |
| `service.batching.max_queued_operations` | positive integer | No | max(`10000`, `service.max_operations`) | Integer at least `service.max_operations` and `service.batching.max_operations` | Maximum operations waiting in each store and method queue. |
| `service.batching.max_queued_bytes` | positive integer | No | max(`134217728`, `grpc.max_receive_message_bytes`) | Integer at least `grpc.max_receive_message_bytes` and `service.batching.max_bytes` | Maximum encoded request bytes waiting in each store and method queue. |
| `service.lua.timeout_milliseconds` | positive integer | No | `100` | Integer greater than `0` | Maximum wall-clock duration of one Lua execution, including document conversion and result encoding. CPU admission wait is bounded by the caller's context instead. |
| `service.lua.max_source_bytes` | positive integer | No | `65536` | Integer greater than `0` | Maximum Lua source size per merge operation. |
| `service.lua.max_result_bytes` | positive integer | No | `16777216` | Integer greater than `0` | Maximum input/current document and encoded result bytes; expanded output is also bounded before conversion. |
| `service.lua.max_cached_programs` | positive integer | No | `256` | Integer greater than `0` | Maximum compiled Lua programs retained in the process-local LRU cache. |
| `service.lua.max_instructions` | positive integer | No | `1000000` | Integer greater than `0` | Maximum VM instruction checkpoints per execution. |
| `storages[].kafka` | object | No | absent | A store-specific Kafka configuration | Holds the asynchronous delivery and Topic-management policy. Kafka remains disabled unless `enabled` is `true`. |
| `storages[].kafka.enabled` | boolean | No | `false` | `true`, `false` | Enables Kafka publication, consumption, and startup Topic reconciliation for this store. |
| `storages[].kafka.brokers` | list of strings | Conditionally | none | One or more Kafka bootstrap addresses | Required when Kafka is enabled. Each store may use a different Kafka cluster. |
| `storages[].kafka.topic` | string | Conditionally | none | Any valid Kafka topic name | Required when Kafka is enabled. The server publishes only mutations whose `address.store` selects this store. |
| `storages[].kafka.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Required for every Kafka-enabled store in `worker` and `all` modes; optional and unused in `server` mode. |
| `storages[].kafka.dead_letter_topic` | string | No | `<storages[].kafka.topic>.dlq` | Non-empty Kafka topic different from the source topic | Destination for malformed, cross-store, and permanent failures. Temporary failures remain in the source Topic. |
| `storages[].kafka.topic_partitions` | positive integer | No | `4` | Integer from `1` through `2147483647` | Required partition count for both Topics. Any mismatch gates this store; changes require an explicit drained migration. |
| `storages[].kafka.topic_replication_factor` | positive integer | No | `2` | Integer from `1` through `32767`, not exceeding available brokers | Required replica count for every partition of both Topics. Sink submits and waits for partition reassignment when it differs. |
| `storages[].kafka.topic_retention_hours` | positive integer | No | `72` (3 days) | Integer greater than `0` within Go duration range | Source Topic retention. DLQ retention is configured separately. |
| `storages[].kafka.max_poll_records` | positive integer | No | `500` | Integer greater than `0` | Maximum number of this store's mutations handled in one consumer fetch batch. |
| `storages[].kafka.max_retry_attempts` | positive integer | No | `10` | Integer greater than `0` | Maximum attempts per processing round. Temporary failures are retained and retried in later rounds. |
| `storages[].kafka.retry_backoff_milliseconds` | positive integer | No | `100` | Integer greater than `0` | Initial worker retry backoff before jitter for this store. |
| `storages[].kafka.max_retry_backoff_milliseconds` | positive integer | No | `10000` | Integer at least this store's initial backoff | Maximum worker retry backoff before jitter for this store. |
| `shutdown_timeout_seconds` | positive integer | No | `15` | Integer greater than `0` | Maximum graceful-shutdown time for gRPC and MongoDB disconnect operations. |

### Reliability limits

Each Lua engine admits at most `GOMAXPROCS` executions concurrently, using the
value at engine startup. Chunk validation and merge execution share these CPU
slots. Waiting for a slot observes the caller's cancellation and deadline; the
Lua execution timeout starts after admission and still covers input conversion,
the script, and output encoding. This bounds active interpreter work without
allowing a queued request to outlive its caller.

All settings in this table are optional; values are positive integers. Limits are process-local; replica
counts multiply capacity. Configure the same Kafka policy on servers and workers.

| Setting | Default | Meaning |
| --- | --- | --- |
| `service.request_timeout_seconds` | `30` | Unary request timeout including batching queue wait and each Scan page; at most 300 seconds. A shorter caller deadline wins. |
| `service.max_in_flight_requests` | `128` | Storage execution request count, at most 10000, including cross-store calls. Asynchronous publishing uses its own pool. |
| `service.max_in_flight_bytes` | `268435456` | Admitted request/output reservation bytes, at most 16 GiB. Reads reserve snapshot and response budgets; Merge and folded conditional Put chains reserve current and output budgets; Lua source expansion and bounded per-operation failure responses are charged. This is not an RSS or VM heap limit. |
| `service.max_publish_requests` | `32` | Concurrent asynchronous Write/Delete requests, at most 10000, independent of storage execution. |
| `service.max_publish_bytes` | `268435456` | Asynchronous request, expanded-source, and bounded failure-response reservations, at most 16 GiB, additional to `max_in_flight_bytes`. Kafka producer buffers are additional. |
| `service.max_store_requests` | `32` | Requests per configured store in each admission pool independently, at most 10000. |
| `service.store_execution_bytes` | empty map | Optional execution byte ceiling per configured store, e.g. `{search: 805306368}` under a 1 GiB global pool. Each value is positive and no greater than `max_in_flight_bytes`; omitted stores share the global limit. Does not limit the publish pool. |
| `service.max_scan_requests` | half `max_in_flight_requests`, at least 1 | Scan-only request sublimit, no greater than the total request limit. |
| `service.max_scan_bytes` | half `max_in_flight_bytes`, at least 1 | Scan-only byte sublimit; global byte admission still applies. BSON Scan reserves 48 MiB driver wire space plus page copies. |
| `service.max_store_scan_requests` | half `max_store_requests`, at least 1 | Per-store Scan sublimit, no greater than the total per-store request limit. |
| `service.scan_admission_wait_milliseconds` | min(`2000`, request timeout in milliseconds) | Maximum Scan admission wait, included in the page deadline. Cannot exceed `request_timeout_seconds`. |
| `service.max_read_bytes` | min(`33554432`, half gRPC send limit) | Per-original-RPC Read or returned-Write documents, conditional write snapshot/output per attempt, and native Execute response. Scan pages use the smaller of this limit and 4 MiB. Cannot exceed half the gRPC send limit. |
| `storages[].kafka.dead_letter_retention_hours` | `720` | Independent DLQ retention, 30 days; bounded by Go duration range. |
| `storages[].kafka.min_insync_replicas` | min(`2`, replication factor) | Minimum ISR, at most replication factor. Publishers require all ISR acknowledgements. |
| `storages[].kafka.max_record_bytes` | `921600` | Encoded mutation envelope plus key, including expanded Lua source; at most 64 MiB and no larger than the producer buffer. Topic/producer batch limits include an extra 16 KiB for framing and DLQ headers. Broker/replica fetch limits must also support increases. |
| `storages[].kafka.max_buffered_bytes` | `67108864` | Producer buffer capacity, at most 1 GiB. Full buffers return retryable resource exhaustion. |
| `storages[].kafka.processing_timeout_milliseconds` | `20000` | Backend work per fetched batch, at most 20 seconds, followed by at most 5 seconds of offset/DLQ settlement. |

Native Execute and Scan share process/store request admission and reserve input
plus response/page buffers. MongoDB native calls additionally reserve 48 MiB for
the driver's complete wire response, which arrives before the smaller Sink
response/page limit can be enforced. Returned writes reserve an additional
response budget per original RPC before execution. See [native access](native-access.md)
for stateless Scan checkpoints, page-local cleanup, cancellation and retry semantics.

Scan waits fairly for execution capacity for at most
`service.scan_admission_wait_milliseconds`. Its separate waiting queue is bounded
by `max_scan_requests`, `max_scan_bytes`, and `max_store_scan_requests`; these
limits apply independently to queued and executing pages. Queued pages are
charged the full conservative reservation, but do not occupy execution slots.
Cancellation and timeout remove their queue entries immediately. Requests that
cannot fit the total or Scan byte limit fail immediately without advertising a
retry. Temporary Scan admission failures carry the retry detail documented in
[native access](native-access.md#limits-deadlines-and-retries).

Conditional writes initially reserve their peak document working set. Once a
chunk's snapshots and final candidates are known, Sink reduces that reservation
to retained payload sizes plus copy allowances for the final storage call,
including `WAIT_UNTIL_VISIBLE`. Input, Lua-source and returned-document allowances
remain reserved. Before a later read or conflict retry it atomically restores the
peak allowance. Restoration never waits while holding documents: if global/store
capacity or an older runnable waiter prevents growth, only the unexecuted
operations receive retryable per-operation `RESOURCE_EXHAUSTED` failures. Earlier
successful operations retain their results. This is document accounting, not an
RSS limit; driver buffers, Lua heaps, transport buffers and garbage collection
still require process memory headroom. BSON Scan's wire allowance is unchanged.

Store byte ceilings isolate slow storage execution without changing completion
semantics. For example, on a 1 GiB global pool a 768 MiB ceiling for a search store
prevents that store from consuming the last 256 MiB alone:

```yaml
service:
  max_in_flight_bytes: 1073741824
  store_execution_bytes:
    pse-search: 805306368
```

The map keys must match configured store names. This example is a sizing starting
point, not an automatic production setting. Ceilings are hard caps; unused global
capacity is shared within them. Cross-store requests charge their complete
reservation to each touched store and once globally. A waiter blocked by its
store ceiling does not reserve free bytes from other stores. Micro-batches split
at the store ceiling, while an individual RPC that cannot fit fails immediately.
`sink_admission_pool_rejected_total` additionally reports `store_bytes` for
store-ceiling refusal and `resize` when a write cannot restore its working set.

MongoDB group concurrency is shared across concurrent calls. Sink sets
`w=majority` and `journal=true` on its client, overriding weaker URI concerns;
server selection is bounded to five seconds. Verify the deployment supports
these settings before upgrading. The service deadline bounds the whole request.

A response budget can yield partial results: an individual oversized document
is a permanent resource failure; exhaustion caused by other records in the same original RPC
is retryable. Other coalesced RPCs retain their own quotas. Retry only failed operations or reduce the batch. Successful
mutation results must not be retried without business idempotence.

### Mode values

| Value | Behavior |
| --- | --- |
| `server` | Opens the gRPC listener and processes API requests. Each Kafka-enabled store gets its own publisher; stores without Kafka remain synchronous-only. |
| `worker` | Creates one consumer for every Kafka-enabled store and applies mutations without opening the gRPC listener. At least one store must enable Kafka. |
| `all` | Runs the `server` role plus one consumer for every Kafka-enabled store in one process. At least one store must enable Kafka. |

### Storage driver values

| Value | Behavior | Required driver-specific configuration |
| --- | --- | --- |
| `mongodb` | Requires and stores BSON documents. | `storages[].mongodb.uri` |
| `elasticsearch` | Requires and stores JSON documents in Elasticsearch. | At least one `storages[].search.endpoints` entry |
| `opensearch` | Requires and stores JSON documents in OpenSearch. | At least one `storages[].search.endpoints` entry |

## Kafka mode combinations

- A `server` can mix Kafka-enabled and synchronous-only stores. An asynchronous
  operation for a synchronous-only store returns a retryable per-operation
  `UNAVAILABLE` failure; synchronous operations are unaffected.
- Every Kafka-enabled store owns its brokers, topic, group, dead-letter topic,
  Topic policy, poll limit, and retry policy. Different stores may use
  unrelated clusters.
- A `server` does not require `storages[].kafka.group_id` because it only
  publishes. `worker` and `all` require a group ID for every Kafka-enabled
  store and require at least one such store.
- Topics, consumer groups, and dead-letter topics must be unique between stores
  that use the same normalized broker list. The same names may be reused on
  different Kafka clusters.

Before publishers or consumers start for a store, Sink reconciles and verifies
its source and DLQ policies. Missing Topics are created automatically. Partition
counts must match exactly. Replica changes use Kafka partition reassignment;
retention, `min.insync.replicas`, `cleanup.policy=delete`,
`unclean.leader.election.enable=false`, and `max.message.bytes` are reconciled.
The principal needs describe/create/alter/describe-config/alter-config rights.
Transient failures retry in the background while this store remains gated.
A committed offset outside source retention stops consumption of that partition
and keeps worker health failing; it is never silently reset to the latest offset.
See the [recovery runbook](reliability.md) before resetting positions.

Each consumer rejects a record whose embedded `address.store` does not match
the store that owns its source Topic. A source record is committed only after
it applies successfully or its original key and value are durably copied to
that store's dead-letter Topic with source Topic, partition, offset, and error
headers.

## Lua merge programs

Each write request declares each unique Lua source chunk once. Merge operations
reference it by SHA-256 digest; a raw protocol client may also embed full source
directly in an operation. The chunk must return exactly one function:

```lua
return function(current, incoming)
    current = current or json.object()
    sink.v1.object.replace_nonempty_string(current, incoming, "title")
    current.updated_at = sink.v1.time.now()
    return current
end
```

`current` is `nil` when the record does not exist; every Merge can create the
document returned by the Lua function.
`incoming` is the operation's incoming object. Current and incoming documents
must use the same encoding. The returned value must be an object and is encoded
as JSON or BSON to match the incoming document. The versioned `sink.v1`
helpers provide common operations and a stable merge observation time. See the
[Lua merge developer guide](lua-merge-guide.md) for the complete API, examples,
retry semantics, and testing checklist.

JSON null is represented by `json.null` instead of Lua `nil`, which would remove
a table key. The bridge preserves empty input objects and arrays. Use
`json.object()` or `json.array()` when creating an intentionally empty table in
Lua; an untagged empty `{}` is encoded as an object. `json.is_null(value)` tests
the null sentinel. Lua integers preserve signed 64-bit JSON integer values.

Sink opens deterministic base, string, table, math, and UTF-8 functionality.
Lua's standard `string.upper` is ASCII-only; `utf8.upper(value)` applies Go's
deterministic Unicode uppercase mapping when application data requires it.
Host I/O, operating-system, package loading, dynamic code loading, coroutines,
debug APIs, output, random numbers, metatable mutation, and unbounded string
repetition are unavailable. Each merge runs in a new VM with fresh global,
library, and metatable state. Immutable standard-library native functions are
initialized once per engine; JSON and `sink.v1` helpers are created for each
execution. Immutable compiled programs use a bounded process-local LRU cache
keyed by the computed SHA-256 digest; a supplied digest must match the source.

`sink.v1` provides validated array, object, and time helpers implemented by the
runtime. `sink.v1.time.now()` is fixed across revision-conflict retries of one
operation. The unrestricted Lua `time` and operating-system libraries remain
unavailable.

Wall-clock, instruction, call-depth, VM-stack, source-size, and result-size
limits protect the service. Call depth is fixed at 256 and VM stack slots at
65,536; the other limits are configurable above. The embedded runtime does not
provide a strict per-VM heap quota, so normal container or pod memory limits
remain required.
`string.pack`, `string.format`, `table.concat`, `string.gsub`, and `utf8.upper`
bound each intermediate result by `max_result_bytes`, checking before allocating
or appending strings.
`table.concat` and `table.move` also check cancellation and consume an instruction
checkpoint per element, including empty strings and nil values.
`utf8.upper` checks cancellation and consumes an instruction checkpoint every
1024 input characters and at completion, including when the text is unchanged.
This bounds those library calls, not cumulative allocations or the complete VM
heap.
The Go client automatically deduplicates identical programs within a synchronous
batch. Before publishing an asynchronous mutation, Sink expands the reference
so every Kafka record contains the full program and remains independently
replayable. This keeps Sink stateless with respect to customer rules. The source
is sent once per request but necessarily travels with every durable Kafka
mutation; transport-level compression can reduce its wire cost further.

## Handling credentials

The configuration can contain database URIs, passwords, or API keys. Do not
commit a production configuration file. Limit its filesystem permissions and
mount it read-only in the container. The example files contain placeholders or
local-development values only.
