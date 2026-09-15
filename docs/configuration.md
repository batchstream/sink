# Server configuration

Sink reads all server runtime parameters from one YAML file. Pass its path when
starting the binary:

```shell
sink --config /etc/sink/config.yaml
```

The `--config` option is required for every server and worker runtime mode.
`sink version` does not load a configuration file. `sink lua test` does not
require one, but accepts `--config` to apply the same `service.merge.lua` resource
limits without opening any configured backend. Environment variables such as
`SINK_MODE` and `SINK_MONGODB_URI` are not read by the server.

Use `sink config check --config FILE` to validate the schema and limits without
connecting to dependencies or printing configured values.

Configuration is loaded once during startup. Unknown fields, malformed YAML,
multiple YAML documents, duplicate storage names, invalid positive-integer
values, and incompatible option combinations prevent the process from
starting. Backend connections are lazy and dependency recovery is independent
per store. A Kafka store remains unavailable for publication and consumption
until its Topic policy has been reconciled and verified. This does not prevent
healthy stores from starting. Restart Sink after changing the file.

## Configuration layout

| Section | Responsibility |
| --- | --- |
| `mode`, `shutdown_timeout` | Process role and shutdown |
| `grpc`, `prometheus` | Transport and observability listeners |
| `storages[]` | Named backend, its execution byte ceiling, and its Kafka path |
| `service.request` | Request deadline, operation count, and returned document budget |
| `service.execution` | Storage execution capacity, with Scan sublimits |
| `service.publish` | Independent Kafka publication capacity |
| `service.batching` | Collection targets and waiting queue capacity |
| `service.merge` | Conflict attempts and Lua execution limits |

Kafka settings are grouped into `topic`, `producer`, `consumer`, and
`dead_letter`. Time values use Go duration strings (`100ms`, `30s`, `72h`);
numeric time values and old field names are rejected. Defaults are resolved once
before the application opens resources. Omitted values use defaults; explicit
zero or negative limits fail validation.

### Human-readable values

All byte limits accept sizes such as `64KiB`, `16MiB`, or `1GiB`. Binary units
`KiB`, `MiB`, `GiB`, and `TiB` use powers of 1024; decimal units `KB`, `MB`, `GB`,
and `TB` use powers of 1000. `B` means bytes. Units are case-sensitive: `1MB`
is 1,000,000 bytes, while `1MiB` is 1,048,576 bytes. A space between the number
and unit is optional. Fractions such as `1.5MiB` are accepted only when they
resolve to an exact whole number of bytes. Ambiguous units such as `M`, overflow,
and fractional bytes are rejected. Integer YAML values still mean raw bytes;
quoted unitless numbers are rejected.

Counts such as `max_requests` and `max_operations` remain integers. Time values
use duration strings such as `2ms`, `30s`, or `1m30s`. The `_bytes` field names
describe what the limit measures; they do not require writing raw byte counts.

This is a breaking YAML change. See the [migration guide](configuration-migration.md)
for the complete old-to-new mapping. The gRPC and SDK contracts are unchanged.

## Multiple storage instances

The required `storages` list can contain any combination of MongoDB,
Elasticsearch, and OpenSearch instances. Every entry has a unique `name`. A
request's `address.store` must exactly match that name:

```yaml
mode: server

grpc:
  address: ":8080"
  max_receive_message_bytes: 64MiB
  max_send_message_bytes: 64MiB

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
      topic:
        name: catalog-mutations
        partitions: 4
        replication_factor: 2
        retention: 72h
      consumer:
        group_id: catalog-sink-workers
        max_poll_records: 500
        retry:
          max_attempts: 10
          backoff: 100ms
          max_backoff: 10s

      dead_letter:
        topic: catalog-mutations.dlq
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
      topic:
        name: search-mutations
      consumer:
        group_id: search-sink-workers

service:
  request:
    max_operations: 1000
  merge:
    max_attempts: 3
    lua:
      timeout: 100ms
      max_source_bytes: 64KiB
      max_result_bytes: 16MiB
      max_cached_programs: 256
      max_instructions: 1_000_000

  batching:
    max_wait: 2ms
    max_operations: 1000
    max_bytes: 16MiB
    queue:
      max_operations: 10_000
      max_bytes: 128MiB
shutdown_timeout: 15s
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

`service.batching` sets the collection targets and the per-store/per-method
`queue` limits. Batching is always active. See [batching behavior](batching.md)
for queue admission, ordering, execution budgets, and completion boundaries.

## Prometheus metrics

`prometheus.address` enables `/metrics`, `/livez`, and `/readyz` in every process
mode. See [metrics and health](observability.md) for the metric catalog, label
budgets, queries, and dependency readiness semantics.

## Configuration reference

“Conditionally required” means a field is mandatory only in the modes or with
the drivers stated in its description. Enum values are case-sensitive and must
use the lowercase spelling shown below. Storage names are also case-sensitive.

| Field | Type | Required | Default | Allowed values | Function |
| --- | --- | --- | --- | --- | --- |
| `mode` | enum string | No | `server` | `server`, `worker`, `all` | Process role. See [Mode values](#mode-values). |
| `grpc.address` | string | No | `:8080` | Any valid TCP listen address | TCP listen address for the gRPC and gRPC health services. Used in `server` and `all` modes. |
| `grpc.max_receive_message_bytes` | byte size | No | `64MiB` | Size greater than `0B` | Maximum encoded gRPC request size accepted by the server. |
| `grpc.max_send_message_bytes` | byte size | No | `64MiB` | Size greater than `0B` | Maximum encoded gRPC response size sent by the server. |
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
| `service.request.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `service.merge.max_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum revision-conflict attempts for Merge and folded conditional Put chains. |
| `service.batching.max_wait` | duration string | No | `2ms` | Positive Go duration within the bounds below | Maximum collection delay measured from the first request in a batch. |
| `service.batching.max_operations` | positive integer | No | `service.request.max_operations` | Integer from `1` through `service.request.max_operations` | Operation target for one automatically formed batch. |
| `service.batching.max_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Encoded-byte target for one automatically formed batch; one larger valid RPC still runs alone. |
| `service.batching.queue.max_operations` | positive integer | No | max(`10000`, `service.request.max_operations`) | Integer at least `service.request.max_operations` and `service.batching.max_operations` | Maximum operations waiting in each store and method queue. |
| `service.batching.queue.max_bytes` | byte size | No | max(`128MiB`, `grpc.max_receive_message_bytes`) | Size at least `grpc.max_receive_message_bytes` and `service.batching.max_bytes` | Maximum encoded request bytes waiting in each store and method queue. |
| `service.merge.lua.timeout` | duration string | No | `100ms` | Positive Go duration within the bounds below | Maximum wall-clock duration of one Lua execution. |
| `service.merge.lua.max_source_bytes` | byte size | No | `64KiB` | Size greater than `0B` | Maximum Lua source size per merge operation. |
| `service.merge.lua.max_result_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Maximum input/current document and encoded result bytes; expanded output is also bounded before conversion. |
| `service.merge.lua.max_cached_programs` | positive integer | No | `256` | Integer greater than `0` | Maximum compiled Lua programs retained in the process-local LRU cache. |
| `service.merge.lua.max_instructions` | positive integer | No | `1000000` | Integer greater than `0` | Maximum VM instruction checkpoints per execution. |
| `storages[].kafka` | object | No | absent | A store-specific Kafka configuration | Holds the asynchronous delivery and Topic-management policy. Kafka remains disabled unless `enabled` is `true`. |
| `storages[].kafka.enabled` | boolean | No | `false` | `true`, `false` | Enables Kafka publication, consumption, and startup Topic reconciliation for this store. |
| `storages[].kafka.brokers` | list of strings | Conditionally | none | One or more Kafka bootstrap addresses | Required when Kafka is enabled. Each store may use a different Kafka cluster. |
| `storages[].kafka.topic.name` | string | Conditionally | none | Any valid Kafka topic name | Required when Kafka is enabled. The server publishes only mutations whose `address.store` selects this store. |
| `storages[].kafka.consumer.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Required for every Kafka-enabled store in `worker` and `all` modes; optional and unused in `server` mode. |
| `storages[].kafka.dead_letter.topic` | string | No | `<storages[].kafka.topic.name>.dlq` | Non-empty Kafka topic different from the source topic | Destination for malformed, cross-store, and permanent failures. Temporary failures remain in the source Topic. |
| `storages[].kafka.topic.partitions` | positive integer | No | `4` | Integer from `1` through `2147483647` | Required partition count for both Topics. Any mismatch gates this store; changes require an explicit drained migration. |
| `storages[].kafka.topic.replication_factor` | positive integer | No | `2` | Integer from `1` through `32767`, not exceeding available brokers | Required replica count for every partition of both Topics. Sink submits and waits for partition reassignment when it differs. |
| `storages[].kafka.topic.retention` | duration string | No | `72h` (3 days) | Positive Go duration within the bounds below | Source Topic retention, at least `1ms`. DLQ retention is configured separately. |
| `storages[].kafka.consumer.max_poll_records` | positive integer | No | `500` | Integer greater than `0` | Maximum number of this store's mutations handled in one consumer fetch batch. |
| `storages[].kafka.consumer.retry.max_attempts` | positive integer | No | `10` | Integer greater than `0` | Maximum attempts per processing round. Temporary failures are retained and retried in later rounds. |
| `storages[].kafka.consumer.retry.backoff` | duration string | No | `100ms` | Positive Go duration within the bounds below | Initial worker retry backoff before jitter for this store. |
| `storages[].kafka.consumer.retry.max_backoff` | duration string | No | `10s` | Positive Go duration within the bounds below | Maximum worker retry backoff before jitter; at least `consumer.retry.backoff` and at most half the Go duration range for safe doubling. |
| `shutdown_timeout` | duration string | No | `15s` | Positive Go duration within the bounds below | Maximum graceful-shutdown time for gRPC and MongoDB disconnect operations. |

### Reliability limits

All settings in this table are optional. Counts and bytes are positive integers; time values are positive duration strings such as `30s` or `2ms`. Limits are process-local; replica
counts multiply capacity. Configure the same Kafka policy on servers and workers.

| Setting | Default | Meaning |
| --- | --- | --- |
| `service.request.timeout` | `30s` | Unary request timeout including batching queue wait and each Scan page; at most 300 seconds. A shorter caller deadline wins. |
| `service.execution.max_requests` | `128` | Storage execution request count, at most 10000, including cross-store calls. Asynchronous publishing uses its own pool. |
| `service.execution.max_bytes` | `256MiB` | Admitted request/output reservation bytes, at most 16 GiB. Reads reserve snapshot and response budgets; Merge and folded conditional Put chains reserve current and output budgets; Lua source expansion and bounded per-operation failure responses are charged. This is not an RSS or VM heap limit. |
| `service.publish.max_requests` | `32` | Concurrent asynchronous Write/Delete requests, at most 10000, independent of storage execution. |
| `service.publish.max_bytes` | `256MiB` | Asynchronous request, expanded-source, and bounded failure-response reservations, at most 16 GiB, additional to `service.execution.max_bytes`. Kafka producer buffers are additional. |
| `service.execution.max_requests_per_store` | `32` | Concurrent storage execution requests per store, at most 10000. |
| `service.publish.max_requests_per_store` | `32` | Concurrent Kafka publishing requests per store, at most 10000, independent of execution. |
| `storages[].limits.max_execution_bytes` | omitted | Optional execution byte ceiling for this store, positive and no greater than `service.execution.max_bytes`. Omitted stores share the global limit. Does not limit publishing. |
| `service.execution.scan.max_requests` | half `service.execution.max_requests`, at least 1 | Scan-only request sublimit, no greater than the total request limit. |
| `service.execution.scan.max_bytes` | half `service.execution.max_bytes`, at least 1 | Scan-only byte sublimit; global byte admission still applies. BSON Scan reserves 48 MiB driver wire space plus page copies. |
| `service.execution.scan.max_requests_per_store` | half `service.execution.max_requests_per_store`, at least 1 | Per-store Scan sublimit, no greater than the total per-store request limit. |
| `service.execution.scan.admission_wait` | min(`2s`, `service.request.timeout`) | Maximum Scan admission wait, included in the page deadline. Cannot exceed `service.request.timeout`. |
| `service.request.max_read_bytes` | min(`32MiB`, half gRPC send limit) | Per-original-RPC Read or returned-Write documents, conditional write snapshot/output per attempt, and native Execute response. Scan pages use the smaller of this limit and 4 MiB. Cannot exceed half the gRPC send limit. |
| `storages[].kafka.dead_letter.retention` | `720h` | Independent DLQ retention, 30 days; at least `1ms` and bounded by Go duration range. |
| `storages[].kafka.topic.min_insync_replicas` | min(`2`, replication factor) | Minimum ISR, at most replication factor. Publishers require all ISR acknowledgements. |
| `storages[].kafka.topic.max_record_bytes` | `900KiB` | Encoded mutation envelope plus key, including expanded Lua source; at most 64 MiB and no larger than the producer buffer. Topic/producer batch limits include an extra 16 KiB for framing and DLQ headers. Broker/replica fetch limits must also support increases. |
| `storages[].kafka.producer.max_buffered_bytes` | `64MiB` | Producer buffer capacity, at most 1 GiB. Full buffers return retryable resource exhaustion. |
| `storages[].kafka.consumer.processing_timeout` | `20s` | Backend work per fetched batch, at most 20 seconds, followed by at most 5 seconds of offset/DLQ settlement. |

Native Execute and Scan share process/store request admission and reserve input
plus response/page buffers. MongoDB native calls additionally reserve 48 MiB for
the driver's complete wire response, which arrives before the smaller Sink
response/page limit can be enforced. Returned writes reserve an additional
response budget per original RPC before execution. See [native access](native-access.md)
for stateless Scan checkpoints, page-local cleanup, cancellation and retry semantics.

Scan waits fairly for execution capacity for at most
`service.execution.scan.admission_wait`. Its separate waiting queue is bounded
by `service.execution.scan.max_requests`, `service.execution.scan.max_bytes`, and `service.execution.scan.max_requests_per_store`; these
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
  execution:
    max_bytes: 1GiB
storages:
  - name: pse-search
    driver: opensearch
    search:
      endpoints: [http://opensearch:9200]
    limits:
      max_execution_bytes: 768MiB
```

The limit belongs to the storage entry. This example is a sizing starting
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
- A `server` does not require `storages[].kafka.consumer.group_id` because it only
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
