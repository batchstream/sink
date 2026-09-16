# Runtime configuration

Use the [Gateway / Engine / Worker guide](store-isolation.md) for the topology and routing configuration. Engine and Worker each require one `storage` object; Gateway accepts routes only.

Sink reads all server runtime parameters from one YAML file. Pass its path when
starting the binary:

```shell
sink --config /etc/sink/config.yaml
```

The `--config` option is required for every Gateway, Engine, and Worker runtime.
`sink version` does not load a configuration file. `sink lua test` does not
require one, but accepts `--config` to apply the same `service.merge.lua` resource
limits without opening any configured backend. Environment variables such as
`SINK_MODE` and `SINK_MONGODB_URI` are not read by the server.

Use `sink config check --config FILE` to validate the schema and limits without
connecting to dependencies or printing configured values.

Configuration is loaded once during startup. Unknown fields, malformed YAML,
multiple YAML documents, invalid Store names, invalid positive-integer
values, and incompatible option combinations prevent the process from
starting. Backend connections are lazy and dependency recovery is independent
within each Engine or Worker. Kafka publication/consumption remains unavailable
until topic policy has been reconciled and verified. Other Store processes are
independent. Restart after changing process configuration; Gateway reloads its
separate routes file atomically.

## Configuration layout

| Section | Responsibility |
| --- | --- |
| `mode`, `shutdown_timeout` | Process role and shutdown |
| `grpc`, `prometheus` | Transport and observability listeners |
| `storage` | Engine/Worker Store identity, backend and Kafka path |
| `gateway` | Gateway route file, forwarding capacity, connection cache and discovery |
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

## One Store per Engine or Worker

Each Engine/Worker process binds to exactly one globally unique `storage.name`.
Choose MongoDB, Elasticsearch, or OpenSearch for that Store. Different Stores must
own different database targets; replicas of the same Store share its identity.
A request's Store must match the process binding before any side effect.

```yaml
mode: engine
grpc:
  address: ":8080"
prometheus:
  address: ":9090"
storage:
  name: catalog
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
    metadata_field: __sink
  kafka:
    enabled: true
    brokers: [kafka:9092]
    topic:
      name: catalog-mutations
      partitions: 4
      replication_factor: 2
      retention: 72h
    dead_letter:
      topic: catalog-mutations.dlq
service:
  request:
    timeout: 30s
    max_operations: 1000
shutdown_timeout: 15s
```

Kafka is optional for Engine. Without it, synchronous calls remain available and
asynchronous mutations return per-operation `UNAVAILABLE`. Worker requires Kafka
and `storage.kafka.consumer.group_id`. Deploy Worker separately with the same
Store name, backend, topic and topic policy.

Gateway handles cross-Store batches and preserves original result order, including
mixed asynchronous batches whose Stores have different publishing availability.
It does not load database or Kafka settings.

Use the annotated example for the role you are configuring:

- [Gateway](../config.gateway.example.yaml): public request limits, forwarding, and connection discovery.
- [Engine](../config.engine.example.yaml): one Store's backend, execution, batching, Lua, and optional Kafka publication.
- [Worker](../config.worker.example.yaml): one Store's backend, consumption, execution, and Lua; no RPC listener or RPC batching.
- [Gateway routes](../routes.example.yaml): a separate routing table referenced by the Gateway configuration.

Each component file loads directly without uncommenting another role's settings.
Engine and Worker include a small commented search-driver alternative to MongoDB.
Together the component examples cover all supported configuration fields.
Use the [quickstart](../examples/quickstart/README.md) to run all three components.

## Address routing

Sink does not use a separate bindings configuration. The client-provided record
address selects both the configured storage instance and the location inside
that instance:

| Address field | MongoDB | Elasticsearch and OpenSearch |
| --- | --- | --- |
| `store` | Exact `storage.name` to use | Exact `storage.name` to use |
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

`service.batching` sets the collection targets and the per-method
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
| `mode` | enum string | Yes | none | `gateway`, `engine`, `worker` | Process role. See [Mode values](#mode-values). |
| `grpc.address` | string | No | `:8080` | Any valid TCP listen address | TCP listen address for the gRPC and gRPC health services. Used in `gateway` and `engine` modes. |
| `grpc.max_receive_message_bytes` | byte size | No | `64MiB` | Size greater than `0B` | Maximum encoded gRPC request size accepted by the server. |
| `grpc.max_send_message_bytes` | byte size | No | `64MiB` | Size greater than `0B` | Maximum encoded gRPC response size sent by the server. |
| `prometheus.address` | string | No | empty (disabled) | Empty or any valid TCP listen address | HTTP listen address for Prometheus `/metrics`. Available in every runtime mode. |
| `storage` | object | Engine/Worker | none | Exactly one storage object | The process-bound Store; forbidden in Gateway. |
| `storage.name` | string | Yes | none | Nonempty UTF-8 identity, at most 256 bytes | Globally unique Store name selected by `address.store`; all replicas of that Store use the same name. |
| `storage.driver` | enum string | Yes | none | `mongodb`, `elasticsearch`, `opensearch` | Adapter used by this storage instance. See [Storage driver values](#storage-driver-values). |
| `storage.mongodb.uri` | string | Conditionally | none | Valid MongoDB connection string | Required when the entry's driver is `mongodb`. |
| `storage.mongodb.metadata_field` | string | No | `__sink` | Any valid MongoDB field except `_id`; cannot contain `.`, `$`, or a null byte | Reserved top-level field where Sink stores internal metadata such as the record revision; removed from documents returned to clients. |
| `storage.mongodb.max_concurrent_writes` | positive integer | No | `64` | Integer greater than `0` | Maximum concurrent MongoDB conditional writes. |
| `storage.mongodb.max_concurrent_groups` | positive integer | No | `16` | Integer greater than `0` | Maximum collection groups executed concurrently across all calls to one store. |
| `storage.search.endpoints` | list of strings | Conditionally | none | One or more HTTP(S) endpoints | Required for `elasticsearch` and `opensearch`. |
| `storage.search.username` | string | Conditionally | empty | Any username accepted by the search service | Basic-auth username. Must be configured together with `password`. |
| `storage.search.password` | string | Conditionally | empty | Any password accepted by the search service | Basic-auth password. Must be configured together with `username`. |
| `storage.search.api_key` | string | No | empty | Any API key accepted by the search service | API key used instead of basic authentication. |
| `service.request.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `service.merge.max_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum revision-conflict attempts for Merge and folded conditional Put chains. |
| `service.batching.max_wait` | duration string | No | `2ms` | Positive Go duration within the bounds below | Maximum collection delay measured from the first request in a batch. |
| `service.batching.max_operations` | positive integer | No | `service.request.max_operations` | Integer from `1` through `service.request.max_operations` | Operation target for one automatically formed batch. |
| `service.batching.max_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Encoded-byte target for one automatically formed batch; one larger valid RPC still runs alone. |
| `service.batching.queue.max_operations` | positive integer | No | max(`10000`, `service.request.max_operations`) | Integer at least `service.request.max_operations` and `service.batching.max_operations` | Maximum operations waiting in each method queue. |
| `service.batching.queue.max_bytes` | byte size | No | max(`128MiB`, `grpc.max_receive_message_bytes`) | Size at least `grpc.max_receive_message_bytes` and `service.batching.max_bytes` | Maximum encoded request bytes waiting in each method queue. |
| `service.merge.lua.timeout` | duration string | No | `100ms` | Positive Go duration within the bounds below | Maximum wall-clock duration of one Lua execution. |
| `service.merge.lua.max_source_bytes` | byte size | No | `64KiB` | Size greater than `0B` | Maximum Lua source size per merge operation. |
| `service.merge.lua.max_result_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Maximum input/current document and encoded result bytes; expanded output is also bounded before conversion. |
| `service.merge.lua.max_cached_programs` | positive integer | No | `256` | Integer greater than `0` | Maximum compiled Lua programs retained in the process-local LRU cache. |
| `service.merge.lua.max_instructions` | positive integer | No | `1000000` | Integer greater than `0` | Maximum VM instruction checkpoints per execution. |
| `storage.kafka` | object | No | absent | A store-specific Kafka configuration | Holds the asynchronous delivery and Topic-management policy. Kafka remains disabled unless `enabled` is `true`. |
| `storage.kafka.enabled` | boolean | No | `false` | `true`, `false` | Enables Kafka publication, consumption, and startup Topic reconciliation for this store. |
| `storage.kafka.brokers` | list of strings | Conditionally | none | One or more Kafka bootstrap addresses | Required when Kafka is enabled. Each store may use a different Kafka cluster. |
| `storage.kafka.topic.name` | string | Conditionally | none | Any valid Kafka topic name | Required when Kafka is enabled. The server publishes only mutations whose `address.store` selects this store. |
| `storage.kafka.consumer.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Required in `worker` mode; optional and unused in `engine` mode. |
| `storage.kafka.dead_letter.topic` | string | No | `<storage.kafka.topic.name>.dlq` | Non-empty Kafka topic different from the source topic | Destination for malformed, cross-store, and permanent failures. Temporary failures remain in the source Topic. |
| `storage.kafka.topic.partitions` | positive integer | No | `4` | Integer from `1` through `2147483647` | Required partition count for both Topics. Any mismatch gates this store; changes require an explicit drained migration. |
| `storage.kafka.topic.replication_factor` | positive integer | No | `2` | Integer from `1` through `32767`, not exceeding available brokers | Required replica count for every partition of both Topics. Sink submits and waits for partition reassignment when it differs. |
| `storage.kafka.topic.retention` | duration string | No | `72h` (3 days) | Positive Go duration within the bounds below | Source Topic retention, at least `1ms`. DLQ retention is configured separately. |
| `storage.kafka.consumer.max_poll_records` | positive integer | No | `500` | Integer greater than `0` | Maximum number of this store's mutations handled in one consumer fetch batch. |
| `storage.kafka.consumer.retry.max_attempts` | positive integer | No | `10` | Integer greater than `0` | Maximum attempts per processing round. Temporary failures are retained and retried in later rounds. |
| `storage.kafka.consumer.retry.backoff` | duration string | No | `100ms` | Positive Go duration within the bounds below | Initial worker retry backoff before jitter for this store. |
| `storage.kafka.consumer.retry.max_backoff` | duration string | No | `10s` | Positive Go duration within the bounds below | Maximum worker retry backoff before jitter; at least `consumer.retry.backoff` and at most half the Go duration range for safe doubling. |
| `shutdown_timeout` | duration string | No | `15s` | Positive Go duration within the bounds below | Maximum graceful-shutdown time for gRPC and MongoDB disconnect operations. |

### Reliability limits

All settings in this table are optional. Counts and bytes are positive integers; time values are positive duration strings such as `30s` or `2ms`. Limits are process-local; replica
counts multiply capacity. Configure the same Kafka policy on servers and workers.

| Setting | Default | Meaning |
| --- | --- | --- |
| `service.request.timeout` | `30s` | Unary request timeout including batching queue wait and each Scan page; at most 300 seconds. A shorter caller deadline wins. |
| `service.execution.max_requests` | `128` | Storage execution request count, at most 10000, for the bound Store. Asynchronous publishing uses its own pool. |
| `service.execution.max_bytes` | `256MiB` | Admitted request/output reservation bytes, at most 16 GiB. Reads reserve snapshot and response budgets; Merge and folded conditional Put chains reserve current and output budgets; Lua source expansion and bounded per-operation failure responses are charged. This is not an RSS or VM heap limit. |
| `service.publish.max_requests` | `32` | Concurrent asynchronous Write/Delete requests, at most 10000, independent of storage execution. |
| `service.publish.max_bytes` | `256MiB` | Asynchronous request, expanded-source, and bounded failure-response reservations, at most 16 GiB, additional to `service.execution.max_bytes`. Kafka producer buffers are additional. |
| `service.execution.queue.max_requests` | `1024` | Direct synchronous RPCs waiting for execution, at most 10000. Separate from batching and Scan queues. |
| `service.execution.queue.max_bytes` | min(`32MiB`, execution byte limit) | Input plus bookkeeping bytes retained by pending direct RPCs, at most 16 GiB. Additional to execution and batching budgets. |
| `service.execution.queue.max_wait` | min(`2s`, `service.request.timeout`) | Maximum direct admission wait; cannot exceed the request timeout. A shorter caller deadline wins. |
| `service.execution.scan.max_requests` | half `service.execution.max_requests`, at least 1 | Scan-only request sublimit, no greater than the total request limit. |
| `service.execution.scan.max_bytes` | half `service.execution.max_bytes`, at least 1 | Scan-only byte sublimit; global byte admission still applies. BSON Scan reserves 48 MiB driver wire space plus page copies. |
| `service.execution.scan.admission_wait` | min(`2s`, `service.request.timeout`) | Maximum Scan admission wait, included in the page deadline. Cannot exceed `service.request.timeout`. |
| `service.request.max_read_bytes` | min(`32MiB`, half gRPC send limit) | Per-original-RPC Read or returned-Write documents, conditional write snapshot/output per attempt, and native Execute response. Scan pages use the smaller of this limit and 4 MiB. Cannot exceed half the gRPC send limit. |
| `storage.kafka.dead_letter.retention` | `720h` | Independent DLQ retention, 30 days; at least `1ms` and bounded by Go duration range. |
| `storage.kafka.topic.min_insync_replicas` | min(`2`, replication factor) | Minimum ISR, at most replication factor. Publishers require all ISR acknowledgements. |
| `storage.kafka.topic.max_record_bytes` | `900KiB` | Encoded mutation envelope plus key, including expanded Lua source; at most 64 MiB and no larger than the producer buffer. Topic/producer batch limits include an extra 16 KiB for framing and DLQ headers. Broker/replica fetch limits must also support increases. |
| `storage.kafka.producer.max_buffered_bytes` | `64MiB` | Producer buffer capacity, at most 1 GiB. Full buffers return retryable resource exhaustion. |
| `storage.kafka.consumer.processing_timeout` | `20s` | Backend work per fetched batch, at most 20 seconds, followed by at most 5 seconds of offset/DLQ settlement. |

Native Execute and Scan share process request admission and reserve input
plus response/page buffers. MongoDB native calls additionally reserve 48 MiB for
the driver's complete wire response, which arrives before the smaller Sink
response/page limit can be enforced. Returned writes reserve an additional
response budget per original RPC before execution. See [native access](native-access.md)
for stateless Scan checkpoints, page-local cleanup, cancellation and retry semantics.

Query, Count, Execute and Read/Write/Delete calls that bypass cross-RPC batching
wait for transient execution saturation instead of immediately rejecting it.
Their shared input queue has independent count, byte and time bounds
under `service.execution.queue`. Waiting consumes no execution slot or document
buffer reservation. The request timeout includes admission wait; caller
cancellation removes the pending entry. A full queue, expired admission wait or
reservation that can never fit still returns `RESOURCE_EXHAUSTED`. A caller's
own deadline returns `DEADLINE_EXCEEDED`. Asynchronous publication retains its
independent pool and immediate backpressure.

For returned Put documents, admission uses the known payload sizes plus envelope
allowances instead of reserving a maximum-size response for every caller. A
mixed batch with any returned Merge retains the conservative per-caller response
allowance because Lua results are not known before execution.

Scan waits fairly for execution capacity for at most
`service.execution.scan.admission_wait`. Its separate waiting queue is bounded
by `service.execution.scan.max_requests` and `service.execution.scan.max_bytes`; these
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
peak allowance. Restoration never waits while holding documents: if process
capacity or an older runnable waiter prevents growth, only the unexecuted
operations receive retryable per-operation `RESOURCE_EXHAUSTED` failures. Earlier
successful operations retain their results. This is document accounting, not an
RSS limit; driver buffers, Lua heaps, transport buffers and garbage collection
still require process memory headroom. BSON Scan's wire allowance is unchanged.

Execution byte limits apply to the whole single-Store process. Scale Engine and
Worker independently, and size each process with `service.execution.max_bytes`.
Gateway per-Store forwarding limits bound pressure before it reaches an Engine.
Micro-batches split at the process byte limit; an individual RPC that cannot fit
fails before execution. Reservation growth failures report the `resize` reason.

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
| `gateway` | Routes public RPCs to per-Store Engines; see [isolated configuration](store-isolation.md). |
| `engine` | Opens the gRPC listener for exactly one `storage`, validates Store name, executes synchronously and publishes async mutations. |
| `worker` | Requires exactly one Kafka-enabled Store, consumes and applies its mutations locally without opening the gRPC listener. |

### Storage driver values

| Value | Behavior | Required driver-specific configuration |
| --- | --- | --- |
| `mongodb` | Requires and stores BSON documents. | `storage.mongodb.uri` |
| `elasticsearch` | Requires and stores JSON documents in Elasticsearch. | At least one `storage.search.endpoints` entry |
| `opensearch` | Requires and stores JSON documents in OpenSearch. | At least one `storage.search.endpoints` entry |

## Kafka mode combinations

- Engine publishes only for its bound Store and does not require a consumer group.
- Worker consumes only its bound Store and requires Kafka and a consumer group.
- Gateway has no Kafka configuration or connections.
- Each Store owns its topic, consumer group and DLQ. Deployment configuration must
  ensure uniqueness between Stores on the same Kafka cluster. A single-process
  configuration check cannot validate another process's resource ownership.

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
