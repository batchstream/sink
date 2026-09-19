# Runtime configuration

Use the [Gateway / Engine / Worker guide](store-isolation.md) for topology.
Each process reads a role configuration. Engine and Worker also read the same
Store configuration through an explicit command-line argument:

```shell
sink --config configs/gateway.yaml
sink --config configs/engine.yaml --store-config configs/stores/primary.yaml
sink --config configs/worker.yaml --store-config configs/stores/primary.yaml
```

Both files use standard YAML. There is no include syntax, merging, or field override
between them. Paths passed on the command line are relative to the working directory.
`--config` is required. `--store-config` is required for Engine/Worker and rejected
for Gateway. `sink config check` accepts the same arguments and validates both files
offline without opening dependencies or printing configured values. `sink version`
requires neither file. `sink lua test` can optionally load the pair to use
`execution.merge.lua` limits. Environment variables such as `SINK_MODE` and
`SINK_MONGODB_URI` are not read by the server.

Configuration is loaded once during startup. Unknown fields, malformed YAML,
multiple YAML documents, invalid Store names, invalid positive-integer
values, and incompatible option combinations prevent the process from
starting. Backend connections are lazy and dependency recovery is independent
within each Engine or Worker. Kafka publication/consumption remains unavailable
until topic policy has been reconciled and verified. Other Store processes are
independent. All configuration, including `forwarding.routes`, is loaded only at
startup. Restart after changes. Configuration files are limited to 4 MiB.

## Configuration layout

| File / section | Responsibility |
| --- | --- |
| Role: `mode`, `shutdown_timeout` | Process role and graceful shutdown |
| Role: `grpc`, `health`, `prometheus`, `logging` | Listeners and observability; Worker has no `grpc` |
| Role: `memory` | Process memory ceiling and admission watermarks |
| Gateway: `request.max_operations` | One public batch across all Stores |
| Gateway: `forwarding` | Store routes, discovery and fanout |
| Engine: `batching`, `producer` | Collection queues and Kafka publishing buffer |
| Worker: `consumer` | Group, polling, processing rounds and retries |
| Engine/Worker: `execution` | Merge conflict retries and Lua sandbox |
| Store: `name`, `storage`, `kafka` | Shared identity, database connection and Topic/DLQ policy |

Request validation belongs to Gateway. Engine has no `request` section and accepts
only the private forwarding protocol and gRPC health. SDKs address Gateway.
Unknown fields and sections belonging to a different role are rejected.

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

Each Engine/Worker process binds to the `name` in its Store file. Replicas of one
Store reuse the same file; independent Stores must own different database targets.
The Store file holds shared connection and delivery policy:

```yaml
name: catalog
storage:
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
    metadata_field: __sink
    max_concurrent_writes: 64
    max_concurrent_groups: 16
kafka:
  enabled: true
  brokers: [kafka:9092]
  partitions: 4
  replication_factor: 2
  min_insync_replicas: 1
  topic:
    name: catalog-mutations
  dead_letter:
    name: catalog-mutations.dlq
```

Kafka is optional for Engine. Without it, synchronous calls remain available and
asynchronous mutations return `UNAVAILABLE`. Worker requires Kafka plus a
`consumer.group_id` in its own role file. Group, retry and poll tuning never enter
the shared Store file. Engine's `producer` buffer tuning is also role-local.

See the complete annotated [Gateway](../configs/gateway.yaml),
[Engine](../configs/engine.yaml), [Worker](../configs/worker.yaml) and
[Store](../configs/stores/primary.yaml) examples, or run the
[quickstart](../examples/quickstart/README.md).

## Address routing

Record addresses use `sink://<store>/<store-defined-path>`. Gateway selects the
Store and hashes the full canonical URI to choose an Engine. MongoDB interprets
`database/collection/typed-key`; Elasticsearch and OpenSearch interpret
`index/typed-key`. Shared execution and queue code use the full URI as identity.
See [record URIs and Engine affinity](record-addresses.md) for canonical spelling,
SDK examples, connection discovery, replica changes and deployment boundaries.

## Synchronous request batching

`batching` sets the collection targets and the per-method
`queue` limits. Batching is always active. See [batching behavior](batching.md)
for queue admission, ordering, execution budgets, and completion boundaries.

## Prometheus metrics

`health.address` defaults to `:8081` and always serves `/livez` and `/readyz` in
every process mode. Prometheus has its own listener at `prometheus.address`
(default `:9090`), started only when `prometheus.enabled` is `true` (default `false`). See
[metrics and health](observability.md) for the metric catalog, label budgets,
queries, and dependency readiness semantics.

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
| `health.address` | string | No | `:8081` | Any valid TCP listen address; empty uses the default | Always-on HTTP listener for `/livez` and `/readyz` in every role. |
| `prometheus.enabled` | boolean | No | `false` | `true`, `false` | Start the separate metrics listener; health endpoints are unaffected. |
| `prometheus.address` | string | No | `:9090` | Any valid TCP listen address; empty uses the default | Listen address for `/metrics`, used only when enabled. |
| `storage` | object | Store file | none | Exactly one storage object | Shared backend configuration. |
| `name` | string | Yes | none | Nonempty UTF-8 identity, at most 256 bytes | Globally unique Store name selected by `address.store`; all replicas of that Store use the same name. |
| `storage.driver` | enum string | Yes | none | `mongodb`, `elasticsearch`, `opensearch` | Adapter used by this storage instance. See [Storage driver values](#storage-driver-values). |
| `storage.mongodb.uri` | string | Conditionally | none | Valid MongoDB connection string | Required when the entry's driver is `mongodb`. |
| `storage.mongodb.metadata_field` | string | No | `__sink` | Any valid MongoDB field except `_id`; cannot contain `.`, `$`, or a null byte | Reserved top-level field where Sink stores internal metadata such as the record revision; removed from documents returned to clients. |
| `storage.mongodb.max_concurrent_writes` | positive integer | No | `64` | Integer greater than `0` | Maximum concurrent MongoDB conditional writes. |
| `storage.mongodb.max_concurrent_groups` | positive integer | No | `16` | Integer greater than `0` | Maximum collection groups executed concurrently across all calls to one store. |
| `storage.search.endpoints` | list of strings | Conditionally | none | One or more HTTP(S) endpoints | Required for `elasticsearch` and `opensearch`. |
| `storage.search.username` | string | Conditionally | empty | Any username accepted by the search service | Basic-auth username. Must be configured together with `password`. |
| `storage.search.password` | string | Conditionally | empty | Any password accepted by the search service | Basic-auth password. Must be configured together with `username`. |
| `storage.search.api_key` | string | No | empty | Any API key accepted by the search service | API key used instead of basic authentication. |
| `request.max_operations` | positive integer | No | `1000` | Integer greater than `0` | Maximum operation count accepted in one Read, Write, or Delete batch request. |
| `execution.merge.max_attempts` | positive integer | No | `3` | Integer greater than `0` | Maximum revision-conflict attempts for Merge and folded conditional Put chains. |
| `batching.max_wait` | duration string | No | `2ms` | Positive Go duration within the bounds below | Maximum collection delay measured from the first request in a batch. |
| `batching.max_operations` | positive integer | No | `32` | Positive integer | Operation target for one automatically formed batch; a larger valid RPC still executes alone. See [default selection](batching.md#default-selection). |
| `batching.max_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Encoded-byte target for one automatically formed batch; one larger valid RPC still runs alone. |
| `batching.queue.max_operations` | positive integer | No | max(`10000`, `batching.max_operations`) | Integer at least `batching.max_operations` | Maximum operations waiting in each method queue. |
| `batching.queue.max_bytes` | byte size | No | max(`128MiB`, `grpc.max_receive_message_bytes`) | Size at least `grpc.max_receive_message_bytes` and `batching.max_bytes` | Maximum encoded request bytes waiting in each method queue. |
| `execution.merge.lua.timeout` | duration string | No | `100ms` | Positive Go duration within the bounds below | Maximum wall-clock duration of one Lua execution. |
| `execution.merge.lua.max_source_bytes` | byte size | No | `64KiB` | Size greater than `0B` | Maximum Lua source size per merge operation. |
| `execution.merge.lua.max_result_bytes` | byte size | No | `16MiB` | Size greater than `0B` | Maximum input/current document and encoded result bytes; expanded output is also bounded before conversion. |
| `execution.merge.lua.max_cached_programs` | positive integer | No | `256` | Integer greater than `0` | Maximum compiled Lua programs retained in the process-local LRU cache. |
| `execution.merge.lua.max_instructions` | positive integer | No | `1000000` | Integer greater than `0` | Maximum VM instruction checkpoints per execution. |
| `kafka` | object | No | absent | A store-specific Kafka configuration | Holds the asynchronous delivery and Topic-management policy. Kafka remains disabled unless `enabled` is `true`. |
| `kafka.enabled` | boolean | No | `false` | `true`, `false` | Enables Kafka publication, consumption, and startup Topic reconciliation for this store. |
| `kafka.brokers` | list of strings | Conditionally | none | One or more Kafka bootstrap addresses | Required when Kafka is enabled. Each store may use a different Kafka cluster. |
| `kafka.topic.name` | string | Conditionally | none | Any valid Kafka topic name | Required when Kafka is enabled. The server publishes only mutations whose `address.store` selects this store. |
| `consumer.group_id` | string | Conditionally | empty | Any valid Kafka consumer group ID | Required in `worker` mode; optional and unused in `engine` mode. |
| `kafka.dead_letter.name` | string | No | `<kafka.topic.name>.dlq` | Non-empty Kafka topic different from the source topic | Destination for malformed, cross-store, and permanent failures. Temporary failures remain in the source Topic. |
| `kafka.partitions` | positive integer | No | `4` | Integer from `1` through `2147483647` | Required partition count for both Topics. Any mismatch gates this store; changes require an explicit drained migration. |
| `kafka.replication_factor` | positive integer | No | `2` | Integer from `1` through `32767`, not exceeding available brokers | Required replica count for every partition of both Topics. Sink submits and waits for partition reassignment when it differs. |
| `kafka.topic.retention` | duration string | No | `72h` (3 days) | Positive Go duration within the bounds below | Source Topic retention, at least `1ms`. DLQ retention is configured separately. |
| `consumer.max_poll_records` | positive integer | No | `500` | Integer greater than `0` | Maximum number of this store's mutations handled in one consumer fetch batch. |
| `consumer.retry.max_attempts` | positive integer | No | `10` | Integer greater than `0` | Maximum attempts per processing round. Temporary failures are retained and retried in later rounds. |
| `consumer.retry.backoff` | duration string | No | `100ms` | Positive Go duration within the bounds below | Initial worker retry backoff before jitter for this store. |
| `consumer.retry.max_backoff` | duration string | No | `10s` | Positive Go duration within the bounds below | Maximum worker retry backoff before jitter; at least `consumer.retry.backoff` and at most half the Go duration range for safe doubling. |
| `shutdown_timeout` | duration string | No | `15s` | Positive Go duration within the bounds below | Maximum graceful-shutdown time for gRPC and MongoDB disconnect operations. |

### Reliability limits

There is no server-wide business request timeout and no default SDK Scan deadline.
Callers choose deadlines or wait until completion, cancellation or process shutdown.
`execution.merge.lua.timeout`
bounds a Lua invocation, and `consumer.processing_timeout` bounds a Worker processing
round; none of these is a whole-RPC deadline.

There is no `request.max_response_bytes` or `request.max_read_bytes` knob. Gateway
uses `grpc.max_send_message_bytes` as the transport ceiling and reserves encoded
result overhead before forwarding. Returned documents share the remaining budget
across all Store groups of the original RPC. Oversized returned writes are rejected
before their own commit. Intermediate snapshots and candidate outputs use process
memory protection without independent quotas. Collected batches execute together.

All settings in this table are optional. Counts and bytes are positive integers; time values are positive duration strings such as `30s` or `2ms`. Limits are process-local; replica
counts multiply capacity. Configure the same Kafka policy on servers and workers.

| Setting | Default | Meaning |
| --- | --- | --- |
| `memory.max_bytes` | automatic | Optional process ceiling, at least 1 KiB; clamped to detected Go/cgroup/host limits. Fallback is 1 GiB. Startup also checks the minimum required working memory. |
| `memory.high_watermark_percent` | `80` | Reject new requests or pause Worker polling at this percentage of the effective ceiling; integer from 1 to 99. |
| `memory.low_watermark_percent` | `70` | Resume admission at or below this percentage; positive integer strictly below the high watermark. |
| `kafka.dead_letter.retention` | `720h` | Independent DLQ retention, 30 days; at least `1ms` and bounded by Go duration range. |
| `kafka.min_insync_replicas` | `1` | Minimum ISR for both Topics, at most replication factor. Publishers require all current ISR acknowledgements; the default permits writes with one in-sync replica during broker maintenance. |
| `kafka.max_record_bytes` | `900KiB` | Encoded mutation envelope plus key, including expanded Lua source; at most 64 MiB; Engine producer buffer must cover it. Topic/producer batch limits include an extra 16 KiB for framing and DLQ headers. Broker/replica fetch limits must also support increases. |
| `producer.max_buffered_bytes` | `64MiB` | Producer buffer capacity, at most 1 GiB. Full buffers return retryable resource exhaustion. |
| `consumer.processing_timeout` | `20s` | Backend work per fetched batch, at most 20 seconds, followed by at most 5 seconds of offset/DLQ settlement. |

`kafka.partitions`, `replication_factor`, `min_insync_replicas`, and
`max_record_bytes` apply to both the source and dead-letter Topics. Sink explicitly
reconciles this shared policy rather than relying on broker defaults. `kafka.topic`
and `kafka.dead_letter` each contain only `name` and `retention`, which are independent.

All roles use the same watermark settings. Linux measures process RSS; other
platforms fall back to Go runtime memory. Sampling is cached for 100 ms. Existing
requests continue after the high watermark is reached; Worker finishes its current
poll before pausing. There is no separate request concurrency cap.

Before opening dependencies, Sink estimates minimum working memory from transport,
Lua, driver and Kafka settings. Startup panics if the high-watermark portion of the
effective ceiling cannot cover it. The panic lists the required components and
available ceiling. This catches undersized configurations; it does not guarantee
against OOM under concurrent load. See the [sizing formula and runtime behavior](design/demand-based-admission.md)
and [metrics/KEDA migration](observability.md#memory-capacity-and-keda).

The former `service` sections, snapshot/output quotas, `memory.burst_percent`,
`memory.wait_timeout` and Gateway count/byte admission fields are rejected.
Use `memory` for process pressure and `batching.queue` for Engine waiting queues.

MongoDB group concurrency is shared across concurrent calls. Sink sets
`w=majority` and `journal=true` on its client, overriding weaker URI concerns;
server selection is bounded to five seconds. Verify the deployment supports
these settings before upgrading. The caller context controls the overall request lifetime.

A response budget can yield partial results: an individual oversized document
is a permanent resource failure; exhaustion caused by other records in the same original RPC
is retryable. Other coalesced RPCs retain their own quotas. Retry only failed operations or reduce the batch. Successful
mutation results must not be retried without business idempotence.

### Mode values

| Value | Behavior |
| --- | --- |
| `gateway` | Routes public RPCs to per-Store Engines; see [isolated configuration](store-isolation.md). |
| `engine` | Exposes private forwarding and health, validates the bound Store, executes and publishes async mutations. |
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


For Kubernetes Secrets or another file-based credential provider, use
`storage.mongodb.uri_file`, `storage.search.username_file`,
`storage.search.password_file`, or `storage.search.api_key_file` instead of the
corresponding inline field. Each must be an absolute path to a readable regular
file. Kubernetes projected-volume symlinks are supported. A field and its `_file`
variant are mutually exclusive, including an explicitly empty inline string.
Search basic authentication still requires both username and password and cannot
be combined with an API key. MongoDB uses a complete URI, including any required
percent-encoding of credentials; there is no password interpolation into URIs.

```yaml
mode: engine
storage:
  name: orders
  driver: mongodb
  mongodb:
    uri_file: /etc/sink-secrets/mongodb-uri
```

Files must contain 1 byte to 1 MiB of UTF-8 text without NUL. Their bytes are used
exactly, including leading/trailing spaces and newlines; avoid adding a trailing
newline when creating a credential. Values are never parsed as YAML or expanded
as environment variables. Missing/unreadable/invalid files fail startup and
`sink config check`; file-resolution errors identify the field without printing
its contents. `config check` needs access to the mounted files but does not
connect to the backend or prove that credentials authenticate successfully.

Credentials are read only at startup. Updating a projected Secret does not reload
a running client's connections. Prefer a new immutable Secret name and a rolling
update; keep both credentials valid until all old Engine and Worker Pods have
fully terminated. Changing only the Secret data requires an explicit workload
restart. A rollback also requires the referenced Secret and its credential to
remain valid. Revoking the old credential before its Pods exit can cause errors,
regardless of discovery and graceful-shutdown budgets.
