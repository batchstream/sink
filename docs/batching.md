# Synchronous request batching

Each Engine coalesces concurrent one-operation RPCs into bounded, process-local
batches for its single configured Store. This batching layer
is always active; every single-store `Read` uses this path. `Write` and
`Delete` use it for `WAIT_UNTIL_APPLIED` and `WAIT_UNTIL_VISIBLE`;
`RETURN_AFTER_ACCEPTED` bypasses it because Kafka already batches asynchronous
mutations. Read, write, and delete have independent queues within each store.
A slow batch therefore does not block another method or another store.

`batching` configures batch and queue limits; batching cannot be disabled.
Remove the former `batching.enabled` field from existing configurations.
The strict configuration parser rejects it as an unknown field for either value.

The first queued request starts `batching.max_wait`.
Collection stops when that timer expires or adding another request would cross
the operation or encoded-byte target. A single valid RPC larger than a batch
target still runs alone. Automatic mutation batches combine only RPCs sharing
adapter resource and completion mode. An explicit RPC spanning datasets
executes alone and keeps its original result boundary. This prevents an index's
refresh wait from entering an unrelated index's storage bulk through automatic
batching. `WAIT_UNTIL_APPLIED` is never promoted to `WAIT_UNTIL_VISIBLE`. A change of
completion mode for the same full record address creates an ordering barrier;
requests touching multiple records wait for all of their predecessors. Same-mode
Puts and Merges fold within a group; repeated Reads and Deletes execute once per
full address. Executions use their live callers' deadlines and cancellation signals.

Write/Delete dispatchers can collect and execute later batches while an earlier
batch waits for refresh. Active batches acquire from the shared memory pool;
there is no separate request concurrency cap. Record
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
Producer references retain input until execution finishes. Each caller retains
its output through transport completion, so early completion cannot release
still-owned memory.

Gateway splits cross-Store requests before forwarding them. Engine accepts only its
bound Store and never bypasses that check through the batching layer. Budget-sensitive
Store groups follow the scheduling policy in the [runtime guide](store-isolation.md).

Read, Write and Delete each have one bounded queue in an Engine process. Queue
operation and byte limits apply per method; the process has three such queues.
Other Stores run in separate Engine processes. A
new single-store request that would cross its queue's limit fails with gRPC
`RESOURCE_EXHAUSTED` and is not applied. Requests canceled before dispatch are
omitted. Once a batch is dispatched, other live callers in that batch continue
even if one caller cancels. Once all callers cancel, execution is cancelled too.
Execution is capped by the server request timeout even without caller deadlines.
Dispatched micro-batches acquire known working allocations from the shared
`memory` pool. Response growth has priority over new arrivals and may borrow its
completion reserve. Request entry fails if ordinary capacity is unavailable;
already admitted queue entries retain their input charge. Async publication
uses the same memory pool; Kafka producer buffers keep their separate bounds.

Each original RPC retains its snapshot/input/output/returned-document quotas
across chunks, retries and shared-key folding. A full read working set defers
records without consuming caller quota; conditional write chunks retain earlier
successful results. Returned documents acquire capacity before their commit.
Batch splitting uses known request sizes and actual working allocations, rather
than maximum legal responses per caller. Graceful shutdown drains gRPC calls
before stopping batch dispatchers. See [memory admission](design/demand-based-admission.md).

Batching happens only among requests for the same store reaching the same Sink
process. More pods increase aggregate queue and storage concurrency, but they
do not share a batcher. Explicit client-side batches still remove gRPC framing,
scheduling, and serialization overhead and are therefore more efficient when
the caller already has several records available.

See the [configuration reference](configuration.md) for all settings.
