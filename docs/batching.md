# Synchronous request batching

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

The first queued request starts `service.batching.max_wait`.
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
`min(service.execution.max_requests, service.execution.max_requests_per_store)` active batches. Record
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
`service.publish.max_requests` and `service.publish.max_bytes`. Synchronous snapshot reservations,
store saturation, and fair byte waiters cannot block Kafka publishing. A full
publishing pool rejects before enqueueing, and acceptance still requires the
publisher's durable acknowledgement. Each pool has its own `max_requests_per_store` limit. The total execution reservation bound is the sum of both byte limits;
producer buffers, batching queues, and VM/driver overhead remain additional.
Each coalesced RPC has its own read, conditional snapshot, and output budgets.
A shared snapshot is fetched if any interested RPC has room, and every response
copy is charged to its original RPC. Conditional chains evaluate each RPC's
budget before incorporating its state into the next caller's operations.
Batches are split at RPC boundaries when worst-case byte reservations would
exceed `service.execution.max_bytes`. Reads reserve both snapshot and response space;
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

See the [configuration reference](configuration.md) for all settings.
