# Synchronous request batching

Each Engine coalesces concurrent one-operation RPCs into bounded, process-local
batches for its single configured Store. This batching layer
is always active; every single-store `Read` uses this path. `Write` and
`Delete` use it for `WAIT_UNTIL_APPLIED` and `WAIT_UNTIL_VISIBLE`;
`RETURN_AFTER_ACCEPTED` bypasses it because Kafka already batches asynchronous
mutations. Read, write, and delete have independent queues within each store.
Record dependencies stay method-local; Store admission is shared across all
methods. A congested Store pauses subsequent dispatch for that Store.

`batching` configures batch and queue limits; batching is always active.

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
batch waits for refresh. New RPCs pass the process memory watermark check;
the adaptive Store window admits each batch before its queue capacity is released. Record
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

Gateway splits cross-Store requests before forwarding them. Engine accepts only its
bound Store and never bypasses that check through the batching layer. Independent
Store groups follow the bounded-fanout policy in the [runtime guide](store-isolation.md).

Read, Write and Delete each have one bounded queue in an Engine process. Queue
operation and byte limits apply per method; the process has three such queues.
Other Stores run in separate Engine processes. A
new single-store request that would cross its queue's limit fails with gRPC
`RESOURCE_EXHAUSTED` and is not applied. Requests canceled before dispatch are
omitted. Once a batch is dispatched, other live callers in that batch continue
even if one caller cancels. Once all callers cancel, execution is cancelled too.
Execution follows the callers' deadlines and cancellation; there is no default
whole-request timeout.
Admitted batches continue through memory pressure. The entire collected batch
executes without independent snapshot/output byte quotas or memory-based splitting.
Record dependencies and adapter/completion grouping still apply. Streaming RPCs
submit one existing microbatch at a time and check each returned result against
the local gRPC ceiling before its own commit. Kafka producer buffers and Lua sandbox limits remain.
Queue limits count waiting work only, excluding dispatched batches. Graceful
shutdown drains gRPC calls before stopping batch dispatchers.
See [memory admission](design/process-memory-admission.md).

Batching happens only among requests for the same store reaching the same Sink
process. More pods increase aggregate application queue capacity, but do not share a
batcher. Each pod independently adapts Store dispatch concurrency to real backend
feedback; replica count never directly determines the execution window. See
[Store backpressure](design/store-backpressure.md). Explicit client-side batches still remove gRPC framing,
scheduling, and serialization overhead and are therefore more efficient when
the caller already has several records available.

See the [configuration reference](configuration.md) for all settings.

## Choosing limits

`batching.max_operations` defaults to **32**, with a **2 ms** collection wait.
Measure throughput, tail latency and memory for the actual document sizes,
request mix and backend latency before adjusting these targets.

The batch target is independent of Gateway's **1,000-operation** public RPC
limit and each method's **10,000-operation** waiting queue. A valid explicit
RPC larger than the batch target executes alone and retains its public result
boundary. Streaming requests submit bounded microbatches to this queue.

Memory watermarks default to 80% for rejection and 70% for recovery. Queue,
byte, backend-concurrency and Kafka-consumer limits require workload-specific
validation. See [production sizing](production-sizing.md).
