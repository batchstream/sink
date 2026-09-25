# Adaptive Store backpressure

Gateway/Engine/Worker scaling adds application processing capacity. Store
backpressure independently regulates the work those processes send to the
database. Queue demand can request growth, but only healthy backend completions
raise the Store window; queue growth or Kafka lag alone cannot do so.
The controller does not consume replica counts, HPA signals, CPU, memory,
connection counts, Ping results, or discovery information.

Each Engine and Worker owns one `internal/backpressure.Controller` for its
configured Store. Replicas converge through real backend feedback, without a
leader, distributed semaphore, external state, or coordination service. This is
adaptive aggregate load control, **not a strict global concurrency invariant**.
Transient overshoot and unequal shares are possible; no finite startup jitter can
guarantee zero overshoot for an arbitrarily large simultaneous deployment.

## Admission boundary

```mermaid
flowchart LR
    G[Gateway] --> B[Read / Write / Delete collection queues]
    B -->|Ready batch; nonblocking handoff| Q[One Store admission FIFO]
    G --> N[Execute / Query / Count / Scan]
    N -->|One ready task| Q
    Q -->|FIFO head acquires permit| C[Local Store controller]
    K[Kafka backlog in Worker] -->|Wait before poll; Acquire before attempt| C
    C --> E[Sequential admitted execution]
    E --> O[Storage observation decorator]
    O --> S[Storage / NativeStorage]
    O -->|Real results and execution latency| C
```

Each Engine has one FIFO of **ready execution tasks** shared by all methods.
A task is a collected batch or an individual non-batch call, not a document,
operation, RPC response frame, or physical database command. Batch producers do
not compete with Native callers through separate `TryAcquire` loops. Even a
new request that could execute immediately cannot overtake an existing ticket.
FIFO is admission order, not original RPC arrival order, completion order, cost
fairness, cross-method record ordering, or a global ordering across replicas.

The batch layer retains collection targets, completion-mode partitioning and
method-local record dependencies. It selects only dependency-ready work and
keeps at most one selected batch waiting for admission per producer. It cannot
select a later batch until that ticket is dispatched or canceled, preserving
its existing dependency order. Pending work, including a selected batch, stays
charged to the original collection queue until execution or cancellation.
Already running batches continue to release individual record dependencies.

A `Ticket` is a nonblocking handoff, not a waiting goroutine. The batch event
loop keeps accepting bounded arrivals and handling cancellations, record
completions, execution completions and shutdown while polling its ticket. A full
admission task queue leaves an accepted batch upstream; it is retried on a
capacity notification, not rejected or removed from accounting. Only the FIFO
head may obtain a permit. Other tickets wait for their turn without contending
for execution slots. Completed executions retain permits until completion
notifications are consumed, bounding goroutines awaiting settlement too.

### Waiting resources and execution resources

- `execution.queue.max_tasks` bounds ready tasks in the shared admission FIFO.
  One batch occupies one task regardless of its operation count.
- `execution.queue.max_bytes` is one Engine-wide budget for encoded request bytes
  retained across **all collection queues and admission**. A `Reservation` is
  obtained once; transferring its request into a task does not release and
  reacquire capacity. Canceling one member releases only that member's bytes.
- `batching.queue.max_operations` and `batching.queue.max_bytes` remain local
  collection bounds per batch method. These do not determine Native admission
  capacity or the Store execution window. They also bound upstream work when
  the shared ready-task FIFO is full.
- Store `max_concurrent` caps simultaneous execution tasks, independently of
  waiting capacity. `batching.max_operations` and `batching.max_bytes` are batch
  formation targets, not concurrency weights.

Bytes are an encoded accounting unit, not a Go heap measurement. Process memory
watermarks, transport size limits, bounded adapter fanout, and batch targets
remain independent protection. An immediately executable non-batch request
needs no waiting-byte reservation. There is no new server-side admission
expiry: each caller's original context controls its wait. Queue/resource bounds
can still reject new work with `RESOURCE_EXHAUSTED`; saturation or cooldown
alone does not. Non-batch calls use their existing RPC goroutine for waiting.

Canceled members are removed before dispatch without canceling surviving members
of their batch. Moving between stages does not reset deadlines or borrow one
caller's deadline for another. Shutdown cancels queued tickets, releases both
stages' charges exactly once, and settles active executions. A canceled caller
never releases an active execution permit before its backend work ends.

One permit covers the complete sequential execution, including parsing, Lua,
snapshots, CAS attempts, cursor sends and result settlement. Nested service
calls reuse the permit rather than enqueueing themselves again. The Storage
observation decorator never acquires a permit or owns another queue.

Engine marks a capacity rejection `sink-forward-not-started`; a backend error
with the same gRPC code does not receive that evidence. Health probes bypass the
business FIFO. Async publication remains on Kafka's bounded producer path, not
behind database admission; Worker controls the later application.

Worker has its own process-local controller. It waits **before polling**, outside
`consumer.processing_timeout`, with fetching paused and group heartbeats/rebalances
intact. It acquires a permit before each handler attempt and releases it before
Kafka retry backoff and settlement. A retained poll remains bounded by
`consumer.max_poll_records`. Admission waiting spends no retry attempt and is
cancellable by processing timeout, rebalance, or shutdown. Unresolved records
remain uncommitted. This change does not add parallel Worker poll processing.

## Window algorithm

The window algorithm has fixed-size feedback state and one short mutex-protected
update per Store observation, alongside the bounded task FIFO and byte reservations. Engine
actively completes randomized initialization in `Run` **before serving readiness
or RPCs**, without issuing a synthetic database request. Worker retains its
first-poll initialization. Subsequent saturation or cooldown does not flap readiness.

| Event | Behavior |
| --- | --- |
| Initialization | Stagger for a random 0.5–1.5 seconds, then open `min(4, maximum)` executions before Engine readiness |
| Healthy demand | Saturation or ready tasks waiting for execution request growth; require at least `max(4, window)` healthy samples and a jittered 125–375 ms control interval |
| Initial growth | Add `max(1, window/2)` below a slow-start threshold of 16 |
| After congestion | Threshold becomes half the previous window; subsequent growth is additive by one |
| Sustained latency inflation | Halve the window, retaining at least one execution so successful work continues and the baseline can adapt |
| Retryable overload/unavailability/timeout | Immediately set the window to zero, then resume real traffic at half the previous window, at least one |
| Repeated failed recovery | Double nominal cooldown from 200 ms up to 10 s; actual delay is uniformly 0.5–1.5 times that value |
| Recovery | Four clean observations reset cooldown escalation, including under light traffic |

All jitter comes from an independently seeded process-local random source. No
synthetic request is sent when cooldown expires: a real waiting execution is the
recovery probe. Old responses from before a decrease cannot apply that decrease
repeatedly or undo it with stale successes. Growth does not suppress a late
overload from an earlier, slower call.

The adaptive window ceiling accepts **1–4096**. Store `max_concurrent` is
optional; when omitted, the default is **64** for MongoDB and **128** for
Elasticsearch or OpenSearch. The ceiling is local to each Engine/Worker process,
not a database capacity estimate or a per-replica allocation of a global quota.
The feature is always wired by application assembly. Internal component tests
can omit the controller to exercise other boundaries independently.

## Feedback and latency

Observation occurs immediately around the existing Storage method invocation,
after dispatch admission and service preparation. It does not include Engine
queue wait, Worker admission wait, Lua execution, or response sending outside
the Store call. Driver work, encoding inside adapters, driver retries and backend
refresh waits remain part of that method's execution time.

One batch contributes one observation. Any explicitly classified retryable
`ResourceExhausted`, `Unavailable` or `DeadlineExceeded` result makes the call a
congestion observation, including a partial batch. `context.DeadlineExceeded`
and corresponding gRPC errors are recognized. Caller cancellation, semantic
errors, unknown errors, malformed result counts and local nonretryable size
limits do not cause decreases or healthy growth. Original results are returned
unchanged. Conflict and precondition failures do not count as overload even if
their existing retry flag is true.

Latency learning is separate for Read, Write, visible Write, Delete, visible
Delete, Execute, Query, Count and Scan, with four bounded operation-count classes:
1, 2–32, 33–128, and 129+. The short EWMA uses weight 0.25. The baseline uses
weight 0.01 in both directions, tracking the long-term mean without bias toward
fast replies in a variable workload. After four samples, two successive short-EWMA observations
above both 1.5 times baseline and baseline + 5 ms trigger a decrease. The
window itself is shared, so a congested method reduces subsequent admission for
every method. No URI, dataset, query text, document identity or error string is
used as a state key or metric label.

Opaque Execute commands can have arbitrarily different costs. They contribute
success/error feedback and measured duration, but do not train latency control.
The optional internal `NativeResponse.Failure` normalizes native overload replies
which otherwise arrive with a nil Go error: HTTP failures, native bulk item
overloads, MongoDB command timeouts/unavailability, and native write concern/item
failures. It is observation metadata only. Native payloads, status codes,
`Success`, and mutation retry rules remain unchanged. Driver-specific code only
normalizes errors; all control decisions use the common storage contract.

The decorator exposes NativeStorage if and only if the wrapped Store supports
it. Application ownership and health checks retain the original Store, preserving
search connection cleanup and MongoDB lifecycle behavior.

## Streaming, cancellation and reliability

Query/Scan wrap the synchronous `Emit` callback and subtract its elapsed time.
Callback failures, deadlines reached inside the callback, and driver timeouts
dominated by callback waiting are ignored as congestion. There is no buffering
goroutine or additional page queue. The execution permit stays held while a
cursor/response body can resume backend work, bounding slow-client resources.
This deliberately favors bounded resource use over counting only active network
waits as occupied slots.

A decrease never cancels a permit, revokes an active record dependency, or
interrupts an accepted mutation. Already admitted sequential work can finish its
existing multi-step algorithm even while the window is zero. Only later
executions wait. Caller cancellation and normal shutdown still propagate through
the existing contexts. A canceled caller does not release its active permit until
the backend actually returns. No timeout, unknown write result, partial result,
or ambiguous native response is newly replayed by the controller.

Health checks continue calling the original backend. They neither consume
permits nor produce feedback. Waiting before a Worker poll does not set its
recovery error or make it unhealthy. Genuine backend/Kafka failures retain their
existing health behavior.

## Scope and operational limits

The controlled unit is an admitted sequence of **Storage interface calls**, not
individual database commands, sockets, records, shards, or transactions. Existing
adapter bulk/group fanout is preserved; one Storage call can issue several
backend commands. Batch targets, request byte bounds and poll bounds therefore
remain important. Changing the request mix within a latency class can also cause
a conservative decrease until its baseline adapts.

There is no capacity estimate without real observations. A backend stuck forever
without returning cannot provide feedback; caller/driver deadlines and graceful
shutdown retain their existing role. Local feedback cannot promise an exact
global maximum, equal per-instance throughput, or protection against non-Sink
database clients. Those limits are inherent in the requested coordination-free
model. The ceiling and cold-start window do not depend on deployment size.

HPA/KEDA should scale application CPU, memory, queueing and consumer capacity.
Monitor Store window reductions and backend latency separately: adding replicas
to a database-bound workload cannot create database capacity. Do not derive
Store concurrency from desired/current replicas or automatically raise its
ceiling when queues grow. Connection-pool sizing remains a separate deployment
concern because adaptive admission does not close idle database connections.

## Verification

Component tests cover healthy growth, latency-only decreases, overload pauses,
cooldown recovery, per-method baselines, batch-size classes, stale replies, mixed
batch results, native errors/capability, callback timing, deadline/cancellation,
permit release, queue saturation, shutdown, same-record ordering, conflict
retries, and known-versus-unknown native admission outcomes. An actual batcher
test grows dispatch concurrency from delayed Store replies without injecting
controller samples.

A seeded discrete-event simulation runs 1, 8 and 100 independent controllers
against a shared backend with capacity 8 → 2 → 8, checking recovery, bounded
overload traffic, startup staggering and progress across instances. It is a
repeatable algorithm regression, not a production throughput benchmark.
The initial window of four retains this startup-overshoot gate: an eight-slot
initial window failed the 100-instance regression. Healthy burst tests also
require progress without rejection, rather than treating a tiny initial window
as sufficient protection. Deterministic virtual-time tests hold a caller for an
hour without a server-added deadline, cancel the head/middle/tail of the FIFO,
exercise byte/count bounds and cooldown, and verify exact permit/queue cleanup.
Mixed-method tests assert one FIFO dispatch sequence across batch and Native calls,
partial-batch cancellation, a full downstream queue without event-loop deadlock,
and byte ownership transfer without double release. Light-traffic tests prevent
queue traversal or startup readiness from fabricating growth demand. Race tests
concurrently release reservations/permits, cancel tickets and collect metrics.
Kafka component tests use an in-process broker to verify that paused intake
does not spend processing timeout, report unhealthy, advance offsets, or produce
dead letters; another test verifies a real processor's overload/cooldown retry.

See [Store metrics](../observability.md#store-backpressure) and
[configuration](../configuration.md). Real-process Elasticsearch/OpenSearch congestion and Kafka backlog regressions
are required by [sink-production-suite](https://github.com/batchstream/sink-production-suite/pull/35).
Its one/four-Engine tests inject delay and 429s in front of disposable real backends;
they are fault/recovery qualification, not a production capacity benchmark.
