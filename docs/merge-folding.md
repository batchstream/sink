# Record operation folding

Synchronous requests fold repeated operations for the same complete record
address within one execution batch. Each original operation still receives its
own indexed result. Folding reduces backend work and visibility waits; it never
acknowledges a mutation before its final backend result.

## Scope and ordering

Identity is the complete canonical record URI, identical for every adapter.
Folding applies to explicit core requests and the micro-batcher's combined
requests. Completion modes remain separate, and a mode change for the same URI
remains an ordering barrier. Folding does not span running batches, replicas or
RPC methods. Automatic mutation batches share an adapter-provided physical
resource (`BatchKey`) and completion mode; explicit multi-resource RPCs keep their
own boundary. See [record addresses](record-addresses.md).

| Operations for one address | Backend work without conflicts |
| --- | --- |
| Upsert only | Write the last document once, without a read |
| One Create or Replace | One direct conditional write |
| Repeated Create/Replace/Upsert, or Put mixed with Merge | Read once, evaluate in order, commit the final successful document once |
| Merge only | Read once, execute each Lua program in order, commit once |
| Write requesting `return_document` | Commit that operation independently and return its own logical document; other operations may still fold |
| Repeated Read | Fetch once, return separate result objects from that observation |
| Repeated synchronous Delete | Delete once, return the same outcome to every operation |

A Put replaces the working document at its position without splitting the write
chain. Interleaved operations for other addresses do not split it either. Invalid addresses, payloads, actions, and Lua declarations retain
their validation failures and are excluded from the execution plan.

Async acceptance publishes every original operation. The worker's per-address
failure barriers and execution waves remain in place, so queued mutations for
one address are still applied separately. Write and Delete use independent RPCs
and queues; a Put and a Delete are not folded together.

## Write commit and result contract

The folding rules below apply to operations that share a commit.
`return_document` guarantees an independent commit for the operation carrying
the option, retaining the batcher's ordering barrier. It does not guarantee
separate commits for every other operation in the same-address chain. Each run
without returned documents, before or after a returned operation, may fold into
one commit. Only APPLIED operations requesting a document receive one.
Asynchronous completion rejects this option.
Returned documents are the logical Put/Merge outputs; backend-generated fields
and ingest transformations are excluded. See [returned writes](native-access.md#returned-writes).

For example, this same-address sequence has three commit segments:

```text
Upsert A, Upsert B, returned Merge C, Upsert D, Merge E
      [A, B]              [C]             [D, E]
```

The dispatcher owns resource/completion grouping and record dependencies.
The write core consumes each segment once, prepares either a direct Put or a
snapshot-based candidate, and uses one commit path for output reservation,
storage results and caller completion. See [code ownership](architecture.md#code-ownership).

1. A chain containing only Upserts writes its last document directly. A single
   Put retains the adapter's existing precondition handling.
2. Other chains read one document and its revision, or observe that it is absent.
3. Evaluate each operation against the preceding successful in-memory state.
   Create succeeds only if that state is absent; Replace succeeds only if it is
   present; Upsert replaces it unconditionally. A successful Put makes it present.
   Merge executes its Lua program with `nil` current state when absent.
4. A failed condition or Lua program leaves the working state unchanged. Commit
   the final successful document using the original snapshot's revision, or a
   record-not-exists condition for an initially absent document.
5. After a successful commit, successful operations return APPLIED. The revision
   remains internal to Sink. Preserve the individually evaluated failures and original
   operation indexes/RPC boundaries. If no operation succeeds, return the
   evaluated failures without writing.

For example, on a missing document, `Create(A), Create(B), Replace(C)` commits C
once and returns APPLIED, PRECONDITION_FAILED, APPLIED. `Upsert(A), Merge(B),
Upsert(C), Merge(D)` evaluates both Lua programs in order and commits the result
of merging D into C. A failed final commit leaves the whole chain unresolved,
including conditional/Lua failures evaluated on its speculative state.

The batcher delivers each original RPC as soon as all of its operations have
final results, preserving operation indexes and independent result objects.
Finished document chains release their scheduling dependencies without waiting
for unrelated documents' reads or retries. A later execution error is returned
only to RPCs whose results are still incomplete. Backend bulk response barriers
remain: results cannot be delivered before the backend provides them. Admission
reservations and execution slots remain held until the execution finishes.

WAIT_UNTIL_VISIBLE waits for the final committed state using the existing
refresh=wait_for path. WAIT_UNTIL_APPLIED does not acquire a stronger requirement.
Repeated Deletes wait for their one backend delete and requested visibility.

This is a **final-state commit contract** for Put as well as Merge. Intermediate
documents are not independently persisted or validated by the backend. Schema
validation, generated/default fields, ingest processing, revision increments,
change streams, audit events, and visibility apply to the final backend write.
Lua sees intermediate program or Put output without backend normalization.
Successful operations may be superseded later in the same chain. Callers needing
independent commits can request returned documents on each operation or issue
sequential calls and wait for each result. Separate read observations require
explicit Reads.

## Conflicts, failures, and resource bounds

A definite revision conflict restarts the whole snapshot-based chain, including
previous conditional/Lua failures. Lua observation times remain fixed. The
existing execution.merge.max_attempts limit also bounds folded conditional Put
chains. Exhaustion returns a retryable CONFLICT for every operation in that
unresolved chain. An ambiguous transport failure, lost acknowledgement, or
cancellation is not replayed internally. Existing business idempotence
requirements still apply; folding does not provide exactly-once execution or
batch transactions.

Reads share one backend observation per address but produce independent payload
copies. Every streamed result, including repetitions, must fit its
own message ceiling. Internal batch callers retain their local response limits.
The backend's unique-document read remains bounded by the active microbatch. Missing/error results are returned to all matching
operations.

Snapshot-based Put chains and Merges use the active batch's working memory.
Operation limits, queue byte limits, request deadlines, bounded
conflict attempts, and Lua limits remain in force. Single Puts and all-Upsert
chains retain their direct path. Existing merge metrics continue counting Lua
operations rather than inflating them with Puts; they are not total folding
metrics for Read/Put/Delete.

Write/Delete dispatchers also track record dependencies across queued and active
batches. Independent later requests can run during a previous refresh wait,
within bounded execution capacity; same-record and multi-record dependency
chains keep their order. This ordering remains local to one store/method queue.

Memory watermarks gate new requests, and Store admission gates dispatch. Admitted
batches execute without snapshot/output memory reservations or memory-based
splitting. Response limits are separate: streamed results each have a message
ceiling, while internal batch calls retain their caller-local output budgets.
Returned documents reserve that output allowance before committing and release
unused allowance after a final failure or a smaller successful CAS result.

## Validation and benchmarks

Service tests compare all 486 five-Put sequences across Create/Replace/Upsert
and initially present/absent records with sequential results. Other regressions
cover mixed Put/Merge chains, CAS recomputation, unknown acknowledgements,
returned documents at first/middle/last and adjacent/separated positions,
per-caller budgets and completion, queue dependencies, cancellation, and
asynchronous failure barriers. The production suite tests real MongoDB and
Search commits and visibility; see [development](development.md#validation).

```shell
go test ./internal/service -run '^$' -bench '^(BenchmarkRecordFolding|BenchmarkMergeFolding|BenchmarkReturnedWriteSegments)$' -benchtime=20x -benchmem -count=3
```

`BenchmarkRecordFolding` varies address repetition and operation type.
`BenchmarkMergeFolding` compares merging the same record with distinct records.
`BenchmarkReturnedWriteSegments` uses 64 Upserts with one returned document in
the middle, exercising three commits: prefix, returned operation, suffix.
These core microbenchmarks report adapter work and simulated latency; streaming
microbatch boundaries still limit folding in public RPCs. Compare identical
workloads across revisions. Results do not predict production capacity.
