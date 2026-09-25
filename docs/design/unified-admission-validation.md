# Unified Store admission validation

## Acceptance scope

An Engine has one FIFO of ready execution tasks. Collection queues retain batch
formation and method-local record ordering; they do not independently compete
for execution permits. Native calls and ready batches share admission. A batch
is one task, irrespective of its operation count. Waiting encoded bytes remain
charged once across collection and admission.

The initial execution window and feedback growth/decrease parameters are not
increased to hide overload. Startup readiness and uncontended queue traversal
must not create false growth demand.

## Required gates

| Area | Required observation |
| --- | --- |
| FIFO | Read, Write, Delete, Execute, Query, Count and Scan reach storage in admission order when only one execution slot is available. This does not promise cross-method record completion ordering with multiple slots. |
| Byte ownership | Transfer from collection into admission neither frees nor duplicates a reservation. Partial cancellation releases only that member; cancellation/dispatch races never make counters negative or leak capacity. |
| Task units | A batch containing 100 reserved operations is one queued task, not 100 execution permits. |
| Full downstream queue | An already accepted batch remains upstream without failing. Its event loop continues handling cancellations and execution completions. |
| Cancellation and deadlines | Every Native method waits while the window is full. Only the caller's context ends waiting. Real gRPC tests cover unary replies and streaming terminal errors, then release capacity to complete the original queued calls. |
| Cold start | Real Elasticsearch and OpenSearch each receive mixed 64-call bursts per Engine with one and four cold Engines, without admission rejection. Read outcomes and Native status are checked, every written record is verified, and task/byte gauges drain. |
| Adaptive protection | Deterministic 1/8/100-controller simulation and real one/four-Engine latency/429/recovery tests retain congestion protection. Light traffic cannot grow the window merely by traversing the FIFO. |
| Worker independence | Kafka retains 32 accepted increments through overload, settles offsets only after recovery, persists exactly 32, and leaves the DLQ empty. |
| Existing contracts | Full candidate/transport coverage gates and public conformance retain record semantics, response limits, partial-result behavior, crash boundaries, native commands, Scan and process lifecycle behavior. |
| Deployment configuration | Helm schema and rendering tests cover Engine-only `executionQueue`, task counts independent of batch operation limits, and rendered configuration accepted by the candidate executable. |

The cold-start fixture declares the query sort-field mapping directly on the
backend before starting Sink. This does not send warmup traffic through Sink.
Its sort values are unique and immutable, so Scan remains valid while the index
refreshes during the burst. An unmapped sort field is an invalid database
request, not an admission failure.

## Reproduction

From the Sink checkout:

```sh
GOPROXY=off go test -race ./... -count=1 -timeout=180s
GOPROXY=off go vet ./...
GOPROXY=off staticcheck ./...
```

From the matching `sink-production-suite` checkout:

```sh
SINK_SERVER_DIR=../sink make test-candidate
SINK_SERVER_DIR=../sink SINK_GO_DIR=../sink-go make test-conformance
SINK_SERVER_DIR=../sink bash scripts/test-mongodb-integration.sh
```

The suite requires named test events and rejects missing/skipped gates. Its
artifacts retain source revisions, diffs, JSON events, process configuration and
logs. Use disposable local infrastructure; these commands do not qualify a
production cluster. The candidate overlay and core queue scenarios must also
pass with the race detector; the key FIFO/ownership/cancellation scenarios are
repeated 50 times during implementation acceptance.

From `sink-charts`, use its development Python environment:

```sh
make lint
make test
python tests/check_runtime.py --sink-binary /path/to/candidate/sink
```

## Configuration and metrics

```yaml
execution:
  queue:
    max_tasks: 10000
    max_bytes: 128MiB
```

`max_tasks` is a ready-task bound, not an operation count or physical database
concurrency estimate. `max_bytes` covers waiting data in both stages. Existing
batch collection bounds and the Store `max_concurrent` ceiling remain separate.
Helm exposes the settings as Engine `config.executionQueue.maxTasks/maxBytes`.
Do not configure them on an older image lacking unified admission support.

`sink_store_admission_queued_tasks` replaces the read-only queued-request gauge.
`sink_store_buffered_bytes` measures shared waiting bytes;
`sink_store_admission_queued_bytes` is its ready-task subset. Do not sum either
with per-method queue bytes as if they were disjoint allocations. Batch request
queue time includes its admission wait; the two histograms are not additive.

## Limits of acceptance

These gates do not prove production Kubernetes rolling-upgrade behavior, a
strict cluster-wide database request limit, an OOM guarantee, a workload-specific
latency SLO, or two-hour soak stability. A full waiting budget, process memory
pressure, oversized payload, or real backend overload may still fail a request.
No production deployment or release is performed by this change.
