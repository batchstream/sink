# Synchronous admission on a small server

This change removes avoidable execution rejections and reservation overhead for
synchronous RPCs. It preserves operation ordering, returned documents, original
RPC byte limits, conditional-write conflict handling and completion modes.

## Results

The table reports successful RPC/s, not attempted traffic. Saturation samples
last 30 seconds, except the 1 MiB Read case, which lasts 20 seconds with 32
clients. Every listed candidate sample reconciles and has zero operation/RPC
errors. Each row compares one baseline sample with one final candidate sample;
this does not establish a universal capacity or statistical confidence interval.

| Workload | Baseline successful RPC/s | Candidate successful RPC/s | Baseline / candidate RPC errors |
| --- | ---: | ---: | ---: |
| Put, returning its document | 2,662 | 17,531 | 0 / 0 |
| Exact Count | 4,788 | 6,153 | 358,291 / 0 |
| Read, 1 KiB padding | 26,261 | 26,062 | 0 / 0 |
| Merge, status only | 8,111 | 8,787 | 0 / 0 |
| Read, 1 MiB padding | 577 | 578 | 0 / 0 |

Returning Put throughput increases **6.6 times**. Its P99 falls from 101.2 ms to
47.9 ms; average Sink CPU rises from 0.37 to 0.99 cores, while sampled peak RSS
changes from 33.3 to 33.7 MiB. This both uses previously idle CPU and performs
more useful operations per CPU-second. Read control workloads remain near their
baseline throughput. The final code retains full Read working reservations.

Exact Count saturation improves about **29%** and eliminates the immediate
capacity rejections in this sample. Its candidate P99 is 36.5 ms. Baseline Count
P99 includes hundreds of thousands of quickly rejected calls and is therefore
not a comparable successful-request latency measure.

At a fixed offered rate of **3,000 Count RPC/s**, the baseline rejects 1,203 of
90,000 requests in 30 seconds despite average CPU of only 0.68 cores. The
candidate completes all **180,000 requests over 60 seconds**, with no errors or
unissued work, the same approximately 0.69-core CPU use, and P99 of 1.87 ms.
This reproduces and fixes transient resource errors without increasing the
execution byte or request limits.

### Three-minute mixed load

Two independent clients then ran concurrently against different datasets on the
same Sink and MongoDB, offering 8,000 returning Put RPC/s and 1,500 exact Count
RPC/s for three minutes. Their measured windows overlap for approximately 180
seconds. Both workloads reconcile with zero errors and zero unissued requests.

| Workload | Successful requests | P99 |
| --- | ---: | ---: |
| Put with returned document | 1,440,000 | 77.6 ms |
| Exact Count | 270,000 | 20.4 ms |

The **same single Sink** completes 1,710,000 requests in total, at approximately
9,500 successful RPC/s, with average CPU of 0.96 cores and sampled peak RSS of
33.4 MiB. Neither run observes an OOM or container failure. The process resource
measurements in the two reports describe the same server and must not be added.
This operating point uses almost the entire CPU quota; production admission
budgets require headroom and a representative workload. Three minutes is a
bounded stability check, not a long endurance result.

The [machine-readable results](../benchmarks/results/synchronous-admission.json)
include workload settings, successes, errors, reconciliation, process resource
samples summarized per run, and both executable SHA-256 hashes.

## Why a lightly used server could reject or stall

An execution reservation is a worst-case allowance, not actual process memory.
Previously, every caller asking a Put to return its document reserved the full
32 MiB response allowance. Under a 128 MiB execution limit, a collected batch of
32 tiny independent returning Puts executed in groups of three. The server
waited for many small backend writes while most CPU and memory remained idle.

Put payload sizes are known before execution. Admission now reserves their
actual returned copy sizes plus envelope allowances. A mixed batch containing a
returned Merge still reserves the original caller allowances because Lua output
is unknown until execution.

Query, Count, Execute and calls bypassing the batching layer previously rejected
transient execution saturation immediately. They now use a bounded input queue
with count, byte, per-store and time limits. Queued calls consume no execution
slot or hypothetical document buffers. Cancellation removes their entries, and
waiting consumes the request deadline. Impossible reservations, full queues and
expired admission waits still return resource exhaustion.

Execution releases now wake the oldest eligible waiter. An admitted or canceled
waiter hands available capacity to the next one. This avoids waking every queued
RPC on every release, while retaining capacity accumulation for older large
requests and allowing independent stores to progress.

See [configuration](configuration.md#reliability-limits) for
`service.execution.queue` and [metrics](observability.md) for queue depth, retained
input bytes, wait duration and rejection reasons.

## Measurement conditions

- Baseline: `3d5b641`, before this change. Both executables use Go 1.27.0,
  Linux ARM64, the production VT protobuf codec and identical configuration.
- One Sink container has a **1 CPU quota and 256 MiB hard memory limit**, with
  swap disabled and `GOMEMLIMIT=192MiB`. Client and database are separate
  containers. The client has 2 CPUs / 512 MiB; MongoDB 8.0.29 has 2 CPUs / 2 GiB
  and a 256 MiB WiredTiger cache. Docker Desktop provides eight CPUs.
- MongoDB is a disposable single-member replica set. Traffic is unauthenticated
  inside the local Docker network. These measurements do not represent a
  replicated production cluster, cross-zone RTT, TLS or business indexes.
- Sink uses a 128 MiB execution reservation, 32 MiB original RPC read/response
  limit, batches of 32 and a 2 ms collection wait. Existing request/store limits
  and backend concurrency remain enabled. There is no change to the Read
  working-buffer or Lua output limit.
- Unless specified otherwise, each RPC has one operation, there are 128 clients
  on four gRPC channels, and 256 independent seeded keys with 1 KiB of repeated
  padding. Put sends and returns the document. Merge increments an existing
  counter with a per-writer idempotency sequence and returns status. Count uses
  a nonempty predicate, requires an exact total, and checks every response.
- Each load creates its own dataset and reconciles the persisted records before
  cleaning it. Setup, warmup and reconciliation are outside the measured
  interval. Any operation error or unissued fixed-rate work disqualifies a
  healthy capacity result, even if final reconciliation succeeds.
- RSS and cumulative process CPU are sampled from Prometheus every 500 ms during
  load. RSS peaks are sampled, not continuous high-water marks. Baseline and
  candidate cases run sequentially without correctness tests or builds running
  alongside them. Short samples are observations, not confidence intervals or
  production sizing guarantees.

## Correctness validation

The final candidate passes:

- `go test -race ./... -count=1 -timeout=2m` across the repository.
- `make lint`: formatting, `go vet` and Staticcheck.
- Linux ARM64 server and load-client builds.
- Real MongoDB 8.0.29 adapter and service qualification, including bounded
  snapshots, output budgets and large-document read chunks.
- Real Elasticsearch 8.19.20 and OpenSearch 3.8.0 adapter qualification.
- Public conformance from production-suite revision `82ccfc6`: all **40
  top-level tests**, with **338 passing test/subtest events**, no failures and no
  skips. Coverage includes commit and crash boundaries, uncertain responses,
  cancellation, original RPC budgets, large working sets, slow-store isolation
  and independent publication/execution saturation.
- The pinned Go SDK's DNS balancing and endpoint-change integration tests.
- Quickstart server/client image builds, synchronous writes and reads, Kafka
  acceptance and worker application, deletes and Prometheus metric checks.
- All 49 relative Markdown links in the changed documentation resolve.

The backend and conformance runners require named tests to execute successfully;
an empty test selection or skipped coverage cannot pass their checks. These
correctness runs use the race detector separately from the performance samples.

The first quickstart build timed out fetching the external
`docker/dockerfile:1.7` frontend. Temporary Dockerfiles omitted only that syntax
directive, using Docker's built-in frontend with the same build instructions.
The resulting image builds and complete example pass. Repository Dockerfiles
remain unchanged. All disposable validation containers and volumes were cleaned
up after the runs.

## Reproduce

Use the existing [Kubernetes harness](../benchmarks/kubernetes/README.md) for
separate Pods with CPU and memory quotas, or disposable local containers with
the limits above. Build `cmd/sink` and `cmd/sink-perf` for the container platform.
The same current load executable can exercise both server revisions.

```sh
# Run inside the client container against the disposable server.
sink-perf --address sink:8080 --store mongo --dataset perf-returned-put-run1 \
  --workload upsert --return-document --concurrency 128 --keys 256 --duration 30s
sink-perf --address sink:8080 --store mongo --dataset perf-count-run1 \
  --workload count --concurrency 128 --keys 256 --duration 30s
sink-perf --address sink:8080 --store mongo --dataset perf-count-fixed-run1 \
  --workload count --concurrency 128 --keys 256 --rate 3000 --duration 60s
sink-perf --address sink:8080 --store mongo --dataset perf-large-read-run1 \
  --workload read --concurrency 32 --keys 32 --padding 1048576 --duration 20s
```

Use unique dataset names and collect process/cgroup metrics over the measured
window. Throughput and error rate must be assessed together: promptly rejected
requests have low latency but do not accomplish application work. Do not
automatically retry a mutation after an uncertain transport outcome.
