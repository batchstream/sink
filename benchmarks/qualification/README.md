# Fixed-resource qualification

Run with Go, Docker Compose and curl installed:

```sh
bash benchmarks/qualification/run.sh
```

The runner creates a unique Compose project and loopback ports, builds the
candidate image and load client, and removes its containers, network and data
volume on exit. Evidence retains the rendered configuration, source revision
and patch, image ID, per-case JSON, container metrics and exit codes.

| Component | CPU quota | Memory limit | Memory target |
| --- | --- | --- | --- |
| Gateway | 1 CPU | 256 MiB | GOMEMLIMIT=192 MiB |
| Engine | 1 CPU | 768 MiB | GOMEMLIMIT=512 MiB |
| MongoDB replica-set member | 2 CPUs | 1 GiB | WiredTiger=256 MiB |

Both Go containers use GOMAXPROCS=2 under their 1 CPU quota. Sink's total quota is
2 CPUs / 1 GiB; database and host load-generator resources are additional. This
is a disposable single-node database, not an election qualification. Stop other
load tests when comparing results. Admission byte budgets do not equal RSS.

The matrix measures 1 KiB upserts across 16/32/64/128 concurrent callers,
16-operation batches, merges, mixed reads/writes, Read and Count, 64 KiB returned
documents, and 500 offered RPC/s. Each cell lasts 30 seconds after seeding and
channel warmup, then reconciles persisted data. JSON reports distinguish useful
throughput, failure codes, P50/P95/P99, unissued work and reconciliation.
`healthy=false` is a saturation result; inspect the exit-code file and do not
count failed setup or failed reconciliation as a capacity sample.

For image comparisons under identical budgets:

```sh
SINK_PERF_IMAGE=sink-baseline:local SINK_PERF_BUILD=0 \
SINK_PERF_PROFILE=compare SINK_PERF_REPEATS=3 SINK_PERF_DURATION=30s \
SINK_PERF_ARTIFACTS=/tmp/sink-perf-baseline bash benchmarks/qualification/run.sh

SINK_PERF_IMAGE=sink-candidate:local SINK_PERF_BUILD=0 \
SINK_PERF_PROFILE=compare SINK_PERF_REPEATS=3 SINK_PERF_DURATION=30s \
SINK_PERF_ARTIFACTS=/tmp/sink-perf-candidate bash benchmarks/qualification/run.sh
```

The comparison profile covers single-operation and 16-operation upserts/merges.
The same host client drives both images. Record the prebuilt images' source
revisions separately: `revision.txt` identifies the runner checkout. Use multiple
runs; allocator improvements do not establish end-to-end capacity gains, and
closed-loop saturation does not guarantee an open-loop SLO.

Run separate [DNS/drain qualification](../../docs/rolling-upgrades.md) and the
public production suite's Kafka/Worker faults. Performance runs inject no faults.
