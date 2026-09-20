# Sizing Gateway, Engine, and Worker

Size and scale each role separately. Engine and Worker serve one Store per process;
Gateway forwards requests across Stores and does not create database connections.
The old multi-Store process measurements are not capacity guarantees for this topology.

Use the [fixed-resource qualification runner](https://github.com/batchstream/sink-production-suite/blob/main/benchmarks/qualification/README.md)
to compare workload profiles with explicit container CPU and memory limits. Pair
capacity measurements with the [rollout drain budget](rolling-upgrades.md).

## Process budgets

All roles use process memory watermarks: reject/pause at 80% and resume at 70%
of the effective Go/container/host ceiling. Startup panics if estimated minimum
working memory exceeds the high-watermark allowance. See the [sizing formula](design/process-memory-admission.md)
and [memory/KEDA signals](observability.md#memory-capacity-and-keda).
This is overload control, not an OOM guarantee. There is no separate execution
concurrency cap. Batch queues, Gateway fanout/connections, backend pools, Kafka
buffers and Lua limits remain separately bounded.

A 2 CPU / 4 GiB allocation alone does not determine safe RPC concurrency. A small
Put, a returned-document Merge, and a BSON Scan have different memory and CPU
costs. Size against the actual request mix and backend latency. Leave memory for
transport, Kafka buffers, Lua VMs, driver buffers and garbage collection within the process budget. Configure Go memory settings within the container
limit and measure rather than reusing settings from old architecture benchmarks.

## Database connection budget

```text
Store database connection capacity
  >= maximum Engine replicas × Engine pool budget
   + maximum Worker replicas × Worker pool budget
   + monitoring connections, rolling-update peaks and other authorized users
```

Adding replicas for one Store must not create another Store's database clients.
Gateway remains shared ingress: a process-wide CPU, memory or admission shortage
can affect multiple Stores, so it needs independent capacity planning.

Each search Store owns its HTTP connection pool and retains at most 128 idle
connections across all endpoints, with a 90-second idle timeout. This avoids
reopening most connections after concurrent bursts. Active requests pass process memory admission; 128 is an idle-cache limit, not a limit on active
connections. Shutdown releases the Store's idle connections after work drains.

## Scaling signals

| Role | Useful signals | Constraint |
| --- | --- | --- |
| Gateway | CPU, in-flight forwarding, retained bytes, admission rejections, downstream latency, connections | Distinguish Gateway pressure from a slow Engine/database |
| Engine | Execution/publishing occupancy, queue wait, rejection rate, CPU, storage latency | More replicas cannot create database capacity |
| Worker | Kafka lag, oldest outstanding work, processing rate, retries and dependency health | Consumer-group partitions bound useful parallelism |

KEDA or another external scaler consumes these metrics. Minimum/maximum replicas,
resource requests, scale-down stabilization and Worker scale-to-zero belong to
deployment configuration. Keep at least one production Engine replica per Store.

Validate the [backend image and kernel combination](backend-environment.md) before
using capacity measurements, including MongoDB allocator settings.

## Qualification before rollout

1. Exercise the matching streaming SDK through Gateway, including asynchronous
   acceptance and Worker delivery.
2. Measure single-Store and cross-Store requests, large returned documents,
   conditional writes, slow dependencies and idle Store counts.
3. Record throughput, P99, CPU, RSS, connections, queue age and failures. Include
   large cross-Store streams and slow receivers under bounded fanout.
4. Verify replica discovery with persistent clients, graceful drain, cancellation,
   dependency recovery and no mutation replay after a lost response.
5. Set replica limits and scaling thresholds against the workload's SLO and the
   database's measured headroom; then verify overload recovery and a sustained run.

Use the production suite's `make build-perf` and `.reports/bin/sink-perf`
against the public Gateway endpoint for workload measurements.
The [local validation record](design/store-isolated-validation.md) reports executed
checks and their limits. The [configuration reference](configuration.md),
[metrics](observability.md), and [single-Store Engine deployment template](../examples/kubernetes/engine-deployment.yaml)
provide the corresponding settings. The template is an example, not a production
capacity recommendation or a dependency of the application.
