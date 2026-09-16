# Sizing Gateway, Engine, and Worker

Size and scale each role separately. Engine and Worker serve one Store per process;
Gateway forwards requests across Stores and does not create database connections.
The old multi-Store process measurements are not capacity guarantees for this topology.

Use the [fixed-resource qualification runner](../benchmarks/qualification/README.md)
to compare workload profiles with explicit container CPU and memory limits. Pair
capacity measurements with the [rollout drain budget](rolling-upgrades.md).

## Process budgets

- Gateway: bound admitted requests, retained bytes, concurrent Store forwards,
  fanout, and cached downstream connections. Reserve CPU and memory for protobuf
  decoding/encoding and result merging in addition to document allowances.
- Engine: size execution, publishing, Scan, batching queues, Lua, and database
  pools for that Store's workload. These are process limits; no second Store
  subquota applies. A logical byte reservation is not an RSS hard limit.
- Worker: size consumption and execution for message size, processing time,
  database capacity and Kafka partition assignment. It connects directly to the
  database and does not consume Engine execution slots.

A 2 CPU / 4 GiB allocation alone does not determine safe RPC concurrency. A small
Put, a returned-document Merge, and a BSON Scan have different memory and CPU
costs. Size against the actual request mix and backend latency. Leave memory for
transport, Kafka buffers, Lua VMs, driver buffers and garbage collection outside
configured document budgets. Configure Go memory settings within the container
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

## Scaling signals

| Role | Useful signals | Constraint |
| --- | --- | --- |
| Gateway | CPU, in-flight forwarding, retained bytes, admission rejections, downstream latency, connections | Distinguish Gateway pressure from a slow Engine/database |
| Engine | Execution/publishing occupancy, queue wait, rejection rate, CPU, storage latency | More replicas cannot create database capacity |
| Worker | Kafka lag, oldest outstanding work, processing rate, retries and dependency health | Consumer-group partitions bound useful parallelism |

KEDA or another external scaler consumes these metrics. Minimum/maximum replicas,
resource requests, scale-down stabilization and Worker scale-to-zero belong to
deployment configuration. Keep at least one production Engine replica per Store.

## Qualification before rollout

1. Exercise the unchanged public SDK through Gateway, including asynchronous
   acceptance and Worker delivery.
2. Measure single-Store and cross-Store requests, large returned documents,
   conditional writes, slow dependencies and idle Store counts.
3. Record throughput, P99, CPU, RSS, connections, queue age and failures. Include
   budget-sensitive cross-Store requests, whose Store groups execute sequentially.
4. Verify replica discovery with persistent clients, graceful drain, cancellation,
   dependency recovery and no mutation replay after a lost response.
5. Set replica limits and scaling thresholds against the workload's SLO and the
   database's measured headroom; then verify overload recovery and a sustained run.

Use `cmd/sink-perf` against the public Gateway endpoint for workload measurements.
The [local validation record](design/store-isolated-validation.md) reports executed
checks and their limits. The [configuration reference](configuration.md),
[metrics](observability.md), and [single-Store Engine deployment template](../examples/kubernetes/engine-deployment.yaml)
provide the corresponding settings. The template is an example, not a production
capacity recommendation or a dependency of the application.
