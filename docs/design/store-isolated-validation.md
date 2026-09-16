# Store-isolated implementation and validation

Historical design record. The current address and deployment contract is defined in [Record URIs and Engine affinity](../record-addresses.md).

Date: 2026-09-16. Development branch: `randy/store-isolated-architecture`.
Changes span Sink and sink-production-suite. The public sink-go protocol and
implementation are unchanged. No release or production rollout is included.

## Verified behavior

- Gateway, Engine, and Worker assemble independently. Gateway opens no database
  or Kafka clients and does not instantiate the execution core.
- Engine and Worker bind one Store name. Misrouted requests are
  rejected before effects. The removed `server`/`all` modes, plural `storages`,
  per-Store execution subquotas, and Store byte ceilings are rejected.
- Cross-Store batches preserve indexes, duplicate keys, Lua references, global
  validation, partial failures, and budgets for returned documents.
- Internal forwarding supports all seven public RPCs, gRPC status details,
  deadlines, and cancellation. Unknown mutation outcomes are never replayed.
- Budgets are checked before commit and remain scoped to each original RPC even
  when requests are coalesced. Saturating one Store's forwarding allowance leaves
  another Store's allowance available.
- A request retains one route snapshot. Invalid reloads retain the previous
  snapshot. Duplicate Store names are rejected at startup and on reload. A removed
  Store can be re-added with a new Engine address without retaining identity history.
- Connections are lazy, bounded, and expire when idle. Active calls are protected
  from eviction. Real loopback DNS tests cover scale-out, scale-in, and SERVFAIL
  without replaying writes or rebuilding healthy connections.
- Engines retain public-method execution metrics behind the private Forward API.

Default tests use memory storage, local TCP gRPC, and in-process Kafka fixtures.
External backend tests remain explicit opt-ins.

## Cleanup validation

- Sink and suite race tests, vet/staticcheck, tagged test compilation, workflow
  syntax, formatting, and offline documentation links pass.
- All 17 shipped role configurations pass `sink config check`.
- The canonical Quickstart runs Gateway, Engine, and Worker with real MongoDB
  and Kafka. Its example verifies synchronous writes, accepted-to-applied
  delivery, reads, deletes, and metrics.
- Existing sink-go `TestSinkCompatibility` and `TestNativeCompatibility` pass
  through Gateway against that topology with the race detector enabled.
- Public protobuf and generated public bindings are unchanged. The public schema
  matches sink-go after its Go package-name substitution.
- The production suite now uses separate Engines and distinct database targets
  in cross-Store scenarios. Its seven-Store deployment has two Gateways, two
  Engines per Store, and one Worker per asynchronous Store.

- The seven-Store production runner passes its backend matrix, concurrent merges,
  live Scan checkpoints, returned writes, Worker restart recovery, representative
  load, lag/DLQ checks, and permanent-failure inspection/repair/replay.
- Its 3-minute fault run completes 648 cycles / 3,888 logical operations while
  injecting a Worker kill, a database outage, and a Kafka restart. Dependency
  probes confirm the affected Store failure and healthy Gateway/other Store.
  This is a bounded local fault test, not a sustained production capacity result.
- The cleanup conformance run executes all 46 top-level tests (355 including
  subtests). It exposes one test-fixture dependency on health-check request order;
  the corrected endpoint-failure scenario passes three repeated runs and an
  additional run asserting recovery on a different endpoint. Every other scenario
  passes in the complete run. CI requires a clean full run against the pinned suite.
- Test containers and disposable volumes are removed after qualification.

Local production evidence:
`/var/folders/91/pzs4g26n4_s925wqcxc1xmd40000gn/T/sink-qualification.49RRfKKK`.

Local SDK evidence:
`/var/folders/91/pzs4g26n4_s925wqcxc1xmd40000gn/T/sink-isolated-smoke.gbOYddm6`.
These temporary evidence directories are not runtime dependencies.

## Earlier architecture validation

Before legacy cleanup, the full conformance run passed 46 top-level tests
(356 including subtests) in about 932 seconds. The focused isolation run also
verified all seven RPCs, healthy-Store progress after another Engine exits,
returned-budget rejection before commit, and Worker cold-start execution after
its Engine exits. Cleanup qualification is recorded separately above.

## Local performance baseline

Command:
`go test ./internal/gateway -run '^$' -bench '^BenchmarkGatewaySmallPut$' -benchtime=1s -benchmem -cpu=2`

Apple M2, local TCP, Gateway and Engine in one process, memory storage, 1 ms batch
wait; measured before legacy cleanup:

| Path | Time per call | Allocation |
| --- | --- | --- |
| Direct Engine | 1.49 ms | 19,046 B / 298 allocations |
| Gateway → Engine | 1.71 ms | 31,964 B / 519 allocations |

This measures the local extra hop. It does not establish production throughput,
P99, or a capacity recommendation for 2 CPU / 4 GiB. Budget-sensitive cross-Store
calls execute sequentially; production sizing still needs actual document sizes,
request mixes, and latency targets.

## Boundaries

- Local qualification does not establish multi-broker Kafka fault tolerance,
  production-network behavior, long-duration soak, or actual KEDA autoscaling.
- Store name validation cannot prove the configured URI targets the intended
  database. The deployment inventory owns global Store name and database uniqueness.
- Resource limits are logical budgets, not hard RSS ceilings. Deployment controls
  replica limits and aggregate database connection capacity.
- Gateway remains a shared entry point. Exhausting its CPU, memory, or global
  admission budget can affect several Stores; independent Engines and Workers
  isolate execution and database connections.
- Production route migration and removal of old deployed workloads are outside
  this source-code cleanup.

## Store name identity follow-up

The configured identity is now only `storage.name` (Gateway routes use `store`).
Local race tests cover duplicate route names, invalid reload snapshot retention,
re-adding a Store at a new Engine address, and rejection of old forwarding versions
or mismatched envelope/operation Stores before writes. The public protobuf is
unchanged. Real MongoDB/Kafka quickstart tests pass through Gateway for public
record operations, native methods, and asynchronous Worker completion.

The annotated [Gateway](../../configs/gateway.yaml),
[Engine](../../configs/engine.yaml), and [Worker](../../configs/worker.yaml)
examples collectively cover all 67 supported main-configuration leaf fields. Each
file contains only settings used by that component. [Routes](../../configs/routes.yaml)
are a separate file. Each component configuration and the Engine/Worker search-driver
alternatives were checked with the offline config command.

Prometheus HTTP serving is opt-in through `prometheus.enabled` (default `false`).
Configuration and application tests cover all three roles, address-only and
explicit-disabled settings, occupied ports, and enabled metrics/health endpoints.
Runnable quickstart and qualification fixtures explicitly enable their probes.
