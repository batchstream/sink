# Rolling upgrades and replica changes

The role-config redesign changes the private forwarding version. Migrate from
0.18 and older through a separate matching Gateway/Engine cluster. The compatible
rolling procedure below does not apply across that boundary.

Treat discovery withdrawal and RPC draining as separate intervals. Sink keeps
established channels when DNS refreshes, and Gateway retains one Engine address
snapshot for each Store throughout an accepted public batch. Stopping an Engine
as soon as one DNS lookup removes it can still interrupt a later group of an
already accepted request.

## Streaming protocol upgrade

The private forwarding protocol is version 8. `Forward` is one server-streaming
RPC carrying typed results and ending with standard gRPC status and EOF. Budget
grants, usage tracking and custom settlement/status envelopes have been removed. The old unary
endpoint and byte-chunk protocol have been removed.

Public `Read`, `Write`, `Query` and `Scan` are server-streaming RPCs. Upgrade the
Gateway, Engine and SDK together; there is no fallback to an older protocol.
`Delete`, `Execute` and `Count` retain their scalar response contracts. Use a
separate matching cluster for cutover across this incompatible boundary.

The SDK collects results by default. Supplying a callback consumes results as
they arrive without retaining document results. Completed record results survive
an interrupted stream; missing write outcomes are unknown and are never replayed
automatically. Query/Scan continuation metadata is valid only after the final
completion frame **and** successful EOF. See [streaming responses](streaming.md).

## Budget both intervals

For a headless Engine Service, use a measured conservative budget:

```text
preStop serving time >= EndpointSlice/DNS publication delay
                     + maximum remaining DNS cache lifetime
                     + Gateway DNS refresh interval
                     + DNS lookup/retry delay
                     + maximum remaining Gateway request lifetime
                     + scheduling margin

terminationGracePeriodSeconds > preStop serving time
                              + complete process shutdown time
                              + scheduling margin
```

The first inequality is an operational sizing model, not a hard bound during
DNS outages. Include CoreDNS, NodeLocal DNSCache, forwarders and sidecars actually
used on the path. A successful refresh may return an old cached answer; a failed
lookup retains the last usable membership and follows resolver backoff. Lowering
`forwarding.dns_refresh_interval` does not invalidate upstream caches or guarantee a
maximum failed-lookup duration.

For example, 5s publication + 30s cache + 30s refresh + 5s lookup + 30s request
+ 10s margin needs 110s of serving time before SIGTERM. These are illustrative
assumptions, not measurements of an EKS cluster. The example Engine Deployment
uses 110s `preStop` and 180s total grace; measure and adjust both. Forty seconds
is insufficient for the illustrated defaults even though it exceeds the 30s
refresh interval.

Kubernetes starts the termination grace clock before running `preStop`, and
sends SIGTERM after the hook. Terminating EndpointSlice entries become unready
while the container can continue serving. Use a headless Service without
`publishNotReadyAddresses: true`; leave the process serving during the withdrawal
period. These behaviors are described in the Kubernetes
[Pod termination lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow)
and [endpoint termination tutorial](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/).
DNS caches are deployment settings; see
[CoreDNS configuration](https://kubernetes.io/docs/tasks/administer-cluster/dns-custom-nameservers/).

After SIGTERM, Sink withdraws both HTTP and gRPC readiness before waiting for
background shutdown. `/livez` remains successful while the health listener is
open. Accepted gRPC calls drain until `shutdown_timeout`, then remaining calls
are terminated. `shutdown_timeout` is **not** a discovery wait and is not a
single total-process deadline: worker settlement/departure, gRPC, HTTP listeners
and database disconnect can consume sequential time. Measure the entire
SIGTERM-to-exit path with unavailable dependencies and budget the Pod grace
accordingly. Long health Watch streams also consume gRPC drain time.

## Each role needs its own rollout

| Role | Withdrawal path | Additional constraints |
| --- | --- | --- |
| Gateway | SDK DNS refresh or Service/proxy endpoint convergence | Long-lived SDK connections, proxies and in-flight public requests; measure the slowest supported client configuration |
| Engine | Gateway headless DNS discovery | Cached answers and already captured batch snapshots; keep sufficient surviving execution capacity |
| Worker | Kafka consumer-group rebalance | Processing cancellation, contiguous offset settlement, group departure and replacement assignment; DNS readiness alone does not prove consumption |

`maxUnavailable: 0`, startup probes and `minReadySeconds` prevent obvious gaps
but cannot prove downstream discovery has converged. Reserve resources for surge
replicas and the peak database connection pools. Keep production Gateway/Engine
replica floors above zero. Worker scale-to-zero is allowed only when accepting
lag is intentional: Engine can keep acknowledging Kafka acceptance, while no
document will be applied until a Worker receives the partitions. Consumer-group
partitions limit useful parallelism; more Workers do not add partition capacity.

Process readiness does not mean every dependency is ready. Before removing old
replicas, explicitly check the new Engines' required capabilities with
`/readyz?service=sink.storage.STORE` and, for acceptance workloads,
`/readyz?service=sink.kafka.STORE`. A sync-only Deployment can use storage
capability readiness directly. A mixed sync/async role intentionally remains
process-ready when just one dependency fails; gate its rollout on the required
capabilities without turning every temporary dependency outage into a restart.
These probes establish dependency connectivity, not durable-write availability
or spare capacity. In particular, MongoDB Ping does not verify majority/journal
confirmation. Include representative acknowledged writes and their outcomes in
rollout acceptance.

Transient membership differences between Gateways can send the same record to
different Engines. Shared storage revisions still arbitrate conditional writes;
record affinity reduces contention but is not distributed ownership or fencing.
Do not scale down immediately after a scale-out solely because new Pods are Ready.
Allow discovery and old requests to settle, and measure conflicts and admission
rejections during the transition.

If DNS is unavailable while an old Engine exits, or every replica disappears,
zero failed synchronous calls cannot be guaranteed. Gateway never replays a
mutation to another Engine merely because transport failed. Reconcile unknown
outcomes with application operation IDs. A local no-endpoint rejection before
forwarding is distinct from a possibly committed mutation. Retries do not turn
an undersized drain window into a zero-interruption rollout.

## Reproduce the boundaries

Run from the production-suite checkout with `SINK_SERVER_DIR=/path/to/sink`.

```sh
bash scripts/server-go.sh test -race ./internal/app ./internal/gateway \
  -run 'Test(HTTPReadinessRejectsClosedRoles|ReadinessProbeCannotRestoreClosedRole|ShutdownWithdrawsReadinessAndDrainsAcceptedRPC|MembershipWithdrawalRetainsInFlightRequestSnapshot)' -count=10
bash scripts/server-go.sh test -race -tags=integration ./internal/gateway \
  -run '^TestGateway(DiscoversDNSScaleChanges|DNSWithdrawalDrainBoundary)$' -count=3 -timeout=3m
```

These tests use real loopback DNS and gRPC, exercise IPv4/IPv6, healthy scale-out,
withdrawal before/after stopping, stale answers, SERVFAIL, zero replicas, recovery,
retained batch snapshots and graceful/forced drain. They assert backend call
counts so hidden mutation replay cannot masquerade as continuity. Insufficient
drain cases deliberately require failures, then require successful recovery.
They are deterministic deployment counterexamples, not measurements of EKS DNS
cache or kube-proxy convergence.

The public production suite also runs business reconciliation while Workers
scale 1 → 3 → 0 → 1, checks actual assigned consumer-group membership, then
injects process and dependency failures. Qualification ends only after lag drains
and normal-workload DLQs remain empty. For a real cluster, repeat with a persistent
external client, actual Services/proxies, representative in-flight request
durations and fixed resource limits; record errors and committed state as well
as Pod readiness.
