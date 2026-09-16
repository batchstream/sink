# Migrate to the Store-isolated architecture

This version requires a new process topology and configuration. There is no
runtime compatibility mode for `server`, `all`, or plural `storages`. The public
gRPC protocol, stored documents and Kafka mutation encoding remain unchanged.

1. Assign a globally unique `storage.name` to each Store's unique database target.
2. Split each old `storages` entry into its own Engine configuration: `mode: engine`
   and a singular `storage` object. Keep the Store's existing unique name.
3. For each asynchronous Store, create a separate `mode: worker` configuration
   with the same `storage` identity, backend, topic and topic policy, plus its
   existing consumer group. Worker requires Kafka to be enabled.
4. Replace old Store subquotas with process limits. Move the intended effective
   concurrency into `service.execution.max_requests`, `service.publish.max_requests`,
   `service.execution.queue.max_requests` and `service.execution.scan.max_requests`.
   Remove their `max_requests_per_store` fields. Move any Store byte ceiling into
   `service.execution.max_bytes` and remove `storage.limits`.
5. Create a Gateway configuration with `mode: gateway`, `service.request`, and
   `gateway` settings. Its separate route file maps each Store name
   to that Store's Engine service. Gateway must not contain `storage`, database
   credentials, Kafka settings, or execution/batching/Lua configuration.
6. Validate every process configuration with `sink config check --config FILE`.
   Validation is offline and does not print configured values. Verify dependency
   access and capabilities separately in an isolated environment.
7. Drain old consumers before transferring their group ownership to the new
   Workers. Preserve topics, consumer groups, key encoding, partition counts and
   offsets. Keep accepted messages and committed writes during rollback.
8. Switch the public endpoint to Gateway after checking the seven public RPCs,
   asynchronous completion, failure isolation and replica discovery. Do not shadow
   production mutations to both old and new paths, or replay writes with unknown
   outcomes. Rollback requires the previous binary and its matching configuration.

Durations use strings such as `30s` and `100ms`; sizes accept `64KiB`, `32MiB`
and integer byte counts. Older flat configuration keys remain rejected. The
[current configuration reference](configuration.md) lists the accepted fields.

All capacities are per process. Engine and Worker replica counts and connection
pools must jointly fit their Store's database capacity. Replica floors, KEDA
triggers and scale-to-zero remain deployment policies.

See the [runtime guide](store-isolation.md) and the complete
[three-component quickstart](../examples/quickstart/README.md).

The Store name is the sole identity. Remove the former `database_id` key from
Engine/Worker configurations and route files; strict decoding rejects it. Private
forwarding now uses protocol version 2. Upgrade Gateway and Engine together using
a coordinated cutover; mixed protocol versions reject forwarding before execution.
The public client protocol and Kafka mutation format remain unchanged.

Prometheus HTTP endpoints now require `prometheus.enabled: true`. An existing
`prometheus.address` alone no longer starts the listener. Add the flag to any
deployment that uses `/metrics`, `/livez`, or `/readyz`; the shipped quickstart and
Kubernetes example enable it explicitly. Annotated component configurations and
the routing table now live together in [`configs/`](../configs/README.md).
