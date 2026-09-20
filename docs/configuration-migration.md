# Configuration migration

This is a breaking configuration and private forwarding change. Deploy matching
Gateway and Engine versions. The public SDK protocol remains on Gateway; direct
public RPCs on Engine are no longer supported. Use a separate cluster for cutover
from an incompatible forwarding version; old and new Engines cannot share routes.

1. Extract `storage.name` to `name` in a Store file. Put database settings under
   `storage` and shared Kafka settings under top-level `kafka` in that file.
2. Pass that same file to Engine and Worker with `--store-config`. Remove embedded
   storage settings from their component files. Gateway takes only `--config`.
3. Move `gateway` to `forwarding`. Keep only `request.max_operations` on Gateway.
4. Move `service.batching` to Engine's `batching`; move `service.merge` to
   `execution.merge`. Put MongoDB concurrency in the shared Store under `storage.mongodb`.
5. Move Kafka `producer` to Engine and `consumer` to Worker. Worker has no `grpc`,
   `request`, `batching`, or `producer`, and uses the same memory watermarks as other roles.
6. Remove `service.request.timeout` and response/read quotas. Caller contexts
   control request lifetime; gRPC message limits bound transport responses.
   Remove snapshot/output byte quotas and old count-based execution/publish admission.
   Process watermarks now control new admission. Remove `memory.burst_percent` and
   `memory.wait_timeout`; use `high_watermark_percent` (80) and
   `low_watermark_percent` (70). Startup panics if estimated minimum working memory
   cannot fit below the high watermark. See the [sizing formula](design/demand-based-admission.md).
7. Move Kafka `topic.partitions`, `topic.replication_factor`,
   `topic.min_insync_replicas`, and `topic.max_record_bytes` directly under `kafka`;
   they apply to both Topics. Rename `dead_letter.topic` to `dead_letter.name`.
   Each Topic retains its own name and retention. `min_insync_replicas` now defaults
   to `1`; set it explicitly if a higher minimum ISR is required.

```sh
sink config check --config configs/gateway.yaml
sink config check --config configs/engine.yaml --store-config configs/stores/primary.yaml
sink config check --config configs/worker.yaml --store-config configs/stores/primary.yaml
```

The [annotated examples](../configs/README.md) and [configuration reference](configuration.md)
cover the complete schema. The Chart mounts one shared Store ConfigMap into both
roles and passes the second argument. Chart 0.8 requires the matching Sink 0.19
release; pre-release validation must override its image with this candidate build.
Existing Kafka data and Store identities need not change for this configuration
migration when the stored mutation protocol is already compatible. This change
performs no deployment or database migration.
