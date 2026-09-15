# Migrate to the grouped configuration

This refactor changes the YAML schema. Old keys are rejected; there is no
runtime compatibility layer. The gRPC protocol, stored documents, Kafka mutation
encoding, topic names and consumer group identities do not change.

## Upgrade procedure

1. Update every server, worker, `all` process, Lua test configuration and DLQ
   command configuration using the tables below.
2. Convert numeric time values to duration strings, keeping their units:
   `30` seconds becomes `30s`, `2` milliseconds becomes `2ms`, and `72` hours
   becomes `72h`. Convert byte limits to readable sizes without changing their
   value: `65536` becomes `64KiB`, `16777216` becomes `16MiB`, and `268435456`
   becomes `256MiB`. Counts remain integers; raw integer bytes also remain valid.
   Use `MiB` for powers of 1024 and `MB` for powers of 1000.
3. Preserve all explicit resource limits, topic names, group IDs, partition
   counts, replication settings, retention and store names. Copy the former
   `service.max_store_requests` into **both**
   `service.execution.max_requests_per_store` and
   `service.publish.max_requests_per_store` to preserve its previous effect.
4. Validate each file with the new binary before starting it:

   ```shell
   sink config check --config /etc/sink/config.yaml
   ```

   This checks the schema and limits without opening listeners, databases or
   Kafka, and does not print configuration values. It does not test dependency
   availability or permissions.
5. Deploy the new binary and matching configuration together. For rollback, use
   the old binary with its old file. Do not change topic policy as part of the
   schema migration; both binaries can use the same existing topics.

Default limits retain their previous values. The publishing per-store request
limit now defaults independently to `32`; custom old per-store limits need the
copy described above. Scan and batching defaults still derive from their parent
limits. Topic and DLQ retention must be at least `1ms`, and retry durations must
fit safe delay doubling. Oversized duration strings fail parsing instead of
wrapping during numeric unit conversion.

See [config.example.yaml](../config.example.yaml) for a synchronous server and
[the quickstart configuration](../examples/quickstart/sink.yaml) for `all` mode.

## Service settings

All paths in this table are relative to `service`.

| Old path | New path |
| --- | --- |
| `request_timeout_seconds` | `request.timeout` |
| `max_operations` | `request.max_operations` |
| `max_read_bytes` | `request.max_read_bytes` |
| `max_in_flight_requests` | `execution.max_requests` |
| `max_in_flight_bytes` | `execution.max_bytes` |
| `max_store_requests` | Both `execution.max_requests_per_store` and `publish.max_requests_per_store` |
| `max_scan_requests` | `execution.scan.max_requests` |
| `max_scan_bytes` | `execution.scan.max_bytes` |
| `max_store_scan_requests` | `execution.scan.max_requests_per_store` |
| `scan_admission_wait_milliseconds` | `execution.scan.admission_wait` |
| `max_publish_requests` | `publish.max_requests` |
| `max_publish_bytes` | `publish.max_bytes` |
| `max_merge_attempts` | `merge.max_attempts` |
| `lua` | `merge.lua` |
| `batching.max_wait_milliseconds` | `batching.max_wait` |
| `batching.max_queued_operations` | `batching.queue.max_operations` |
| `batching.max_queued_bytes` | `batching.queue.max_bytes` |
| `lua.timeout_milliseconds` | `merge.lua.timeout` |
| `store_execution_bytes.<name>` | `storages[]` entry named `<name>` → `limits.max_execution_bytes` |

`batching.max_operations`, `batching.max_bytes`, and the remaining Lua limit
names retain their meaning. Lua limits move under `merge.lua`.
`shutdown_timeout_seconds` becomes top-level `shutdown_timeout`.
`mode`, `grpc`, `prometheus`, backend connection fields, and storage names keep
their existing paths.

## Kafka settings

All paths are relative to each `storages[].kafka` object.
`enabled` and `brokers` retain their paths and defaults.

| Old path | New path |
| --- | --- |
| `topic` | `topic.name` |
| `topic_partitions` | `topic.partitions` |
| `topic_replication_factor` | `topic.replication_factor` |
| `topic_retention_hours` | `topic.retention` |
| `min_insync_replicas` | `topic.min_insync_replicas` |
| `max_record_bytes` | `topic.max_record_bytes` |
| `max_buffered_bytes` | `producer.max_buffered_bytes` |
| `group_id` | `consumer.group_id` |
| `max_poll_records` | `consumer.max_poll_records` |
| `processing_timeout_milliseconds` | `consumer.processing_timeout` |
| `max_retry_attempts` | `consumer.retry.max_attempts` |
| `retry_backoff_milliseconds` | `consumer.retry.backoff` |
| `max_retry_backoff_milliseconds` | `consumer.retry.max_backoff` |
| `dead_letter_topic` | `dead_letter.topic` |
| `dead_letter_retention_hours` | `dead_letter.retention` |

The old scalar `topic: catalog-mutations` becomes:

```yaml
kafka:
  enabled: true
  brokers: [kafka:9092]
  topic:
    name: catalog-mutations
    partitions: 4
    replication_factor: 2
    retention: 72h
  consumer:
    group_id: catalog-workers
    processing_timeout: 20s
    retry:
      max_attempts: 10
      backoff: 100ms
      max_backoff: 10s
  dead_letter:
    topic: catalog-mutations.dlq
    retention: 720h
```

Keep the entire block under its storage entry. Topic policy still applies to
both the source and DLQ topics, with separate retention. `server` mode does not
require a consumer group; `worker` and `all` modes require one for each enabled
Kafka store.
