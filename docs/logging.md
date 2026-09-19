# Internal diagnostic logs

Gateway, Engine and Worker use structured `slog` logs to diagnose failures, slow
work and retries. Logging defaults to **warn**, with JSON on stderr. There is no
file output. Optional OTLP logs go directly to an existing Collector; Sink does
not deploy a Collector or configure its downstream Kafka/Elasticsearch pipeline.
No traces, spans or request tracing are configured or included in these logs.

## Configure outputs

Add this section to the normal runtime YAML. The endpoint below is an example;
replace it with the existing Collector's address. This example expects TLS with
a certificate trusted by the system CA pool. For a plaintext internal receiver,
explicitly set `tls.enabled: false`.

```yaml
logging:
  level: warn
  console:
    enabled: true
    format: json
  labels:
    environment: production
    cluster: example-eks
  otlp:
    enabled: true
    protocol: grpc
    endpoint: otel-collector.observability.svc.cluster.local:4317
    tls:
      enabled: true
```

Use `protocol: http/protobuf` with the HTTP receiver's port (usually 4318) to
export to `/v1/logs`. The endpoint is always `host:port`, without a scheme, path,
userinfo or query. Custom paths, authentication headers and client certificates
are not configured by this initial module. TLS verifies the hostname and system
trust; verification cannot be disabled independently of TLS.

YAML controls endpoint, protocol, headers (empty), TLS, resource identity and
batching rather than inheriting standard OTEL exporter defaults. No connection
is made by `sink config check`. An unreachable Collector does not prevent Sink
startup or affect readiness. Invalid YAML/settings do prevent startup. Restart
the process after changing logging configuration.

| Setting | Required / default | Values and meaning |
| --- | --- | --- |
| `logging.level` | Optional; `warn` | `debug`, `info`, `warn`, `error` |
| `logging.components` | Optional; empty | Per-component level overrides; `runtime`, `rpc`, `batcher`, `execution`, `kafka`, `storage`, `health`, `logging` |
| `logging.console.enabled` | Optional; `true` | stderr output; at least console or OTLP must remain enabled |
| `logging.console.format` | Optional; `json` | `json` or `text` |
| `logging.labels` | Optional; empty | Only `environment`, `cluster`, `namespace`, `pod`, `node`; single-line strings up to 256 bytes |
| `logging.failure_body` | Optional; `false` | Allow a bounded document in ERROR body for supported severe final failures |
| `logging.max_body_bytes` | Optional; `16KiB` | Failure-body limit, 1KiB–64KiB, including message and truncation marker |
| `logging.otlp.enabled` | Optional; `false` | Direct OTLP log export |
| `logging.otlp.protocol` | Optional; `grpc` | `grpc` or `http/protobuf` |
| `logging.otlp.endpoint` | Required when OTLP is enabled | `host:port`; brackets required for IPv6 |
| `logging.otlp.tls.enabled` | Optional; `true` | System-trusted TLS or explicitly selected plaintext |
| `logging.otlp.queue_size` | Optional; `1024` | Pending records, 1–8192 |
| `logging.otlp.batch_size` | Optional; `128` | Records per export, 1–512 and no larger than queue size |
| `logging.otlp.flush_interval` | Optional; `1s` | Positive duration, at most 1m |
| `logging.otlp.export_timeout` | Optional; `3s` | Positive duration, at most 30s, including bounded retries |
| `logging.otlp.shutdown_timeout` | Optional; `5s` | Positive duration, at most 30s; additional grace after application resources close |

For a focused investigation, keep the default and enable one component:

```yaml
logging:
  level: warn
  components:
    kafka: debug
```

## Labels that survive ingestion

The target ingestion consumer indexes log body, timestamp and record attributes
as string-valued `labels`. It does not retain OTel Resource attributes, scope or
severity. Sink therefore emits lowercase `level` and all identities directly in
record attributes. Numeric diagnostic values are strings too; do not use them
as numeric ES aggregations without an explicit ingestion/mapping change.

| Labels | Purpose |
| --- | --- |
| `service_name`, `service_version`, `role`, `instance_id` | Sink, build, process role and instance |
| `store` | Bound Store on Engine/Worker; Gateway summaries may omit it when a request spans Stores |
| `environment`, `cluster`, `namespace`, `pod`, `node` | Deployment context, when supplied |
| `component`, `event`, `level` | Stable event selection and severity |
| `method`, `phase`, `reason`, `status`, `error_code`, `error_type` | Operation and failure classification |
| `duration_ms`, `queue_ms`, `operations`, `failed`, `bytes`, `attempt` | Work size, timing and retries |
| `topic`, `partition`, `offset` | Acknowledged dead-letter location or relevant Kafka topic |
| `source_topic`, `source_partition`, `source_offset` | Original source position for a quarantined mutation |
| `dropped`, `suppressed` | Local queue loss and repeated-event suppression |

Only fixed, flat keys are supported. Unknown attributes and nested slog groups
are omitted; arbitrary maps cannot create ES field mappings. Attributes are
limited to 256 UTF-8 bytes each; ordinary messages to 1024 bytes. Instance/Pod IDs
and offsets have high cardinality and belong in searches, not metric labels.
The field `namespace` always means the Kubernetes namespace, not a database.

Configure Kubernetes Downward API environment variables to supply instance
identity without granting Sink permission to query the Kubernetes API:

```yaml
env:
  - name: POD_UID
    valueFrom:
      fieldRef: {fieldPath: metadata.uid}
  - name: POD_NAME
    valueFrom:
      fieldRef: {fieldPath: metadata.name}
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef: {fieldPath: metadata.namespace}
  - name: NODE_NAME
    valueFrom:
      fieldRef: {fieldPath: spec.nodeName}
```

`logging.labels` overrides the matching deployment environment values. Without
`POD_UID`, Sink uses hostname plus a random process suffix for `instance_id`.
The Sink Helm chart's Pod `env` settings can carry these Downward API entries;
the deployed configuration must also include the new `logging` section when
OTLP is enabled. Use an image that includes this module.

## Events and diagnosis

Default warnings include failed RPCs (including per-operation failures delivered
over gRPC OK), work taking more than five seconds, unavailable dependencies,
Kafka retry/fetch/publication failures and incomplete shutdowns. Error logs cover
internal/data-loss RPC errors and acknowledged dead-letter quarantine. Recovery
and lifecycle events are info. Debug records successful RPCs, batch flush reasons,
write-phase durations and Kafka publish/consume summaries, not one success per
document. These observations work with Prometheus disabled.

RPC summaries report counts and the first per-operation failure code, never
request bodies or backend error text. Private forwarding stream summaries report
transport status; the public Gateway response also captures application failures.
Health polling logs state transitions rather than each probe.

Useful filters in the current ES index include `labels.service_name=sink`,
`labels.role=worker`, `labels.store=<store>`, `labels.level=warn` and
`labels.event=kafka_retry`. Match `source_*` or dead-letter coordinates to inspect
the durable Kafka record when a summary needs more context.

For severe final failures, explicitly enable `logging.failure_body: true`.
Currently `kafka_quarantined` can include the failed write's document after DLQ
acknowledgement. JSON is readable, BSON is base64 with its encoding marked, and
truncated payloads end in `[truncated]`. Unknown encoding values are marked with
their numeric enum value and use base64. Deletes and undecodable envelopes have no
document body. Bodies may contain business data: this switch deliberately permits
that diagnostic content. Payloads never become labels. Lua source, native commands,
record addresses and connection settings are not included. Normal warnings never
include document content, even when the switch is enabled.

## Load, outages and shutdown

OTLP network I/O runs in the SDK batch worker. Its queue and batch sizes are
bounded; older pending records are discarded when the queue is full. Record size
limits bound queued content too. Increasing queue/body limits increases memory
use, especially with debug enabled. stderr retains ordinary synchronous writes.

Warn/error events allow ten emissions per component/event/level per 30 seconds.
State is bounded to 256 distinct keys plus an overflow bucket. The next emission
after a window carries its suppressed count; shutdown reports remaining counts
to the console-only diagnostic path, regardless of the configured log level.
Errors have a separate budget from warnings. Debug is not rate limited and should
be enabled briefly. These are diagnostic logs, not a lossless audit trail.

SDK queue drops and failed export record counts are reported cumulatively through
rate-limited console-only diagnostics (`log_queue_dropped`, `log_export_failed`).
They stay visible even if normal console output is disabled and are never exported
recursively. Process crashes may lose queued logs; a Collector acknowledgement
does not prove subsequent Kafka/ES indexing. Avoid collecting console logs into
the same backend as direct OTLP without deduplication.

Application resources close before the bounded log shutdown flush. Give the Pod
termination grace period room for both application shutdown and the extra logging
grace. An interrupted or expired flush emits `log_shutdown_incomplete` to stderr.

The [design contract](design/internal-logging.md) records scope and the ingestion
compatibility requirement. Tests use local OTLP receivers and require no EKS,
Kafka or Elasticsearch deployment.
