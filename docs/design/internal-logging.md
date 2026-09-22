# Internal diagnostic logging

## Scope

Sink logs help operators diagnose and improve Gateway, Engine and Worker behavior.
The default level is `warn`. Outputs are stderr (JSON or text) and optional OTLP
logs sent directly to an existing Collector. File output, traces, spans, trace
context propagation, audit delivery and an embedded Collector are out of scope.

The deployment pipeline is Sink → OTLP Collector → Kafka → ingestion →
Elasticsearch. Sink owns neither the Kafka log topic nor the Elasticsearch index.
The ingestion contract retains log body, timestamp and **record
attributes** as string-valued `labels`; it ignores Resource attributes, scope and
OTel severity. Consequently every essential identity and the lowercase level
must also be present as a flat string record attribute. Trace/span IDs are not
generated or copied. This contract must have an OTLP wire regression test.

## Field contract

Use fixed lowercase snake_case keys, string-valued OTLP attributes, and short
stable event names. Do not derive attribute names from record keys, datasets,
request input or arbitrary user maps. Resource identity may supplement but must
never replace record attributes.

| Fields | Meaning and cardinality |
| --- | --- |
| `service_name`, `service_version`, `role` | `sink`, build version, and gateway/engine/worker |
| `instance_id` | Process identity; Pod UID when provided, otherwise hostname plus a process-unique suffix |
| `store` | Bound configured Store; Gateway uses the routed Store when known |
| `component`, `event`, `level` | Bounded subsystem, stable event name, debug/info/warn/error |
| `environment`, `cluster`, `namespace`, `pod`, `node` | Optional deployment identity; allowlisted configuration or Downward API values |
| `method`, `phase`, `reason`, `status`, `error_code`, `error_type` | Bounded diagnostic classifications; no raw request/driver error messages |
| `duration_ms`, `queue_ms`, `operations`, `failed`, `bytes`, `attempt` | Numeric diagnostic values encoded as strings for the current consumer |
| `topic`, `partition`, `offset`, `source_topic`, `source_partition`, `source_offset` | DLQ and original Kafka locations; offsets are search fields, never metric labels |
| `dropped`, `suppressed` | Cumulative SDK queue loss and rate-limit suppression counts |

High-cardinality values (instance, Pod, offset) are for filtering and investigation,
not metric labels or dashboard aggregation dimensions. Document bodies are omitted
by default. For severe final failures, `logging.failure_body: true` permits a
bounded diagnostic document **in body only**, never in labels. The supported
event is `kafka_quarantined`, after acknowledged DLQ publication. JSON is readable;
BSON uses base64 with an encoding marker. Truncation is explicit. No payload is
decoded/formatted when this feature is disabled or the event is rate limited.
Do not log record keys, Lua source, native commands, credentials, connection URLs,
or full configuration. Bound message and attribute sizes and count. Drop unapproved
attributes at the common handler so future call sites cannot grow the ES schema.

## Logging behavior

- stderr is enabled by default in text format; JSON is optional.
- OTLP is opt-in and supports gRPC and HTTP/protobuf with explicit endpoint and
  TLS selection. Configuration is loaded once; malformed settings fail config
  validation, but an unreachable Collector does not prevent startup/readiness.
- Default warn/error events cover failed RPC/operation summaries, slow work,
  dependency state changes, retries and dead letters. Debug adds successful RPC,
  batch dispatch, write-phase and Kafka publish/consume summaries. Info records
  lifecycle and recovery. Do not emit a success log per document.
- `logging.level` applies to all components and defaults to warn. Repeated
  warn/error events are rate limited by a bounded event/component/level key;
  suppressed counts are reported. Debug is opt-in and should be enabled briefly.
- Business goroutines never wait for OTLP network I/O. Use the official slog
  bridge and log SDK batching with finite queue/batch/export time limits. Console
  writes retain normal synchronous stderr behavior.
- Queue saturation and delivery failure may lose diagnostics; count/report loss.
  This is best-effort logging, not durable audit delivery. Process crashes can
  lose buffered logs. Memory use must remain bounded during Collector outages.
- Exporter diagnostics go only to a rate-limited stderr path, avoiding recursive
  export. Shutdown closes application resources before a bounded log flush.

## Implementation and verification

Keep configuration/default validation in `internal/config`, logging setup and
handlers in `internal/logging`, and lifecycle ownership in `internal/app`. Retain
direct `slog` calls at operational boundaries. Prometheus metrics provide the
corresponding aggregate signals.

Verify default warn filtering, strict config validation, identity and severity
survival through the ingestion projection, no trace/span data, label allowlisting,
size bounds, rate limiting, actual local gRPC and HTTP OTLP receivers, Collector
failure/queue pressure, bounded shutdown, and request/batch diagnostic events.
Default tests must not contact EKS, Kafka, Elasticsearch or a live Collector.

See [the runtime logging reference](../logging.md) for defaults, configuration,
deployment identity, loss behavior and troubleshooting.
