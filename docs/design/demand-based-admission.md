# Process memory admission

Sink uses one high/low watermark guard per process. It does not track individual
allocations, ownership, reservations, or separate snapshot/output quotas.

## Startup sizing

At startup, the effective ceiling is the smallest detected finite Go memory
limit (`GOMEMLIMIT`), cgroup v1/v2 limit (including ancestors), or host memory
size. If none is available, the fallback is 1 GiB. `memory.max_bytes` may lower
that ceiling; on the fallback path it supplies the ceiling directly. Detection
runs once, so resource-limit changes require a restart.

Before opening dependencies or listeners, Sink estimates minimum working memory:

| Component | Estimate | Roles |
| --- | --- | --- |
| Runtime and drivers | 64 MiB | All |
| Request decoding | 2 × `grpc.max_receive_message_bytes` | Gateway, Engine |
| Response encoding | 2 × `grpc.max_send_message_bytes` | Gateway, Engine |
| Merge documents | 2 × `execution.merge.lua.max_result_bytes` | Engine, Worker |
| Lua source cache | `max_source_bytes` × `max_cached_programs` | Engine, Worker |
| MongoDB wire buffers | 48 MiB | MongoDB Engine, Worker |
| Kafka producer buffer | `producer.max_buffered_bytes` | Engine with Kafka |
| Kafka fetch and decoded batch | 48 MiB | Worker |
| Dead-letter producer buffer | max(64 MiB, `kafka.max_record_bytes` + 16 KiB) | Worker |

The sum must fit **below or at the high watermark**. Otherwise startup panics
with role, effective ceiling and source, high watermark, required bytes, and the
component breakdown. `sink config check` validates files only; it deliberately
does not validate the checking machine's available resources.

With default settings, minimum working memory is 320 MiB for Gateway,
416 MiB for a MongoDB Engine without Kafka, 480 MiB for one with Kafka, and
272 MiB for a MongoDB Worker. At the default 80% high watermark, the effective
ceilings must be at least 400, 520, 600, and 340 MiB respectively. Search omits
the 48 MiB MongoDB allowance. Overrides change these estimates.

These are sizing estimates for a normal work unit, not reservations or a promise
that every concurrent workload fits. Compiled Lua programs, expanded documents,
driver pools and simultaneous work can use more memory. The estimate intentionally
does not multiply every legal maximum by unbounded concurrency or by queue size.

## Runtime behavior

`memory.high_watermark_percent` defaults to 80 and
`memory.low_watermark_percent` to 70, with `0 < low < high < 100`.
Linux measures resident process memory through `/proc/self/statm`. When that is
unavailable or invalid, Sink uses Go runtime memory (`Sys - HeapReleased`). The
metric's `source` label distinguishes these measurements. The latter does not
include all native allocations.

Admission checks and metric scrapes sample at most once per 100 ms. Reaching the
high watermark stops new admission. Admission resumes only at or below the low
watermark, avoiding rapid oscillation around one threshold.

- Gateway rejects a new business RPC with `RESOURCE_EXHAUSTED` before forwarding.
- Engine checks its own process pressure before execution. Private responses mark
  this rejection `not_started`, so Gateway preserves safe retry semantics.
- Worker pauses fetching before the next poll and resumes below the low watermark.
  Already polled work completes its normal processing and settlement. Pausing does
  not acknowledge records or advance offsets; group heartbeats remain active.
- Admitted work continues, including collected batches, conflict retries and
  response encoding. Crossing a watermark never cancels that work.

The request body is already decoded when admission runs. Concurrent arrivals
between samples, admitted work, Kafka prefetch, the runtime, and drivers can still
raise memory above the ceiling. This is overload control, **not an OOM guarantee**.
GC reclamation and RSS release are asynchronous; memory may remain high after a
request finishes. No allocation-level counters or forced GC loops are used.

## Batch and response boundaries

Engine processes the collected batch without further snapshot/output memory
splitting. Record ordering, adapter grouping, completion modes and conflict
retries still determine execution groups. The waiting queue retains its encoded
byte and operation limits; dispatched work no longer counts toward those limits.

The gRPC message ceiling limits each streamed result independently. Gateway
and Engine do not exchange response allowances. A requested returned document
must fit the Engine limit before its own write commits. This is a wire-format limit,
separate from process memory protection. Lua sandbox and Kafka buffer limits also
remain in effect.

See [configuration](../configuration.md#reliability-limits) and
[memory metrics and KEDA](../observability.md#memory-capacity-and-keda).
