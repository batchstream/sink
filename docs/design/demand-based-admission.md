# Demand-based memory admission

The application uses one process-local capacity pool for Gateway, Engine or
Worker. Logical per-RPC document limits remain independent of allocation
ownership: increasing `grpc.max_send_message_bytes` no longer prepays that
maximum for every ordinary request.

## Capacity and admission

`memory.max_bytes` overrides automatic sizing. Otherwise Sink takes half of the
smallest available Go memory limit, cgroup v1/v2 ancestor limit, or Linux host
memory total. An environment without a discoverable limit uses 256 MiB. Detection
runs at startup; changing container limits requires restarting the process.
The other half provides headroom for runtime metadata, ingress decoding, Kafka,
Lua heaps, caches and GC. This is a managed capacity limit, **not an RSS cap**.

`memory.burst_percent` defaults to 10, leaving 90% for ordinary allocations.
The default is the smallest tested reserve that completed every accepted request
in the saturation experiment; see [the experiment](https://github.com/batchstream/sink-production-suite/blob/main/benchmarks/memory-admission/README.md).
It is configurable from 1 to 99 and is not a universal optimum.

New requests acquire known input, small completion envelopes and forwarding
scratch space from ordinary capacity. They fail immediately when unavailable,
so decoded requests cannot accumulate in an unaccounted admission queue.
Already admitted requests in the batching queue keep their input ownership.
There is no request concurrency cap in this policy. Existing backend connection,
fanout, batch queue and Kafka buffer bounds still apply.

Response/working-set growth has priority over new arrivals. Growth acquires the
whole next allocation atomically; only one completion owner can borrow the
reserve at a time. Coalesced work is one owner, but each original RPC retains and
releases its own output leases. This prevents a batch waiting on its own reserve
while preserving original caller ownership.

Response waits end at `memory.wait_timeout` (default 2s) or the caller deadline,
whichever comes first. At most 1024 growth acquisitions can wait. An allocation
that cannot fit even after every other owner releases fails immediately. A
reserve smaller than the next allocation cannot guarantee progress when other
requests retain all ordinary space; bounded failure is intentional. Oversized
responses and sustained saturation still require smaller requests or capacity.

## What is charged

- Generated `SizeVT()` measures protobuf wire size, not Go heap size. Input
  accounting adds object/envelope allowances. Known returned documents reserve
  their retained payload and eventual encoded copy together, before commit.
  Shared Lua declarations remain shared during parsing; asynchronous writes
  reserve their expanded queue envelopes before serialization and enqueueing.
- Read snapshots acquire capacity before adapter copies. Original caller outputs
  acquire separately before response copies. Logical snapshot/input/output/
  returned-document quotas still belong to the original RPC across retries,
  batching and Gateway fanout.
- Search HTTP bodies acquire backing-array capacity before growth, including the
  overlap of old/new arrays and JSON decoding space. Lua VM allocation is not
  controlled by protobuf size; existing Lua limits and process headroom remain
  necessary. Discarded HTTP bodies and rejected candidates release capacity
  immediately; retained decoded documents keep their own reservations.
  Candidates acquire capacity before retention and commit.
- MongoDB hides complete wire allocation inside its driver. Reads and native
  operations reserve a temporary 48 MiB allowance around database work. It is
  included in used bytes and reported separately as opaque reservation. It is
  not charged at RPC entry and does not scale with the configured response limit.
  Count's internal cursor pages release working-set capacity after consumption.
- Small completion envelopes are acquired before execution. Failure to acquire a
  returned document fails that operation before its write; completed operations
  keep their result. Transport failure after a write remains an ambiguous reply,
  and never authorizes replaying a mutation.

## Ownership and transport

RPC scopes are reference counted. Cancellation does not free bytes while a
batch producer still owns its input. Service completion does not release a
response's encoding allocation while HTTP/2 still holds buffer references.
The VT codec attaches ownership to the final `grpc/mem.Buffer` release. Tiny
messages use an allocation above grpc's small-buffer threshold, since
`SliceBuffer.Free` otherwise provides no release callback.

Public Sink RPCs and generated public messages are unchanged. The private Engine
adds `ForwardStream`: first an encoded-size frame, then chunks of at most 32 KiB.
Gateway acquires the announced actual size before continuing receipt. Its fixed
frame receive limit also bounds a dishonest peer; the announced size cannot
raise that limit. Static 64 KiB stream and 1 MiB connection windows avoid
unbounded response prefetch while capacity is unavailable.

Upgrade Engines before Gateways. Old Gateways can continue calling the retained
unary `Forward` method. A new Gateway does not fall back to unbounded unary
receiving or replay an uncertain request when `ForwardStream` is unavailable.
Rollback Gateways before Engines. See [rolling upgrades](../rolling-upgrades.md).

## Observability and compatibility

The `sink_memory_*` family exposes ordinary/reserved capacity, owned bytes,
opaque driver reservations, pending growth bytes/count/age, reserve use,
acquisitions, rejections and wait duration, with bounded `role`/`store` labels.
The KEDA example uses fleet pressure with `metricType: Value`, plus a rejection
ratio to capture bursts that disappear between scrapes. Missing telemetry is an
error, not zero load. Keep synchronous replica floors above zero and monitor
process RSS separately. See [observability](../observability.md#memory-capacity-and-keda).

The CLI always enables this policy. Older `gateway.max_bytes`, Gateway request
count fields, `service.execution` and `service.publish` admission fields were previously
accepted for compatibility; the role-config schema now rejects them. Application assembly always supplies the shared pool.
Batch queue limits, request/response size limits, deadlines and backend resource
limits remain active. Migrate autoscaling away from the old reservation gauges.
