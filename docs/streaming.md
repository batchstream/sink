# Streaming responses

`Read`, `Write`, `Query` and `Scan` use server-streaming RPCs. Requests remain
batch-native. Each document/result frame contains one item. `Read` and `Write`
use `operation_index` to identify final results; consumers validate uniqueness,
coverage and successful EOF. Different records may complete out of order.
Writes for the same record retain request order and conditional revision checks.

`Query` and `Scan` send documents in query order followed by a document-free
`complete` frame containing `has_more` or `next_cursor`. EOF must succeed before
those fields can be used. A late backend error can follow document frames. A
callback may therefore have processed part of a failed page: do not checkpoint
it; use idempotent processing when resuming from the previous cursor.

Gateway forwards typed Engine results directly. It never serializes or
reassembles a complete response. Forward protocol version 8 has one `Forward`
stream containing typed results. EOF and errors use standard gRPC status. The
protocol has no Budget, grant/used fields, Tracker or settlement frame. A private
rejection trailer proves non-execution only when no results have been delivered.
No unary or binary-chunk compatibility endpoint remains.

Record execution pulls one existing synchronous microbatch at a time, using the
configured operation/byte batch boundaries. Sending happens outside the shared
batch executor; a slow receiver cannot hold its record scheduling locks. The next
microbatch starts only after sends finish. Merge snapshots/candidates and returned
documents are consequently bounded by the active microbatch rather than the
entire RPC. MongoDB native pages iterate cursors; search pages decode HTTP JSON
hits incrementally and validate trailing shard/timeout metadata before completion.
Search multi-get decodes directly from the HTTP body without a second whole-body
buffer. Existing message, backend response, scan-page and Lua bounds remain;
this change introduces no additional memory-limit setting. Concurrent requests,
transport/driver buffers and one large document still contribute to process RSS.

The Go SDK methods accept `ctx` and a typed request. Request fields hold record
slices, completion mode, and an optional `OnResult` or `OnDocument` callback:

```go
readRequest := sink.ReadRequest{Addresses: addresses}
results, err := client.Read(ctx, readRequest) // Collected in request order.
readRequest.OnResult = onRead
results, err = client.Read(ctx, readRequest) // results is nil.
writeRequest := sink.WriteRequest{
    CompletionMode: mode,
    Operations: operations,
    OnResult: onWrite,
}
written, err := client.Write(ctx, writeRequest) // written is nil.
query.OnDocument = onDocument
queryPage, err := client.Query(ctx, query) // queryPage.Documents is nil.
scan.OnDocument = onDocument
scanPage, err := client.Scan(ctx, scan) // scanPage.Documents is nil.
```

Dataset record methods use `DatasetReadRequest.Keys` and
`DatasetWriteRequest.Records`, with the same `OnResult` behavior. Nil callbacks
collect results; non-nil callbacks consume them without collection. Query/Scan
still return page metadata after successful EOF. New per-call options extend
request structs without adding positional arguments.

Callbacks execute serially, provide backpressure, and cancel the RPC when they
return an error. They receive owned immutable documents. Callback errors are not
retried. Reads retry only unfinished/retryable operations without redelivering
completed results. Writes never retry transport errors. In collection mode,
record methods return acknowledged partial results together with a stream error.
In callback mode, the already invoked callbacks are the acknowledgement record;
the SDK does not retain their documents. Dataset methods still aggregate failure
metadata into `BatchError` without collecting callback documents.

Deploy matching server and SDK versions. Worker consumption, prefetch pausing and
queue processing behavior are unchanged by this refactor.
