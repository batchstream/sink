# Record URIs and Engine affinity

Every record address is one canonical URI:

```text
sink://<store>/<store-defined-path>
```

`store` selects a configured logical Store, not a driver or network endpoint.
Gateway interprets only this name. Each Store adapter validates and interprets
its own path. Engine's shared execution code never interprets database names,
index names, path length, or key types.

| Adapter | Record URI example | Adapter interpretation |
| --- | --- | --- |
| MongoDB | `sink://orders-db/catalog/orders/s:order-42` | database / collection / typed key |
| Elasticsearch or OpenSearch | `sink://orders-search/orders/s:order-42` | index or designated alias / typed key |
| Future adapter | `sink://archive/tenant/bucket/object/version` | entirely defined by that adapter |

The public protobuf `RecordAddress` contains only `string uri = 1`. Native
commands retain their backend-specific command fields; they are not record
addresses and have no record affinity.

## One canonical identity

The complete canonical URI is the record identity in Gateway, Engine record
folding, Kafka message keys, and Worker ordering. There is no backend-specific
identity hook. Storage's `BatchKey` only selects which physical resource can
share a backend batch. Different records in the same collection or index can
share that batch without sharing record identity.

Canonical syntax is implemented once in `github.com/batchstream/sink-protocol/uri`:

- Scheme is exactly `sink://`. Store names are lowercase ASCII, start with a
  letter or digit, and otherwise contain letters, digits, `.`, `_`, or `-`.
- A URI has at least one nonempty path segment and is at most 16 KiB. Store names
  are at most 256 bytes. Empty and dot segments are rejected.
- Segments contain UTF-8 and use Go `url.PathEscape` canonical escaping. Parsers
  reject alternate spellings instead of silently normalizing them. Encoded `/`
  remains part of one segment. No path cleaning or case folding occurs.
- Query, fragment and user information are not address fields. Characters such
  as `?` and `#` inside a segment must be percent encoded.

Built-in adapters use typed keys in the final segment:

| Key type | Decoded segment |
| --- | --- |
| String | `s:order-42` |
| int64 | `i:42` (canonical decimal) |
| Bytes | `b:<unpadded base64url>` |
| Opaque | `o:<base64url type>:<base64url data>` |

String `42` and integer `42` remain distinct records. Use the URI builder for
slashes, percent signs, Unicode or binary keys. Each adapter enforces its own
supported key types and key limits. A custom adapter may use a different path
grammar; core routing does not require typed keys.

Each logical record must have one URI. Adapters must consume every segment and
reject extra segments or alternative key spellings. Configure one designated
resource name for a search index: writing through both an index and its alias
creates two URI identities. Similarly, do not expose one database target through
multiple Store names when record ordering matters. Canonical syntax cannot
infer administrative aliases inside a database.

## Routing and connections

Gateway keeps a periodically refreshed DNS membership view for each Store.
DNS must return individual Engine addresses (for example, a Kubernetes headless
Service), so Gateway can select a replica directly. A load-balancer VIP hides
replica membership and cannot provide this affinity. Plain service targets use
DNS discovery; explicit `passthrough:///IP:port` targets select one literal endpoint.
A public record RPC takes one membership snapshot per Store, then uses
rendezvous hashing of the full URI and each endpoint to choose an Engine.
DNS answer order and Gateway process identity do not affect the choice.

Gateway groups operations by Store and chosen Engine, preserving operation order
for repeated records and restoring original result indexes. Each result must fit
the local message ceiling; independent groups run with bounded fanout. Native requests select an
endpoint in round-robin order because they have no record key.

Connections are opened lazily per endpoint and reused. `forwarding.max_connections`
bounds active/idle endpoint channels and cached discovery entries; the DNS view
for a Store cannot exceed that endpoint bound. Idle entries expire or are evicted
under pressure. Active entries are not evicted. TLS verification uses the original
service hostname or the configured `server_name`, even when dialing a resolved IP.
DNS refresh does not reconnect healthy channels. A DNS error retains the last
successful membership view.

With the same membership set, the same URI selects the same Engine. Adding an
endpoint moves only records won by that endpoint; removal moves only its records.
A selected endpoint failure fails that attempt. Gateway does not replay a mutation
on another endpoint when its result is unknown. Recovery follows connection
recovery or refreshed membership.

Affinity improves local batching; it is not exclusive ownership or global
ordering. Scaling, faults and temporarily different Gateway DNS views can send a
record to old and new Engines concurrently. There are no leases, fencing tokens
or migration handoffs. Engine concurrency control and Kafka's at-least-once
processing contracts continue to apply.

## SDK

```go
address, err := sink.NewAddress("sink://orders-db/catalog/orders/s:order-42")
address, err = sink.NewRecordAddress("sink://orders-db/catalog/orders", sink.StringKey("order/42"))
opts := sink.DatasetOptions{
    URI: "sink://orders-db/catalog/orders",
    Encoding: sink.DocumentEncodingBSON,
}
orders, err := sink.NewDataset(client, opts)
```

## Native resources

Execute, Query, Count and Scan also select their resource with `Command.uri`.
The URI contains no operation: `sink://search/products` identifies an index;
`Command.path = "/_search"` selects an HTTP operation on it. MongoDB uses
`sink://mongo/catalog` to select the database and an ordered BSON command for
its operation and collection. `sink://store` selects a Store-level resource.
Gateway routing reads only the URI Store. The adapter validates the resource
and combines it with the native operation. There are no separate public Store
or Namespace fields, aliases, or old Command decoders. Scan checkpoints bind
the complete URI and command, so changing resources invalidates a checkpoint.
