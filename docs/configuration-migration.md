# New-cluster deployment

This release uses a new URI-only record protocol and canonical URI Kafka keys.
Deploy matching Gateway, Engine, Worker and SDK builds in a separate cluster.
Use new Kafka topics and consumer groups; old mutation envelopes and scan
checkpoints are not accepted migration inputs. Perform the client cutover through
blue/green deployment after validating the new cluster.

Use the current [configuration reference](configuration.md),
[component examples](../configs/README.md), and [runtime guide](store-isolation.md).
Only `gateway`, `engine` and `worker` modes are supported. Engine and Worker use
one `storage` object. Gateway uses a route file and has no database configuration.
Store names use the lowercase syntax in [record addresses](record-addresses.md).
HTTP health endpoints always run at `http.address` (default `:9090`); replace
`prometheus.address` with `http.address`. Prometheus `/metrics` alone requires
`prometheus.enabled: true`. Disabling metrics does not disable health checks.

Validate configurations offline with `sink config check --config FILE`, then
verify the seven public RPCs, key affinity and asynchronous processing against
isolated resources before switching clients. This repository change does not
perform deployment or database migration.
