# Component configuration examples

Copy the file for the component you are deploying. Every field has a comment;
MongoDB is the active backend example, with a search-driver alternative in the
Engine and Worker files.

| File | Component |
| --- | --- |
| [gateway.yaml](gateway.yaml) | Public request limits, routing, and forwarding capacity. |
| [engine.yaml](engine.yaml) | One Store's database, execution, batching, Lua, and optional Kafka publishing. |
| [worker.yaml](worker.yaml) | One Store's database, Kafka consumption, execution, and Lua. |
| [routes.yaml](routes.yaml) | Gateway's separate routing table; keep it alongside `gateway.yaml`. |

HTTP `/livez` and `/readyz` are always available at `http.address` (default
`:9090`). Prometheus is disabled by default. Set `prometheus.enabled: true` to
add `/metrics` on the same listener; the flag does not affect health checks.

Validate without connecting to databases or Kafka, from the repository root:

```sh
sink config check --config configs/gateway.yaml
sink config check --config configs/engine.yaml
sink config check --config configs/worker.yaml
```

See the [configuration reference](../docs/configuration.md) for constraints and
the [quickstart](../examples/quickstart/README.md) for a runnable three-role stack.
