# Component configuration examples

Copy the file for the component you are deploying. Every field has a comment;
MongoDB is the active backend example, with a search-driver alternative in the
Engine and Worker files.

| File | Component |
| --- | --- |
| [gateway.yaml](gateway.yaml) | Public request limits, inline Store routes, and forwarding capacity. |
| [engine.yaml](engine.yaml) | One Store's database, execution, batching, Lua, and optional Kafka publishing. |
| [worker.yaml](worker.yaml) | One Store's database, Kafka consumption, execution, and Lua. |

Every role serves `/livez` and `/readyz` on `health.address` (default `:8081`).
Prometheus uses its own `prometheus.address` (default `:9090`) and is disabled
unless `prometheus.enabled: true`. Disabling metrics never disables health checks.

Logs default to warn-level JSON on stderr. See the [logging reference](../docs/logging.md)
for direct OTLP export, component debug levels and optional severe-failure bodies.

Gateway routes live under `gateway.routes` and all configured routes are active.
Configuration is loaded at startup; restart the component after changes.

Validate without connecting to databases or Kafka, from the repository root:

```sh
sink config check --config configs/gateway.yaml
sink config check --config configs/engine.yaml
sink config check --config configs/worker.yaml
```

See the [configuration reference](../docs/configuration.md) for constraints and
the [quickstart](../examples/quickstart/README.md) for a runnable three-role stack.
