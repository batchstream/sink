# Component configuration examples

Copy the file for the component you are deploying. Every field has a comment;
MongoDB is the active backend example in the shared Store file.

Both backend examples include file-based credential alternatives (`uri_file`,
`username_file`, `password_file`, and `api_key_file`). Remove the matching inline
key before using its file variant, even when the inline value is empty. Basic
authentication requires a username and password and cannot be combined with an
API key. Credential files are read at startup; restart after rotating them.

| File | Component |
| --- | --- |
| [gateway.yaml](gateway.yaml) | Public request limits, inline Store routes, and forwarding capacity. |
| [engine.yaml](engine.yaml) | Execution, batching, Lua, and Kafka publishing buffer. |
| [worker.yaml](worker.yaml) | Kafka consumption, execution, and Lua. |
| [stores/primary.yaml](stores/primary.yaml) | Shared Store identity, database connection and Kafka Topic policy. |

Every role serves `/livez` and `/readyz` on `health.address` (default `:8081`).
Prometheus uses its own `prometheus.address` (default `:9090`) and is disabled
unless `prometheus.enabled: true`. Disabling metrics never disables health checks.

Logs default to warn-level text on stderr. See the [logging reference](../docs/logging.md)
for JSON output, direct OTLP export, log levels and optional severe-failure bodies.

Gateway routes live under `forwarding.routes` and all configured routes are active.
Configuration is loaded at startup; restart the component after changes.

Validate without connecting to databases or Kafka, from the repository root:

```sh
sink config check --config configs/gateway.yaml
sink config check --config configs/engine.yaml --store-config configs/stores/primary.yaml
sink config check --config configs/worker.yaml --store-config configs/stores/primary.yaml
```

See the [configuration reference](../docs/configuration.md) for constraints and
the [quickstart](../examples/quickstart/README.md) for a runnable three-role stack.
