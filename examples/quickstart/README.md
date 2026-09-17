# Gateway / Engine / Worker quickstart

This example runs Gateway, Engine and Worker as separate containers with real
MongoDB and Kafka. Run `make quickstart` to start them and exercise the public API.

```shell
docker compose --env-file /dev/null -f examples/quickstart/compose.yaml build
docker compose --env-file /dev/null -f examples/quickstart/compose.yaml up -d --wait
```

The public endpoint is `127.0.0.1:8080`. Gateway, Engine and Worker metrics are at
ports `9090`, `19091` and `19092`, respectively. Their separate health ports are `8081`, `18081`, and `18082`.
Check producer readiness with
`http://127.0.0.1:18081/readyz?service=sink.kafka.primary` and Worker readiness with
`http://127.0.0.1:18082/readyz` before testing async completion.

With the existing sink-go checkout, exercise the public contract:

```shell
cd ../sink-go
SINK_INTEGRATION_ADDRESS=127.0.0.1:8080 go test -race -tags=integration \
  -run '^(TestSinkCompatibility|TestNativeCompatibility)$' -count=1
```

Stop the example with:

```shell
docker compose --env-file /dev/null -f examples/quickstart/compose.yaml down
```

Store routes are configured in `gateway.yaml` under `gateway.routes`. All listed
routes are active. Restart Gateway after changing its configuration.

For another Store, add its own database target, Engine, Worker and route. Replicas
of one Store share that Store's `storage.name`; different Stores must not share a
database target. The example's plaintext route is for the local Compose network.
See [configuration, migration and scaling](../../docs/store-isolation.md).
