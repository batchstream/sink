# Sink documentation

[Back to the project](../README.md)

## Get started

| Guide | What you will learn |
| --- | --- |
| [Docker Compose quickstart](../examples/quickstart/README.md) | Start Sink, MongoDB, and Kafka and verify the public API |
| [Go client](https://github.com/batchstream/sink-go#quick-start) | Connect an application and read or write typed documents |
| [Store isolation](store-isolation.md) | Gateway, per-Store Engine/Worker, deployment and scaling |
| [Architecture and behavior](architecture.md) | Understand addresses, encoding, batching, ordering, and completion modes |
| [Document write flow](document-write-flow.md) | Follow one document from an RPC through storage or a Kafka worker |

## Build an application

| Guide | What you will learn |
| --- | --- |
| [Native access](native-access.md) | Execute backend commands, query, count, scan, and return atomic update results |
| [Lua merge developer guide](lua-merge-guide.md) | Write merge programs using the `sink.v1` tools |
| [Testing Lua merge programs](lua-testing.md) | Test JSON and BSON fixtures with Sink's production Lua runtime |
| [Record addresses](record-addresses.md) | Canonical URIs, Store paths and Engine affinity |
| [Merge folding](merge-folding.md) | Understand how compatible operations share one storage write |
| [Protocol definition](https://github.com/batchstream/sink-protocol/blob/main/proto/sink/sink.proto) | Inspect the authoritative gRPC messages and services |

## Deploy and operate

| Guide | What you will learn |
| --- | --- |
| [Configuration reference](configuration.md) | Configure named stores, resource limits, Kafka, and process modes |
| [Metrics and health](observability.md) | Inspect metrics, label budgets, and dependency health |
| [Internal diagnostic logs](logging.md) | Configure warn-level console/OTLP logs, ES labels and bounded failure bodies |
| [Adaptive Store backpressure](design/store-backpressure.md) | Dispatch admission, congestion feedback, recovery and independence from application scaling |
| [Batching behavior](batching.md) | Understand collection queues, ordering, and capacity |
| [Reliability and recovery](reliability.md) | Plan idempotence, monitor dependencies, handle failures, and replay dead letters |
| [Production sizing](production-sizing.md) | Size Gateway, Engine and Worker against workload and backend capacity |

## Contribute and qualify changes

| Guide | What you will learn |
| --- | --- |
| [Internal logging design](design/internal-logging.md) | Diagnostic logging scope, field contract and reliability boundaries |
| [Contributing](../CONTRIBUTING.md) | Report an issue and prepare a focused pull request |
| [Development](development.md) | Build, lint, generate protobuf code, run tests, and package releases |
| [Lua benchmarks](https://github.com/batchstream/sink-production-suite/blob/main/benchmarks/lua/README.md) | Compare Lua runtime workloads |
| [Public production suite](https://github.com/batchstream/sink-production-suite) | Run public API conformance and sustained fault qualification |
| [Security policy](../SECURITY.md) | Report a vulnerability privately |
