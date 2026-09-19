# Development

Sink requires Go 1.27 or newer. Docker with Compose is required for the
quickstart and external storage integration suites.

## Next architecture design

Before working on the Gateway and single-store Engine/Worker redesign, read the
[design agreement and review decisions](design/store-isolated-architecture.md).
The user has authorized implementation and testing. Preserve the confirmed
constraints and record implementation decisions and validation in that document.
This authorization does not include a release or a production deployment.

## Validation

Build and run the normal checks from the repository root:

```shell
make build
make test
make lint
go test -race ./... -count=1
```

`make build` writes `bin/sink`. The default tests use local test doubles and an
in-process Kafka broker; they do not require external services. Go may download
module dependencies on the first run. `make lint` checks formatting, vet, and
staticcheck without changing source files. Use `make fmt` to apply formatting.

The Sink checkout contains only component unit tests. Real transport and process
assembly tests, backend integrations, fuzz targets, benchmarks and experiments
belong to [sink-production-suite](https://github.com/batchstream/sink-production-suite).
Controlled storage doubles, offline MongoDB wire fixtures and fake HTTP/Kafka
backends used to test a single adapter remain unit tests. Tests which assemble
multiple Sink components or run real gRPC/OTLP paths belong to the suite.

Run qualification from that checkout:

```shell
SINK_SERVER_DIR=/path/to/sink make test-candidate
SINK_SERVER_DIR=/path/to/sink make test-server-integration
SINK_SERVER_DIR=/path/to/sink make test-isolated-quickstart
```

The suite owns the [test runner and classification](https://github.com/batchstream/sink-production-suite/blob/main/server-tests/README.md).
Sink CI calls its pinned reusable workflow; the required reliability gate still
requires compatibility, backend, conformance, quickstart and benchmark checks.
`make lint` prevents benchmark/fuzz entries, integration tags and gRPC server
fixtures from being added back to this repository. Product examples remain here;
use `make quickstart` to try the documented example and `make quickstart-down`
when finished.

### Repository checks

`make lint-workflows` runs the pinned actionlint version against GitHub Actions
workflows. ShellCheck is excluded from this target so the result does not depend
on an optional local installation. CI also checks local Markdown links,
images, and heading anchors with [lychee](https://github.com/lycheeverse/lychee).
Install the version used by the pinned lychee action (currently `v0.24.2`) to
reproduce the check:

```shell
make lint-workflows
make lint-docs
```

Link checks run offline to avoid making repository CI depend on third-party
website availability. External URLs should still be checked when editing them.
All repository checks feed the required **Sink reliability gate** along with
the existing behavior and packaging jobs.

Dependabot proposes weekly Go module, GitHub Actions, and base-image updates
with bounded open PR counts. Review them through the normal qualification gate.
The production suite's reusable workflow references are excluded: update the
workflow SHA and `suite_ref` together in CI and release configuration.

### Protobuf changes

Only regenerate protobuf files when changing the protocol or generator versions.
Install `protoc` and the Go plugins used by the generated files:

```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1-0.20240319094008-0393e58bdf10
export PATH="$(go env GOPATH)/bin:$PATH"
make proto
```

Include changes under `gen/sink` in the PR. CI compares the public protocol with
the Go client, using a matching client branch when available and `main` otherwise.

## Performance qualification

Server microbenchmarks, Lua runtime comparisons, the memory reserve experiment
and the fixed-resource load runner are owned by the production suite. See its
[benchmark measurements](https://github.com/batchstream/sink-production-suite/blob/main/docs/server-benchmarks.md)
and [runner commands](https://github.com/batchstream/sink-production-suite/blob/main/server-tests/README.md).

## Repository layout

- `proto/sink` defines the public gRPC contract.
- `internal/service` implements validation, ordering, batching, puts, and Lua
  merge retries.
- `internal/storage` routes operations to independently configured adapters.
- `internal/storage/mongodb` implements MongoDB storage.
- `internal/storage/search` implements the shared Elasticsearch and OpenSearch
  adapter.
- `internal/queue` routes asynchronous mutations to the selected publisher.
- `internal/queue/kafka` implements durable publication and manual-offset
  consumption.
- `internal/worker` applies queued mutations through the synchronous service
  path.
- `internal/storage/memory` is the deterministic test and local-development
  adapter.
- `cmd/sink` loads configuration and assembles a Gateway, Engine, or Worker process.

## Release artifacts

Publishing a GitHub Release triggers `.github/workflows/release-image.yml`.
Semantic version tags such as `v0.3.2` publish `0.3.2`, `0.3`, and `0` image
tags. A non-prerelease also publishes `latest`. Images are available for
`linux/amd64` and `linux/arm64` at `ghcr.io/batchstream/sink`.

The legacy `ghcr.io/liran/sink` package remains unchanged. The manual
`copy-legacy-images.yml` workflow copies its existing tags to the organization
package, preserving manifest digests, all platforms, and embedded attestations.
It refuses to overwrite an organization tag with a different digest. New
releases publish to the current repository's GHCR namespace only. The
organization package must be public for anonymous pulls.

The Release description is updated after publication with the complete tagged
image address, pull command, and immutable image digest. For `v0.3.2`, the
primary image is:

```text
ghcr.io/batchstream/sink:0.3.2
```

Every Release also includes `checksums.txt` and standalone archives for these
targets:

| Platform | Architecture | Asset format |
| --- | --- | --- |
| Linux | amd64 | `sink_v0.3.2_linux_amd64.tar.gz` |
| Linux | arm64 | `sink_v0.3.2_linux_arm64.tar.gz` |
| macOS | amd64 | `sink_v0.3.2_darwin_amd64.tar.gz` |
| macOS | arm64 | `sink_v0.3.2_darwin_arm64.tar.gz` |

`amd64` means 64-bit x86 and `arm64` means 64-bit ARM. Archives contain the
Sink binary, `LICENSE`, and `README.md`. CI builds all four targets before a
Release can rely on the packaging script. To reproduce the assets locally in
an empty output directory:

```shell
scripts/build-release-binaries.sh v0.3.2 dist
```

Windows archives are not currently published because the pinned Lua runtime
does not compile for Windows. The workflow deliberately excludes Windows
instead of attaching an untested binary.

## Reliability regression and sustained validation

Default race tests include fake-broker outage recovery beyond the retry budget,
DLQ publication failure, CREATE replay continuation, partition-prefix commits,
rebalance cancellation, admission/cancellation, and read/Lua output budgets.
The suite fuzzes mutation envelopes and BSON inputs for 30 seconds each on every change.

The public [production suite](https://github.com/liran/sink-production-suite)
owns release and sustained qualification. Sink's release workflow pins both the
reusable workflow and suite source to the same immutable commit. The manual
`.github/workflows/reliability.yml` entry point invokes that suite's two-hour
workflow against the selected Sink revision; the nightly schedule lives in the
suite repository.

The shared runner uses a unique disposable Compose project, product worker retry
defaults, an active-worker SIGKILL, a 45-second backend outage, and a broker
restart. It checks dependency readiness, API size/read/Lua-output limits, lag,
and DLQ inspection/repair/replay, retaining revisions, fault timelines, test
output, container resource samples and Prometheus samples. With a working Docker
engine and available qualification ports, run it from the suite checkout:

```sh
SINK_SERVER_DIR=/path/to/sink make test-production
SINK_SERVER_DIR=/path/to/sink make test-reliability
```

The first command includes a six-minute fault workload; the second uses two
hours. The business counter implements application idempotence and reconciles
stored results. Passing the short gate does not establish a completed two-hour
run. Real multi-node failover, disk pressure and backup restoration remain
deployment qualification; see [the reliability runbook](reliability.md).

## Coverage regression gate

`make test-coverage` runs unit tests with the race detector and writes
coverage, JSON test events and a package summary to `.reports/coverage/`.
CI enforces the package floors in `.github/coverage-minimums.json`; generated
protobuf files do not count. Keep floors stable or raise them when adding tests.
The report is statement coverage, not branch or end-to-end scenario coverage.
The unit-only baseline excludes the migrated transport and assembly scenarios.
The suite retains the previous combined package floors in its
`.github/candidate-coverage-minimums.json` and enforces them over unit plus
suite-owned component tests. Lower unit-only numbers are a change of measurement
scope; the combined gate must not be reduced to accommodate this migration.
Real backend profiles remain separate and are retained by the suite.
