#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose=(docker compose --env-file /dev/null --project-directory "${script_dir}" --file "${script_dir}/compose.yaml")

# Build the shared local image before starting image-only roles.
"${compose[@]}" build
"${compose[@]}" up --detach --wait
for role in engine worker; do
  health_address="$("${compose[@]}" port "${role}" 8081)"
  endpoint="http://${health_address}/readyz"
  if [[ "${role}" == engine ]]; then
    endpoint+="?service=sink.kafka.primary"
  fi
  ready=false
  for _ in $(seq 1 60); do
    if curl --fail --silent --max-time 2 "${endpoint}" >/dev/null; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != true ]]; then
    echo "${role} did not become ready" >&2
    exit 1
  fi
done
"${compose[@]}" --profile test run --build --rm --no-deps example

metrics_file="$(mktemp)"
trap 'rm -f "${metrics_file}"' EXIT
curl --fail --silent --show-error http://127.0.0.1:19091/metrics --output "${metrics_file}"
grep -q '^sink_build_info' "${metrics_file}"
grep -Eq '^sink_grpc_server_requests_total\{.*method="Read".*\} [1-9][0-9]*$' "${metrics_file}"

echo
echo "Sink quickstart is running."
echo "gRPC endpoint: 127.0.0.1:8080"
echo "Prometheus metrics: http://127.0.0.1:9090/metrics"
echo "Engine metrics: http://127.0.0.1:19091/metrics"
echo "Worker metrics: http://127.0.0.1:19092/metrics"
echo "Stop it with: docker compose -f ${script_dir}/compose.yaml down"
