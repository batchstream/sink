#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sink_dir="$(cd "${script_dir}/.." && pwd)"
sdk_dir="${SINK_GO_DIR:-${sink_dir}/../sink-go}"
artifacts="$(mktemp -d "${TMPDIR:-/tmp}/sink-isolated-smoke.XXXXXXXX")"
project="sink-isolated-smoke-$$"
compose=(docker compose --env-file /dev/null --project-name "${project}" --file "${sink_dir}/examples/quickstart/compose.yaml" --file "${artifacts}/ports.yaml")
cleanup() {
  result="$?"
  trap - EXIT
  "${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  echo "Isolated smoke evidence: ${artifacts}"
  exit "${result}"
}
trap cleanup EXIT
cat > "${artifacts}/ports.yaml" <<'YAML'
services:
  gateway:
    ports: !override
      - "127.0.0.1::8080"
      - "127.0.0.1::8081"
      - "127.0.0.1::9090"
  engine:
    ports: !override
      - "127.0.0.1::8081"
      - "127.0.0.1::9090"
  worker:
    ports: !override
      - "127.0.0.1::8081"
      - "127.0.0.1::9090"
YAML
"${compose[@]}" up --build --detach --wait --wait-timeout 180
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
SINK_INTEGRATION_ADDRESS="$("${compose[@]}" port gateway 8080)" \
  go -C "${sdk_dir}" test -race -tags=integration \
  -run '^(TestSinkCompatibility|TestNativeCompatibility)$' -count=1 -timeout=3m -v | tee "${artifacts}/client-tests.log"
