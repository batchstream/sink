#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "${script_dir}/../.." && pwd)"
artifacts="${SINK_PERF_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-perf.XXXXXXXX")}"
mkdir -p "${artifacts}"
project="sink-perf-$(date +%s)-$$"
export SINK_PERF_IMAGE="${SINK_PERF_IMAGE:-${project}:local}"
compose=(docker compose --env-file /dev/null --project-name "${project}" --file "${script_dir}/compose.yaml")
sampler=""
cleanup() {
  result="$?"
  trap - EXIT
  if [[ -n "${sampler}" ]]; then
    kill "${sampler}" 2>/dev/null || true
    wait "${sampler}" 2>/dev/null || true
  fi
  "${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
  "${compose[@]}" ps --all --format json > "${artifacts}/containers.json" 2>&1 || true
  if ! "${compose[@]}" down --volumes --remove-orphans > "${artifacts}/cleanup.log" 2>&1; then
    result=1
  fi
  echo "Performance evidence: ${artifacts} (exit ${result})"
  exit "${result}"
}
trap cleanup EXIT
git -C "${repo_dir}" rev-parse HEAD > "${artifacts}/revision.txt"
git -C "${repo_dir}" diff HEAD > "${artifacts}/source.patch"
"${compose[@]}" config > "${artifacts}/compose.yaml"
docker info --format '{{json .}}' > "${artifacts}/docker-info.json"
if [[ "${SINK_PERF_BUILD:-1}" == 1 ]]; then
  docker build --tag "${SINK_PERF_IMAGE}" "${repo_dir}" > "${artifacts}/build.log" 2>&1
fi
docker image inspect "${SINK_PERF_IMAGE}" --format '{{.Id}}' > "${artifacts}/image.txt"
go -C "${repo_dir}" build -o "${artifacts}/sink-perf" ./cmd/sink-perf
"${compose[@]}" up --detach --wait --wait-timeout 180
health="$("${compose[@]}" port engine 8081)"
ready=0
for _ in $(seq 1 60); do
  if curl --fail --silent --max-time 3 "http://${health}/readyz?service=sink.storage.mongo" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == 1 ]] || { echo 'MongoDB did not become ready' >&2; exit 1; }
address="$("${compose[@]}" port gateway 8080)"
engine_metrics="$("${compose[@]}" port engine 9090)"
gateway_metrics="$("${compose[@]}" port gateway 9090)"
(
  while true; do
    date -u +%FT%TZ >> "${artifacts}/sample-times.txt"
    "${compose[@]}" stats --no-stream --format json >> "${artifacts}/resources.jsonl" || true
    curl --max-time 3 --silent "http://${engine_metrics}/metrics" >> "${artifacts}/engine-metrics.txt" || true
    curl --max-time 3 --silent "http://${gateway_metrics}/metrics" >> "${artifacts}/gateway-metrics.txt" || true
    sleep 2
  done
) &
sampler="$!"
case "${SINK_PERF_PROFILE:-matrix}" in
  offered)
    cases='fixed-500 upsert 32 1 1024 false 500
fixed-5000 upsert 32 1 1024 false 5000
fixed-20000 upsert 32 1 1024 false 20000'
    ;;
  read-budgets)
    cases='mixed-32 mixed 32 1 1024 false 0
read-32 read 32 16 1024 false 0
returned-large merge 8 1 65536 true 0'
    ;;
  compare)
    cases='upsert-32 upsert 32 1 1024 false 0
upsert-batch upsert 16 16 1024 false 0
merge-32 merge 32 1 1024 false 0
merge-batch merge 16 16 1024 false 0'
    ;;
  matrix)
    cases='upsert-16 upsert 16 1 1024 false 0
upsert-32 upsert 32 1 1024 false 0
upsert-64 upsert 64 1 1024 false 0
upsert-128 upsert 128 1 1024 false 0
upsert-batch upsert 16 16 1024 false 0
merge-32 merge 32 1 1024 false 0
merge-batch merge 16 16 1024 false 0
mixed-32 mixed 32 1 1024 false 0
mixed-8 mixed 8 1 1024 false 0
read-8 read 8 16 1024 false 0
read-32 read 32 16 1024 false 0
count-32 count 32 1 1024 false 0
returned-large merge 8 1 65536 true 0
fixed-500 upsert 32 1 1024 false 500'
    ;;
  *) echo 'SINK_PERF_PROFILE must be compare, matrix, read-budgets or offered' >&2; exit 1 ;;
esac
printf '%s\n' "${cases}" > "${artifacts}/cases.txt"
failed=0
for repeat in $(seq 1 "${SINK_PERF_REPEATS:-1}"); do
  while read -r label workload concurrency batch padding returned rate; do
    dataset="perf-${label}-${repeat}"
    echo "Measuring ${label} repeat ${repeat}"
    # A healthy=false report is evidence of overload, not a reason to skip the
    # remaining cells. Setup/reconciliation errors are retained separately.
    set +e
    "${artifacts}/sink-perf" -address "${address}" -dataset "${dataset}" \
      -workload "${workload}" -concurrency "${concurrency}" -batch "${batch}" \
      -padding "${padding}" -return-document="${returned}" -rate "${rate}" \
      -keys 1024 -duration "${SINK_PERF_DURATION:-30s}" -timeout 3s \
      > "${artifacts}/${label}-${repeat}.json" 2> "${artifacts}/${label}-${repeat}.log"
    result="$?"
    set -e
    printf '%s %s %d\n' "${label}" "${repeat}" "${result}" >> "${artifacts}/exit-codes.txt"
    if [[ "${result}" != 0 ]]; then
      failed=1
    fi
  done <<< "${cases}"
done
exit "${failed}"
