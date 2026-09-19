#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
server_dir="$(cd "${script_dir}/.." && pwd)"
artifacts="${SINK_FUZZ_ARTIFACTS:-${server_dir}/.reports/fuzz}"
mkdir -p "${artifacts}"
artifacts="$(cd "${artifacts}" && pwd)"
echo "Unit fuzz evidence: ${artifacts}"
for target in queue:FuzzMutationEnvelope storage:FuzzValidateBSONDocument; do
  package="${target%%:*}"
  name="${target#*:}"
  directory="${artifacts}/${package}"
  mkdir -p "${directory}"
  go -C "${server_dir}" test -mod=readonly "./internal/${package}" -c \
    -fuzz="^${name}$" -o "${directory}/fuzz.test"
  # Go writes minimized failures relative to the test process. Run from the
  # evidence directory so minimized failures stay with the evidence, outside package sources.
  (
    cd "${directory}"
    ./fuzz.test -test.run '^$' -test.fuzz="^${name}$" \
      -test.fuzztime="${FUZZ_TIME:-30s}" -test.parallel=2 \
      -test.fuzzcachedir="${directory}/corpus"
  ) 2>&1 | tee "${artifacts}/${name}.log"
  grep -q 'new interesting:' "${artifacts}/${name}.log"
done
