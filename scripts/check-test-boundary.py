#!/usr/bin/env python3
"""Keep non-unit qualification owned by sink-production-suite."""

import re
from pathlib import Path


root = Path(__file__).resolve().parents[1]
forbidden = re.compile(
    r"^//go:build.*(?:integration|memoryexperiment)|"
    r"\b(?:grpc\.NewServer|bufconn\.Listen|net\.Listen(?:Packet)?)\(",
    re.MULTILINE,
)
failures = []
for directory in ("cmd", "internal"):
    for source in sorted((root / directory).rglob("*_test.go")):
        if forbidden.search(source.read_text()):
            failures.append(str(source.relative_to(root)))
for relative in ("benchmarks", "cmd/sink-perf"):
    if (root / relative).exists():
        failures.append(relative)
for source in (root / "scripts").glob("test-*integration.sh"):
    failures.append(str(source.relative_to(root)))
if failures:
    raise SystemExit("Move non-unit qualification to sink-production-suite: " + ", ".join(failures))
print("Sink test boundary: component unit tests, input fuzzers, and microbenchmarks")
