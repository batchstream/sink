# Memory completion reserve experiment

Run the opt-in local allocator experiment:

```sh
SINK_MEMORY_EXPERIMENT=1 go test ./internal/capacity -run '^TestBurstExperiment$' -v -count=1
```

The [recorded sample](burst-sample.csv) was collected on 2026-09-18 with Go 1.27
on macOS/arm64. Each combination ran three times with a 32 MiB managed pool,
96 simultaneous arrivals, a barrier before response growth, a 20 ms growth
wait, real backing-array allocations and 200 microseconds of simulated response
retention. Allocation charges already include the modeled response copies.

| Profile | Input per call | Response capacity per call |
| --- | --- | --- |
| small | 2 KiB | 8 KiB |
| mixed | 128 KiB | 8 KiB for 90%, 3 MiB for 10% |
| large-completion | 512 KiB | 3 MiB |

Large-completion results:

| Reserve | Accepted of 96 | Completed per trial | Median elapsed | Peak managed bytes |
| --- | --- | --- | --- | --- |
| 5% | 60 | 0, 2, 3 | 23.661 ms | 30 MiB |
| 10% | 57 | 57, 57, 57 | 8.767 ms | 31.5 MiB |
| 15% | 54 | 54, 54, 54 | 8.393 ms | 30 MiB |
| 20% | 51 | 51, 51, 51 | 7.458 ms | 28.5 MiB |
| 25% | 48 | 48, 48, 48 | 7.555 ms | 27 MiB |

All small and mixed trials completed 96/96 with no response failures. Small
median duration ranged from 0.536 to 0.679 ms; mixed from 2.150 to 2.728 ms.
These timings are scheduler-sensitive, not a statistically established RPS
ranking. Choose **10%** as the lowest tested reserve providing completion
progress in the saturation profile while preserving the largest admission
capacity among successful candidates. Larger reserves admitted fewer calls.

This is an allocator saturation experiment, not an HTTP-server comparison or a
production throughput/RSS qualification. A different response/input distribution
can need another ratio. Keep `memory.burst_percent` configurable and measure
response wait, rejection rate, opaque reservation and RSS in the target workload.
The full test suite separately checks real framed forwarding, actual-size
admission, cancellation, transport reference lifetime, backend document copying,
logical quotas and write outcomes.
