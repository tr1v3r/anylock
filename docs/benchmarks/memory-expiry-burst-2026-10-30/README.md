# Memory driver expiry-burst renewal benchmark

Measured on 2026-10-30, Apple M3 Pro, darwin/arm64, Go 1.27.0,
GOMAXPROCS=12. Both runs used the same `BenchmarkRenewAfterExpiryBurst` fixture,
without the race detector. The baseline was commit
`55dc446d574c52004bb12a7b837b3c22e1e5439b` plus the benchmark file; the changed
implementation caps opportunistic expired-node reclamation at 64 per operation.

Each iteration constructs a new map and indexed heap with the indicated number
of unrelated entries, all already expired at the same tick, plus one live entry
with a one-hour TTL. Fixture construction is outside the timer. The timer covers
one successful public `Driver.Renew` call for the live key, including its cleanup.
The reported values below are the median `ns/op` across three runs of 20
iterations each. Raw results are in [before.txt](before.txt) and [after.txt](after.txt).

| Expired entries | Before | After |
| --- | ---: | ---: |
| 10,000 | 536,988 ns | 4,658 ns |
| 100,000 | 5,290,383 ns | 6,771 ns |
| 500,000 | 51,792,152 ns | 13,115 ns |

All measured calls reported zero allocations. This is a single-operation cleanup
benchmark, not a production capacity estimate, a hard latency bound, or proof
that total reclamation became cheaper. The changed operation leaves the rest of
the expired backlog for subsequent calls; an idle driver may retain it. The
global gate still serializes unrelated keys. Heap shape, memory locality, GC,
contention, scheduling, CPU load and different expiry distributions affect timing.
No correctness test asserts these timing values.

From the repository root:

```sh
go test -run '^$' -bench '^BenchmarkRenewAfterExpiryBurst$' \
  -benchtime=20x -count=3 ./driver/memory/...
```

Use the same benchmark file on the baseline to reproduce the comparison, in a
separate checkout. A fixed iteration count avoids excessive fixture rebuilding
after measured renewal time becomes small. The existing historical experiments
under `docs/experiments` were not changed.
