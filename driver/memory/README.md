# In-process memory driver

A standard-library-only implementation of `anylock.Driver`, intended for
coordination inside one process. No service, connection, worker, or shutdown hook
is required.

```go
import (
    "context"
    "time"

    "github.com/tr1v3r/anylock"
    "github.com/tr1v3r/anylock/driver/memory"
)

func run(ctx context.Context) error {
    // Normally construct and share these at application startup.
    driver := memory.New()
    locker, err := anylock.New(driver, anylock.WithAutoRenew())
    if err != nil {
        return err
    }
    lease, err := locker.Lock(ctx, "jobs:123", 10*time.Second)
    if err != nil {
        return err
    }
    defer func() {
        cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
        defer cancel()
        _ = lease.Release(cleanup) // Handle/log cleanup errors in production.
    }()
    return performWork(lease.Context()) // Application-defined cooperative work.
}
```

## Sharing and namespaces

`memory.New() *Driver` creates an independent store and cannot fail. Construct the
driver with `New`: a nil receiver or zero-value `Driver` returns an error. A Driver
is safe for concurrent use but must not be copied. It has no `Close` method.

- Pass the **same pointer** to multiple `anylock.New` calls to share ownership.
- Different Driver instances are independent, even with identical key strings.
- All callers of one instance share the same key space, including execution gates
  and lease locks. Use logical keys such as `gates:event:123` and `leases:event:123`
  to separate domains.
- There is no constructor prefix: the instance itself selects the backend, and a
  prefix on a private instance would not make it share another instance's store.
  Compose application namespaces into the logical key instead.

This is not a distributed lock: different processes, restarted processes, and
separate Driver instances do not coordinate. There is no persistence,
exactly-once execution, or durable business idempotency. Process pauses and the
platform's monotonic-clock behavior (notably system sleep) remain relevant.

## Atomic operations and TTLs

`TryAcquire` creates an absent/expired lease, but returns a zero `AcquireResult`
for any live lease, **including the same owner**. Ordinary success returns
`AcquireResult{Acquired: true}` with Fence zero. `Renew` resets only the matching live owner's
TTL from the current operation; a shorter TTL may shorten ownership. It never
creates or revives an expired lease. `Release` deletes a stored record only when
its owner matches, without requiring remaining validity. Operations sweep expired
entries first, so an expired record may already be absent. Renew returns
`false, nil` for absent, expired, or wrong-owner records; Release returns
`false, nil` if no matching record remains. A stale owner cannot affect its successor. The driver
bool reports an owner-checked deletion, not prior validity. Core `Lease.Release`
treats a missing/successor-owned record as successful, idempotent cleanup.

Keys and owners must be nonempty; other Go string bytes, including NUL and invalid
UTF-8, are accepted without normalization. Acquire/renew require positive TTLs.
Every positive `time.Duration`, including `time.Duration(math.MaxInt64)`, is
supported at exact nanosecond precision without rounding. Expiry is inclusive:
`now >= expiry` is expired. Direct driver users must generate fresh, unguessable
owners for every acquisition; normally the core supplies them.

Expiry uses unsigned nanosecond ticks from `time.Since` a driver-local monotonic
epoch. Unsigned addition accommodates the maximum TTL without overflowing signed
durations or causing `time.Time.Add` to drop monotonic metadata. The ordinary Go
monotonic-clock/duration lifetime horizon applies; this is not centuries-long
persistent timekeeping. Context deadlines retain their normal Go semantics.

## Contexts and storage lifecycle

Operations reject nil contexts and check both cancellation and actual deadlines.
A context-selectable gate bounds synchronization waits; deadline timers also cover
contexts whose `Err`/`Done` have not yet reported expiry. Context and ownership
expiry are checked again **after waiting and sweeping**, rather than using stale
call-entry time. Large sweeps check context between removals. An expired/canceled
wait does not mutate the requested ownership; opportunistic cleanup may already
have removed unrelated expired entries. Every operation checks cancellation and
actual deadlines again before returning either an ownership success or contention.
A late cancellation/deadline failure returns `false, error` even if the mutation
already committed; **an error does not prove that ownership was unchanged**. There
is no rollback: an acquired/renewed marker may remain until TTL, and a release may
already have removed it. As with other atomic APIs, cancellation can also race the
final check and return. The core supplies finite operation deadlines; this backend
adds no arbitrary default timeout and starts no goroutine.

A map plus indexed minimum-expiry heap stores one node per resident key. Renewals
update the existing node, releases remove it, and every valid operation sweeps
expired roots, even when it targets a different key. There are no permanent
per-key tombstones or accumulated renewal records. Normal operations cost
`O(log n)` plus `O(k log n)` for the `k` expired entries swept that time.

**Idle expiry is logical, not immediate physical reclamation.** With no further
operations, expired entries can remain allocated until the driver is collected.
The next operation sweeps them subject to its context. Removed heap slots are
cleared; when the store becomes empty its map and heap are discarded. A partially
occupied store may retain Go map buckets and slice capacity from a high-water
mark. No ticker, maintenance goroutine, exported clock, or manual GC API is needed.

The core's `TryRun`/`Run` gates retain reservations after callback success, error,
panic, or cancellation; they never release or renew, even with `WithAutoRenew`.
The callback may outlive TTL. `TryLock`/`Lock` instead return cooperative leases;
explicit/automatic renewal and `Done` observation belong to the core, not the
driver. Stop protected work when lease validity ends, and release with a live
cleanup context. See the [core contract](../../driver.go) and
[core usage guide](../../README.md).

## Module

Install this driver explicitly with `go get github.com/tr1v3r/anylock/driver/memory`.
It is a separate module depending only on the standard-library-only core.
The following test commands run from this directory (`cd driver/memory`);
repository-wide checks are documented in [MODULES.md](../../MODULES.md).

## Tests

The package includes executable examples for every exported symbol, portable
Go 1.24 unit/concurrency tests, and Go 1.25+ build-tagged `testing/synctest` tests for
exact expiry, post-wait behavior, gates, and automatic renewal:

```sh
go test -race -count=1 ./...
go test -run '^Example' -v ./...
```

### Mandatory bounded concurrency regressions

The ordinary suite (no environment opt-in) also runs:

- Barrier-started 64/256/1024 unique owners against one hot key, exactly one winner.
- Fixed-seed concurrent acquire/renew/release churn with 64 workers on one key and
  64 keys, including wrong/stale-owner operations. An atomic occupancy observer
  checks live ownership; it **does not wrap/serialize driver calls** and is cleared
  before release. Each workload has a 15s context and a 1h TTL, so these checks do
  not mistake callbacks continuing after TTL for a driver defect.
- Repeated controlled expiry turnover with competing successors and stale owners,
  plus a batch of 1024 simultaneously expired keys swept by unrelated operations
  racing 64 successor workers. Old map/heap storage must be cleared, each successor
  must survive stale-owner calls, and final cleanup must discard both containers.
  Only quiescent, gate-protected private expiry state is changed; no production
  clock hook or wall-clock sleeps are needed. This coverage runs on Go 1.24 too;
  existing Go 1.25+ synctest tests additionally exercise real exact clock boundaries.
- 256 canceled gate waiters across all three APIs, observing actual gate-wait
  arrival before cancellation, plus heap/map consistency and no renewal/release
  tombstones after concurrent churn. An anchor keeps storage allocated during
  churn so empty-store disposal cannot hide leaked nodes; final empty containers
  must be discarded.

These are part of the existing default CI Go 1.24/current race and coverage jobs,
not optional stress-only coverage. Repeat just the bounded tests locally:

```sh
go test -race -count=10 -run '^TestBounded' ./...
```

### Optional sustained diagnostic stress

`TestMemoryStress` skips unless `ANYLOCK_MEMORY_STRESS=1`. It runs **both** hotkey
and multikey profiles, each for the configured duration. Bounds are validated
before starting any worker; invalid values fail rather than silently clamp:

| Environment variable | Default | Inclusive bounds |
| --- | --- | --- |
| `ANYLOCK_MEMORY_STRESS_DURATION` | `10s` per profile | `1s`..`5m` |
| `ANYLOCK_MEMORY_STRESS_WORKERS` | `64` | `1`..`1024` |
| `ANYLOCK_MEMORY_STRESS_KEYS` | `64` (multikey only) | `1`..`4096` |

```sh
ANYLOCK_MEMORY_STRESS=1 ANYLOCK_MEMORY_STRESS_DURATION=30s \
  ANYLOCK_MEMORY_STRESS_WORKERS=128 ANYLOCK_MEMORY_STRESS_KEYS=256 \
  go test -race -count=1 -timeout=12m -run '^TestMemoryStress$' -v ./...
```

Workers call the real shared driver concurrently with independent fixed-seed key
streams. A 1h TTL is longer than the maximum bounded run; expiry correctness is
covered separately, not by asserting callback exclusion after TTL. The workload
mixes acquisitions, matching renewals/releases, and wrong/stale-owner attempts.
Ownership invariants, at least one successful acquire, attempts on all three APIs,
unexpected errors, and final heap/map cleanup are hard checks; **there are no fixed
throughput/latency floors**. Renew/release need not report a success before shutdown:
those calls may only complete with an expected, potentially committed context error.
Shutdown cancellation/deadline errors are expected, including an operation that
committed before its final context check failed. The recorder also conservatively
discards a **nil-error result first observed after the workload context ends**:
that locally late observation contributes to `expected_cancel_or_deadline`, not
success/contention or the occupancy observer. This does not claim that the driver
returned a cancellation error, that ownership remained unchanged, or that a
committed mutation was rolled back. Genuine non-context errors remain unexpected.
Cleanup uses actual resident owners and a live context, never a rollback assumption.

Each profile emits a `MEMORY_STRESS_JSON ` line containing one JSON object:

- Per API: attempts, successes, false results (contention/absent/wrong owner),
  expected cancellation/deadline errors, unexpected errors, and attempts/second.
  Outcome counts partition attempts. Overall QPS counts **API calls**, not lease
  cycles; the denominator is actual elapsed time from worker setup through join,
  excluding cleanup and reporting (not merely the requested timer duration).
- Per API: approximate p50/p95/p99 in **nanoseconds**, reported as inclusive upper
  bounds of fixed log2 histogram buckets over **all** outcomes. Bucket 0 contains
  zero; bucket i contains `[2^(i-1), 2^i)` ns. These are quantized bounds, not exact
  percentiles. No unbounded latency sample list is retained: recorder storage is
  `O(workers * 3 * 64)` counters (about 1.6 MiB at 1024 workers), plus bounded
  key/occupancy state. Allocations made by the workload itself remain measurable.
- Process-global allocation bytes/malloc deltas, heap bytes/objects, GC cycles and
  cumulative GC pause deltas, before/after goroutines, and 100ms-sampled heap and
  goroutine peaks. Sampling can miss transients; before/after are not leak proofs
  or isolated driver-only allocation costs. Cleanup and forced GC are not included
  in these snapshots. Machine/toolchain/GOMAXPROCS are recorded for context.

The backend's **single global gate serializes unrelated keys too**: multikey does
not imply parallel driver execution. Contention, gate waiting, PRNG/string creation,
telemetry, sampling, scheduling and especially `-race` affect these diagnostics;
they are not production capacity benchmarks or a cross-backend ranking. This new
live report is separate from the repository's immutable historical performance
archive and does not revise or reproduce those archived measurements.

### Backend-specific CI and pre-merge evidence

[Memory stress diagnostics](../../.github/workflows/memory-stress.yml) runs on
**every pull request**, explicitly enabling the optional reporter for a short
**1s per profile / 16 workers** race smoke. This proves the reporter actually runs
pre-merge; mandatory bounded regressions still run in the existing default CI.
`workflow_dispatch` supports longer bounded runs with duration/worker/key inputs
passed only via environment, never interpolated into shell code. No scheduled
stress is installed. The job has a 20-minute limit; each binary has a 12-minute
limit covering both maximum 5-minute profiles plus cleanup.

The workflow builds the race test binary first, then Ubuntu `/usr/bin/time -v`
measures **test execution, not compilation**, including CPU time and maximum RSS.
Metrics JSONL, verbose test/build logs, environment and resource data are uploaded
with `always()` even after test failure (subject to runner cancellation/failure).
The extractor also fails if both profile reports are not present. Locally, use a
separately compiled binary with the platform's own time tool for comparable process
resources; macOS `/usr/bin/time -l` is not Ubuntu's `-v` interface.

GitHub only discovers a **new manual workflow after its file lands on the default
branch**. Before merge, do not claim a successful manual dispatch of this new file:
use the PR smoke plus a separately recorded sustained local run as current evidence.
A green short smoke alone is not sustained-load evidence.

## Explicit fencing

Use `anylock.New(driver, anylock.WithFencing())`. Successful fenced acquisitions
use an instance-wide `uint64` counter under the same gate as ownership. Renewals
do not increment it; release/expiry/sweeping never reset it. Exhaustion errors
before installing a new lease. Tokens are meaningful only while sharing the same
Driver instance; replacing/restarting it resets the sequence.

Read [the shared fencing contract](../../docs/fencing.md): business storage must
atomically validate `Lease.Fence()` with protected writes.
