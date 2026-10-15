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
separate Driver instances do not coordinate. There is no persistence, fencing,
exactly-once execution, or durable business idempotency. Process pauses and the
platform's monotonic-clock behavior (notably system sleep) remain relevant.

## Atomic operations and TTLs

`TryAcquire` creates an absent/expired lease, but returns `false, nil` for any live
lease, **including the same owner**. `Renew` resets only the matching live owner's
TTL from the current operation; a shorter TTL may shorten ownership. It never
creates or revives an expired lease. `Release` removes only a matching live lease.
Absent, expired, or wrong-owner renewals/releases return `false, nil`; a stale owner
cannot affect its successor. A repeated direct driver release returns false,
unlike successful core `Lease.Release` calls, which are locally idempotent.

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

## Tests

The package includes executable examples for every exported symbol, portable
Go 1.24 unit/concurrency tests, and Go 1.25+ build-tagged `testing/synctest` tests for
exact expiry, post-wait behavior, gates, and automatic renewal:

```sh
go test -race -count=1 ./driver/memory
go test -run '^Example' -v ./driver/memory
```
