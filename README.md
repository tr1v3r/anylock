# anylock

Backend-independent fixed-TTL execution gates and owner-checked lease locks for
Go 1.24+. The core uses only standard-library packages. Redis, PostgreSQL and
process-local memory are provided as independent driver modules. Each driver
implements the same atomic contract; see [module boundaries](MODULES.md).

```sh
go get github.com/tr1v3r/anylock
# Add only the backend you use:
go get github.com/tr1v3r/anylock/driver/redis
# Or: /driver/postgres, /driver/memory
```

## Fully commented SDK for agents

Anylock is a **fully commented SDK** intended to help coding agents use it correctly.
Source comments cover packages, types, interfaces, functions, methods, fields,
constants and variables, explaining input constraints, concurrency and lifecycle
rules, and error handling. Read the comments for your pinned version alongside
the examples when integrating the SDK.

Start with the [Locker API](lock.go), [lease lifecycle](lease.go), and [driver contract](driver.go).

## Choose the behavior

| API | Purpose | After work finishes | Renewal |
| --- | --- | --- | --- |
| `TryRun` / `Run` | Suppress repeated execution within a fixed TTL window | Keep the reservation until TTL, including on callback error or panic | Never |
| `TryLock` / `Lock` | Hold an owner-checked lease for cooperative protected work | Caller explicitly releases the lease | Explicit, or opt-in automatic |

Both modes share the driver and key namespace, so a gate and a lease using the
same key contend. Reserve distinct prefixes/keys when their domains should differ.

## Choose a driver

Each driver is a separate module. All three support both execution gates and
leases; the backend determines where callers coordinate and how expiry is stored.
The core and every driver have the same cooperative safety limits described
[below](#safety-boundaries). Leases support [explicit fencing](docs/fencing.md);
protected storage must validate tokens, and exactly-once execution is not promised.

| Driver | Main dependencies and characteristics | Atomic operations and expiry | Suitable uses |
| --- | --- | --- | --- |
| [memory](driver/memory/README.md) | Core and standard library only; no external service, connection or driver worker. Only callers sharing one Driver instance coordinate. | A context-selectable gate protects a map and indexed min-heap. Monotonic-clock TTLs have nanosecond precision; operations sweep expired entries, so idle entries can remain allocated. | Tests, local tools and coordination between goroutines in one process. Separate instances/processes and restarts have independent state. |
| [Redis](driver/redis/README.md) | Core, go-redis and a shared Redis service; callers own the client. Configure context and socket deadlines. | Acquire uses `SET NX PX`; Lua checks the owner before renewal/deletion. Redis expires keys; TTLs round up to milliseconds. | Cross-process TTL gates and leases when Redis is already available. Network latency and asynchronous failover remain part of the safety model. |
| [PostgreSQL](driver/postgres/README.md) | Core and `database/sql` in production, a caller-chosen SQL adapter (for example pgx), and a shared permanent table. PostgreSQL 12+ syntax; caller owns the pool. | Acquire uses an UPSERT. Renewal locks before checking owner and expiry; release directly deletes owner-matched rows, even if expired. Database-clock TTLs round up to microseconds; callers clean up unreleased expired rows. | Cross-process coordination backed by an existing PostgreSQL database, with provisioned schema and capacity for SQL traffic/row contention. Leases do not make protected business writes transactional. |

Backend clients/simulators used in driver tests stay in their own modules; they
are not dependencies of the core. See [module boundaries](MODULES.md).
Create and share drivers/Lockers at application startup. All examples below return
an ordinary `*anylock.Locker` for use with the APIs in the following sections.

### Memory

Share the same Driver pointer wherever callers must contend. `memory.New()` starts
no worker and needs no `Close`; calling it again creates an independent store.

```go
import (
    "github.com/tr1v3r/anylock"
    "github.com/tr1v3r/anylock/driver/memory"
)

func newMemoryLocker() (*anylock.Locker, error) {
    driver := memory.New()
    return anylock.New(driver)
}
```

### Redis

Create a shared client with context deadlines enabled and socket deadlines left
enabled, for example:

```go
client := goredis.NewClient(&goredis.Options{
    Addr: "127.0.0.1:6379",
    ContextTimeoutEnabled: true,
})
defer client.Close() // application-owned; normally closed at shutdown
```

The driver disables ambiguous transport retries for its own commands on known
go-redis clients, without changing the shared client's options or other calls.
Cluster `MOVED`/`ASK` routing remains enabled. Lost replies remain errors even if
the mutation committed. Custom clients/hooks must preserve this no-replay policy;
see the [Redis driver guide](driver/redis/README.md#retries-and-uncertain-outcomes).

Pass it to the driver; reserve a prefix shared by every cooperating process:

```go
import (
    goredis "github.com/redis/go-redis/v9"
    "github.com/tr1v3r/anylock"
    redisdriver "github.com/tr1v3r/anylock/driver/redis"
)

func newRedisLocker(client *goredis.Client) (*anylock.Locker, error) {
    driver, err := redisdriver.New(client, "myapp:locks:")
    if err != nil {
        return nil, err
    }
    return anylock.New(driver)
}
```

### PostgreSQL

Choose a context-aware `database/sql` adapter. With pgx, register
`github.com/jackc/pgx/v5/stdlib` via a blank import and open a shared pool with
`sql.Open("pgx", dsn)`; the application closes it at shutdown. All cooperating
processes must use the same database, table and prefix.

```go
import (
    "context"
    "database/sql"

    "github.com/tr1v3r/anylock"
    postgresdriver "github.com/tr1v3r/anylock/driver/postgres"
)

func newPostgresLocker(ctx context.Context, db *sql.DB) (*anylock.Locker, error) {
    driver, err := postgresdriver.New(db, "myapp:locks:")
    if err != nil {
        return nil, err
    }
    // Explicit one-time setup; prefer a serialized migration in production.
    if err := driver.EnsureSchema(ctx); err != nil {
        return nil, err
    }
    return anylock.New(driver)
}
```

`New` does not connect or create the table. `EnsureSchema` creates the default
`public.anylock_leases` table if absent; it does not validate/migrate an existing
table. Use `WithTable` for another table and see the [driver guide](driver/postgres/README.md)
for schema, permissions and expiry cleanup. `EnsureSchema` may be omitted when the
table is already provisioned; do not run concurrent startup DDL on every replica.

## Use multiple backends

Compose Lockers in the application, not by changing a live Locker's Driver. Each
Locker binds one Driver at construction; each acquired Lease keeps that Driver
reference and owner for renewal/release. There is no `SetDriver` or built-in
`LockerManager`, dynamic router, cross-backend fallback, or multi-backend lock protocol.

Prefer named business domains over a string-keyed backend registry. For example,
create `redisLocker`, `postgresLocker` and `memoryLocker` once at startup using the
setup functions above, check each initialization error, then inject an application-owned bundle:

```go
// Application code, not a type exported by anylock.
type AppLocks struct {
    Orders        *anylock.Locker // Redis: cross-process order coordination
    ScheduledJobs *anylock.Locker // PostgreSQL: cross-process job coordination
    LocalRefresh  *anylock.Locker // Memory: goroutines in this process only
}

locks := AppLocks{
    Orders:        redisLocker,
    ScheduledJobs: postgresLocker,
    LocalRefresh:  memoryLocker,
}
```

Order handlers use `locks.Orders`; job workers use `locks.ScheduledJobs`. Each field
supports both fixed-TTL gates and leases. Keep these bindings and the Drivers'
backend configuration stable while the application is serving work; consumers
should not select a backend afresh on each attempt. The application still owns and
closes Redis clients/SQL pools, after stopping work and settling leases; the bundle
and Lockers do not own those resources.

### Coordination boundaries

- All callers for one logical lock must agree on its backend data space and key
  mapping: Redis service/database/prefix, PostgreSQL database/table/prefix, or the
  same memory Driver instance. Different Locker/Driver objects can coordinate
  through the same shared backend; separate memory Driver instances cannot.
- The same key text in Redis and PostgreSQL names **two independent locks**. A
  replica using `Orders` in Redis does not contend with one using it in PostgreSQL.
  Use consistent domain bindings across replicas and deliberate keys/prefixes for
  domains that should not contend. Gates and leases share a namespace too.
- Return backend errors; do not try another backend as a fallback. A timeout or
  cancellation can follow a committed reservation. Even a busy result does not
  authorize switching stores: the other store may happily grant a second lock.
- Replacing an application field affects subsequent calls, not existing Lease
  handles or fixed-TTL reservations. It does **not** migrate a lock domain. Backend
  migration needs an explicit coordinated cutover: stop old-path acquisition and
  work, settle renewals/in-flight or uncertain requests, and account for remaining
  leases and retained gate markers before enabling the new path everywhere. Simply
  waiting one TTL while old callers can still acquire/renew is not a cutover.
- This is workload selection, not stronger distributed locking. Memory is not an
  outage fallback for Redis/PostgreSQL; composing backends adds no fencing,
  exactly-once guarantee, or permission to continue work after lease loss.

See the executable [`ExampleLocker_multipleBackends`](composition_example_test.go)
for same-store contention, independent domains and Lease binding. It uses only
standard-library test fixtures, not Redis/PostgreSQL services. Its `Output` is
checked by the existing CI test matrix; run it locally with:

```sh
go test -race -run '^ExampleLocker_multipleBackends$' -count=1 .
```

## Fixed-TTL execution

Using a Locker created with any of the drivers above:

```go
executed, err := locker.TryRun(ctx, "event:123", time.Minute, func(work context.Context) error {
    return processEvent(work) // application function
})
if err != nil {
    // executed == true means the callback ran and returned this exact error.
    // executed == false means failure occurred before callback invocation.
    return err
}
if !executed {
    return nil // another caller already reserved this key; no callback ran
}
// No Release: the marker remains until its original TTL expires.
```

`TryRun` makes one acquisition attempt. `Run` retries only contention, every 50ms
by default (`WithRetryInterval`), until acquisition or context cancellation. Use a
caller deadline to bound waiting; backend and callback errors are never retried.

- `(false, nil)`: the key was busy; callback was not invoked (`TryRun` only).
- `(false, err)`: invalid arguments, cancellation, backend failure, or a successful
  acquisition reply received after its operation deadline/local TTL; no callback ran.
- `(true, err)`: callback was invoked once and returned this exact error, including
  nil. A callback returning `ErrNotAcquired` is still a callback failure, not a retry.

Keys must be nonempty, TTLs positive, and callbacks non-nil (`ErrInvalidKey`,
`ErrInvalidTTL`, `ErrNilFunc`). Every attempt gets a fresh owner token. Acquisition
requests are bounded by `WithOperationTimeout`; the request context is canceled
before invoking the callback, which receives the **original caller context**.

Neither `WithOperationTimeout` nor the reservation TTL sets a callback timeout.
Work may continue past TTL and overlap a later execution; callers must cooperate
with their own context. Reservations are not renewed, deleted, or rolled back on
callback success, error, panic, or cancellation. Even an uncertain/late acquisition
is left to expire without cleanup. `WithAutoRenew` has no effect on these APIs.
No lease handles, lifecycle workers, or result caching are created by them. This is
bounded-window execution suppression, **not** strict mutual exclusion, exactly-once
execution, durable idempotency, or shared-result singleflight.

## Lease usage

Use the same Locker with the lease APIs. To enable automatic renewal, pass
`anylock.WithAutoRenew()` to `anylock.New` during setup; it is off by default.
Use `WithOperationTimeout` to change the default one-second request budget.

```go
import (
    "context"
    "time"

    "github.com/tr1v3r/anylock"
)

func run(ctx context.Context, locker *anylock.Locker) error {
    lease, err := locker.Lock(ctx, "resource:123", 10*time.Second)
    if err != nil {
        return err
    }
    defer func() {
        // Acquisition-context cancellation stops renewal, but cleanup needs a
        // fresh, bounded context. Handle/log the error in production.
        cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
        defer cancel()
        _ = lease.Release(cleanup)
    }()

    // Pass lease.Context() to work that cooperatively stops on cancellation.
    // Consult lease.Err() to distinguish expiry/loss from a closed handle.
    return performWork(lease.Context()) // application function
}
```

## API and lifecycle

`Lease` is the interface for an acquired lease. It exposes the key, cooperative
work context, validity deadline and terminal state, plus renewal and release.
`TryLock` and `Lock` return a non-nil `Lease` on success and `nil` on error.

```go
type Lease interface {
    Key() string
    Context() context.Context
    Deadline() time.Time
    Done() <-chan struct{}
    Err() error
    Renew(ctx context.Context) error
    Release(ctx context.Context) error
}
```

- `New(driver, options...)`: validates the driver and configuration; it does not
  start workers or take ownership of backend connections.
- `TryRun(ctx, key, ttl, fn)` / `Run(ctx, key, ttl, fn)`: fixed-TTL execution as
  described above; neither method creates a lease or uses renewal/release.
- `TryLock(ctx, key, ttl)`: one attempt; contention is `ErrNotAcquired`, while a
  backend error is a separate failure. Empty keys and nonpositive TTLs are rejected.
- `Lock(ctx, key, ttl)`: retries contention only, every 50ms by default. Change the
  interval with `WithRetryInterval`; context cancellation interrupts the wait.
- Every acquisition attempt uses a fresh cryptographic owner token, including
  attempts by the same Locker after a previous lease expired or was released.
- `Lease.Renew(ctx)`: renews by the original TTL. No new acquisition, no revival
  of a closed/expired handle. A canceled wait before a request does not abandon
  an otherwise-valid lease; an error after dispatch fails closed.
- `WithAutoRenew()`: off by default. Enables renewal around TTL/3 after acquisition
  and each successful renewal. Explicit Renew remains available in this mode.
  This option affects lease acquisition only, never `TryRun` or `Run`.
- `Lease.Done()`, `Err()`, `Context()`: local invalidation notification/cause and a
  context for cooperative work. The first terminal cause is retained. Expiry,
  cancellation, ownership loss, and uncertain renewal errors invalidate the handle.
- `Lease.Deadline()`: conservative local deadline, measured from before the
  acquisition/renewal request, not from its response. It is not ownership proof.
- `Lease.Release(ctx)`: closes the local handle even if cleanup fails; stops its
  workers before owner-checked remote cleanup, including expired records that
  remain stored. Cleanup is idempotent: missing/successor-owned records are
  successful no-ops. Backend/context errors remain errors and cleanup can be
  retried with another live context, but the handle can never renew again or reset
  its first terminal cause. A nil result is not proof of prior ownership/validity;
  `ErrClosed` denotes local closure, not proof that a remote record was deleted.

```text
[Fixed-TTL execution: TryRun / Run]
  fresh owner -> atomic Driver.TryAcquire
                busy: TryRun skips / Run waits with caller context
                acquired -> fn(original ctx) -> preserve marker until TTL
                error/panic/cancel ----------> preserve marker until TTL
  no lease, renewal, cleanup, or lifecycle worker; TTL does not time out fn

[Lease acquire: TryLock / Lock]
  fresh owner -> atomic Driver.TryAcquire -> Lease
                 contention: no work; backend error: uncertain outcome

[Live lease]
  expiry/cancellation watcher ---------------------------> Done / Context closed
  explicit Renew / optional renewal worker -> bounded RPC -> update local deadline
                                            failure ----> permanent local loss

[Release]
  close local handle -> cancel requests -> wait for workers (caller-context bounded)
                     -> owner-checked Driver.Release -> deleted / no-op / uncertain
```

Each lease has one expiry/cancellation watcher; opt-in automatic renewal adds one
separate worker. A slow renewal RPC cannot hold local loss notification hostage.
Release waits for workers, but its caller context bounds that wait. Otherwise
expiry or acquisition-context cancellation eventually ends the workers. Always
Release when finished rather than waiting for the TTL unnecessarily.

### Timeouts, cancellation, and uncertainty

Every core driver request has a finite deadline (1s by default, configurable with
`WithOperationTimeout`). Earlier caller deadlines win; renewal is additionally
bounded by the remaining local lease validity.
No untracked goroutines are used to return early while a remote request continues.

Some transports, including go-redis, cannot immediately interrupt an already
blocked socket read on plain context cancellation. An in-flight operation can
drain until its deadline even though Done/Context have already signaled local
loss. Redis drivers require deadline-capable known client configurations; custom
Client implementations must honor provided deadlines too.

Acquisition errors may mean the key was created remotely; it is left to expire.
For lease APIs, successful replies observed after the request deadline or
cancellation are rejected even if the local TTL has not elapsed. Late successful
acquisitions are best-effort released using a fresh bounded context; late renewal
replies permanently invalidate the handle. Acquisitions that outlived local
validity are also rejected and best-effort released.
Gate APIs instead always leave their marker
to expire and never invoke the callback after a canceled/expired acquisition.
Renewal errors permanently abandon the handle even if
Redis might have renewed it; subsequent work must not rely on that uncertainty.
Cancellation does not automatically delete remote ownership; use Release with a
separate live cleanup context. A failed release can leave ownership until TTL.

## Driver contract

```go
type AcquireRequest struct {
    Key string
    Owner string
    TTL time.Duration
    Fencing bool
}

type AcquireResult struct {
    Acquired bool
    FencingToken uint64 // zero for ordinary acquisition
}

type Driver interface {
    TryAcquire(ctx context.Context, req AcquireRequest) (AcquireResult, error)
    Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
    Release(ctx context.Context, key, owner string) (bool, error)
}
```

Drivers must be concurrent-safe and honor operation deadlines. Acquisition of an
absent/expired key, owner-checked renewal, and owner-checked release must each be
atomic. Renew requires a live owner and cannot revive expired leases. Release
checks owner, not expiry, and may delete an expired record that remains stored;
stale owners cannot touch successors. TTLs must not be rounded down.
`AcquireResult.Acquired == false` means contention and requires a zero FencingToken.
Ordinary acquisition returns FencingToken zero. Renewal/release retain their bool
results.
False means lost ownership for renew, or no matching stored record for release. Release's bool reports deletion, not prior validity; expired records may
already have been reclaimed. Errors are failures with potentially ambiguous
outcomes. A file driver needs real cross-process atomic coordination and
expiration checks for acquisition/renewal; plain file read/write is insufficient.

The Redis driver uses one `SET ... NX PX ...`, and Lua owner-check-plus-`PEXPIRE`
or owner-check-plus-`DEL`. Sub-millisecond TTLs round up, never down. Prefixes are
combined with logical keys and hex-encoded into automatic Redis hash tags.
Lease and persistent counter keys share a slot in every mode; reserve that
encoded namespace from unrelated writes. It accepts go-redis's normal/cluster
clients through a small Do/Eval interface.

## Safety boundaries

Leases carry fencing tokens only with `New(driver, WithFencing())`. The core
passes `AcquireRequest{Key: key, Owner: owner, TTL: ttl, Fencing: true}` to
`Driver.TryAcquire`, the same entry point
used for ordinary locks. A driver that does not support fencing must return
`ErrFencingUnsupported` without granting ownership. Construction does not probe
driver capabilities; unsupported fencing is reported when acquiring.
`token, enabled := lease.FencingToken()` exposes the immutable acquisition token.
Renewal never changes it; an expired/released handle is never new authorization.
`WithFencing` affects only `TryLock`/`Lock`; `TryRun`/`Run` always use ordinary
fixed-TTL reservations and expose no fencing token. See the
[fencing contract, backend limits and migration guide](docs/fencing.md).

Neither mode provides exactly-once execution or a complete business-idempotency solution. An expired or paused owner can continue executing
application code; cancellation cannot forcibly stop it or undo side effects.
For correctness-critical external mutations, the target resource needs fencing,
transactions, unique constraints, or another independent safety mechanism.

Redis asynchronous failover may lose an acknowledged lock; clock changes,
partitions and process pauses also affect the safety model. No Redlock/quorum
algorithm, reentrancy, fairness, or multi-key transactions are provided. Core abstractions do not strengthen a backend's actual guarantees.

## Executable examples

[`example_test.go`](example_test.go) demonstrates construction/options, both
execution and lease entry points, renewal/release, and lease state/context queries.
Examples also distinguish skipped execution from callback failure and show how to
cancel a wait. Core examples use a standard-library-only in-memory fixture;
applications inject a driver for their coordination backend. Driver examples live
with [Redis](driver/redis/example_test.go), [PostgreSQL](driver/postgres/example_test.go)
and [memory](driver/memory/example_test.go). All examples run in the default
workspace suite; real backend semantics have opt-in integration tests.

## Modules and testing

The core, each driver and the cross-module integration suite have separate
`go.mod` files. Installing the core does not require backend clients, simulators
or SQL adapters. See [module layout, standalone
checks and release order](MODULES.md).

```sh
# From a checkout: go.work connects the local modules.
./scripts/modules.sh go test -race -count=1 ./...
./scripts/modules.sh go vet ./...
./scripts/modules.sh go test -run '^Example' ./...
# Optional real Redis tests; unique namespace, no FLUSHDB:
ANYLOCK_REDIS_ADDR=127.0.0.1:6379 ./scripts/modules.sh go test -race -tags=integration ./...
```

PostgreSQL tests use sqlmock by default; see its [driver guide](driver/postgres/README.md)
for PostgreSQL 12–18 × Go 1.24/stable integration tests. The
[memory driver](driver/memory/README.md) has deterministic expiry and concurrency
tests, plus optional stress diagnostics.

Real Redis CI covers 6.2, 7.2, 7.4, 8.2, 8.4, 8.6, 8.8 and 8.10, each with
Go 1.24/stable and race detection. Default Redis tests use miniredis and exercise
owner checks, expiry, Lua,
contention, rounding and failure responses. Core tests include a memory driver,
concurrency/cancellation scenarios, plus Go 1.25+ virtual-time tests for renewal,
expiry, conservative deadlines and stalled requests. Core/default tests also
run on Go 1.24; the virtual-time files are build-tagged for newer toolchains.
