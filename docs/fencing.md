# Explicit fencing

```go
locker, err := anylock.New(driver, anylock.WithFencing())
if err != nil { return err }
lease, err := locker.Lock(ctx, key, ttl)
if err != nil { return err } // includes ErrFencingUnsupported from the driver
defer lease.Release(cleanupCtx) // independent live, bounded cleanup context
token, enabled := lease.FencingToken()
if !enabled { return errors.New("fencing required") }
// Pass lease.Context(), key, and token to storage. The storage atomically
// validates token AND writes the protected value, in one transaction/operation.
return storage.Write(lease.Context(), key, token, value)
```

Driver generates; Lease carries; protected storage validates. anylock does not
perform business writes or enforce downstream usage. Without that validation,
there is no fencing protection. See the executable `ExampleWithFencing` in the
core examples for a downstream high-water mark that rejects a stale owner.

## Acquisition and lifetime contract

- `err != nil`: failed or uncertain outcome. Ignore the result and do no work.
- `Acquired == false`: contention, FencingToken must be zero.
- `Acquired == true`: success. FencingToken is zero for ordinary acquisition and strictly
  positive when explicitly requested. Invalid results return
  `ErrInvalidAcquireResult`; known acquired ownership is best-effort cleaned up.
- `WithFencing()` sets `AcquireRequest.Fencing` on the single `Driver.TryAcquire`
  request; `AcquireRequest.TTL` carries the TTL. Ordinary and fenced acquisition
  share the same method, namespace and owner protocol. There is no capability
  interface, separate acquisition method, or fallback. `New` does not check
  backend capabilities. Unsupported drivers must return `ErrFencingUnsupported`
  before granting ownership, including when reached through wrappers.
- Each successful fenced acquisition has a new, strictly increasing token for
  that key in its backend's declared fault model. Tokens may have gaps and are
  not timestamps or business operation IDs. A retry after an uncertain outcome
  gets a new owner and must never reuse a token returned with an error.
- Renew/auto-renew never change the token. Release, expiry, cancellation, and
  ownership loss never make an old handle valid again. `FencingToken()` is immutable
  and remains available for diagnostics after invalidation; it is not a validity
  check or new authorization. Use Err/Done and stop work after invalidation.
- `WithFencing()` applies only to `TryLock`/`Lock`. `TryRun`/`Run` always send
  `Fencing: false`, retain their fixed-TTL marker and pass the original callback
  context. They never renew or release the marker, never expose a token, and gain
  no fencing protection from the option.

The core validates the shape of results; it cannot prove a driver's distributed
counter monotonicity. Custom drivers must atomically bind acquisition and token
allocation, fail on exhaustion, and document their persistence/failover model.

## Downstream storage contract

Maintain a durable highest accepted token for each protected resource. In the
same atomic operation as the business mutation, reject tokens below that mark
and advance it when accepting a newer token. Equal tokens may be accepted for
multiple writes by the same lease, according to business requirements. Never
check the token in a separate read followed by an unguarded write. Keep the mark
across business-row deletion/recreation and storage restores as required by the
resource's lifetime.

Once storage accepts a newer token, a paused older owner cannot overwrite its
work. A token alone does not let storage discover lease expiry or acquisition of
a successor: before observing the higher token, it may still accept an old one.
Fencing does not stop application execution, make external effects reversible,
or provide exactly-once execution. Separate idempotency/transaction rules remain
necessary where the business needs them.

Use a stable key-to-resource mapping and a stable counter domain. Switching
backends, counters, prefixes, or memory instances does not preserve token order;
a coordinated migration must preserve/advance the counter above all previously
accepted tokens or establish a separate resource epoch enforced by storage.

## Backend scope

| Driver | Counter | Required assumptions |
| --- | --- | --- |
| memory | Instance-wide uint64, protected by the ownership gate | Same live Driver instance; restarts/replacement reset state |
| Redis | Persistent per-key signed 64-bit INCR counter, allocated in acquisition Lua | No counter eviction/loss/rollback/reset; Cluster keys share a slot |
| PostgreSQL | Permanent logged ascending bigint sequence; CACHE 1, NO CYCLE | Same sequence for a lock domain; no counter rollback/reset; durability and failover preserve history |

Read each driver's README before choosing a backend. Redis asynchronous failover
can lose acknowledged increments; this implementation cannot turn that deployment
into a failover-safe fencing service.

PostgreSQL token allocation happens in a SELECT consuming the acquired row from
a modifying CTE, after its row-lock wait. The sequence uses `CACHE 1` because
larger per-session caches can produce out-of-order values. See the official
[sequence documentation](https://www.postgresql.org/docs/current/sql-createsequence.html)
and [modifying CTE semantics](https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-MODIFYING).
Redis returns the counter via GET's decimal string, preserving precision beyond
Lua's numeric range; INCR remains bounded by signed 64-bit arithmetic. See
[Redis INCR](https://redis.io/docs/latest/commands/incr/).

## Driver protocol

```go
type AcquireRequest struct {
    Key string
    Owner string
    TTL time.Duration
    Fencing bool
}

type AcquireResult struct {
    Acquired bool
    FencingToken uint64
}

type Driver interface {
    TryAcquire(ctx context.Context, req AcquireRequest) (AcquireResult, error)
    Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
    Release(ctx context.Context, key, owner string) (bool, error)
}
```

An ordinary acquisition returns `AcquireResult{Acquired: true}` on success. With
`req.Fencing`, success requires a strictly positive FencingToken. An unsupported driver
returns the zero result and `ErrFencingUnsupported`, without acquiring the key.
Every backend error takes precedence over its result; there is no valid lease
or token to use after an error. The core additionally rejects malformed results,
including a successful fenced acquisition with FencingToken zero.

Renew and Release continue to check the owner; they need no fenced variants.
Custom Lease implementations expose `FencingToken() (uint64, bool)`; ordinary leases
return `(0, false)`. Decorators forward FencingToken along with the other Lease methods.

Built-in driver clients, owner matching, and lease table layout remain the same.
Redis encodes physical lease and counter keys with the same hash tag.
PostgreSQL fenced callers must explicitly provision its sequence. For the compatible release set and nested-module publishing order, see
[MODULES.md](../MODULES.md).
