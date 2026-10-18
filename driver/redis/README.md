# Redis driver

This is the independent module `github.com/tr1v3r/anylock/driver/redis`. It depends
on the anylock core and go-redis; its tests and executable examples use miniredis.
These dependencies are not requirements of the core module.

```sh
go get github.com/tr1v3r/anylock/driver/redis
```

See the [core guide](../../README.md) for client configuration, execution gates,
lease lifecycle and safety limits. The [executable examples](example_test.go) show
construction and direct driver operations.

`Release` atomically deletes a key only when its owner matches. Redis treats expired
keys as absent; the driver returns `false, nil` when no matching key remains. Its
bool reports deletion, not prior lease validity. Core `Lease.Release` treats a
missing/successor-owned key as successful, idempotent cleanup. Backend/context
errors still need handling; cleanup never revives a closed handle or deletes a
successor's key. `Renew` continues to require a live matching owner.

From this directory:

```sh
go test -race -count=1 ./...
ANYLOCK_REDIS_ADDR=127.0.0.1:6379 go test -race -tags=integration ./...
GOWORK=off go test -race ./...
```

See [module development and release order](../../MODULES.md) before changing core
version requirements or publishing a directory-prefixed driver tag.

## Explicit fencing

Use `anylock.New(driver, anylock.WithFencing())`. Fenced acquisition uses one
Lua script to check the lease, INCR a persistent per-key counter, and install
ownership. The counter key is `<prefix><key>:__anylock_fence`; the suffix is
reserved and rejected as a logical lease key, even for ordinary operations.
Reserve the entire physical namespace across applications/prefixes. Counter keys
never expire and must not be purged/evicted/restored to older values.

Redis Cluster requires a nonempty hash tag (for example prefix `{locks}:`) so
lease and counter keys share a slot. A missing tag may cause CROSSSLOT errors;
there is no fallback. Values are returned as exact decimal strings to avoid Lua
number rounding above 2^53. The supported range is 1 through MaxInt64; exhaustion
fails without installing a lease. Contention and renewal do not increment it.

Asynchronous replication/failover, eviction, FLUSH, snapshot rollback, or manual
resets can repeat/regress tokens. This driver cannot promise failover-safe fencing
on such a deployment. Choose a backend/fault model that preserves counter history.

Read [the shared fencing contract](../../docs/fencing.md): business storage must
atomically validate `Lease.Fence()` with protected writes.
