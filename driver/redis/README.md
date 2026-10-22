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

## Retries and uncertain outcomes

For known go-redis `*Client` and `*ClusterClient` values, the driver marks its own
commands `NoRetry`: a lost reply must remain an error, not be hidden by replaying
an already committed mutation. This covers ordinary/fenced acquisition, renewal,
and release, including Cluster's outer network-retry loop and ASK pipelines.
Safe `MOVED`/`ASK` routing remains enabled. The caller's options, hooks, and retry
policy for unrelated client calls are unchanged; no dedicated client is required.

An error does **not** prove rollback. For example, a lost acquisition reply may
leave an owner/TTL marker (and an advanced fencing counter) without returning a
lease. Do not infer successful ownership from that uncertainty or blindly replay
an acquisition with the same owner.

Custom `Client` implementations must likewise avoid replaying commands whose
mutation may have committed. The driver cannot enforce that policy through the
minimal `Do`/`Eval` interface. Custom go-redis hooks still run and receive a
`Cmder` wrapper; they must preserve its `NoRetry` policy rather than reconstruct
an ordinary command or implement their own unsafe retries.

From this directory:

```sh
go test -race -count=1 ./...
ANYLOCK_REDIS_ADDR=127.0.0.1:6379 go test -race -tags=integration ./...
GOWORK=off go test -race ./...
```

See [module development and release order](../../MODULES.md) before changing core
version requirements or publishing a directory-prefixed driver tag.

## CI compatibility matrix

The [real-server workflow](../../.github/workflows/redis.yml) tests Redis 6.2,
7.2, 7.4, 8.2, 8.4, 8.6, 8.8 and 8.10, each on Go 1.24 and stable with race
detection. It runs all workspace modules, including the driver and cross-module
ownership/expiry/fencing tests. Versioned Alpine tags track patch updates within
each selected release line. The PostgreSQL workflow similarly covers majors
12 through 18. These are server-version checks, not Cluster/failover guarantees.

## Explicit fencing

Use `anylock.New(driver, anylock.WithFencing())`. The core passes
`AcquireRequest{Key: key, Owner: owner, TTL: ttl, Fencing: true}` to `TryAcquire`. One Lua script
checks the lock, increments a persistent counter, installs the owner with TTL,
and returns the exact counter string. Owner strings still control renewal and
release; fencing tokens never replace owners.

Every operation uses this physical-key format, including ordinary acquisition:

```text
namespace = hex(prefix + logicalKey)          # lowercase hex of the raw bytes
lease     = {anylock:<namespace>}:lease
counter   = {anylock:<namespace>}:fence
```

The driver generates the hash tag automatically; callers do not need to embed
one. Prefixes and logical keys containing braces, empty tags or binary bytes are
encoded, so they cannot override the tag or collide with the counter namespace.
Ordinary/fenced acquisition, renewal and release use the same lease key. Changing
this format is a lock-domain migration; drain old-domain owners before cutover.
Reserve the encoded physical namespace from external writes.

Counter keys never expire and are not removed by release or lease expiry. They
must not be purged/evicted/restored to older values. Values are returned as exact
decimal strings to avoid Lua number rounding above 2^53. The supported range is
1 through MaxInt64; exhaustion fails without installing a lease. Contention and
renewal do not increment the counter.

Asynchronous replication/failover, eviction, FLUSH, snapshot rollback, or manual
resets can repeat/regress tokens. This driver cannot promise failover-safe fencing
on such a deployment. Read the [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
for hash-slot and asynchronous-replication limits.

Real three-master Cluster tests run on Go 1.24/stable in CI, including same-slot
Lua execution, special-character namespaces, precision and mixed ordinary/fenced
ownership. To run locally with Docker:

```sh
./scripts/redis-cluster.sh start anylock-test-cluster
ANYLOCK_REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
  ./scripts/modules.sh go test -race -tags=integration -run '^TestIntegrationRedisClusterFencing$' ./...
./scripts/redis-cluster.sh stop anylock-test-cluster
```

Run those commands from the repository root. The fixture binds only loopback
client ports, uses no host-mounted data and removes only its named container.

Read [the shared fencing contract](../../docs/fencing.md): business storage must
atomically validate `Lease.FencingToken()` with protected writes.
