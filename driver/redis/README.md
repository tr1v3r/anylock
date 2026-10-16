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
