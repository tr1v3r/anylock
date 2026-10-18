# Module boundaries

| Directory | Module | Dependencies |
| --- | --- | --- |
| `.` | `github.com/tr1v3r/anylock` | Standard library only, including tests and examples |
| `driver/memory` | `github.com/tr1v3r/anylock/driver/memory` | Core and the standard library only |
| `driver/postgres` | `github.com/tr1v3r/anylock/driver/postgres` | Core and database/sql; pgx and sqlmock are local test/example dependencies |
| `driver/redis` | `github.com/tr1v3r/anylock/driver/redis` | Core and go-redis; miniredis is used by driver tests/examples |
| `integration` | `github.com/tr1v3r/anylock/integration` | Core, selected drivers and backend clients for cross-module tests |

The integration module is a test harness, not a public API or separately released
library. Redis, PostgreSQL and memory each have an independent module. Installing
the core or memory driver does not pull Redis or PostgreSQL client dependencies.

## Development and validation

`go.work` selects all local modules. There are no local-path `replace` directives
in distributable `go.mod` files. Package import paths have not changed.

```sh
./scripts/modules.sh go test -race -count=1 ./...
./scripts/modules.sh go vet ./...
./scripts/modules.sh golangci-lint run --config "$(pwd)/.golangci.yml"
ANYLOCK_REDIS_ADDR=127.0.0.1:6379 ./scripts/modules.sh go test -race -tags=integration ./...
```

The script discovers module roots and runs the command from each one, failing on
the first error. A root `go test ./...` alone does not test nested modules.

Also check the dependencies users receive without the local workspace:

```sh
GOWORK=off ./scripts/modules.sh go mod tidy -diff
GOWORK=off ./scripts/modules.sh go test -race -tags=integration ./...
GOWORK=off go list -m all  # must list only the core module
```

The integration-tag tests skip real backends unless their opt-in environment
variables are set; standalone CI still compiles them and executes default driver
tests. Core executable examples retain their API-associated names using a small
test-only memory Driver. Backend setup examples stay in the driver module.
Cross-module behavior tests live in `integration`, so `go mod tidy` cannot bring
driver dependencies back into the core through root tests or build tags.

## Version pins and release order

Each driver requires a published core version. The integration module pins the
core and drivers it tests. Workspace tests exercise current local source; the
`GOWORK=off` job checks that the pinned versions are independently consumable.

Before the split, core versions also contained backend packages. A driver that
requires a core version that contains its backend package can cause an ambiguous
import because both modules provide the driver package. The initial split therefore
pins the core to a reachable commit that already contains the nested module boundary. This
bootstrap uses ordinary commits/pseudo-versions; it does not create a release.
The first core release, `v0.1.0`, contains all nested module boundaries. Driver
modules now pin that release instead of the bootstrap pseudo-version.

For future releases:

1. Release the core when its API changes, for example `v0.1.0` at the repository root.
2. Update the driver's core requirement and validate with `GOWORK=off`.
3. Release drivers with directory-prefixed tags, for example
   `driver/redis/v0.1.0`. New drivers follow the same rule, such as
   `driver/postgres/v0.1.0`.
4. Update integration module pins and run both workspace and standalone checks.

Do not publish a driver with placeholder versions or a local `replace`; workspace
success alone is insufficient. Consumers install the selected driver explicitly,
for example `go get github.com/tr1v3r/anylock/driver/redis@<version>`.

When adding a module, add it to `go.work` and the lint matrix in CI. The test script
discovers its `go.mod` automatically. Include its own license notice, pin a
compatible core version, and keep backend-specific test dependencies local.

## Fencing release set

The compatible fencing release set is:

| Module | Released version | Core requirement |
| --- | --- | --- |
| Core | `v0.3.0` | Standard library only |
| Memory driver | `driver/memory/v0.2.0` | `v0.3.0` |
| Redis driver | `driver/redis/v0.2.0` | `v0.3.0` |
| PostgreSQL driver | `driver/postgres/v0.3.0` | `v0.3.0` |

The core changes `Driver.TryAcquire` to `AcquireRequest` / `AcquireResult` and
adds `Lease.FencingToken`. These versions intentionally break the older interface.
Core and drivers were tested, merged and released in separate PRs in dependency
order. The integration test harness pins the released core and Redis driver.

Workspace CI tests the current local sources together. Independent-module CI
uses `GOWORK=off` and the released pins without local replacements, including
real PostgreSQL integration. Both checks must pass; workspace success alone
does not prove the independently downloadable modules work together. Backend
version matrices and real Redis Cluster tests remain enabled.
