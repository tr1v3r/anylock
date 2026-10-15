# anylock contributor guide

Go 1.24+ multi-module repository; dependency-free core `github.com/tr1v3r/anylock`, root package `anylock`.

## Boundaries

- Root is standard-library-only: atomic Driver contract, fixed-TTL execution gates and owner-checked leases.
- `TryRun` / `Run` retain markers on success/error/panic/cancellation, never renew/release, and pass the original context. They do not promise strict mutual exclusion or exactly-once work.
- `TryLock` / `Lock` return leases. Renewal is explicit or opt-in; independent expiry observation and bounded requests must remain intact. Never revive closed handles or affect successors.
- Backends belong under `driver/` as independent modules. Do not import backend clients into root, including its tests/examples. Callers own client/pool resources.
- Cross-module integration tests belong in `integration/`. Root examples use a standard-library fixture; Backend examples live with each driver. See `MODULES.md` for module pins and release order.
- New drivers are independent PRs. Do not silently weaken expiry/owner/context contracts to fit a backend; document and resolve semantic differences explicitly.
- No fencing guarantee; clock changes, pauses, partitions and failover remain backend limitations.

## Checks

```sh
./scripts/modules.sh go test -race -count=1 ./...
./scripts/modules.sh go vet ./...
./scripts/modules.sh golangci-lint run --config "$(pwd)/.golangci.yml"
GOWORK=off ./scripts/modules.sh go mod tidy -diff
GOWORK=off ./scripts/modules.sh go test -race -tags=integration ./...
ANYLOCK_REDIS_ADDR=127.0.0.1:6379 ./scripts/modules.sh go test -race -tags=integration ./...
```

Keep Go1.24 support; Go1.25+ `testing/synctest` tests retain version build tags. Test default and integration build modes. Executable examples need recognized, separate `Output:` comment groups and must actually RUN, not just compile.

Format owned Go files with `goimports-reviser -project-name github.com/tr1v3r/anylock -rm-unused ...`; lint v2.12.2 also enforces gofmt/goimports with the module-local prefix. Production lines at most120 characters. Table tests and deterministic barriers beat scheduler sleeps. Backend errors are not contention; cancellation does not prove rollback. Preserve error identity and post-wait actual-deadline checks.

Use targeted staging, Conventional Commits and personal identity `tr1v3r <tr1v3r@outlook.com>`. Preserve unrelated changes; do not amend, force-push, merge PRs or tag releases without explicit authorization. Check `MIGRATION.md` and immutable historical experiment provenance before claiming performance or changing archived artifacts.
