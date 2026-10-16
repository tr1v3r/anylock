# PostgreSQL lock driver

`github.com/tr1v3r/anylock/driver/postgres` implements `anylock.Driver` with a shared,
permanent PostgreSQL **12+** table. Production code depends only on `database/sql`
and the lock core. It works with both fixed-TTL `TryRun`/`Run` gates and
owner-checked `TryLock`/`Lock` leases, including explicit/optional automatic renewal.
PostgreSQL 12 is the syntax-compatibility minimum; use a maintained server release
for production deployments.

## Setup

The caller chooses a PostgreSQL SQL client and owns its connection pool. For
example, using pgx's `database/sql` adapter:

```go
import (
    "context"
    "database/sql"
    "os"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/tr1v3r/anylock"
    postgresdriver "github.com/tr1v3r/anylock/driver/postgres"
)

func run(ctx context.Context) error {
    db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
    if err != nil {
        return err
    }
    defer db.Close() // application-owned; normally shared until shutdown

    driver, err := postgresdriver.New(db, "myapp:gates:",
        postgresdriver.WithTable("public.app_leases")) // optional; default is public.anylock_leases
    if err != nil {
        return err
    }
    // Explicit setup only. Prefer your normal serialized migrations in production.
    if err := driver.EnsureSchema(ctx); err != nil {
        return err
    }
    locker, err := anylock.New(driver)
    if err != nil {
        return err
    }
    executed, err := locker.TryRun(ctx, "event:123", time.Minute,
        func(work context.Context) error { return processEvent(work) })
    _ = executed // false,nil means skipped; true means the callback ran
    return err
}
```

`processEvent` above is application code. For renewable leases, use the same
`driver` with `anylock.WithAutoRenew()` or call `Lease.Renew` explicitly; always release
finished leases with a separate live cleanup context. See the [core guide](../../README.md).
Gate callbacks never renew or release their marker, even on failure or panic.
Both modes share keys: choose distinct prefixes/keys when their domains differ.

## Schema and ownership

The default is `public.anylock_leases`. Different tables or namespaces are
independent lock domains even for identical business keys: keep the same database,
table, and prefix across all cooperating clients. A deployment whose table was
created as `public.pkg_lock_leases` — the earlier default, kept to match the
original pkg driver — stays in that domain by passing
`WithTable("public.pkg_lock_leases")`; `EnsureSchema` does not migrate data or
provide a coordination cutover protocol, so do not hot-switch an active lock domain.

`New` does not connect, ping, run DDL, or close the pool. `EnsureSchema` explicitly
executes `CREATE TABLE IF NOT EXISTS`; the schema must already exist. Equivalent
migration for the default table:

```sql
CREATE TABLE public.anylock_leases (
    key        text COLLATE pg_catalog."C" PRIMARY KEY,
    owner      text COLLATE pg_catalog."C" NOT NULL,
    expires_at timestamp with time zone NOT NULL
);
```

- Use a dedicated, ordinary logged table with this shape, bytewise `C` collation,
  and full timestamp precision. Do not attach triggers/rules, RLS policies, or
  external writers that alter the protocol. A table need not be per process;
  all cooperating processes must use the same database/table/namespace.
- `EnsureSchema` does **not** validate or migrate an existing table. Concurrent
  `CREATE TABLE IF NOT EXISTS` can still race in PostgreSQL's catalogs; provision
  it through serialized migrations rather than every contender's hot path.
- `WithTable("leases")` uses `public.leases`; `WithTable("locks.leases")` uses that
  explicit schema. Each component is an ASCII identifier, at most 63 bytes,
  quoted and case-sensitive. SQL fragments and temporary schemas are rejected.
  Fixed qualification avoids `search_path` or temporary-table shadowing.
- Prefixes are concatenated literally. Keys/owners must be nonempty UTF-8 without
  NUL; prefixes may be empty. PostgreSQL's B-tree index size limit still applies
  to long keys: use short business identifiers or application-level hashes.
- The runtime role needs `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on the table;
  `SELECT FOR UPDATE` participates in owner/expiry checks. Grant DDL separately
  if using `EnsureSchema`, or omit the call for a restricted runtime role.

## Atomicity, expiry, and timeouts

```text
[Acquire]
  INSERT ... ON CONFLICT DO UPDATE
    live row -> false (including same owner)
    expired row -> lock conflict row -> check DB clock -> install fresh owner/TTL

[Renew / release]
  MATERIALIZED held-row CTE -> SELECT ... FOR UPDATE (key only)
    -> check held.owner AND held.expires_at against current DB clock
    -> extend / delete, or false for absent/expired/successor-owned rows
```

Checking expiry in a plain `UPDATE ... WHERE expires_at > clock_timestamp()` can
happen **before** waiting for a row lock. The materialized CTE deliberately checks
its locked row's values **after** that wait, including updated successor ownership.
Do not replace it with an unlocked read or a pre-lock expiry filter.

Expiry uses `pg_catalog.clock_timestamp()`, not transaction-start `now()`.
Positive TTLs round **up** to microseconds and bind as exact integer hour/minute/
second/microsecond intervals: no floating-point multiplication, calendar-day/DST
arithmetic, integer overflow, or PostgreSQL 12 oversized interval fields.

TTL is not a fresh validity promise starting at response receipt. In particular,
first-INSERT values are evaluated before a unique-conflict wait; if the competing
insert rolls back, that wait can consume this reservation's TTL. Network/commit
latency can also consume validity. The core conservatively counts from **before**
the request and rejects exhausted TTLs; do not extend validity from reply time.

All operations, including schema setup, check cancellation before dispatch. A
supplied deadline is preserved; without one the driver adds a one-second deadline.
Late successful replies are rejected. SQL/context errors retain their identity
through wrapping, including uncertain/aborted outcomes; they are never busy results.
No extra goroutine/select wrapper returns while a provider call is still running.
A failed/canceled request can nevertheless have committed, or still commit, on the
server: cancellation is not proof of rollback.

**`database/sql` forwards contexts; it cannot force a noncompliant provider to
interrupt I/O.** Use a PostgreSQL SQL driver and connection configuration that honor
contexts and bound network I/O (the integration suite uses pgx stdlib). Custom
context watchers/dialers must retain those guarantees. Do not assume an auxiliary
cancel request alone bounds a blocked socket. Pool acquisition/row-lock waits can
consume the operation budget; failures do not prove a write never committed.

Database clock jumps, pauses, network partitions, replication/failover settings,
and administrative writes remain part of the safety model. This adapter does not
provide fencing, strict gate mutual exclusion, exactly-once work, or stronger
consistency/durability than the database itself. No external transaction is accepted:
operations use the pool's own single-statement transactions, never a caller's
long-lived `*sql.Tx`.

## Expired-row maintenance

Expiry is logical: expired rows cannot renew or block new acquisition even if not
physically deleted. Unique, never-reused gate keys otherwise accumulate, so arrange
periodic cleanup, for example:

```sql
DELETE FROM public.anylock_leases
WHERE expires_at <= clock_timestamp();
```

For large tables, use bounded batches, a suitable expiry index, and an operational
statement/lock timeout. Reserve the table for this protocol; no background cleaner
is started by the driver. Never truncate a live lock table or delete unexpired rows.

Implement cleanup as a scheduled statement, not as a trigger on the lease table:
triggers only fire on writes, so an idle table never gets purged, and a trigger
would couple every lock operation to the purge. `pg_cron` can host the schedule
inside PostgreSQL; it is time-driven, runs the same expired-only delete, and stays
decoupled from the lock path. Self-hosted servers need the extension installed
and `shared_preload_libraries = 'pg_cron'` before `CREATE EXTENSION pg_cron`
(superuser); managed services expose it through their own extension settings.

```sql
-- One-time setup: extension plus the expiry index the schema does not create.
CREATE EXTENSION pg_cron;
CREATE INDEX CONCURRENTLY anylock_leases_expires_idx
    ON public.anylock_leases (expires_at);

SELECT cron.schedule(
    'anylock-lease-purge',
    '*/10 * * * *',  -- cron syntax; interpreted in the cluster-wide
                     -- cron.timezone setting (GMT by default)
    $$DELETE FROM public.anylock_leases WHERE ctid IN (
        SELECT ctid FROM public.anylock_leases
        WHERE expires_at <= clock_timestamp() LIMIT 5000)$$);
```

pg_cron never runs two instances of the same job concurrently: an overlapping
batch queues instead of doubling, so raise the frequency rather than the batch
size when falling behind. Inspect runs through `cron.job` and
`cron.job_run_details`, and remove the job with
`cron.unschedule('anylock-lease-purge')`. Jobs run on the primary, not on a hot
standby, and execute in the database where they were scheduled; use
`cron.schedule_in_database` for other databases. Adjust the table name when
`WithTable` selected another one. Without `pg_cron`, any external scheduler
issuing the same statement is equivalent.

## Module

Install this driver with `go get github.com/tr1v3r/anylock/driver/postgres`.
Its production code uses `database/sql` and the core; pgx and sqlmock belong to
this module for tests/examples. Callers choose their own SQL adapter.
Run the following commands from this directory (`cd driver/postgres`); see
[MODULES.md](../../MODULES.md) for repository-wide checks.

## Tests and examples

```sh
go test -race -count=1 ./...
go vet ./...
# Local integration opt-in; every fixture creates/drops only its own random table:
export ANYLOCK_POSTGRES_DSN='postgres://user:password@127.0.0.1:5432/testdb?sslmode=disable'
go test -race -count=1 -tags=integration -timeout=2m ./...
```

[`example_test.go`](example_test.go) has executable `Output` examples for all six
exported operations/options. Its SQL mock is test-only plumbing, not an in-memory
PostgreSQL substitute. The real-server suite covers atomic ownership, database-clock
expiry, row-lock/deadline races, and core integration. Local integration tests skip
without a DSN; **CI supplies one and runs the bounded regressions unconditionally**
in the existing PostgreSQL 12/16 × Go 1.24/stable matrix. No stress environment
variable is needed for these mandatory regressions:

| Regression | Size and safety checks |
| --- | --- |
| Same-key acquisition | Separate 64, 256, and 1024-contender waves, each with unique owners and exactly one winner matching the stored row. |
| Concurrent mixed churn | Hot key and 16-key profiles, each 32 workers × 16 seeded iterations; acquire, live-owner renew/release, and retired/rejected-owner probes run concurrently. No overlapping tracked long-TTL holders, no stale mutation, and no leftover rows. |
| Expiry takeover | Three server-clock-controlled same-key expiry phases, each followed by a 64-contender wave. A separate batch expires 64 keys, then starts 128 fresh acquisition contenders plus 128 expired-owner renew/release attacks together; exactly one winner per key and no predecessor mutation. |
| Cancellation/pool wait | 256 concurrent mixed requests: 128 pre-canceled, 128 observed queued behind one checked-out connection before cancellation; no ownership claim or row mutation, pool remains usable. |

These are **contender goroutines**, not 1024 PostgreSQL connections. Each fixture
normally caps the caller-owned pool at **24 open/idle connections**; the pool-wait
regression deliberately uses one. Fixtures/waves are not run in parallel with each
other. Every request has an explicit overall context (15 seconds for each bounded
fixture), avoiding the driver's default one-second timeout under a queue. One-hour
TTLs are much longer than the bounded workload; expiry is tested separately through
observed server-clock conditions, not guessed sleeps. Occupancy is cleared **before**
Release, because successors can acquire before the releasing caller receives its
reply. There is no harness mutex around driver calls. These tests do not imply that
gate callbacks remain mutually exclusive beyond their TTL.

### Optional sustained diagnostics

The sustained test skips unless `ANYLOCK_POSTGRES_STRESS=1` is explicitly set. An
explicit opt-in without a DSN fails, rather than silently skipping the workload.
It always runs **both** a hot-key profile and a seeded multikey profile sequentially;
the configured duration applies to **each** profile:

```sh
# Local sustained verification works before the new workflow reaches master.
ANYLOCK_POSTGRES_STRESS=1 \
ANYLOCK_POSTGRES_STRESS_DURATION=30s \
ANYLOCK_POSTGRES_STRESS_CONCURRENCY=64 \
ANYLOCK_POSTGRES_STRESS_KEYS=32 \
  go test -race -count=1 -tags=integration -timeout=12m -v \
    -run '^TestIntegrationPostgresSustainedStress$' ./...
```

| Environment variable | Default | Strict accepted bounds |
| --- | --- | --- |
| `ANYLOCK_POSTGRES_STRESS_DURATION` | `2s` | Go duration, `1s` through `5m` per profile |
| `ANYLOCK_POSTGRES_STRESS_CONCURRENCY` | `64` | Integer, 1 through 1024 contender workers |
| `ANYLOCK_POSTGRES_STRESS_KEYS` | `32` | Integer, 1 through 4096 multikey keys; hot profile always uses one |

All operations retain the same long-TTL ownership/stale-owner invariants as bounded
churn. The fixture budget is the profile duration plus 15 seconds, not the default
15-second fixture timeout. Workers join before cleanup; only the fixture's own table
is dropped and its own pool closed. End-context cancellation/deadline outcomes are
expected **ambiguous** outcomes, never rollback proof or late ownership. Such owners
are not released as an attempted rollback; fixture cleanup is independent of the
expired workload context. A reply first observed after the workload deadline is
conservatively discarded into the ambiguous count even if it returned no error;
this does not claim the driver reported cancellation or that the server rolled back.
Sustained activity requires a successful acquisition, not a completed API mix. An
end-canceled matching Renew can stop a worker **before any Release is attempted**;
this is reported honestly, without issuing canceled calls to pad the counts.
`require_complete_mix` is false for sustained runs; `missing_api_attempts` and
`missing_api_successes` identify absent activity (omitted when empty), not driver
failures. In contrast, mandatory fixed-round regressions set `require_complete_mix`
to true, require successful acquire/renew/release activity, reject cancellation,
and verify an empty final table. Zero unexpected errors, zero invariant violations,
and consistent metric counts are asserted in both modes. There are **no fixed QPS,
latency, heap, or goroutine thresholds**.

Each profile emits one `ANYLOCK_POSTGRES_METRICS {JSON}` line, even before reporting
invariant failures. It includes:

- Per-operation and total attempts, successes, contention/owner rejections, expected
  cancellation/ambiguous outcomes, and unexpected errors. The outcome counts sum
  to attempts. A false stale-owner probe is a rejection, not a backend error.
- Actual elapsed seconds and **all attempted API operations/second**, not business
  transactions/second or successful acquisitions/second. One iteration can issue
  several API operations. The latency denominator is this same attempted-operation
  count, including canceled and rejected requests and pool/SQL round-trip wait.
- **Approximate** P50/P95/P99 in microseconds from a fixed 64-bin log2-nanosecond
  histogram, merged from bounded per-worker histograms. Values are bucket upper
  bounds (less than 2× quantization except the saturated final bucket), not exact
  samples or interpolated percentiles. No unbounded latency list is retained.
- Process heap snapshots, total allocated-byte/GC-cycle/GC-pause deltas, goroutine
  snapshots, and pool open/in-use/idle counts plus WaitCount/WaitDuration deltas.
  These are client-process observations, not server memory/GC or peak goroutines;
  final snapshots precede fixture cleanup and can include provider shutdown work.

The new [`postgres-stress.yml`](../../.github/workflows/postgres-stress.yml) runs a
**two-second/profile, 16-worker, 16-key PR smoke** so the workload, JSON reporter,
and workflow execute before merge. It also offers `workflow_dispatch` for longer
runs with validated environment inputs. GitHub only exposes a new manually
triggered workflow after its definition exists on the default branch; selecting
the PR branch does not remove that limitation. Use PR smoke and the local command
above before merge; dispatch becomes available afterward. There is no schedule.

CI compiles the race-enabled test binary first, then `/usr/bin/time -v` measures
execution, excluding compilation. Uploaded artifacts (also on test failure) include
build/run logs, per-profile `metrics.jsonl`, environment details, and client-process
CPU/peak RSS. The extractor rejects missing/duplicate profiles and inconsistent
outcome/latency denominators, preventing a skipped or mistargeted workload from
passing. It validates sustained activity mode and missing-mix diagnostics without
requiring renew/release attempts or successes at shutdown. CPU/RSS cover the test
process and both profiles/fixture work, **not the
PostgreSQL service**. Setup failures may yield no profile JSON; failure logs remain
available. A hard runner termination cannot guarantee an artifact upload.

Race instrumentation, database configuration, pool queueing, machine load, and the
seeded operation sequence all affect observations; scheduling is still nondeterministic.
This is a correctness/load diagnostic, **not a production benchmark or capacity
claim**. The repository's immutable historical experiment archive describes a
different workload/provenance and must not be relabeled with these results.
