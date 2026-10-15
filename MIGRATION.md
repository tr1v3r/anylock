# Migration and provenance

This repository is the standalone home of the former `github.com/tr1v3r/pkg/lock`
package, migrated on 2026-10-15 from public source commit
[`dd55966c8f365934185a82ad26e5647623f4ffa5`](https://github.com/tr1v3r/pkg/commit/dd55966c8f365934185a82ad26e5647623f4ffa5).
The original MIT notice is preserved verbatim in [LICENSE](LICENSE).

## Initial baseline

- Move the existing core, Redis adapter, all corresponding tests/examples and
  experiment archives into their own module. Root package is now `anylock`.
- Imports change from `github.com/tr1v3r/pkg/lock` to `github.com/tr1v3r/anylock`;
  adapter imports become `github.com/tr1v3r/anylock/driver/redis`.
- Calls such as `lock.New` become `anylock.New`; API shapes and execution/lease
  lifecycle behavior are unchanged. No compatibility copy is maintained in pkg.
- Integration opt-in variable changes from `PKG_LOCK_REDIS_ADDR` to
  `ANYLOCK_REDIS_ADDR`. The initial baseline was a single module; current
  multi-module test commands and release rules are documented in [MODULES.md](MODULES.md).
- PostgreSQL deliberately is **not** in the migration baseline: it is ported in a
  separate driver PR, alongside separate etcd and memory driver proposals.
- Only relevant dependencies remain. The initial repository history starts with
  a provenance-labelled migration commit rather than copying unrelated pkg history.
- The old [pkg PR #54](https://github.com/tr1v3r/pkg/pull/54) remains historical
  evidence, not a second maintained implementation or a migration to pkg master.

## Historical measurements

The eight files under
[`docs/experiments/concurrency-2026-10-15`](docs/experiments/concurrency-2026-10-15)
are copied byte-for-byte, including original checksums and reproducer archives.
They measured **pkg commit6168a82**, not the new module/import layout. Embedded old
imports, commands and URLs are intentional provenance and are not migration bugs.
Do not rewrite archived payloads or imply that those throughput/latency numbers
were remeasured against anylock, PostgreSQL, etcd, or memory. They are local
experiments, never production capacity guarantees.
