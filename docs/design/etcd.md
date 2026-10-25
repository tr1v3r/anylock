# etcd backend proposal: strict expiry is not implemented

Status: **design only; blocked on a suitable server-side primitive.**

The strict `Driver` contract is unchanged: acquisition is non-reentrant and may
create only an absent/expired lock; renewal and release must atomically require
a matching, unexpired owner. Backend errors are not contention. Requests must
be finite, context-aware, and must not report success past their deadline.

Stock etcd v3 has atomic, strongly consistent KV transactions, but cannot combine
an owner comparison, **native lease expiry validation**, and a changed-TTL update
or deletion in one operation. This proposal adds no driver, runtime dependency,
background worker, or claim of conformance. It does not redefine expiry to mean
“the key has finally been deleted.”

## Concrete counterexample

A native lease can expire before its key deletion is committed through Raft:

```text
【Native expiry and KV deletion are separate】
Grant lease A → Put K=(owner A, lease A)
                    |
                    ├─ native deadline passes
                    ├─ K remains visible pending lease revocation
                    |   ├─ Value(K)==owner A still passes
                    |   ├─ ModRevision(K)==observed revision still passes
                    |   └─ LeaseValue(K)==lease A still passes
                    |
                    ├─ CAS Put K with fresh lease B → revives expired ownership
                    └─ CAS Delete K                → reports release of expired owner

【Successor safety is necessary, but insufficient】
K changes to owner C / lease C / newer revision
  → delayed A CAS fails; revoking detached lease A preserves C
  → this protects successors, but does not solve expiry validation above
```

A read-only investigation reproduced both bad outcomes using the HTTP gateway
of Docker etcd **v3.6.8**. `Grant(TTL=1)` returned a server-clamped TTL of 2s.
Approximately 2.035s after the grant response, the old key was still visible,
the old lease's reported remaining TTL was 0, and owner-CAS reattachment to a
fresh lease succeeded. A separate owner-CAS deletion succeeded at approximately
2.049s. Keepalive of the already expired old lease returned TTL=0 only about
0.45s later, after revocation. These times describe that run, not timing bounds.
The isolated container and downloaded image were removed afterward.

`TimeToLive.TTL == 0` **alone is not evidence of expiry**: positive remaining
fractions of a second also truncate to 0. The experiment waited past the full
returned grant TTL, measured from receipt of the grant response, without any
renewal. Repeating this real-time observation can miss the deletion window;
that is inconclusive, not a passing conformance result.

## Source evidence

Pinned sources make the distinction independently of timing experiments:

- [etcd v3.6.8 lessor](https://github.com/etcd-io/etcd/blob/v3.6.8/server/lease/lessor.go):
  `runLoop` checks every 500ms; `revokeExpiredLeases` rate-limits and queues
  expiry work; `Lookup` returns a still-present lease without checking expiry.
  Revocation deletes attached keys. Queueing, quorum loss, and leadership changes
  can extend deletion lag; 500ms is not a maximum expiry-to-deletion delay.
- [v3.6.8 transactions](https://github.com/etcd-io/etcd/blob/v3.6.8/server/etcdserver/txn/txn.go):
  `compareKV` supports value/create/mod/version/lease-ID comparisons, not native
  remaining TTL or server time. `checkPut` checks only that the target lease can
  be looked up, so even the *new* attached lease may be expired but not revoked.
- [v3.6.8 server lease operations](https://github.com/etcd-io/etcd/blob/v3.6.8/server/etcdserver/v3_server.go):
  keepalive and TTL queries are separate from KV transactions; TTL queries
  truncate remaining seconds; proposed writes can have ambiguous timeout outcomes.
- [v3.6.8 Go lease client](https://github.com/etcd-io/etcd/blob/v3.6.8/client/v3/lease.go)
  and [gRPC lease server](https://github.com/etcd-io/etcd/blob/v3.6.8/server/etcdserver/api/v3rpc/lease.go):
  `KeepAliveOnce` restores the original granted TTL, takes no new TTL argument,
  retries recoverable failures, and maps a nonpositive reply to lease-not-found.
  A lease already expired at the lessor's initial check waits for revocation,
  demotion, or shutdown rather than immediately returning a successful keepalive.
- The lessor's `Renew` checks expiry **before** optional checkpoint Raft I/O and
  before acquiring its later write lock; afterward it rechecks existence rather
  than expiry before refreshing. This remains visible in
  [v3.6.15](https://github.com/etcd-io/etcd/blob/v3.6.15/server/lease/lessor.go).
  It must not be advertised as owner-and-live validation after every possible wait.
- [KV API guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/)
  describe strong KV consistency, not an additional transaction expiry predicate.

## Alternatives that do not meet the unchanged contract

| Approach | Why it is insufficient |
| --- | --- |
| Compare owner, revision, and lease ID, then rotate leases | Protects successors; all three comparisons still pass during expired-but-visible state. |
| `TimeToLive` before the CAS | Expiry can occur during the following network/Raft/lock wait. TTL=0 is also ambiguous. |
| `KeepAliveOnce` before the CAS | Separate operations leave a gap; keepalive changes the old granted TTL and cannot accept a replacement TTL. |
| Keepalive before release | Adds an extension side effect to release, including failed/ambiguous paths; still not atomic owner+expiry+delete. |
| Revoke the observed lease directly | Revocation is not a conditional owner-and-unexpired check; shared leases can also delete unrelated keys. |
| Read TTL again after the CAS | May reject a reply, but cannot erase an invalid mutation already committed. |
| Set an RPC deadline or check local validity afterward | Prevents late *reported* success; cancellation does not guarantee that a submitted transaction cannot commit later. |
| Store a client `expiresAt`, or keep a process-local ownership map | Changes the clock/process assumptions and still adds no server-side atomic current-time check. |
| An extra marker key on the same native lease | Its deletion has the same revocation lag. |
| `concurrency.Session` / `Mutex` | Default continuous keepalive and session-key reuse/reentrancy do not match this driver's fixed-TTL, non-reentrant operations. |
| Define expiry as committed key deletion | A viable different API, but explicitly not the strict contract selected here. |

The core's conservative validity interval starts before the request. That is
necessary for callers to stop work safely, but does not make an invalid remote
renewal/release compliant. “Errors may have ambiguous outcomes” is not permission
to perform operations forbidden by the backend contract.

## What would make implementation possible?

A suitable server-side lease-lock API must atomically:

1. Read ownership and native liveness at the operation's linearization point,
   **after relevant queue, lock, and network waits**.
2. Acquire only when absent/expired, without treating same-owner acquisition as
   renewal; return contention separately from backend failure.
3. For renewal, require a matching live owner and replace validity using the
   supplied positive TTL, including shorter and longer TTLs.
4. For release, require a matching live owner and remove only that incarnation.
5. Keep expiry, ownership mutation, and cleanup ordered so a delayed request or
   old-lease revocation cannot revive an expired owner or remove a successor.

An etcd extension would need to define how leader-local native expiry is
coordinated with replicated state-machine application; simply adding a
member-local wall-clock comparison in Raft apply is not a specification and can
make replicas disagree. A distinct server-side transactional lease-lock service
could also qualify, but would be a different deployment requirement, not a stock
etcd client driver. Either route requires an explicit safety argument and tests
before any implementation PR claims conformance.

If that prerequisite is eventually met, the expected production shape remains
`New(*clientv3.Client, prefix string, ...Option)`: caller-owned client, verbatim
prefix in an exclusive namespace, no default keepalive workers. Use dedicated
lease incarnations, exact owner/incarnation conditions, one overall finite
operation budget, pre-dispatch cancellation checks, and post-wait deadline checks.
Cleanup must preserve successors and must not extend the public request deadline.

## Version baseline and quantization

- [client v3.6.8](https://proxy.golang.org/go.etcd.io/etcd/client/v3/@v/v3.6.8.mod)
  declares Go 1.24.0 and toolchain Go 1.24.13; it is the last released 3.6 client
  compatible with the Go 1.24 baseline examined here. Pin matching client/API/
  client-pkg module versions rather than `@latest`.
- [v3.6.9](https://proxy.golang.org/go.etcd.io/etcd/client/v3/@v/v3.6.9.mod)
  requires Go 1.25;
  [v3.6.15](https://proxy.golang.org/go.etcd.io/etcd/client/v3/@v/v3.6.15.mod)
  and [v3.7.2](https://proxy.golang.org/go.etcd.io/etcd/client/v3/@v/v3.7.2.mod)
  require Go 1.26. These are observations of those exact versions, not a promise
  about future releases. An older client pin also needs a separate dependency/
  security assessment; it does not make an old server patch desirable.
- Proposed tested server floor: v3.6.8, with independent exact-tag/digest Docker
  coverage of v3.6.15 and v3.7.2. This is a proposed test policy, not a claim that
  the protocol requires v3.6.8 or that those additional servers were tested here.
- Lease TTL units are integer seconds. Round positive durations up by division
  and remainder, not overflow-prone addition. Respect `MaxLeaseTTL=9_000_000_000`.
  The server may further increase TTL to its configured minimum. Lease time
  begins at grant, before key attachment; do not claim a full requested interval
  starts at a later Put response. Failover/checkpoint/expiry lag remain separate
  backend considerations.

## Acceptance gates and tests

The [deterministic model tests](etcd/model_test.go) use only the Go standard
library. They encode expiry and KV revocation as separate transitions and assert
that the **bad** owner-CAS outcomes are possible. Green tests document the blocker;
they do not certify a driver or emulate all etcd behavior.

From the repository root:

```sh
go test ./docs/design/etcd
```

For a standalone copy outside a Go module:

```sh
GO111MODULE=off go test ./docs/design/etcd
```

Before replacing this design with an implementation:

- Prove and deterministically test expired-but-visible rejection, including
  expiry while waiting for a lock/network/Raft stage, plus delayed submitted
  operations after caller cancellation.
- Use barriers to pause after reading owner A; revoke A and install B; resume
  A's renew/release. B's value, incarnation, TTL, and attachments must survive.
- Test lease loss after grant/before attachment, changed-TTL renewal in both
  directions, same-owner non-reentrancy, and detached-old-lease cleanup.
- Test one shared deadline across stages, pre-cancellation, actual clock deadline
  before context timer notification, malformed replies, and backend error identity.
- Run explicit Go 1.24 compatibility jobs if that baseline is retained, and real
  Docker server tests for contention, eventual expiry, externally revoked leases,
  namespace isolation, client ownership, no hidden workers, and unavailable servers.
- Keep timing-window probes optional/manual; use a deterministic model or an
  instrumented server to test the semantic gap without flaky sleeps. No CI or
  runtime dependency is introduced by this proposal.
