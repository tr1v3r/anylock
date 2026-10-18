// Package anylock provides backend-independent fixed-TTL execution gates and
// owner-checked lease locks.
//
// TryRun and Run reserve a key for a fixed TTL without renewing or releasing it
// when the callback finishes. Their TTL does not bound callback execution time.
// TryLock and Lock instead return time-bounded leases; callers must stop protected
// work when a lease expires or its Done channel closes. Leases can optionally
// carry fencing tokens; downstream storage must atomically
// validate them with protected writes. Neither mode provides exactly-once execution
// or durable business idempotency.
package anylock

import (
	"context"
	"time"
)

// Driver implements atomic lease operations on a shared backend. Implementations
// must be safe for concurrent use, honor operation deadlines, check cancellation
// before dispatch, and must not invoke methods on the Lease whose operation they
// are handling. Cancellation need not interrupt in-flight I/O immediately, but
// every call must settle by its deadline (apart from scheduling delays).
//
// Every acquisition uses a fresh, unguessable owner. TryAcquire must atomically
// create an absent/expired lease with that owner and a positive TTL. Renew must
// atomically check the owner and require an unexpired lease. Release atomically
// deletes a stored record only when its owner matches, even if it has expired;
// an old owner must never extend or remove a successor's lease. Its bool reports
// whether the owner-checked deletion removed a record, not prior lease validity.
// Expired records may already have been reclaimed. Acquired false is contention,
// with a zero Fence. Ordinary TryAcquire returns Fence zero on success too. Renew
// false means lost ownership; Release false means no matching record. Errors
// may have ambiguous outcomes and never authorize protected work.
//
// TTL is measured by the backend; it must not be rounded down. Clock jumps,
// backend failover, pauses, and partitions remain part of the backend's safety
// model. The core cannot turn a weak backend into a strongly consistent lock.
// Driver implementations own their connection resources; Locker never closes them.
// TryRun and Run use only TryAcquire; they never renew or release reservations.
type Driver interface {
	TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (AcquireResult, error)
	Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, owner string) (bool, error)
}

// AcquireResult describes an acquisition. Errors always take precedence over this
// value. Contention returns the zero value. Fence is zero for ordinary acquisition
// and strictly positive for successful fenced acquisition.
type AcquireResult struct {
	Acquired bool
	Fence    uint64
}

// FencingDriver declares support for opt-in fencing. It must use the SAME lease
// namespace and owner protocol as Driver.TryAcquire. Successful acquisitions
// atomically bind ownership to a new, strictly increasing token for that key;
// renewals preserve it. Gaps are allowed. Exhaustion must fail, never wrap.
// Implementations must document persistence, counter scope and failover limits.
// Wrappers must explicitly forward this capability; there is no fallback.
type FencingDriver interface {
	Driver
	TryAcquireFenced(ctx context.Context, key, owner string, ttl time.Duration) (AcquireResult, error)
}
