// Package memory implements process-local, owner-checked leases using only the
// standard library. Share one Driver pointer between Lockers that must contend;
// separate Driver instances have independent stores, even for identical keys.
// Logical key prefixes can subdivide a shared store.
//
// TTLs use the process's monotonic clock at nanosecond precision. Expired entries
// are logically absent and are swept in bounded batches on subsequent operations,
// including those on unrelated keys. There is no background worker: an idle
// Driver may retain expired entries until later operations or collection.
//
// Fenced acquisition uses an instance-wide monotonic counter. This backend provides
// no cross-process coordination, restart durability, or exactly-once execution.
// Process pauses and the platform's monotonic clock behavior, including whether
// it advances during system sleep, still apply.
// The anylock core, not this driver, generates owners and manages lease lifecycle.
//
// Operations recheck cancellation and actual deadlines before returning. A late
// error may follow a committed acquisition, renewal, or release; it does not prove
// rollback or unchanged ownership. Use a live cleanup context or let TTL expire.
package memory

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"time"

	"github.com/tr1v3r/anylock"
)

// Driver is an independent in-process lease store. It is safe for concurrent use
// but must not be copied. Construct it with New; its zero value is not usable.
// Driver owns no worker or external resource and does not need to be closed.
type Driver struct {
	// gate serializes store access while allowing context-aware waits.
	gate chan struct{}
	// epoch anchors nanosecond expiry ticks to the process monotonic clock.
	epoch time.Time
	// entries indexes stored ownership by logical key; gate protects it.
	entries map[string]*entry
	// expiry orders the same entries by expiry for opportunistic reclamation.
	expiry expiryHeap
	// fence is the instance-wide counter; gate protects it and exhaustion must not wrap.
	fence uint64
}

// Verify the memory backend implements the shared driver contract.
var _ anylock.Driver = (*Driver)(nil)

// entry links one ownership record to both the key map and expiry heap.
type entry struct {
	// key indexes this entry in the owning Driver.
	key string
	// owner is the acquisition token checked by renewal and release.
	owner string
	// expires is the exclusive deadline in nanoseconds since Driver.epoch.
	expires uint64
	// index is the expiry heap position, or -1 after removal.
	index int
}

// expiryHeap is a min-heap of ownership records ordered by monotonic expiry ticks.
type expiryHeap []*entry

// Len reports the number of records in the heap.
func (h expiryHeap) Len() int { return len(h) }

// Less places earlier expiry before later expiry.
func (h expiryHeap) Less(i, j int) bool { return h[i].expires < h[j].expires }

// Swap exchanges heap entries and updates their removal indexes.
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push appends a record for container/heap and records its index.
func (h *expiryHeap) Push(value any) {
	e := value.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}

// Pop removes the last heap entry and clears its reference and index.
func (h *expiryHeap) Pop() any {
	last := len(*h) - 1
	e := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	e.index = -1
	return e
}

// New constructs an empty, independent store without starting any goroutines.
// Share the returned pointer to coordinate multiple Lockers. A second New call
// never shares ownership with the first, regardless of the logical keys used.
func New() *Driver {
	return &Driver{gate: make(chan struct{}, 1), epoch: time.Now()}
}

// TryAcquire atomically reserves an absent or expired key for ttl. A live key
// returns Acquired false even when owner matches: acquiring is not renewing. Direct callers
// must use a fresh, unguessable owner for every new acquisition attempt. Fencing
// uses an instance-wide counter that survives expiry/release/sweeping, but not
// process or Driver replacement.
func (d *Driver) TryAcquire(
	ctx context.Context, req anylock.AcquireRequest,
) (anylock.AcquireResult, error) {
	key, owner := req.Key, req.Owner
	ttl := req.TTL
	if err := d.validate(ctx, key, owner); err != nil {
		return anylock.AcquireResult{}, err
	}
	if ttl <= 0 {
		return anylock.AcquireResult{}, errors.New("anylock/memory: TTL must be positive")
	}
	now, err := d.begin(ctx)
	if err != nil {
		return anylock.AcquireResult{}, err
	}
	defer d.leave()
	if e := d.entries[key]; e != nil {
		if now < e.expires {
			return anylock.AcquireResult{}, requestError(ctx)
		}
		d.remove(e)
	}
	var fence uint64
	if req.Fencing {
		if d.fence == math.MaxUint64 {
			return anylock.AcquireResult{}, errors.New("anylock/memory: fence exhausted")
		}
		d.fence++
		fence = d.fence
	}
	if d.entries == nil {
		d.entries = make(map[string]*entry)
	}
	e := &entry{key: key, owner: owner, expires: now + uint64(ttl)}
	heap.Push(&d.expiry, e)
	d.entries[key] = e
	if err := requestError(ctx); err != nil {
		return anylock.AcquireResult{}, err
	}
	return anylock.AcquireResult{Acquired: true, FencingToken: fence}, nil
}

// Renew atomically resets the matching live owner's expiry to ttl from this
// operation. It never creates or revives a lease. False means absent, expired, or
// different ownership. Like TryAcquire, ttl must be positive; it may shorten a
// lease when a direct caller supplies a shorter duration.
func (d *Driver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, errors.New("anylock/memory: TTL must be positive")
	}
	now, err := d.begin(ctx)
	if err != nil {
		return false, err
	}
	defer d.leave()
	e := d.entries[key]
	if e == nil || now >= e.expires || e.owner != owner {
		return result(ctx, false)
	}
	e.expires = now + uint64(ttl)
	heap.Fix(&d.expiry, e.index)
	return result(ctx, true)
}

// Release atomically deletes a stored record only when its owner matches. Expired
// entries may already have been swept; false means no matching record remains.
// A stale owner never deletes a successor. The bool reports an owner-checked
// deletion, not prior validity; core Lease.Release treats false as successful cleanup.
func (d *Driver) Release(ctx context.Context, key, owner string) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	if _, err := d.begin(ctx); err != nil {
		return false, err
	}
	defer d.leave()
	e := d.entries[key]
	if e == nil || e.owner != owner {
		return result(ctx, false)
	}
	d.remove(e)
	return result(ctx, true)
}

// validate rejects uninitialized drivers and invalid input before waiting for the gate.
func (d *Driver) validate(ctx context.Context, key, owner string) error {
	if d == nil || d.gate == nil {
		return errors.New("anylock/memory: nil or uninitialized driver; use New")
	}
	if ctx == nil {
		return errors.New("anylock/memory: nil context")
	}
	if key == "" || owner == "" {
		return errors.New("anylock/memory: key and owner must not be empty")
	}
	return requestError(ctx)
}

// begin returns with the gate held only on success. Waiting and sweeping remain
// bounded by ctx, and ownership is checked against time sampled AFTER both.
func (d *Driver) begin(ctx context.Context) (uint64, error) {
	if err := d.enter(ctx); err != nil {
		return 0, err
	}
	if err := d.sweep(ctx); err != nil {
		d.leave()
		return 0, err
	}
	if err := requestError(ctx); err != nil {
		d.leave()
		return 0, err
	}
	return d.ticks(), nil
}

// enter acquires the store gate unless cancellation or the actual context deadline wins.
func (d *Driver) enter(ctx context.Context) error {
	// Avoid a timer allocation when the gate is immediately available.
	select {
	case d.gate <- struct{}{}:
		return d.entered(ctx)
	default:
	}
	var deadline <-chan time.Time
	if limit, ok := ctx.Deadline(); ok {
		timer := time.NewTimer(time.Until(limit))
		defer timer.Stop()
		deadline = timer.C
	}
	select {
	case d.gate <- struct{}{}:
		return d.entered(ctx)
	case <-ctx.Done():
		return ctx.Err()
	case <-deadline:
		return context.DeadlineExceeded
	}
}

// entered rechecks the request after acquiring the gate and releases it on failure.
func (d *Driver) entered(ctx context.Context) error {
	if err := requestError(ctx); err != nil {
		d.leave()
		return err
	}
	return nil
}

// leave releases the store gate held by the current operation.
func (d *Driver) leave() { <-d.gate }

// result is called while the gate is still held, after either a lookup or a
// mutation. A late failure is ambiguous: report the context error without rolling
// back ownership or treating an already-committed change as successful.
func result(ctx context.Context, ok bool) (bool, error) {
	if err := requestError(ctx); err != nil {
		return false, err
	}
	return ok, nil
}

// requestError checks both recorded cancellation and the actual deadline clock.
func requestError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A deadline can pass before the context's timer records cancellation.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// ticks returns monotonic nanoseconds since this store was created.
func (d *Driver) ticks() uint64 {
	// time.Since uses epoch's monotonic reading. A positive duration and elapsed
	// time are each at most MaxInt64, so adding their uint64 values cannot wrap.
	// In contrast, time.Time.Add(MaxInt64) can overflow its monotonic component
	// and silently fall back to the wall clock. Normal Go duration/monotonic
	// lifetime limits apply; this store is not intended to survive centuries.
	return uint64(time.Since(d.epoch))
}

// sweepBudget caps unrelated reclamation per operation so expiry bursts do not
// monopolize the store gate. Target-key validity is checked separately.
const sweepBudget = 64

// sweep removes up to sweepBudget expired entries while honoring ctx. The store
// gate must be held; a remaining backlog is reclaimed by later operations.
func (d *Driver) sweep(ctx context.Context) error {
	for removed := 0; removed < sweepBudget && len(d.expiry) > 0; removed++ {
		if err := requestError(ctx); err != nil {
			return err
		}
		if d.ticks() < d.expiry[0].expires {
			break
		}
		d.remove(d.expiry[0])
	}
	return nil
}

// remove deletes an entry from both indexes; the store gate must be held.
func (d *Driver) remove(e *entry) {
	heap.Remove(&d.expiry, e.index)
	delete(d.entries, e.key)
	if len(d.entries) == 0 {
		// Release high-water storage once the store becomes empty. Partial stores
		// may retain Go map buckets/slice capacity, but no per-key tombstones.
		d.entries = nil
		d.expiry = nil
	}
}
