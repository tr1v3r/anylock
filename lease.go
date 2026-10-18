package anylock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Lease is a single-use, time-bounded ownership handle. Implementations must be
// safe for concurrent use. A lost or closed handle must never become valid again.
// Copying an interface value does not create another acquisition.
//
// Lock and TryLock return a non-nil Lease on success and nil on any error. Their
// handles retain the acquisition Driver and observe expiry and acquisition-context
// cancellation independently of renewal I/O. Cancellation does not automatically
// release remote ownership. Implementations and wrappers must preserve these
// guarantees and the method contracts below.
type Lease interface {
	// Key returns the logical key, without any driver's namespace prefix.
	Key() string

	// Fence returns the immutable token from this acquisition, and whether fencing
	// was enabled. It remains available after expiry/release for diagnostics, but
	// is never fresh authorization. Check Err/Done and stop work on invalidation.
	// The target storage must atomically validate the token with each write.
	Fence() (token uint64, enabled bool)

	// Context returns a context canceled on local lease invalidation. Pass it to
	// cooperative protected work; use Err for the terminal lease cause. Its Deadline
	// is inherited from the caller, not the renewable local validity deadline.
	Context() context.Context

	// Deadline returns the conservative local validity deadline. It is not proof of
	// ownership: a backend can lose data or ownership before this deadline.
	Deadline() time.Time

	// Done closes permanently on expiry, lost ownership, renewal failure, context
	// cancellation, or a Release attempt. It signals LOCAL invalidation, not proof
	// that the remote key was deleted. Stop protected work when it closes.
	Done() <-chan struct{}

	// Err is nil while locally valid. It returns ErrClosed after a Release attempt,
	// ErrLeaseLost on expiry/ownership loss, and joins ErrLeaseLost with backend or
	// context errors on failure. The first terminal cause is retained permanently.
	Err() error

	// Renew extends ownership by the original TTL. It serializes with other Renew
	// and Release calls. Any failure AFTER calling the driver invalidates the handle,
	// even when the backend outcome is uncertain. A canceled wait makes no request
	// and does not itself abandon a still-valid lease. A successful reply received
	// after the request deadline or cancellation also invalidates the handle.
	Renew(ctx context.Context) error

	// Release permanently closes the local handle and stops its worker BEFORE
	// releasing remote ownership. An expired/lost handle may still try owner-checked
	// cleanup. Use a live cleanup context, not a canceled acquisition context.
	//
	// Cleanup is idempotent: a missing or successor-owned record is a successful
	// no-op. A nil result is not proof of prior ownership or lease validity.
	// Backend/context errors remain errors and cleanup can be retried with another
	// context, but Renew can never restart this handle or reset its terminal cause.
	Release(ctx context.Context) error
}

// lease is Locker's shared implementation of Lease and must not be copied.
// Each lease has an independent expiry/cancellation watcher. Automatic renewal
// adds a second worker, so a slow backend cannot delay Done. The Driver must
// honor operation deadlines so in-flight requests and shutdown remain bounded.
type lease struct {
	driver           Driver
	key              string
	owner            string
	fence            uint64
	ttl              time.Duration
	operationTimeout time.Duration
	auto             bool
	ctx              context.Context
	cancel           context.CancelFunc

	mu           sync.Mutex
	deadline     time.Time
	err          error
	done         chan struct{}
	stopped      chan struct{}
	updated      chan struct{}
	renewWake    chan struct{}
	renewStopped chan struct{}

	// gate serializes remote renewal/release without making context-aware callers
	// wait unconditionally for a mutex held across an in-flight network operation.
	gate        chan struct{}
	releaseDone bool
}

var _ Lease = (*lease)(nil)

func newLease(
	ctx context.Context, locker *Locker, key, owner string, ttl time.Duration, deadline time.Time, fence uint64,
) *lease {
	ctx, cancel := context.WithCancel(ctx)
	l := &lease{
		driver: locker.driver, key: key, owner: owner, ttl: ttl, auto: locker.autoRenew, fence: fence,
		operationTimeout: locker.operationTimeout,
		ctx:              ctx, cancel: cancel, deadline: deadline,
		done: make(chan struct{}), stopped: make(chan struct{}),
		updated: make(chan struct{}, 1), gate: make(chan struct{}, 1),
		renewWake: make(chan struct{}, 1), renewStopped: make(chan struct{}),
	}
	l.gate <- struct{}{}
	go l.run()
	return l
}

// Key returns the logical key, without any driver's namespace prefix.
func (l *lease) Key() string { return l.key }

// Fence returns this acquisition's immutable token, even after local invalidation.
func (l *lease) Fence() (uint64, bool) { return l.fence, l.fence != 0 }

// Context returns a context canceled on local lease invalidation. Pass it to
// cooperative protected work; use Err for the terminal lease cause. Its Deadline
// is inherited from the caller, not the renewable local validity deadline.
func (l *lease) Context() context.Context { return l.ctx }

// Deadline returns the conservative local validity deadline. It is not proof of
// ownership: a backend can lose data or ownership before this deadline.
func (l *lease) Deadline() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.deadline
}

// Done closes permanently on expiry, lost ownership, renewal failure, context
// cancellation, or a Release attempt. It signals LOCAL invalidation, not proof
// that the remote key was deleted. Stop protected work when it closes.
func (l *lease) Done() <-chan struct{} {
	_ = l.Err()
	return l.done
}

// Err is nil while locally valid. It returns ErrClosed after a Release attempt,
// ErrLeaseLost on expiry/ownership loss, and joins ErrLeaseLost with backend or
// context errors on failure. The first terminal cause is retained permanently.
func (l *lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stateLocked()
}

func (l *lease) stateLocked() error {
	if l.err == nil {
		if err := l.ctx.Err(); err != nil {
			_ = l.finishLocked(errors.Join(ErrLeaseLost, err))
		} else if !time.Now().Before(l.deadline) {
			_ = l.finishLocked(ErrLeaseLost)
		}
	}
	return l.err
}

func (l *lease) finishLocked(err error) error {
	if l.err == nil {
		l.err = err
		close(l.done)
		l.cancel()
	}
	return l.err
}

func (l *lease) finish(err error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.finishLocked(err)
}

func (l *lease) enter(ctx context.Context, checkLease bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var done <-chan struct{}
	if checkLease {
		done = l.done
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return l.Err()
	case <-l.gate:
		if err := ctx.Err(); err != nil {
			l.leave()
			return err
		}
		return nil
	}
}

func (l *lease) leave() { l.gate <- struct{}{} }

// Renew extends ownership by the original TTL. It serializes with other Renew
// and Release calls. Any failure AFTER calling the driver invalidates the handle,
// even when the backend outcome is uncertain. A canceled wait makes no request
// and does not itself abandon a still-valid lease. A successful reply received
// after the request deadline or cancellation also invalidates the handle.
func (l *lease) Renew(ctx context.Context) error {
	if err := l.enter(ctx, true); err != nil {
		return err
	}
	defer l.leave()

	l.mu.Lock()
	err := l.stateLocked()
	deadline := l.deadline
	l.mu.Unlock()
	if err != nil {
		return err
	}

	if limit := time.Now().Add(l.operationTimeout); limit.Before(deadline) {
		deadline = limit
	}
	request, cancel := context.WithDeadline(ctx, deadline)
	stop := context.AfterFunc(l.ctx, cancel)
	defer stop()
	defer cancel()
	started := time.Now()
	owned, err := l.driver.Renew(request, l.key, l.owner, l.ttl)
	if err != nil {
		return l.finish(errors.Join(ErrLeaseLost, fmt.Errorf("anylock: renew %q: %w", l.key, err)))
	}
	if requestErr := requestError(request); requestErr != nil {
		return l.finish(errors.Join(ErrLeaseLost, fmt.Errorf("anylock: renew %q: %w", l.key, requestErr)))
	}
	if !owned {
		return l.finish(ErrLeaseLost)
	}

	l.mu.Lock()
	if stateErr := l.stateLocked(); stateErr != nil {
		l.mu.Unlock()
		return stateErr
	}
	deadline = started.Add(l.ttl)
	if !time.Now().Before(deadline) {
		err = l.finishLocked(ErrLeaseLost)
		l.mu.Unlock()
		return err
	}
	l.deadline = deadline
	l.mu.Unlock()
	for _, wake := range []chan struct{}{l.updated, l.renewWake} {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Release permanently closes the local handle and stops its worker BEFORE
// releasing remote ownership. An expired/lost handle may still try owner-checked
// cleanup. Use a live cleanup context, not a canceled acquisition context.
//
// Cleanup is idempotent: a missing or successor-owned record is a successful
// no-op. A nil result is not proof of prior ownership or lease validity.
// Backend/context errors remain errors and cleanup can be retried with another
// context, but Renew can never restart this handle or reset its terminal cause.
func (l *lease) Release(ctx context.Context) error {
	l.mu.Lock()
	_ = l.stateLocked()
	_ = l.finishLocked(ErrClosed)
	l.mu.Unlock()
	select {
	case <-l.stopped:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := l.enter(ctx, false); err != nil {
		return err
	}
	defer l.leave()
	if l.releaseDone {
		return nil
	}
	request, cancel := context.WithTimeout(ctx, l.operationTimeout)
	defer cancel()
	_, err := l.driver.Release(request, l.key, l.owner)
	if err != nil {
		return fmt.Errorf("anylock: release %q: %w", l.key, err)
	}
	l.releaseDone = true
	return nil
}

func (l *lease) run() {
	defer close(l.stopped)
	if l.auto {
		go func() {
			defer close(l.renewStopped)
			l.watch(true, l.renewWake)
		}()
	}
	l.watch(false, l.updated)
	if l.auto {
		<-l.renewStopped
	}
}

func (l *lease) watch(renew bool, wake <-chan struct{}) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		l.mu.Lock()
		err := l.stateLocked()
		target := l.deadline
		if renew {
			target = target.Add(-l.ttl + max(l.ttl/3, time.Nanosecond))
		}
		l.mu.Unlock()
		if err != nil {
			return
		}
		timer.Reset(time.Until(target))
		select {
		case <-l.ctx.Done():
			_ = l.finish(errors.Join(ErrLeaseLost, l.ctx.Err()))
			return
		case <-l.done:
			return
		case <-wake:
		case <-timer.C:
			if renew {
				if err := l.Renew(l.ctx); err != nil {
					_ = l.finish(errors.Join(ErrLeaseLost, err))
					return
				}
			}
		}
	}
}
