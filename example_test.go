package anylock_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tr1v3r/anylock"
)

// This in-memory fixture keeps executable core examples dependency-free. It is
// for examples only; applications inject a driver for their coordination backend.
type exampleRecord struct {
	owner    string
	deadline time.Time
}

type exampleDriver struct {
	mu      sync.Mutex
	records map[string]exampleRecord
}

func newExampleDriver() *exampleDriver {
	return &exampleDriver{records: make(map[string]exampleRecord)}
}

func (d *exampleDriver) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if record, ok := d.records[key]; ok && time.Now().Before(record.deadline) {
		return false, nil
	}
	d.records[key] = exampleRecord{owner, time.Now().Add(ttl)}
	return true, nil
}

func (d *exampleDriver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, ok := d.records[key]
	if !ok || record.owner != owner || !time.Now().Before(record.deadline) {
		return false, nil
	}
	d.records[key] = exampleRecord{owner, time.Now().Add(ttl)}
	return true, nil
}

func (d *exampleDriver) Release(ctx context.Context, key, owner string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, ok := d.records[key]
	if !ok || record.owner != owner {
		return false, nil
	}
	delete(d.records, key)
	return true, nil
}

func newExampleLocker(options ...anylock.Option) *anylock.Locker {
	locker, err := anylock.New(newExampleDriver(), options...)
	if err != nil {
		panic(err)
	}
	return locker
}

// releaseExampleLease stops all lease workers before the example returns.
// Cleanup needs its own live context, not the possibly canceled work context.
func releaseExampleLease(lease anylock.Lease) {
	cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.Release(cleanup); err != nil {
		panic(err)
	}
}

func ExampleNew() {
	// Inject a Driver; the core package does not own backend resources.
	driver := newExampleDriver()
	locker, err := anylock.New(driver)
	if err != nil {
		panic(err)
	}
	executed, err := locker.TryRun(context.Background(), "event:123", time.Hour,
		func(context.Context) error { return nil })
	fmt.Println("executed:", executed, "error:", err)
	// Output:
	// executed: true error: <nil>
}

func ExampleWithRetryInterval() {
	// Only contention is retried, with this delay between attempts.
	locker := newExampleLocker(anylock.WithRetryInterval(20 * time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	executed, err := locker.Run(ctx, "job:123", time.Hour, func(context.Context) error { return nil })
	fmt.Println("executed:", executed, "error:", err)
	// Output:
	// executed: true error: <nil>
}

func ExampleWithOperationTimeout() {
	// Each backend request is bounded; the callback still receives the caller ctx.
	locker := newExampleLocker(anylock.WithOperationTimeout(500 * time.Millisecond))
	ctx := context.Background()
	executed, err := locker.TryRun(ctx, "job:123", time.Hour, func(callbackCtx context.Context) error {
		fmt.Println("original caller context:", callbackCtx == ctx)
		return nil
	})
	fmt.Println("executed:", executed, "error:", err)
	// Output:
	// original caller context: true
	// executed: true error: <nil>
}

func ExampleWithAutoRenew() {
	// Auto-renew is opt-in for leases; it has no effect on TryRun or Run gates.
	locker := newExampleLocker(anylock.WithAutoRenew())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease, err := locker.TryLock(ctx, "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)

	// Cooperative work must stop when the lease's context is canceled.
	work := lease.Context()
	fmt.Println("work active:", work.Err() == nil)
	cancel()
	<-work.Done()
	fmt.Println("work canceled:", errors.Is(work.Err(), context.Canceled))
	// Output:
	// work active: true
	// work canceled: true
}

func ExampleLocker_TryRun() {
	locker := newExampleLocker()
	process := func(context.Context) error {
		fmt.Println("process event:123")
		return nil
	}
	executed, err := locker.TryRun(context.Background(), "event:123", time.Hour, process)
	fmt.Println("first:", executed, err)
	executed, err = locker.TryRun(context.Background(), "event:123", time.Hour, process)
	fmt.Println("duplicate:", executed, err)
	// There is no Release: the marker remains until the original TTL expires.

	// Output:
	// process event:123
	// first: true <nil>
	// duplicate: false <nil>
}

func ExampleLocker_TryRun_callbackError() {
	locker := newExampleLocker()
	failure := errors.New("processing failed")
	executed, err := locker.TryRun(context.Background(), "event:123", time.Hour,
		func(context.Context) error { return failure })
	fmt.Println("callback executed:", executed, "original error:", err == failure)

	// An error does not delete the marker or retry the callback.
	executed, err = locker.TryRun(context.Background(), "event:123", time.Hour,
		func(context.Context) error { panic("duplicate callback must not run") })
	fmt.Println("duplicate:", executed, err)
	// Output:
	// callback executed: true original error: true
	// duplicate: false <nil>
}

func ExampleLocker_Run() {
	locker := newExampleLocker(anylock.WithRetryInterval(20 * time.Millisecond))
	// A caller deadline also bounds waiting for a contended window to expire.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	executed, err := locker.Run(ctx, "job:123", time.Hour, func(work context.Context) error {
		fmt.Println("job context active:", work.Err() == nil)
		return nil
	})
	fmt.Println("executed:", executed, "error:", err)
	// Output:
	// job context active: true
	// executed: true error: <nil>
}

func ExampleLocker_Run_cancellation() {
	locker := newExampleLocker()
	executed, err := locker.TryRun(context.Background(), "job:123", time.Hour,
		func(context.Context) error { return nil })
	if err != nil || !executed {
		panic("failed to reserve initial window")
	}
	// Bound the wait for a busy key instead of waiting for the hour-long marker.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	executed, err = locker.Run(ctx, "job:123", time.Hour,
		func(context.Context) error { panic("canceled callback must not run") })
	fmt.Println("executed:", executed, "timed out:", errors.Is(err, context.DeadlineExceeded))
	// Output:
	// executed: false timed out: true
}

func ExampleLocker_TryLock() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	fmt.Println("acquired:", lease.Key())

	_, err = locker.TryLock(context.Background(), "resource:123", time.Hour)
	fmt.Println("contended:", errors.Is(err, anylock.ErrNotAcquired))
	// Output:
	// acquired: resource:123
	// contended: true
}

func ExampleLocker_Lock() {
	locker := newExampleLocker(anylock.WithRetryInterval(20 * time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := locker.Lock(ctx, "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	fmt.Println("acquired:", lease.Key())
	// Output:
	// acquired: resource:123
}

func ExampleLease_Renew() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)

	// Manual renewal extends ownership by the original TTL, not a new duration.
	err = lease.Renew(context.Background())
	fmt.Println("renewed:", err == nil)
	// Output:
	// renewed: true
}

func ExampleLease_Release() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	fmt.Println("release:", lease.Release(context.Background()))
	fmt.Println("release again:", lease.Release(context.Background()))
	// Output:
	// release: <nil>
	// release again: <nil>
}

func ExampleLease_Key() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	// The logical key does not include a backend namespace prefix.
	fmt.Println(lease.Key())
	// Output:
	// resource:123
}

func ExampleLease_Deadline() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	// This is a conservative local deadline, not proof of remote ownership.
	fmt.Println("validity remaining:", time.Until(lease.Deadline()) > 0)
	// Output:
	// validity remaining: true
}

func ExampleLease_Done() {
	locker := newExampleLocker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease, err := locker.TryLock(ctx, "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)

	cancel()
	<-lease.Done() // Stop protected work; this does not imply remote deletion.
	fmt.Println("caller canceled:", errors.Is(lease.Err(), context.Canceled))
	// Output:
	// caller canceled: true
}

func ExampleLease_Err() {
	locker := newExampleLocker()
	lease, err := locker.TryLock(context.Background(), "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)
	fmt.Println("initial error:", lease.Err())
	if releaseErr := lease.Release(context.Background()); releaseErr != nil {
		panic(releaseErr)
	}
	fmt.Println("closed:", errors.Is(lease.Err(), anylock.ErrClosed))
	// Output:
	// initial error: <nil>
	// closed: true
}

func ExampleLease_Context() {
	locker := newExampleLocker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease, err := locker.TryLock(ctx, "resource:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(lease)

	// Pass this context to I/O or other cooperative work protected by the lease.
	work := lease.Context()
	fmt.Println("work active:", work.Err() == nil)
	cancel()
	<-work.Done()
	fmt.Println("work canceled:", errors.Is(work.Err(), context.Canceled))
	// Output:
	// work active: true
	// work canceled: true
}
