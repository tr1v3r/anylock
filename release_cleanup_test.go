package anylock_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

// cleanupDriver models owner-checked deletion independently of lease validity:
// expired matching records can be deleted, but can never be renewed.
// Hooks are configured before acquisition and run outside mu so channel barriers
// can observe in-flight requests without blocking record inspection.
type cleanupDriver struct {
	mu          sync.Mutex
	records     map[string]cleanupRecord
	releases    int
	renews      int
	releaseHook func(context.Context, int) error
	renewHook   func(context.Context, int) error
}

type cleanupRecord struct {
	owner    string
	deadline time.Time
}

var _ anylock.Driver = (*cleanupDriver)(nil)

func newCleanupDriver() *cleanupDriver {
	return &cleanupDriver{records: make(map[string]cleanupRecord)}
}

func (d *cleanupDriver) tryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if record, ok := d.records[key]; ok && time.Now().Before(record.deadline) {
		return false, nil
	}
	d.records[key] = cleanupRecord{owner: owner, deadline: time.Now().Add(ttl)}
	return true, nil
}

func (d *cleanupDriver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	d.mu.Lock()
	d.renews++
	call := d.renews
	d.mu.Unlock()
	if d.renewHook != nil {
		if err := d.renewHook(ctx, call); err != nil {
			return false, err
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, ok := d.records[key]
	if !ok || record.owner != owner || !time.Now().Before(record.deadline) {
		return false, nil
	}
	d.records[key] = cleanupRecord{owner: owner, deadline: time.Now().Add(ttl)}
	return true, nil
}

func (d *cleanupDriver) Release(ctx context.Context, key, owner string) (bool, error) {
	d.mu.Lock()
	d.releases++
	call := d.releases
	d.mu.Unlock()
	if d.releaseHook != nil {
		if err := d.releaseHook(ctx, call); err != nil {
			return false, err
		}
	}
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

func (d *cleanupDriver) record(key string) cleanupRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.records[key]
}

func (d *cleanupDriver) replace(key string, record cleanupRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if record.owner == "" {
		delete(d.records, key)
		return
	}
	d.records[key] = record
}

func (d *cleanupDriver) counts() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.releases, d.renews
}

func acquireCleanupLease(t *testing.T, ctx context.Context, driver *cleanupDriver,
	ttl time.Duration, options ...anylock.Option,
) anylock.Lease {
	t.Helper()
	locker, err := anylock.New(driver, options...)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := locker.TryLock(ctx, "cleanup:key", ttl)
	if err != nil || lease == nil {
		t.Fatalf("TryLock = %v, %v; want non-nil Lease, nil", lease, err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lease.Release(cleanup); err != nil {
			t.Errorf("final cleanup: %v", err)
		}
	})
	return lease
}

func awaitCleanupSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	select {
	case <-signal:
	case <-guard.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func assertCleanupInvalid(t *testing.T, lease anylock.Lease, cause error) {
	t.Helper()
	if !errors.Is(lease.Err(), cause) {
		t.Errorf("lease.Err() = %v; want %v", lease.Err(), cause)
	}
	select {
	case <-lease.Done():
	default:
		t.Error("invalid handle has open Done")
	}
	if !errors.Is(lease.Context().Err(), context.Canceled) {
		t.Errorf("invalid handle Context().Err() = %v", lease.Context().Err())
	}
}

func TestReleaseCleanupAbsentAndSuccessorAreSuccess(t *testing.T) {
	for _, successor := range []bool{false, true} {
		t.Run(fmt.Sprintf("successor=%t", successor), func(t *testing.T) {
			driver := newCleanupDriver()
			lease := acquireCleanupLease(t, context.Background(), driver, time.Hour)
			oldOwner := driver.record(lease.Key()).owner
			driver.replace(lease.Key(), cleanupRecord{})
			var next anylock.Lease
			if successor {
				next = acquireCleanupLease(t, context.Background(), driver, time.Hour)
				if driver.record(lease.Key()).owner == oldOwner {
					t.Fatal("successor reused the old owner")
				}
			}
			retained := driver.record(lease.Key())
			for range 3 {
				if err := lease.Release(context.Background()); err != nil {
					t.Errorf("owner-checked cleanup of absent/successor record = %v; want nil", err)
				}
			}
			if calls, _ := driver.counts(); calls != 1 {
				t.Errorf("driver.Release calls = %d; successful cleanup must be cached", calls)
			}
			if driver.record(lease.Key()) != retained {
				t.Fatal("old owner's cleanup changed the successor record")
			}
			if next != nil && next.Err() != nil {
				t.Errorf("old owner's cleanup invalidated successor: %v", next.Err())
			}
			assertCleanupInvalid(t, lease, anylock.ErrClosed)
			if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrClosed) {
				t.Errorf("Renew after cleanup = %v; want ErrClosed", err)
			}
			if _, calls := driver.counts(); calls != 0 {
				t.Errorf("closed handle made %d driver.Renew calls", calls)
			}
		})
	}
}

func TestReleaseCleanupRetriesBackendAndContextErrors(t *testing.T) {
	backendFailure := errors.New("cleanup transport failed")
	for _, scenario := range []string{"backend", "ambiguous backend", "request deadline", "caller cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			driver := newCleanupDriver()
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := backendFailure
			driver.releaseHook = func(request context.Context, call int) error {
				if call != 1 {
					return nil
				}
				switch scenario {
				case "ambiguous backend":
					// The backend deleted the record but the response was lost.
					driver.replace("cleanup:key", cleanupRecord{})
				case "request deadline":
					<-request.Done()
					return request.Err()
				case "caller cancellation":
					cancel()
					<-request.Done()
					return request.Err()
				}
				return fmt.Errorf("transport: %w", backendFailure)
			}
			switch scenario {
			case "request deadline":
				want = context.DeadlineExceeded
			case "caller cancellation":
				want = context.Canceled
			}
			lease := acquireCleanupLease(t, context.Background(), driver, time.Hour,
				anylock.WithOperationTimeout(50*time.Millisecond))
			if err := lease.Release(caller); !errors.Is(err, want) {
				t.Fatalf("first Release = %v; want identity %v", err, want)
			}
			assertCleanupInvalid(t, lease, anylock.ErrClosed)
			if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrClosed) {
				t.Errorf("failed cleanup allowed renewal: %v", err)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Errorf("Release with a fresh context = %v; want nil", err)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Errorf("Release after successful retry = %v; want nil", err)
			}
			if releases, renews := driver.counts(); releases != 2 || renews != 0 {
				t.Errorf("driver release/renew calls = %d/%d; want 2/0", releases, renews)
			}
			if record := driver.record(lease.Key()); record.owner != "" {
				t.Errorf("retry left a matching owner record: %+v", record)
			}
			assertCleanupInvalid(t, lease, anylock.ErrClosed)
		})
	}
}

func TestReleaseCleanupPreCanceledContextStillClosesHandle(t *testing.T) {
	driver := newCleanupDriver()
	lease := acquireCleanupLease(t, context.Background(), driver, time.Hour, anylock.WithAutoRenew())
	before := driver.record(lease.Key())
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lease.Release(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Release(pre-canceled) = %v; want context.Canceled", err)
	}
	assertCleanupInvalid(t, lease, anylock.ErrClosed)
	if calls, _ := driver.counts(); calls != 0 || driver.record(lease.Key()) != before {
		t.Fatal("pre-canceled cleanup dispatched a remote deletion")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release(fresh context) = %v", err)
	}
	if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrClosed) {
		t.Errorf("fresh cleanup context revived the handle: %v", err)
	}
	if releases, renews := driver.counts(); releases != 1 || renews != 0 {
		t.Errorf("driver release/renew calls = %d/%d; want 1/0", releases, renews)
	}
}

func TestReleaseCleanupInvalidatesBeforeBoundedRemoteRequest(t *testing.T) {
	driver := newCleanupDriver()
	entered := make(chan struct{})
	proceed := make(chan struct{})
	var unblock sync.Once
	const operationTimeout = 2 * time.Second
	driver.releaseHook = func(request context.Context, call int) error {
		if call != 1 {
			return nil
		}
		deadline, bounded := request.Deadline()
		if !bounded || time.Until(deadline) > operationTimeout {
			t.Error("remote cleanup did not receive an operation-bounded context")
		}
		close(entered)
		select {
		case <-proceed:
			return nil
		case <-request.Done():
			return request.Err()
		}
	}
	lease := acquireCleanupLease(t, context.Background(), driver, time.Hour,
		anylock.WithAutoRenew(), anylock.WithOperationTimeout(operationTimeout))
	originalDeadline := lease.Deadline()
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		result <- lease.Release(context.Background())
	}()
	t.Cleanup(func() {
		unblock.Do(func() { close(proceed) })
		awaitCleanupSignal(t, finished, "Release goroutine exit")
	})
	awaitCleanupSignal(t, entered, "remote Release entry")
	assertCleanupInvalid(t, lease, anylock.ErrClosed)
	if lease.Deadline() != originalDeadline {
		t.Error("local invalidation changed the validity deadline")
	}
	if driver.record(lease.Key()).owner == "" {
		t.Error("backend record disappeared before the blocked deletion")
	}
	// Renew must return from the terminal local state, not wait for remote cleanup.
	renewCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lease.Renew(renewCtx); !errors.Is(err, anylock.ErrClosed) {
		t.Errorf("Renew during blocked cleanup = %v; want ErrClosed", err)
	}
	unblock.Do(func() { close(proceed) })
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("unblocked Release = %v", err)
		}
	case <-renewCtx.Done():
		t.Fatal("unblocked Release did not finish")
	}
	if calls, _ := driver.counts(); calls != 1 {
		t.Errorf("remote Release calls = %d; want 1", calls)
	}
}

func TestReleaseCleanupRetainsPriorLoss(t *testing.T) {
	renewFailure := errors.New("renewal backend failed")
	for _, scenario := range []string{"expired matching record", "missing owner", "renewal backend", "acquisition canceled"} {
		t.Run(scenario, func(t *testing.T) {
			driver := newCleanupDriver()
			acquisition, cancel := context.WithCancel(context.Background())
			defer cancel()
			ttl := time.Hour
			if scenario == "expired matching record" {
				ttl = 150 * time.Millisecond
			}
			if scenario == "renewal backend" {
				driver.renewHook = func(context.Context, int) error { return renewFailure }
			}
			lease := acquireCleanupLease(t, acquisition, driver, ttl)
			switch scenario {
			case "expired matching record":
				awaitCleanupSignal(t, lease.Done(), "natural lease expiration")
				record := driver.record(lease.Key())
				record.deadline = time.Now().Add(-time.Hour)
				driver.replace(lease.Key(), record)
			case "missing owner":
				driver.replace(lease.Key(), cleanupRecord{})
				if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrLeaseLost) {
					t.Fatalf("Renew of missing record = %v", err)
				}
			case "renewal backend":
				if err := lease.Renew(context.Background()); !errors.Is(err, renewFailure) {
					t.Fatalf("Renew lost backend error identity: %v", err)
				}
			case "acquisition canceled":
				cancel()
				awaitCleanupSignal(t, lease.Done(), "acquisition cancellation")
			}
			prior := lease.Err()
			assertCleanupInvalid(t, lease, anylock.ErrLeaseLost)
			if scenario == "acquisition canceled" && !errors.Is(prior, context.Canceled) {
				t.Errorf("acquisition cancellation cause lost: %v", prior)
			}
			_, renewCalls := driver.counts()
			deadline, done, work := lease.Deadline(), lease.Done(), lease.Context()
			if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrLeaseLost) {
				t.Errorf("Renew before cleanup revived lost/expired handle: %v", err)
			}
			for range 2 {
				if err := lease.Release(context.Background()); err != nil {
					t.Errorf("cleanup after prior loss = %v; want nil", err)
				}
				if lease.Err() != prior || lease.Done() != done || lease.Context() != work ||
					lease.Deadline() != deadline {
					t.Error("cleanup replaced the first terminal cause or lifecycle observations")
				}
				assertCleanupInvalid(t, lease, anylock.ErrLeaseLost)
				if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrLeaseLost) {
					t.Errorf("Renew revived lost/expired handle: %v", err)
				}
			}
			if releases, renews := driver.counts(); releases != 1 || renews != renewCalls {
				t.Errorf("cleanup/rejected Renew dispatched extra requests: release=%d renew=%d", releases, renews)
			}
			if driver.record(lease.Key()).owner != "" {
				t.Error("cleanup retained the expired/lost matching owner")
			}
		})
	}
}

func TestReleaseCleanupStopsAutoRenewBeforeRetry(t *testing.T) {
	driver := newCleanupDriver()
	started := make(chan struct{})
	stopped := make(chan struct{})
	extraRenew := make(chan struct{}, 1)
	driver.renewHook = func(request context.Context, call int) error {
		if call != 1 {
			select {
			case extraRenew <- struct{}{}:
			default:
			}
			return errors.New("renewal restarted after Release")
		}
		close(started)
		<-request.Done()
		close(stopped)
		return request.Err()
	}
	backendFailure := errors.New("first cleanup failed")
	driver.releaseHook = func(_ context.Context, call int) error {
		select {
		case <-stopped:
		default:
			t.Error("remote Release entered before the automatic renewal request stopped")
		}
		if call == 1 {
			return backendFailure
		}
		return nil
	}
	const ttl = 900 * time.Millisecond
	lease := acquireCleanupLease(t, context.Background(), driver, ttl, anylock.WithAutoRenew())
	awaitCleanupSignal(t, started, "automatic renewal request")
	if err := lease.Release(context.Background()); !errors.Is(err, backendFailure) {
		t.Fatalf("first Release = %v; want backend failure", err)
	}
	assertCleanupInvalid(t, lease, anylock.ErrClosed)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("cleanup retry = %v", err)
	}
	if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrClosed) {
		t.Errorf("Renew after cleanup retry = %v", err)
	}
	// Observe two renewal intervals after retry; this timer is a negative-event
	// guard, not a sleep used to order the in-flight renewal/shutdown barriers.
	guard := time.NewTimer(2 * ttl / 3)
	defer guard.Stop()
	select {
	case <-extraRenew:
		t.Error("automatic renewal restarted after cleanup retry")
	case <-guard.C:
	}
	if releases, renews := driver.counts(); releases != 2 || renews != 1 {
		t.Errorf("driver release/renew calls = %d/%d; want 2/1", releases, renews)
	}
	assertCleanupInvalid(t, lease, anylock.ErrClosed)
}

func (d *cleanupDriver) TryAcquire(ctx context.Context, req anylock.AcquireRequest) (anylock.AcquireResult, error) {
	key, owner := req.Key, req.Owner
	if req.Fencing {
		return anylock.AcquireResult{}, anylock.ErrFencingUnsupported
	}
	ttl := req.TTL
	ok, err := d.tryAcquire(ctx, key, owner, ttl)
	return anylock.AcquireResult{Acquired: ok}, err
}
