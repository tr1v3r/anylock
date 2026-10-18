package anylock

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fencedDriver struct {
	Driver
	acquire func(context.Context, string, string, time.Duration) (AcquireResult, error)
}

func (d fencedDriver) TryAcquireFenced(ctx context.Context, key, owner string, ttl time.Duration) (AcquireResult, error) {
	return d.acquire(ctx, key, owner, ttl)
}

func TestFencingCapability(t *testing.T) {
	if l, operationErr := New(newMemoryDriver(), WithFencing()); l != nil || !errors.Is(operationErr, ErrFencingUnsupported) {
		t.Fatalf("unsupported fencing = %v, %v", l, operationErr)
	}
	ordinary := testLease(t, testLocker(t, newMemoryDriver()), "ordinary")
	if token, enabled := ordinary.Fence(); token != 0 || enabled {
		t.Fatalf("ordinary fence = %d, %v", token, enabled)
	}
}

func TestFencingResultContract(t *testing.T) {
	failure := errors.New("ambiguous acquisition")
	for _, tc := range []struct {
		name             string
		fenced           bool
		result           AcquireResult
		backendErr, want error
		releases         int
	}{
		{name: "fenced zero token", fenced: true, result: AcquireResult{Acquired: true}, want: ErrInvalidAcquireResult, releases: 1},
		{name: "busy token", fenced: true, result: AcquireResult{Fence: 7}, want: ErrInvalidAcquireResult},
		{name: "ordinary token", result: AcquireResult{Acquired: true, Fence: 7}, want: ErrInvalidAcquireResult, releases: 1},
		{name: "ordinary busy token", result: AcquireResult{Fence: 7}, want: ErrInvalidAcquireResult},
		{name: "contention", fenced: true, want: ErrNotAcquired},
		{name: "error wins", fenced: true, result: AcquireResult{Acquired: true, Fence: 7}, backendErr: failure, want: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releases := 0
			base := hookedDriver{
				acquire: func(context.Context, string, string, time.Duration) (bool, error) {
					return tc.result.Acquired, tc.backendErr
				},
				release: func(context.Context, string, string) (bool, error) { releases++; return true, nil },
			}
			var driver Driver = acquireResultDriver{Driver: base, result: tc.result, err: tc.backendErr}
			var options []Option
			if tc.fenced {
				driver = fencedDriver{Driver: driver, acquire: func(context.Context, string, string, time.Duration) (AcquireResult, error) {
					return tc.result, tc.backendErr
				}}
				options = append(options, WithFencing())
			}
			lease, err := testLocker(t, driver, options...).TryLock(context.Background(), "key", time.Hour)
			if lease != nil || !errors.Is(err, tc.want) || releases != tc.releases {
				t.Fatalf("acquire = %v, %v, cleanup=%d", lease, err, releases)
			}
		})
	}
}

type acquireResultDriver struct {
	Driver
	result AcquireResult
	err    error
}

func (d acquireResultDriver) TryAcquire(context.Context, string, string, time.Duration) (AcquireResult, error) {
	return d.result, d.err
}

func TestFenceSurvivesRenewAndClosure(t *testing.T) {
	base := newMemoryDriver()
	driver := fencedDriver{Driver: base, acquire: func(ctx context.Context, key, owner string, ttl time.Duration) (AcquireResult, error) {
		result, err := base.TryAcquire(ctx, key, owner, ttl)
		if result.Acquired {
			result.Fence = 42
		}
		return result, err
	}}
	lease := testLease(t, testLocker(t, driver, WithFencing()), "key")
	for _, action := range []func() error{
		func() error { return nil },
		func() error { return lease.Renew(context.Background()) },
		func() error { return lease.Release(context.Background()) },
	} {
		if operationErr := action(); operationErr != nil {
			t.Fatal(operationErr)
		}
		if token, enabled := lease.Fence(); token != 42 || !enabled {
			t.Fatalf("fence = %d, %v", token, enabled)
		}
	}
	if !errors.Is(lease.Err(), ErrClosed) {
		t.Fatal(lease.Err())
	}
	if operationErr := lease.Renew(context.Background()); !errors.Is(operationErr, ErrClosed) {
		t.Fatal("revived closed fenced lease", operationErr)
	}
}

func TestFencedGatesRejectBeforeDispatch(t *testing.T) {
	driver := fencedDriver{acquire: func(context.Context, string, string, time.Duration) (AcquireResult, error) {
		t.Fatal("dispatched")
		return AcquireResult{}, nil
	}}
	locker := testLocker(t, driver, WithFencing())
	callback := func(context.Context) error { t.Fatal("callback executed"); return nil }
	for _, call := range []func(context.Context, string, time.Duration, func(context.Context) error) (bool, error){locker.TryRun, locker.Run} {
		if executed, operationErr := call(context.Background(), "key", time.Hour, callback); executed || !errors.Is(operationErr, ErrFencingRequiresLease) {
			t.Fatalf("gate = %v, %v", executed, operationErr)
		}
	}
}

func TestFencedCanceledSuccessIsCleanedUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releases := 0
	driver := fencedDriver{Driver: hookedDriver{release: func(ctx context.Context, _, _ string) (bool, error) {
		if ctx.Err() != nil {
			t.Fatal("cleanup received canceled context")
		}
		releases++
		return true, nil
	}}, acquire: func(context.Context, string, string, time.Duration) (AcquireResult, error) {
		cancel()
		return AcquireResult{Acquired: true, Fence: 9}, nil
	}}
	lease, err := testLocker(t, driver, WithFencing()).TryLock(ctx, "key", time.Hour)
	if lease != nil || !errors.Is(err, context.Canceled) || releases != 1 {
		t.Fatalf("late acquire = %v, %v, releases=%d", lease, err, releases)
	}
}
