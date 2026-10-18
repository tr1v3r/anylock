package anylock

import (
	"context"
	"errors"
	"testing"
	"time"
)

// acquisitionDriver observes the single acquisition protocol without an optional
// capability interface or separate fenced acquisition method.
type acquisitionDriver struct {
	Driver
	acquire func(context.Context, string, string, AcquireOptions) (AcquireResult, error)
}

func (d acquisitionDriver) TryAcquire(ctx context.Context, key, owner string, opts AcquireOptions) (AcquireResult, error) {
	return d.acquire(ctx, key, owner, opts)
}

func TestFencingUnsupportedAtAcquisition(t *testing.T) {
	// Construction does not call the driver or negotiate capabilities. The driver's
	// single TryAcquire must reject the request before granting any ownership.
	driver := newMemoryDriver()
	locker, err := New(driver, WithFencing())
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func(context.Context, string, time.Duration) (Lease, error){locker.TryLock, locker.Lock} {
		lease, acquireErr := call(context.Background(), "key", time.Hour)
		if lease != nil || !errors.Is(acquireErr, ErrFencingUnsupported) {
			t.Fatal(lease, acquireErr)
		}
	}
	if len(driver.records) != 0 {
		t.Fatal("unsupported fencing acquired ownership")
	}
	ordinary := testLease(t, testLocker(t, driver), "key")
	if token, enabled := ordinary.Fence(); token != 0 || enabled {
		t.Fatal(token, enabled)
	}
}

func TestAcquisitionOptionsAndResultContract(t *testing.T) {
	failure := errors.New("ambiguous acquisition")
	for _, tc := range []struct {
		name             string
		fenced           bool
		result           AcquireResult
		backendErr, want error
		releases         int
	}{
		{name: "ordinary success", result: AcquireResult{Acquired: true}},
		{name: "fenced success", fenced: true, result: AcquireResult{Acquired: true, Token: 7}},
		{name: "fenced zero token", fenced: true, result: AcquireResult{Acquired: true}, want: ErrInvalidAcquireResult, releases: 1},
		{name: "busy token", fenced: true, result: AcquireResult{Token: 7}, want: ErrInvalidAcquireResult},
		{name: "ordinary token", result: AcquireResult{Acquired: true, Token: 7}, want: ErrInvalidAcquireResult, releases: 1},
		{name: "ordinary busy token", result: AcquireResult{Token: 7}, want: ErrInvalidAcquireResult},
		{name: "ordinary contention", want: ErrNotAcquired},
		{name: "fenced contention", fenced: true, want: ErrNotAcquired},
		{name: "unsupported", fenced: true, backendErr: ErrFencingUnsupported, want: ErrFencingUnsupported},
		{name: "error wins", fenced: true, result: AcquireResult{Acquired: true, Token: 7}, backendErr: failure, want: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releases, calls := 0, 0
			driver := acquisitionDriver{Driver: hookedDriver{release: func(context.Context, string, string) (bool, error) { releases++; return true, nil }},
				acquire: func(_ context.Context, key, owner string, opts AcquireOptions) (AcquireResult, error) {
					calls++
					if key != "key" || owner == "" || opts != (AcquireOptions{TTL: time.Hour, Fencing: tc.fenced}) {
						t.Fatalf("request = %q, %q, %+v", key, owner, opts)
					}
					return tc.result, tc.backendErr
				},
			}
			var options []Option
			if tc.fenced {
				options = append(options, WithFencing())
			}
			// A wrapper embedding Driver forwards both modes without another interface.
			locker := testLocker(t, struct{ Driver }{Driver: driver}, options...)
			if calls != 0 {
				t.Fatal("New dispatched acquisition")
			}
			lease, err := locker.TryLock(context.Background(), "key", time.Hour)
			if !errors.Is(err, tc.want) || calls != 1 || releases != tc.releases {
				t.Fatalf("result = %v, calls=%d, releases=%d", err, calls, releases)
			}
			if tc.want != nil {
				if lease != nil {
					t.Fatal("failed acquisition returned a lease")
				}
			} else {
				if lease == nil {
					t.Fatal("successful acquisition returned nil")
				}
				token, enabled := lease.Fence()
				if token != tc.result.Token || enabled != tc.fenced {
					t.Fatal(token, enabled)
				}
				if cleanupErr := lease.Release(context.Background()); cleanupErr != nil {
					t.Fatal(cleanupErr)
				}
			}
		})
	}
}

func TestFenceSurvivesRenewAndClosure(t *testing.T) {
	base := newMemoryDriver()
	driver := acquisitionDriver{Driver: base, acquire: func(ctx context.Context, key, owner string, opts AcquireOptions) (AcquireResult, error) {
		// This fixture injects a token after storing ownership; production drivers
		// must bind the token and ownership atomically, as their own tests verify.
		result, err := base.TryAcquire(ctx, key, owner, AcquireOptions{TTL: opts.TTL})
		if result.Acquired {
			result.Token = 42
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
			t.Fatal(token, enabled)
		}
	}
	if !errors.Is(lease.Err(), ErrClosed) {
		t.Fatal(lease.Err())
	}
	if operationErr := lease.Renew(context.Background()); !errors.Is(operationErr, ErrClosed) {
		t.Fatal(operationErr)
	}
}

func TestFencedGatesRejectBeforeDispatch(t *testing.T) {
	driver := acquisitionDriver{acquire: func(context.Context, string, string, AcquireOptions) (AcquireResult, error) {
		t.Fatal("dispatched")
		return AcquireResult{}, nil
	}}
	locker := testLocker(t, driver, WithFencing())
	callback := func(context.Context) error { t.Fatal("callback executed"); return nil }
	for _, call := range []func(context.Context, string, time.Duration, func(context.Context) error) (bool, error){locker.TryRun, locker.Run} {
		if executed, err := call(context.Background(), "key", time.Hour, callback); executed || !errors.Is(err, ErrFencingRequiresLease) {
			t.Fatal(executed, err)
		}
	}
}

func TestOrdinaryGateOptions(t *testing.T) {
	calls := 0
	driver := acquisitionDriver{acquire: func(_ context.Context, key, owner string, opts AcquireOptions) (AcquireResult, error) {
		calls++
		if key != "key" || owner == "" || opts != (AcquireOptions{TTL: time.Hour}) {
			t.Fatal(key, owner, opts)
		}
		return AcquireResult{Acquired: true}, nil
	}}
	locker := testLocker(t, driver)
	for _, call := range []func(context.Context, string, time.Duration, func(context.Context) error) (bool, error){locker.TryRun, locker.Run} {
		if executed, err := call(context.Background(), "key", time.Hour, func(context.Context) error { return nil }); !executed || err != nil {
			t.Fatal(executed, err)
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestFencedCanceledSuccessIsCleanedUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releases := 0
	driver := acquisitionDriver{Driver: hookedDriver{release: func(cleanup context.Context, _, _ string) (bool, error) {
		if cleanup.Err() != nil {
			t.Fatal("cleanup received canceled context")
		}
		releases++
		return true, nil
	}}, acquire: func(context.Context, string, string, AcquireOptions) (AcquireResult, error) {
		cancel()
		return AcquireResult{Acquired: true, Token: 9}, nil
	}}
	lease, err := testLocker(t, driver, WithFencing()).TryLock(ctx, "key", time.Hour)
	if lease != nil || !errors.Is(err, context.Canceled) || releases != 1 {
		t.Fatal(lease, err, releases)
	}
}
