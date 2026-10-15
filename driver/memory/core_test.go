package memory_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
	"github.com/tr1v3r/anylock/driver/memory"
)

func newLocker(t *testing.T, driver anylock.Driver, options ...anylock.Option) *anylock.Locker {
	t.Helper()
	locker, err := anylock.New(driver, options...)
	if err != nil {
		t.Fatal(err)
	}
	return locker
}

type observedDriver struct {
	anylock.Driver
	renewals atomic.Int32
	releases atomic.Int32
}

func (d *observedDriver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	d.renewals.Add(1)
	return d.Driver.Renew(ctx, key, owner, ttl)
}

func (d *observedDriver) Release(ctx context.Context, key, owner string) (bool, error) {
	d.releases.Add(1)
	return d.Driver.Release(ctx, key, owner)
}

func TestCoreGateRetention(t *testing.T) {
	failure := errors.New("callback failed")
	for _, mode := range []string{"success", "error", "panic", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			driver := &observedDriver{Driver: memory.New()}
			locker := newLocker(t, driver, anylock.WithAutoRenew())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			panicked := false
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						if recovered != failure {
							t.Fatal("wrong callback panic", recovered)
						}
						panicked = true
					}
				}()
				executed, err := locker.TryRun(ctx, "gate", time.Hour, func(work context.Context) error {
					calls++
					if work != ctx {
						t.Fatal("gate callback received a request context, not original context")
					}
					switch mode {
					case "error":
						return failure
					case "panic":
						panic(failure)
					case "cancellation":
						cancel()
						return work.Err()
					}
					return nil
				})
				want := error(nil)
				switch mode {
				case "error":
					want = failure
				case "cancellation":
					want = context.Canceled
				}
				if !executed || err != want {
					t.Fatal("unexpected gate callback result", executed, err)
				}
			}()
			if calls != 1 || panicked != (mode == "panic") {
				t.Fatal("callback did not execute exactly once", calls, panicked)
			}
			executed, err := locker.TryRun(context.Background(), "gate", time.Hour, func(context.Context) error {
				t.Fatal("retained gate callback unexpectedly ran")
				return nil
			})
			if executed || err != nil {
				t.Fatal("gate reservation was not retained", executed, err)
			}
			if _, err := newLocker(t, driver).TryLock(context.Background(), "gate", time.Hour); !errors.Is(
				err, anylock.ErrNotAcquired,
			) {
				t.Fatal("lease did not contend with gate", err)
			}
			if driver.renewals.Load() != 0 || driver.releases.Load() != 0 {
				t.Fatal("gate renewed/released reservation despite fixed-TTL contract")
			}
		})
	}
}

func TestCoreSharedLockersAndIndependentInstances(t *testing.T) {
	ctx := context.Background()
	driver := memory.New()
	first, second := newLocker(t, driver), newLocker(t, driver)
	lease, err := first.TryLock(ctx, "k", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Release(ctx) }()
	if _, err = second.TryLock(ctx, "k", time.Hour); !errors.Is(err, anylock.ErrNotAcquired) {
		t.Fatal("shared lockers did not contend", err)
	}
	independent, err := newLocker(t, memory.New()).TryLock(ctx, "k", time.Hour)
	if err != nil {
		t.Fatal("independent driver contended", err)
	}
	if err = independent.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = lease.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	successor, err := second.TryLock(ctx, "k", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = successor.Release(ctx) }()
	if err = lease.Renew(ctx); !errors.Is(err, anylock.ErrClosed) {
		t.Fatal("closed handle revived", err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal("successful core release was not idempotent", err)
	}
	if err := successor.Renew(ctx); err != nil {
		t.Fatal("stale handle affected successor", err)
	}
}
