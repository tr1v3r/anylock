//go:build go1.25

package memory_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tr1v3r/anylock"
	"github.com/tr1v3r/anylock/driver/memory"
)

func TestSynctestCoreAutoRenewAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := &observedDriver{Driver: memory.New()}
		locker := newLocker(t, driver, anylock.WithAutoRenew())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		lease, err := locker.TryLock(ctx, "k", 9*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if lease.Err() != nil || driver.renewals.Load() < 3 {
			t.Fatal("auto renewal did not retain lease beyond original TTL", lease.Err(), driver.renewals.Load())
		}
		if _, err := locker.TryLock(context.Background(), "k", time.Second); !errors.Is(err, anylock.ErrNotAcquired) {
			t.Fatal("renewed backend lease was lost", err)
		}
		cancel()
		synctest.Wait()
		select {
		case <-lease.Done():
		default:
			t.Fatal("cancellation did not invalidate lease")
		}
		if !errors.Is(lease.Err(), context.Canceled) || !errors.Is(lease.Err(), anylock.ErrLeaseLost) {
			t.Fatal("canceled lease cause lost", lease.Err())
		}
		if _, err := locker.TryLock(context.Background(), "k", time.Second); !errors.Is(err, anylock.ErrNotAcquired) {
			t.Fatal("cancellation automatically deleted ownership", err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal("live cleanup context failed", err)
		}
		synctest.Wait()
	})
}

func TestSynctestCoreExpiryStaleHandleAndRunRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := memory.New()
		locker := newLocker(t, driver, anylock.WithRetryInterval(time.Millisecond))
		lease, err := locker.TryLock(context.Background(), "k", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if !errors.Is(lease.Err(), anylock.ErrLeaseLost) {
			t.Fatal("expired handle remained valid", lease.Err())
		}
		successor, err := locker.TryLock(context.Background(), "k", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Release(context.Background()); !errors.Is(err, anylock.ErrLeaseLost) {
			t.Fatal("stale handle release succeeded", err)
		}
		if err := lease.Renew(context.Background()); !errors.Is(err, anylock.ErrLeaseLost) {
			t.Fatal("expired handle revived", err)
		}
		if err := successor.Renew(context.Background()); err != nil {
			t.Fatal("stale owner affected successor", err)
		}
		if err := successor.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := 0
		fn := func(context.Context) error { calls++; return nil }
		if executed, err := locker.TryRun(context.Background(), "gate", time.Second, fn); !executed || err != nil {
			t.Fatal("first gate did not run", executed, err)
		}
		if executed, err := locker.Run(context.Background(), "gate", time.Second, fn); !executed || err != nil {
			t.Fatal("Run did not retry contention to TTL expiry", executed, err)
		}
		if calls != 2 {
			t.Fatal("unexpected gate callback count", calls)
		}
		synctest.Wait()
	})
}

func TestSynctestCoreGateNeverAutoRenews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		driver := &observedDriver{Driver: memory.New()}
		locker := newLocker(t, driver, anylock.WithAutoRenew())
		if executed, err := locker.TryRun(context.Background(), "gate", 3*time.Second,
			func(context.Context) error { return nil }); !executed || err != nil {
			t.Fatal("gate failed", executed, err)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if driver.renewals.Load() != 0 || driver.releases.Load() != 0 {
			t.Fatal("gate created lease renewal/cleanup lifecycle")
		}
		time.Sleep(time.Second)
		if executed, err := locker.TryRun(context.Background(), "gate", time.Second,
			func(context.Context) error { return nil }); !executed || err != nil {
			t.Fatal("gate did not expire at original TTL", executed, err)
		}
	})
}
