package anylock_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

// Business-owned interfaces accept *Locker directly, without an adapter.
type businessTryLocker interface {
	TryLock(context.Context, string, time.Duration) (anylock.Lease, error)
}

type businessLocker interface {
	Lock(context.Context, string, time.Duration) (anylock.Lease, error)
}

var (
	_ businessTryLocker = (*anylock.Locker)(nil)
	_ businessLocker    = (*anylock.Locker)(nil)
	_ anylock.Lease     = externalLease{}
	_ anylock.Lease     = observedLease{}
)

// Explicit methods (rather than embedding) prove Lease has no private seal.
// Delegate to a real lease: this is not a fake lifecycle implementation.
type externalLease struct{ delegate anylock.Lease }

func (l externalLease) Key() string                       { return l.delegate.Key() }
func (l externalLease) Context() context.Context          { return l.delegate.Context() }
func (l externalLease) Deadline() time.Time               { return l.delegate.Deadline() }
func (l externalLease) Done() <-chan struct{}             { return l.delegate.Done() }
func (l externalLease) Err() error                        { return l.delegate.Err() }
func (l externalLease) Renew(ctx context.Context) error   { return l.delegate.Renew(ctx) }
func (l externalLease) Release(ctx context.Context) error { return l.delegate.Release(ctx) }

func TestLeaseInterfaceExternalImplementation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, err := newExampleLocker().TryLock(ctx, "external:123", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseExampleLease(base) })
	var lease anylock.Lease = externalLease{delegate: base}
	if lease.Key() != "external:123" || lease.Context() != base.Context() ||
		!lease.Deadline().Equal(base.Deadline()) || lease.Done() != base.Done() || lease.Err() != nil {
		t.Fatal("external implementation did not forward lease observations")
	}
	if err := lease.Renew(ctx); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	releaseExampleLease(lease)
	if !errors.Is(lease.Err(), anylock.ErrClosed) || lease.Context().Err() == nil {
		t.Fatal("external implementation did not forward release invalidation")
	}
	select {
	case <-lease.Done():
	default:
		t.Fatal("released lease Done remains open")
	}
}

func TestLeaseInterfaceDecorator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, err := newExampleLocker().Lock(ctx, "decorator:123", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseExampleLease(base) })
	calls := 0
	var observed error
	var lease anylock.Lease = observedLease{Lease: base, released: func(err error) {
		calls++
		observed = err
	}}
	if lease.Key() != base.Key() || lease.Context() != base.Context() ||
		lease.Done() != base.Done() || !lease.Deadline().Equal(base.Deadline()) || lease.Err() != nil {
		t.Fatal("embedded Lease did not forward observations")
	}
	if err := lease.Renew(ctx); err != nil {
		t.Fatalf("promoted Renew: %v", err)
	}
	// A canceled cleanup request still invalidates the real handle. The deferred
	// cleanup uses an independent live context to finish remote cleanup/workers.
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := lease.Release(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("decorated Release lost cancellation: %v", err)
	}
	if calls != 1 || !errors.Is(observed, context.Canceled) || !errors.Is(lease.Err(), anylock.ErrClosed) {
		t.Fatalf("release observation = %d, %v; lease state = %v", calls, observed, lease.Err())
	}
}

// Hooks wrap the standard-library example fixture, not a real backend service.
type leaseInterfaceDriver struct {
	anylock.Driver
	acquire func(context.Context, string, string, time.Duration) (bool, error)
	release func(context.Context, string, string) (bool, error)
}

func (d leaseInterfaceDriver) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return d.acquire(ctx, key, owner, ttl)
}

func (d leaseInterfaceDriver) Release(ctx context.Context, key, owner string) (bool, error) {
	if d.release != nil {
		return d.release(ctx, key, owner)
	}
	return d.Driver.Release(ctx, key, owner)
}

func TestLeaseInterfaceAcquisitionErrorsReturnNil(t *testing.T) {
	backendFailure := errors.New("backend unavailable")
	for _, api := range []string{"TryLock", "Lock"} {
		for _, tc := range []struct {
			name     string
			key      string
			ttl      time.Duration
			canceled bool
			acquired bool
			want     error
			attempts int
		}{
			{name: "empty key", ttl: time.Hour, want: anylock.ErrInvalidKey},
			{name: "zero TTL", key: "k", want: anylock.ErrInvalidTTL},
			{name: "negative TTL", key: "k", ttl: -time.Hour, want: anylock.ErrInvalidTTL},
			{name: "pre-canceled", key: "k", ttl: time.Hour, canceled: true, want: context.Canceled},
			{name: "backend failure", key: "k", ttl: time.Hour, want: backendFailure, attempts: 1},
			{name: "ambiguous backend failure", key: "k", ttl: time.Hour, acquired: true,
				want: backendFailure, attempts: 1},
		} {
			t.Run(api+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if tc.canceled {
					cancel()
				}
				attempts := 0
				driver := leaseInterfaceDriver{Driver: newExampleDriver(),
					acquire: func(context.Context, string, string, time.Duration) (bool, error) {
						attempts++
						return tc.acquired, fmt.Errorf("transport: %w", backendFailure)
					},
				}
				locker, err := anylock.New(driver)
				if err != nil {
					t.Fatal(err)
				}
				acquire := locker.TryLock
				if api == "Lock" {
					acquire = locker.Lock
				}
				lease, err := acquire(ctx, tc.key, tc.ttl)
				if lease != nil {
					t.Cleanup(func() { releaseExampleLease(lease) })
					t.Fatalf("error returned a non-nil Lease interface: %v", err)
				}
				if !errors.Is(err, tc.want) || attempts != tc.attempts {
					t.Fatalf("error/attempts = %v/%d, want %v/%d", err, attempts, tc.want, tc.attempts)
				}
			})
		}
	}
	t.Run("TryLock/busy", func(t *testing.T) {
		locker := newExampleLocker()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		base, err := locker.TryLock(ctx, "busy:123", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { releaseExampleLease(base) })
		lease, err := locker.TryLock(ctx, base.Key(), time.Hour)
		if lease != nil || !errors.Is(err, anylock.ErrNotAcquired) {
			if lease != nil {
				t.Cleanup(func() { releaseExampleLease(lease) })
			}
			t.Fatalf("busy acquisition = %v, %v; want nil, ErrNotAcquired", lease, err)
		}
	})
}

func TestLeaseInterfaceLateSuccessReturnsNil(t *testing.T) {
	for _, api := range []string{"TryLock", "Lock"} {
		t.Run(api, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			memory := newExampleDriver()
			attempts, releases := 0, 0
			driver := leaseInterfaceDriver{Driver: memory,
				acquire: func(request context.Context, key, owner string, ttl time.Duration) (bool, error) {
					attempts++
					acquired, err := memory.TryAcquire(request, key, owner, ttl)
					cancel() // The backend succeeded before cancellation was observed.
					return acquired, err
				},
				release: func(cleanup context.Context, key, owner string) (bool, error) {
					releases++
					if _, bounded := cleanup.Deadline(); !bounded || cleanup.Err() != nil {
						t.Error("late-acquisition cleanup must use a bounded live context")
					}
					return memory.Release(cleanup, key, owner)
				},
			}
			locker, err := anylock.New(driver)
			if err != nil {
				t.Fatal(err)
			}
			acquire := locker.TryLock
			if api == "Lock" {
				acquire = locker.Lock
			}
			lease, err := acquire(ctx, "late:123", time.Hour)
			if lease != nil {
				t.Cleanup(func() { releaseExampleLease(lease) })
				t.Fatalf("late success returned a non-nil Lease interface: %v", err)
			}
			if !errors.Is(err, context.Canceled) || !errors.Is(err, anylock.ErrLeaseLost) {
				t.Fatalf("late success lost error identities: %v", err)
			}
			if attempts != 1 || releases != 1 {
				t.Fatalf("attempts/releases = %d/%d, want 1/1", attempts, releases)
			}
			memory.mu.Lock()
			_, remains := memory.records["late:123"]
			memory.mu.Unlock()
			if remains {
				t.Fatal("late successful acquisition was not cleaned up in the fixture")
			}
		})
	}
}
