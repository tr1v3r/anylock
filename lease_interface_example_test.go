package anylock_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tr1v3r/anylock"
)

// observedLease adds a release observation while promoting the other seven
// methods unchanged. The observer must support concurrent calls if callers do.
type observedLease struct {
	anylock.Lease
	released func(error)
}

func (l observedLease) Release(ctx context.Context) error {
	err := l.Lease.Release(ctx)
	l.released(err)
	return err
}

func ExampleLease_decorator() {
	// A standard-library in-memory fixture, not a real coordination service.
	var locks businessTryLocker = newExampleLocker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, err := locks.TryLock(ctx, "order:123", time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(base)

	var lease anylock.Lease = observedLease{Lease: base, released: func(err error) {
		fmt.Println("release observed:", err)
	}}
	// Consumers use the interface, including the lease's cooperative work context.
	consume := func(lease anylock.Lease) {
		fmt.Println("work:", lease.Key(), "active:", lease.Context().Err() == nil)
	}
	consume(lease)
	// Cleanup is independent of the acquisition/work context and stops workers.
	releaseExampleLease(lease)

	// Output:
	// work: order:123 active: true
	// release observed: <nil>
}
