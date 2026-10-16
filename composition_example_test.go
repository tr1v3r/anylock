package anylock_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tr1v3r/anylock"
)

// appLocks belongs to the application, not the anylock public API. Construct it
// once at startup and inject only the business domains each consumer needs.
type appLocks struct {
	Orders        *anylock.Locker
	ScheduledJobs *anylock.Locker
	LocalRefresh  *anylock.Locker
}

func ExampleLocker_multipleBackends() {
	// Independent standard-library fixtures stand in for separately configured
	// backend stores. This example does not connect to Redis or PostgreSQL.
	// In production, wire Orders to Redis, ScheduledJobs to PostgreSQL, and
	// LocalRefresh to one shared memory Driver, as shown in the README.
	ordersDriver := newExampleDriver()
	ordersLocker, err := anylock.New(ordersDriver)
	if err != nil {
		panic(err)
	}
	locks := appLocks{
		Orders:        ordersLocker,
		ScheduledJobs: newExampleLocker(),
		LocalRefresh:  newExampleLocker(),
	}

	ctx := context.Background()
	// Deliberately use identical key text to demonstrate store isolation, not
	// interchangeable backends. Real callers agree on the domain for each key.
	const key = "resource:123"
	ordersLease, err := locks.Orders.TryLock(ctx, key, time.Hour)
	if err != nil {
		panic(err) // Return the error in application code; never fall back to Jobs.
	}
	defer releaseExampleLease(ordersLease)

	// A separately constructed Locker sharing the orders store still contends.
	ordersPeer, err := anylock.New(ordersDriver)
	if err != nil {
		panic(err)
	}
	_, err = ordersPeer.TryLock(ctx, key, time.Hour)
	fmt.Println("same store contends:", errors.Is(err, anylock.ErrNotAcquired))

	// A different store grants its own lock even while Orders holds this key.
	jobsLease, err := locks.ScheduledJobs.TryLock(ctx, key, time.Hour)
	if err != nil {
		panic(err)
	}
	defer releaseExampleLease(jobsLease)
	fmt.Println("independent jobs store acquires:", jobsLease.Key() == key)

	// The local domain can use a fixed-TTL gate, with no Release or renewal.
	executed, err := locks.LocalRefresh.TryRun(ctx, key, time.Hour, func(context.Context) error { return nil })
	if err != nil {
		panic(err)
	}
	fmt.Println("independent local gate executes:", executed)

	// Both stores now hold this key with different owners. The Lease, not an
	// application-wide current-backend selection, routes each renewal/release.
	if err := ordersLease.Renew(ctx); err != nil {
		panic(err)
	}
	if err := jobsLease.Renew(ctx); err != nil {
		panic(err)
	}
	fmt.Println("leases renew through their original drivers:", true)

	// Output:
	// same store contends: true
	// independent jobs store acquires: true
	// independent local gate executes: true
	// leases renew through their original drivers: true
}
