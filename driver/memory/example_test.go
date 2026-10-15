package memory_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/tr1v3r/anylock"
	"github.com/tr1v3r/anylock/driver/memory"
)

func ExampleNew() {
	driver := memory.New()
	locker, err := anylock.New(driver)
	fmt.Println("ready:", locker != nil, "error:", err)
	// No client, connection, background worker, or Close is needed.

	// Output:
	// ready: true error: <nil>
}

func ExampleDriver() {
	// Sharing the pointer, not merely using matching keys, shares ownership.
	driver := memory.New()
	first, err := anylock.New(driver)
	if err != nil {
		panic(err)
	}
	second, err := anylock.New(driver)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	lease, err := first.TryLock(ctx, "jobs:123", time.Hour)
	if err != nil {
		panic(err)
	}
	_, err = second.TryLock(ctx, "jobs:123", time.Hour)
	fmt.Println("shared contention:", errors.Is(err, anylock.ErrNotAcquired))
	if err = lease.Release(ctx); err != nil {
		panic(err)
	}
	successor, err := second.TryLock(ctx, "jobs:123", time.Hour)
	fmt.Println("successor acquired:", err == nil)
	if err != nil {
		panic(err)
	}
	if err := successor.Release(ctx); err != nil {
		panic(err)
	}

	// Output:
	// shared contention: true
	// successor acquired: true
}

func ExampleDriver_TryAcquire() {
	driver := memory.New()
	ctx := context.Background()
	// Direct driver users supply fresh, unguessable owners. The core does this
	// automatically and also manages local lease validity and renewal.
	owner := rand.Text()
	acquired, err := driver.TryAcquire(ctx, "resource:123", owner, time.Hour)
	fmt.Println("acquired:", acquired, "error:", err)
	acquired, err = driver.TryAcquire(ctx, "resource:123", rand.Text(), time.Hour)
	fmt.Println("contended:", !acquired, "error:", err)

	// Output:
	// acquired: true error: <nil>
	// contended: true error: <nil>
}

func ExampleDriver_Renew() {
	driver := memory.New()
	ctx := context.Background()
	owner := rand.Text()
	acquired, err := driver.TryAcquire(ctx, "resource:123", owner, time.Hour)
	if err != nil || !acquired {
		panic("initial acquisition failed")
	}
	renewed, err := driver.Renew(ctx, "resource:123", "not-the-owner", time.Hour)
	fmt.Println("other owner renewed:", renewed, "error:", err)
	renewed, err = driver.Renew(ctx, "resource:123", owner, time.Hour)
	fmt.Println("current owner renewed:", renewed, "error:", err)

	// Output:
	// other owner renewed: false error: <nil>
	// current owner renewed: true error: <nil>
}

func ExampleDriver_Release() {
	driver := memory.New()
	ctx := context.Background()
	owner := rand.Text()
	acquired, err := driver.TryAcquire(ctx, "resource:123", owner, time.Hour)
	if err != nil || !acquired {
		panic("initial acquisition failed")
	}
	released, err := driver.Release(ctx, "resource:123", "not-the-owner")
	fmt.Println("other owner released:", released, "error:", err)
	released, err = driver.Release(ctx, "resource:123", owner)
	fmt.Println("current owner released:", released, "error:", err)
	released, err = driver.Release(ctx, "resource:123", owner)
	fmt.Println("already absent:", !released, "error:", err)

	// Output:
	// other owner released: false error: <nil>
	// current owner released: true error: <nil>
	// already absent: true error: <nil>
}
