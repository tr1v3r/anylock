//go:build integration

package redis

import (
	"context"
	"crypto/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

func TestIntegrationRedisFencing(t *testing.T) {
	addr := os.Getenv("ANYLOCK_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ANYLOCK_REDIS_ADDR")
	}
	prefix := "anylock-fencing:{" + rand.Text() + "}:"
	d, client := newDriver(t, addr, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if operationErr := client.Del(cleanup, prefix+"key", prefix+"key"+fenceSuffix).Err(); operationErr != nil {
			t.Error(operationErr)
		}
	})
	const previous = uint64(1 << 53)
	if operationErr := client.Set(ctx, prefix+"key"+fenceSuffix, strconv.FormatUint(previous, 10), 0).Err(); operationErr != nil {
		t.Fatal(operationErr)
	}
	locker, err := anylock.New(d, anylock.WithFencing())
	if err != nil {
		t.Fatal(err)
	}
	first, err := locker.TryLock(ctx, "key", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })
	token, enabled := first.Fence()
	if !enabled || token != previous+1 {
		t.Fatal(token, enabled)
	}
	if operationErr := first.Renew(ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	if busy, operationErr := d.TryAcquireFenced(ctx, "key", "other", time.Second); operationErr != nil || busy.Acquired || busy.Fence != 0 {
		t.Fatal(busy, operationErr)
	}
	if operationErr := first.Release(ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	other, _ := newDriver(t, addr, prefix)
	second, err := other.TryAcquireFenced(ctx, "key", "successor", 20*time.Millisecond)
	if err != nil || second.Fence != token+1 {
		t.Fatal(second, err)
	}
	waitForRealExpiry(t, ctx, func() (bool, error) {
		n, existsErr := client.Exists(ctx, prefix+"key").Result()
		return n == 0, existsErr
	})
	third, err := d.TryAcquireFenced(ctx, "key", "third", time.Hour)
	if err != nil || third.Fence != second.Fence+1 {
		t.Fatal(third, err)
	}
}
