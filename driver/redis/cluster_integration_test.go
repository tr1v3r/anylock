//go:build integration

package redis

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/anylock"
)

func TestIntegrationRedisClusterFencing(t *testing.T) {
	addresses := os.Getenv("ANYLOCK_REDIS_CLUSTER_ADDRS")
	if addresses == "" {
		t.Skip("set ANYLOCK_REDIS_CLUSTER_ADDRS for real Redis Cluster tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: strings.Split(addresses, ","), ContextTimeoutEnabled: true, MaxRetries: -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	// An empty brace pair in the prefix and braces/binary text in keys must never
	// override the driver's hash tag or cause a lease/counter namespace collision.
	prefix := "{}anylock-cluster:" + rand.Text() + ":"
	d, err := New(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	fenced, err := anylock.New(d, anylock.WithFencing())
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := anylock.New(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"plain", "{tag}:key", "{}empty-tag", "key:fence", "锁\x00key"} {
		t.Run(key, func(t *testing.T) {
			lease, counter := leaseKey(prefix+key), counterKey(prefix+key)
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if cleanupErr := client.Del(cleanup, lease, counter).Err(); cleanupErr != nil {
					t.Error(cleanupErr)
				}
			})
			leaseSlot, slotErr := client.Do(ctx, "CLUSTER", "KEYSLOT", lease).Int64()
			if slotErr != nil {
				t.Fatal(slotErr)
			}
			counterSlot, slotErr := client.Do(ctx, "CLUSTER", "KEYSLOT", counter).Int64()
			if slotErr != nil || leaseSlot != counterSlot {
				t.Fatal("different cluster slots", leaseSlot, counterSlot, slotErr)
			}
			const previous = uint64(1 << 53)
			if setupErr := client.Set(ctx, counter, strconv.FormatUint(previous, 10), 0).Err(); setupErr != nil {
				t.Fatal(setupErr)
			}
			first, acquireErr := fenced.TryLock(ctx, key, time.Minute)
			if acquireErr != nil {
				t.Fatal(acquireErr)
			}
			defer first.Release(context.Background())
			token, enabled := first.FencingToken()
			if !enabled || token != previous+1 {
				t.Fatal(token, enabled)
			}
			owner, ownerErr := client.Get(ctx, lease).Result()
			if ownerErr != nil || owner == "" || owner == strconv.FormatUint(token, 10) {
				t.Fatal(owner, ownerErr)
			}
			if renewErr := first.Renew(ctx); renewErr != nil {
				t.Fatal(renewErr)
			}
			if renewed, _ := first.FencingToken(); renewed != token {
				t.Fatal("renewal changed token")
			}
			if busy, busyErr := ordinary.TryLock(ctx, key, time.Second); busy != nil || !errors.Is(busyErr, anylock.ErrNotAcquired) {
				t.Fatal(busy, busyErr)
			}
			if cleanupErr := first.Release(ctx); cleanupErr != nil {
				t.Fatal(cleanupErr)
			}
			plain, plainErr := ordinary.TryLock(ctx, key, time.Minute)
			if plainErr != nil {
				t.Fatal(plainErr)
			}
			defer plain.Release(context.Background())
			if plainToken, enabled := plain.FencingToken(); plainToken != 0 || enabled {
				t.Fatal(plainToken, enabled)
			}
			if busy, busyErr := fenced.TryLock(ctx, key, time.Second); busy != nil || !errors.Is(busyErr, anylock.ErrNotAcquired) {
				t.Fatal(busy, busyErr)
			}
			if cleanupErr := plain.Release(ctx); cleanupErr != nil {
				t.Fatal(cleanupErr)
			}
			successor, nextErr := fenced.TryLock(ctx, key, time.Minute)
			if nextErr != nil {
				t.Fatal(nextErr)
			}
			defer successor.Release(context.Background())
			if next, _ := successor.FencingToken(); next != token+1 {
				t.Fatal(next, token)
			}
			if deleted, cleanupErr := d.Release(ctx, key, owner); deleted || cleanupErr != nil {
				t.Fatal("stale owner deleted successor", deleted, cleanupErr)
			}
		})
	}
}
