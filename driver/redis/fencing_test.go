package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/anylock"
)

func TestFencingLifecycleAndPrecision(t *testing.T) {
	server := miniredis.RunT(t)
	d, client := newDriver(t, server.Addr(), "{locks}:")
	ctx := context.Background()
	const key = "resource"
	counter := counterKey(d.prefix + key)
	// The token must cross Lua's exact integer range without rounding.
	const previous = uint64(1 << 53)
	if operationErr := client.Set(ctx, counter, strconv.FormatUint(previous, 10), 0).Err(); operationErr != nil {
		t.Fatal(operationErr)
	}
	first, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: "old", TTL: time.Second, Fencing: true})
	if err != nil || !first.Acquired || first.FencingToken != previous+1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	busy, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: "other", TTL: time.Second, Fencing: true})
	if err != nil || busy != (anylock.AcquireResult{}) {
		t.Fatalf("busy = %+v, %v", busy, err)
	}
	if result, operationErr := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: "plain", TTL: time.Second}); operationErr != nil || result.Acquired {
		t.Fatal(result, operationErr)
	}
	if ok, operationErr := d.Renew(ctx, key, "old", time.Second); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	if value, operationErr := client.Get(ctx, counter).Uint64(); operationErr != nil || value != first.FencingToken {
		t.Fatal("renew changed counter", value, operationErr)
	}
	server.FastForward(2 * time.Second)
	second, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: "new", TTL: time.Second, Fencing: true})
	if err != nil || second.FencingToken != first.FencingToken+1 {
		t.Fatalf("successor = %+v, %v", second, err)
	}
	if ok, operationErr := d.Release(ctx, key, "old"); ok || operationErr != nil {
		t.Fatal("stale owner released successor", ok, operationErr)
	}
	if ok, operationErr := d.Release(ctx, key, "new"); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	third, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: "third", TTL: time.Second, Fencing: true})
	if err != nil || third.FencingToken != second.FencingToken+1 {
		t.Fatal(third, err)
	}
	if ttl := server.TTL(counter); ttl != 0 {
		t.Fatal("counter expires", ttl)
	}
}

func TestFenceOverflowDoesNotAcquireLease(t *testing.T) {
	server := miniredis.RunT(t)
	d, client := newDriver(t, server.Addr(), "locks:")
	ctx := context.Background()
	if operationErr := client.Set(ctx, counterKey("locks:key"), strconv.FormatInt(math.MaxInt64, 10), 0).Err(); operationErr != nil {
		t.Fatal(operationErr)
	}
	result, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "owner", TTL: time.Second, Fencing: true})
	if err == nil || result.Acquired || server.Exists(leaseKey("locks:key")) {
		t.Fatal(result, err)
	}

}

func TestFencedReplyErrors(t *testing.T) {
	failure := errors.New("network outcome unknown")
	for _, tc := range []struct {
		name    string
		value   any
		err     error
		wantErr bool
	}{
		{name: "contention", err: goredis.Nil},
		{name: "network error", err: failure, wantErr: true},
		{name: "integer reply", value: int64(1), wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "overflow", value: "9223372036854775808", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(&stubClient{process: replyProcess(tc.value, tc.err)}, "")
			if err != nil {
				t.Fatal(err)
			}
			result, err := d.TryAcquire(context.Background(), anylock.AcquireRequest{Key: "key", Owner: "owner", TTL: time.Second, Fencing: true})
			if (err != nil) != tc.wantErr || result != (anylock.AcquireResult{}) {
				t.Fatal(result, err)
			}
			if tc.err == failure && !errors.Is(err, failure) {
				t.Fatal("lost error identity", err)
			}
		})
	}
}

func TestConcurrentFencedAcquisition(t *testing.T) {
	server := miniredis.RunT(t)
	d, _ := newDriver(t, server.Addr(), "mixed:")
	ctx := context.Background()
	const workers = 16
	var previous uint64
	var previousOwner string
	for wave := range 3 {
		type reply struct {
			owner  string
			result anylock.AcquireResult
			err    error
		}
		replies := make(chan reply, workers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range workers {
			owner := fmt.Sprintf("%d/%d", wave, i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				result, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "shared", Owner: owner, TTL: time.Minute, Fencing: true})
				replies <- reply{owner, result, err}
			}()
		}
		close(start)
		wg.Wait()
		close(replies)
		winners := 0
		var winner reply
		for r := range replies {
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.result.Acquired {
				winners++
				winner = r
			} else if r.result.FencingToken != 0 {
				t.Fatal("contended request received token", r.result)
			}
		}
		if winners != 1 || winner.result.FencingToken <= previous {
			t.Fatal("invalid winner/token", winners, winner.result, previous)
		}
		if previousOwner != "" {
			if renewed, renewErr := d.Renew(ctx, "shared", previousOwner, time.Minute); renewed || renewErr != nil {
				t.Fatal("stale renewal", renewed, renewErr)
			}
			if removed, releaseErr := d.Release(ctx, "shared", previousOwner); removed || releaseErr != nil {
				t.Fatal("stale release", removed, releaseErr)
			}
		}
		previous, previousOwner = winner.result.FencingToken, winner.owner
		if removed, releaseErr := d.Release(ctx, "shared", winner.owner); !removed || releaseErr != nil {
			t.Fatal(removed, releaseErr)
		}
		// Ordinary acquisitions use the exact same lease space and never allocate tokens.
		plain, plainErr := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "shared", Owner: "plain", TTL: time.Minute})
		if plainErr != nil || !plain.Acquired || plain.FencingToken != 0 {
			t.Fatal(plain, plainErr)
		}
		busy, busyErr := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "shared", Owner: "fenced", TTL: time.Minute, Fencing: true})
		if busyErr != nil || busy != (anylock.AcquireResult{}) {
			t.Fatal(busy, busyErr)
		}
		if removed, releaseErr := d.Release(ctx, "shared", "plain"); !removed || releaseErr != nil {
			t.Fatal(removed, releaseErr)
		}
	}
}

func TestPhysicalLeaseAndCounterNamespaces(t *testing.T) {
	seen := map[string]bool{}
	for _, namespace := range []string{"key", "key:lease", "key:fence", "{}key", "{tag}:key", "锁\x00key"} {
		lease, counter := leaseKey(namespace), counterKey(namespace)
		for _, physical := range []string{lease, counter} {
			if seen[physical] {
				t.Fatal("physical namespace collision", namespace, physical)
			}
			seen[physical] = true
		}
	}
}
