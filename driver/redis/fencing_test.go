package redis

import (
	"context"
	"errors"
	"math"
	"strconv"
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
	counter := d.prefix + key + fenceSuffix
	// The token must cross Lua's exact integer range without rounding.
	const previous = uint64(1 << 53)
	if operationErr := client.Set(ctx, counter, strconv.FormatUint(previous, 10), 0).Err(); operationErr != nil {
		t.Fatal(operationErr)
	}
	first, err := d.TryAcquire(ctx, key, "old", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
	if err != nil || !first.Acquired || first.Token != previous+1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	busy, err := d.TryAcquire(ctx, key, "other", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
	if err != nil || busy != (anylock.AcquireResult{}) {
		t.Fatalf("busy = %+v, %v", busy, err)
	}
	if result, operationErr := d.TryAcquire(ctx, key, "plain", anylock.AcquireOptions{TTL: time.Second}); operationErr != nil || result.Acquired {
		t.Fatal(result, operationErr)
	}
	if ok, operationErr := d.Renew(ctx, key, "old", time.Second); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	if value, operationErr := client.Get(ctx, counter).Uint64(); operationErr != nil || value != first.Token {
		t.Fatal("renew changed counter", value, operationErr)
	}
	server.FastForward(2 * time.Second)
	second, err := d.TryAcquire(ctx, key, "new", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
	if err != nil || second.Token != first.Token+1 {
		t.Fatalf("successor = %+v, %v", second, err)
	}
	if ok, operationErr := d.Release(ctx, key, "old"); ok || operationErr != nil {
		t.Fatal("stale owner released successor", ok, operationErr)
	}
	if ok, operationErr := d.Release(ctx, key, "new"); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	third, err := d.TryAcquire(ctx, key, "third", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
	if err != nil || third.Token != second.Token+1 {
		t.Fatal(third, err)
	}
	if ttl := server.TTL(counter); ttl != 0 {
		t.Fatal("counter expires", ttl)
	}
}

func TestFenceOverflowAndReservedNamespace(t *testing.T) {
	server := miniredis.RunT(t)
	d, client := newDriver(t, server.Addr(), "locks:")
	ctx := context.Background()
	if operationErr := client.Set(ctx, "locks:key"+fenceSuffix, strconv.FormatInt(math.MaxInt64, 10), 0).Err(); operationErr != nil {
		t.Fatal(operationErr)
	}
	result, err := d.TryAcquire(ctx, "key", "owner", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
	if err == nil || result.Acquired || server.Exists("locks:key") {
		t.Fatal(result, err)
	}
	for _, fenced := range []bool{false, true} {
		result, err := d.TryAcquire(ctx, "key"+fenceSuffix, "owner", anylock.AcquireOptions{TTL: time.Second, Fencing: fenced})
		if err == nil || result.Acquired {
			t.Fatal("reserved counter could be acquired", result, err)
		}
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
		{name: "nil command", wantErr: true},
		{name: "contention", err: goredis.Nil},
		{name: "network error", err: failure, wantErr: true},
		{name: "integer reply", value: int64(1), wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "overflow", value: "9223372036854775808", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(&stubClient{eval: func(context.Context, string, []string, ...any) *goredis.Cmd {
				if tc.name == "nil command" {
					return nil
				}
				return resultCommand(tc.value, tc.err)
			}}, "")
			if err != nil {
				t.Fatal(err)
			}
			result, err := d.TryAcquire(context.Background(), "key", "owner", anylock.AcquireOptions{TTL: time.Second, Fencing: true})
			if (err != nil) != tc.wantErr || result != (anylock.AcquireResult{}) {
				t.Fatal(result, err)
			}
			if tc.err == failure && !errors.Is(err, failure) {
				t.Fatal("lost error identity", err)
			}
		})
	}
}
