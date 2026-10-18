package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

func TestFencingLifecycle(t *testing.T) {
	d := New()
	ctx := context.Background()
	first, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "old", TTL: time.Hour, Fencing: true})
	if err != nil || !first.Acquired || first.FencingToken == 0 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	busy, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "other", TTL: time.Hour, Fencing: true})
	if err != nil || busy != (anylock.AcquireResult{}) {
		t.Fatalf("busy = %+v, %v", busy, err)
	}
	if ok, operationErr := d.Renew(ctx, "key", "old", time.Hour); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	if d.fence != first.FencingToken {
		t.Fatal("renew allocated a token")
	}
	release(t, d, "key", "old", true)
	second, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "new", TTL: time.Hour, Fencing: true})
	if err != nil || !second.Acquired || second.FencingToken <= first.FencingToken {
		t.Fatalf("successor = %+v, %v", second, err)
	}
	release(t, d, "key", "old", false)
	// Directly expire under the driver gate; no scheduler sleeps are needed.
	d.gate <- struct{}{}
	d.entries["key"].expires = 0
	d.leave()
	third, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "third", TTL: time.Hour, Fencing: true})
	if err != nil || third.FencingToken <= second.FencingToken {
		t.Fatalf("expired successor = %+v, %v", third, err)
	}
	release(t, d, "key", "third", true)
	acquire(t, d, "key", "plain", time.Hour, true)
	if result, operationErr := d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "fourth", TTL: time.Hour, Fencing: true}); operationErr != nil || result.Acquired || result.FencingToken != 0 {
		t.Fatalf("mixed contention = %+v, %v", result, operationErr)
	}
}

func TestFenceExhaustionAndCancellation(t *testing.T) {
	d := New()
	d.fence = math.MaxUint64
	result, err := d.TryAcquire(context.Background(), anylock.AcquireRequest{Key: "key", Owner: "owner", TTL: time.Hour, Fencing: true})
	if err == nil || result.Acquired || len(d.entries) != 0 {
		t.Fatalf("overflow = %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = d.TryAcquire(ctx, anylock.AcquireRequest{Key: "key", Owner: "owner", TTL: time.Hour, Fencing: true})
	if !errors.Is(err, context.Canceled) || result.Acquired || d.fence != math.MaxUint64 {
		t.Fatal(result, err)
	}
	// Exhausted fencing does not disable ordinary acquisition.
	acquire(t, d, "plain", "owner", time.Hour, true)
}

func TestFencingConcurrentTokens(t *testing.T) {
	const count = 64
	d := New()
	tokens := make(chan uint64, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := d.TryAcquire(context.Background(), anylock.AcquireRequest{Key: string(rune('a' + i)), Owner: "owner", TTL: time.Hour, Fencing: true})
			if err != nil || !result.Acquired {
				t.Error(result, err)
				return
			}
			tokens <- result.FencingToken
		}()
	}
	wg.Wait()
	close(tokens)
	seen := map[uint64]bool{}
	for token := range tokens {
		if token == 0 || seen[token] {
			t.Fatal("duplicate/zero", token)
		}
		seen[token] = true
	}
	if len(seen) != count {
		t.Fatal(len(seen))
	}
}

func TestCoreFencingWithMemory(t *testing.T) {
	locker, err := anylock.New(New(), anylock.WithFencing())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first, err := locker.TryLock(ctx, "key", time.Hour)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	token, enabled := first.FencingToken()
	if !enabled || token == 0 {
		t.Fatal(token, enabled)
	}
	if operationErr := first.Renew(context.Background()); operationErr != nil {
		t.Fatal(operationErr)
	}
	if renewed, _ := first.FencingToken(); renewed != token {
		t.Fatal("renew changed token")
	}
	cancel()
	<-first.Done()
	if operationErr := first.Release(context.Background()); operationErr != nil {
		t.Fatal(operationErr)
	}
	second, err := locker.Lock(context.Background(), "key", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(context.Background())
	if next, enabled := second.FencingToken(); !enabled || next <= token {
		t.Fatal(next, enabled)
	}
	if retained, _ := first.FencingToken(); retained != token {
		t.Fatal("lost historical token")
	}
}

func TestConcurrentFencedAcquisition(t *testing.T) {
	d := New()
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
