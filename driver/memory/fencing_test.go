package memory

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

func TestFencingLifecycle(t *testing.T) {
	d := New()
	ctx := context.Background()
	first, err := d.TryAcquire(ctx, "key", "old", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
	if err != nil || !first.Acquired || first.Token == 0 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	busy, err := d.TryAcquire(ctx, "key", "other", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
	if err != nil || busy != (anylock.AcquireResult{}) {
		t.Fatalf("busy = %+v, %v", busy, err)
	}
	if ok, operationErr := d.Renew(ctx, "key", "old", time.Hour); !ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	if d.fence != first.Token {
		t.Fatal("renew allocated a token")
	}
	release(t, d, "key", "old", true)
	second, err := d.TryAcquire(ctx, "key", "new", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
	if err != nil || !second.Acquired || second.Token <= first.Token {
		t.Fatalf("successor = %+v, %v", second, err)
	}
	release(t, d, "key", "old", false)
	// Directly expire under the driver gate; no scheduler sleeps are needed.
	d.gate <- struct{}{}
	d.entries["key"].expires = 0
	d.leave()
	third, err := d.TryAcquire(ctx, "key", "third", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
	if err != nil || third.Token <= second.Token {
		t.Fatalf("expired successor = %+v, %v", third, err)
	}
	release(t, d, "key", "third", true)
	acquire(t, d, "key", "plain", time.Hour, true)
	if result, operationErr := d.TryAcquire(ctx, "key", "fourth", anylock.AcquireOptions{TTL: time.Hour, Fencing: true}); operationErr != nil || result.Acquired || result.Token != 0 {
		t.Fatalf("mixed contention = %+v, %v", result, operationErr)
	}
}

func TestFenceExhaustionAndCancellation(t *testing.T) {
	d := New()
	d.fence = math.MaxUint64
	result, err := d.TryAcquire(context.Background(), "key", "owner", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
	if err == nil || result.Acquired || len(d.entries) != 0 {
		t.Fatalf("overflow = %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = d.TryAcquire(ctx, "key", "owner", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
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
			result, err := d.TryAcquire(context.Background(), string(rune('a'+i)), "owner", anylock.AcquireOptions{TTL: time.Hour, Fencing: true})
			if err != nil || !result.Acquired {
				t.Error(result, err)
				return
			}
			tokens <- result.Token
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
	token, enabled := first.Fence()
	if !enabled || token == 0 {
		t.Fatal(token, enabled)
	}
	if operationErr := first.Renew(context.Background()); operationErr != nil {
		t.Fatal(operationErr)
	}
	if renewed, _ := first.Fence(); renewed != token {
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
	if next, enabled := second.Fence(); !enabled || next <= token {
		t.Fatal(next, enabled)
	}
	if retained, _ := first.Fence(); retained != token {
		t.Fatal("lost historical token")
	}
}
