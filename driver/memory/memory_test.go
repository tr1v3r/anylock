package memory

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func requireResult(t *testing.T, got bool, err error, want bool) {
	t.Helper()
	if got != want || err != nil {
		t.Fatalf("result = (%v, %v), want (%v, nil)", got, err, want)
	}
}

func acquire(t *testing.T, d *Driver, key, owner string, ttl time.Duration, want bool) {
	t.Helper()
	got, err := d.tryAcquire(context.Background(), key, owner, ttl)
	requireResult(t, got, err, want)
}

func renew(t *testing.T, d *Driver, key, owner string, ttl time.Duration, want bool) {
	t.Helper()
	got, err := d.Renew(context.Background(), key, owner, ttl)
	requireResult(t, got, err, want)
}

func release(t *testing.T, d *Driver, key, owner string, want bool) {
	t.Helper()
	got, err := d.Release(context.Background(), key, owner)
	requireResult(t, got, err, want)
}

func checkHeap(t *testing.T, d *Driver) {
	t.Helper()
	if len(d.entries) != len(d.expiry) {
		t.Fatalf("map size %d != heap size %d", len(d.entries), len(d.expiry))
	}
	for i, e := range d.expiry {
		if e == nil || e.index != i || d.entries[e.key] != e {
			t.Fatalf("invalid heap/map entry at %d: %+v", i, e)
		}
		if i > 0 && d.expiry.Less(i, (i-1)/2) {
			t.Fatalf("heap order broken at %d", i)
		}
	}
}

func TestValidation(t *testing.T) {
	operations := []struct {
		name string
		call func(*Driver, context.Context, string, string, time.Duration) (bool, error)
	}{
		{"acquire", (*Driver).tryAcquire},
		{"renew", (*Driver).Renew},
		{"release", func(d *Driver, ctx context.Context, key, owner string, _ time.Duration) (bool, error) {
			return d.Release(ctx, key, owner)
		}},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			for _, tc := range []struct {
				name   string
				driver *Driver
				ctx    context.Context
				key    string
				owner  string
			}{
				{"nil driver", nil, context.Background(), "k", "o"},
				{"zero driver", &Driver{}, context.Background(), "k", "o"},
				{"nil context", New(), nil, "k", "o"},
				{"empty key", New(), context.Background(), "", "o"},
				{"empty owner", New(), context.Background(), "k", ""},
				{"canceled", New(), canceled, "k", "o"},
				{"past actual deadline", New(), deadlineContext{time.Now().Add(-time.Second)}, "k", "o"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := op.call(tc.driver, tc.ctx, tc.key, tc.owner, time.Hour)
					if got || err == nil {
						t.Fatalf("invalid input accepted: %v, %v", got, err)
					}
					if tc.name == "canceled" && !errors.Is(err, context.Canceled) {
						t.Fatal("cancellation identity lost", err)
					}
					if tc.name == "past actual deadline" && !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("deadline identity lost", err)
					}
				})
			}
		})
	}
	for _, ttl := range []time.Duration{0, -time.Nanosecond, time.Duration(math.MinInt64)} {
		d := New()
		for _, op := range operations[:2] {
			got, err := op.call(d, context.Background(), "k", "o", ttl)
			if got || err == nil || len(d.entries) != 0 {
				t.Fatalf("%s accepted TTL %v: %v, %v", op.name, ttl, got, err)
			}
		}
	}
}

// This intentionally leaves Err/Done unset, like a context whose cancellation
// timer has not yet published its deadline. Operations must also check the clock.
type deadlineContext struct{ limit time.Time }

func (c deadlineContext) Deadline() (time.Time, bool) { return c.limit, true }
func (deadlineContext) Done() <-chan struct{}         { return nil }
func (deadlineContext) Err() error                    { return nil }
func (deadlineContext) Value(any) any                 { return nil }

func TestInvalidRequestsDoNotMutateOrSweep(t *testing.T) {
	d := New()
	acquire(t, d, "live", "a", time.Hour, true)
	acquire(t, d, "expired", "a", time.Hour, true)
	d.entries["expired"].expires = 0
	heap.Fix(&d.expiry, d.entries["expired"].index)
	original := d.entries["live"].expires
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var nilContext context.Context
	for _, call := range []func() (bool, error){
		func() (bool, error) { return d.tryAcquire(nilContext, "new", "b", time.Hour) },
		func() (bool, error) { return d.tryAcquire(context.Background(), "new", "b", 0) },
		func() (bool, error) { return d.Renew(context.Background(), "live", "a", -1) },
		func() (bool, error) { return d.Release(context.Background(), "live", "") },
		func() (bool, error) { return d.Renew(ctx, "live", "a", time.Hour) },
		func() (bool, error) { return d.Release(ctx, "live", "a") },
	} {
		if got, err := call(); got || err == nil {
			t.Fatal("invalid request accepted", got, err)
		}
		if len(d.entries) != 2 || d.entries["live"].expires != original || len(d.gate) != 0 {
			t.Fatal("invalid request mutated ownership or performed maintenance")
		}
		checkHeap(t, d)
	}
}

type cancelWhenCommitted struct {
	context.Context
	cancel    context.CancelFunc
	committed func() bool
	observed  bool
}

func (c *cancelWhenCommitted) Err() error {
	if c.committed() {
		c.observed = true
		c.cancel()
	}
	return c.Context.Err()
}

func TestLateCancellationReportsCommittedOutcome(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release"} {
		t.Run(operation, func(t *testing.T) {
			d := New()
			if operation != "acquire" {
				acquire(t, d, "k", "a", time.Hour, true)
			}
			var original uint64
			if e := d.entries["k"]; e != nil {
				original = e.expires
			}
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			gateHeldAtCommit := false
			committed := func() bool {
				e := d.entries["k"]
				switch operation {
				case "acquire":
					return e != nil && e.owner == "a"
				case "renew":
					return e != nil && e.expires != original
				case "release":
					return e == nil
				}
				return false
			}
			ctx := &cancelWhenCommitted{Context: base, cancel: cancel, committed: func() bool {
				if !committed() {
					return false
				}
				gateHeldAtCommit = len(d.gate) == 1
				return true
			}}
			var got bool
			var err error
			switch operation {
			case "acquire":
				got, err = d.tryAcquire(ctx, "k", "a", time.Hour)
			case "renew":
				got, err = d.Renew(ctx, "k", "a", 2*time.Hour)
			case "release":
				got, err = d.Release(ctx, "k", "a")
			}
			if got || !errors.Is(err, context.Canceled) {
				t.Fatal("late cancellation was reported as successful ownership", got, err)
			}
			if !ctx.observed || !gateHeldAtCommit || len(d.gate) != 0 {
				t.Fatal("final context check did not observe the commit while holding the gate")
			}
			if !committed() {
				t.Fatal("late failure rolled back the committed operation")
			}
			checkHeap(t, d)
		})
	}
}

func TestResultRechecksCancellationAndActualDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ok := range []bool{false, true} {
		for _, tc := range []struct {
			ctx  context.Context
			want error
		}{
			{ctx, context.Canceled},
			{deadlineContext{time.Now().Add(-time.Second)}, context.DeadlineExceeded},
		} {
			if got, err := result(tc.ctx, ok); got || !errors.Is(err, tc.want) {
				t.Fatal("final ownership/contended result ignored request expiry", got, err)
			}
		}
		got, err := result(context.Background(), ok)
		requireResult(t, got, err, ok)
	}
}

func TestOwnershipAndIsolation(t *testing.T) {
	d := New()
	acquire(t, d, "k", "a", time.Hour, true)
	original := d.entries["k"].expires
	acquire(t, d, "k", "a", 2*time.Hour, false)
	acquire(t, d, "k", "b", 2*time.Hour, false)
	renew(t, d, "missing", "a", time.Hour, false)
	renew(t, d, "k", "b", time.Hour, false)
	release(t, d, "k", "b", false)
	if d.entries["k"].expires != original {
		t.Fatal("contending owner changed expiry")
	}
	renew(t, d, "k", "a", 2*time.Hour, true)
	if d.entries["k"].expires <= original {
		t.Fatal("matching owner did not extend expiry")
	}
	renew(t, d, "k", "a", time.Minute, true)
	if d.entries["k"].expires >= original {
		t.Fatal("renew must reset from now, not add to old TTL")
	}
	acquire(t, New(), "k", "b", time.Hour, true)
	acquire(t, d, "other:k", "b", time.Hour, true)
	release(t, d, "k", "a", true)
	release(t, d, "k", "a", false)
	release(t, d, "missing", "a", false)
	acquire(t, d, "k", "b", time.Hour, true)
	renew(t, d, "k", "a", time.Hour, false)
	release(t, d, "k", "a", false)
	release(t, d, "k", "b", true)
	checkHeap(t, d)
}

func TestExpiredOwnersAndUnrelatedSweep(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release", "unrelated"} {
		t.Run(operation, func(t *testing.T) {
			d := New()
			acquire(t, d, "k", "a", time.Hour, true)
			e := d.entries["k"]
			e.expires = 0
			heap.Fix(&d.expiry, e.index)
			switch operation {
			case "acquire":
				acquire(t, d, "k", "b", time.Hour, true)
			case "renew":
				renew(t, d, "k", "a", time.Hour, false)
			case "release":
				release(t, d, "k", "a", false)
			case "unrelated":
				release(t, d, "unrelated", "a", false)
			}
			if e.index != -1 || d.entries["k"] == e {
				t.Fatal("expired entry retained")
			}
			if operation != "acquire" && (d.entries != nil || d.expiry != nil) {
				t.Fatal("empty store retained high-water storage")
			}
			checkHeap(t, d)
		})
	}
}

func TestExactTTLAndMaximumDuration(t *testing.T) {
	for _, ttl := range []time.Duration{time.Nanosecond, 1234567, time.Hour, time.Duration(math.MaxInt64)} {
		t.Run(ttl.String(), func(t *testing.T) {
			d := New()
			before := d.ticks()
			acquire(t, d, "k", "a", ttl, true)
			after := d.ticks()
			expires := d.entries["k"].expires
			if expires < before+uint64(ttl) || expires > after+uint64(ttl) {
				t.Fatalf("expiry %d rounded/overflowed outside [%d,%d]", expires, before+uint64(ttl), after+uint64(ttl))
			}
			if ttl == time.Duration(math.MaxInt64) {
				if expires <= uint64(math.MaxInt64) {
					t.Fatal("maximum TTL should use the unsigned range")
				}
				before = d.ticks()
				renew(t, d, "k", "a", ttl, true)
				after = d.ticks()
				if got := d.entries["k"].expires; got < before+uint64(ttl) || got > after+uint64(ttl) {
					t.Fatal("maximum renewal overflowed", got)
				}
				release(t, d, "k", "a", true)
			}
		})
	}
}

func TestBinaryKeysAndOwners(t *testing.T) {
	d := New()
	for _, key := range []string{"\x00", "\xff", "a\x00b", "世界"} {
		acquire(t, d, key, "\xff\x00", time.Hour, true)
		renew(t, d, key, "\xff\x00", time.Hour, true)
		release(t, d, key, "\xff\x00", true)
	}
}

func TestNoRenewalOrReleaseTombstones(t *testing.T) {
	d := New()
	acquire(t, d, "anchor", "a", time.Hour, true)
	acquire(t, d, "k", "a", time.Hour, true)
	e := d.entries["k"]
	for range 2000 {
		renew(t, d, "k", "a", time.Hour, true)
		if len(d.expiry) != 2 || d.entries["k"] != e {
			t.Fatal("renewal allocated another entry")
		}
	}
	for i := range 1000 {
		release(t, d, "k", "a", true)
		acquire(t, d, "k", "a", time.Hour, true)
		if len(d.expiry) != 2 {
			t.Fatalf("release/reacquire accumulated storage at %d", i)
		}
	}
	backing := d.expiry[:cap(d.expiry)]
	release(t, d, "k", "a", true)
	for _, removed := range backing[len(d.expiry):] {
		if removed != nil {
			t.Fatal("heap pop retained removed pointer")
		}
	}
	release(t, d, "anchor", "a", true)
	if d.entries != nil || d.expiry != nil {
		t.Fatal("empty store did not release containers")
	}
}

func TestRandomizedHeapMapInvariant(t *testing.T) {
	d := New()
	random := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		key := fmt.Sprintf("k%d", random.IntN(30))
		switch random.IntN(4) {
		case 0:
			_, err := d.tryAcquire(context.Background(), key, "o", time.Duration(random.IntN(100)+1)*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
		case 1:
			_, err := d.Renew(context.Background(), key, "o", time.Duration(random.IntN(100)+1)*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
		case 2:
			_, err := d.Release(context.Background(), key, "o")
			if err != nil {
				t.Fatal(err)
			}
		case 3:
			if e := d.entries[key]; e != nil {
				e.expires = 0
				heap.Fix(&d.expiry, e.index)
			}
		}
		checkHeap(t, d)
	}
}

func TestConcurrentSingleWinner(t *testing.T) {
	d := New()
	start := make(chan struct{})
	const contenders = 64
	var workers sync.WaitGroup
	var winners atomic.Int32
	for i := range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			got, err := d.tryAcquire(context.Background(), "k", fmt.Sprintf("o%d", i), time.Hour)
			if err != nil {
				t.Error(err)
			}
			if got {
				winners.Add(1)
			}
		}()
	}
	close(start)
	workers.Wait()
	if winners.Load() != 1 {
		t.Fatal("acquisition winners", winners.Load())
	}
	checkHeap(t, d)
}

func TestConcurrentStaleOwnerCannotAffectSuccessor(t *testing.T) {
	d := New()
	acquire(t, d, "k", "a", time.Hour, true)
	d.entries["k"].expires = 0
	acquire(t, d, "k", "b", time.Hour, true)
	var workers sync.WaitGroup
	for range 30 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 50 {
				got, err := d.Renew(context.Background(), "k", "a", time.Hour)
				if got || err != nil {
					t.Error("stale renewal succeeded", got, err)
				}
				got, err = d.Release(context.Background(), "k", "a")
				if got || err != nil {
					t.Error("stale release succeeded", got, err)
				}
			}
		}()
	}
	for range 100 {
		renew(t, d, "k", "b", time.Hour, true)
	}
	workers.Wait()
	release(t, d, "k", "b", true)
}

func TestCanceledAndDeadlineWaits(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "deadline without Done"} {
		t.Run(mode, func(t *testing.T) {
			d := New()
			acquire(t, d, "k", "a", time.Hour, true)
			original := d.entries["k"].expires
			d.gate <- struct{}{}
			defer d.leave()
			ctx, cancel := context.WithCancel(context.Background())
			operation := ctx
			want := context.Canceled
			switch mode {
			case "deadline":
				cancel()
				operation, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
				want = context.DeadlineExceeded
			case "deadline without Done":
				operation = deadlineContext{time.Now().Add(10 * time.Millisecond)}
				want = context.DeadlineExceeded
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := d.Renew(operation, "k", "a", 2*time.Hour)
				result <- err
			}()
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatal("wrong wait result", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("context failed to interrupt held gate")
			}
			if d.entries["k"].expires != original {
				t.Fatal("canceled wait mutated lease")
			}
		})
	}
}

func TestPostGateContextRecheck(t *testing.T) {
	d := New()
	for _, ctx := range []context.Context{
		deadlineContext{time.Now().Add(-time.Second)},
		func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}(),
	} {
		// Both the channel send and cancellation can be ready. Even when the
		// channel case wins, enter must release it and return the context error.
		if err := d.enter(ctx); err == nil || len(d.gate) != 0 {
			t.Fatal("post-gate context recheck failed", err)
		}
	}
}

type cancelDuringSweep struct {
	context.Context
	cancel context.CancelFunc
	calls  int
}

func (c *cancelDuringSweep) Err() error {
	c.calls++
	if c.calls == 7 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCancellationDuringSweep(t *testing.T) {
	d := New()
	for i := range 20 {
		acquire(t, d, fmt.Sprintf("expired%d", i), "a", time.Hour, true)
	}
	for _, e := range d.expiry {
		e.expires = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, err := d.tryAcquire(&cancelDuringSweep{Context: ctx, cancel: cancel}, "new", "a", time.Hour)
	if got || !errors.Is(err, context.Canceled) {
		t.Fatal("sweep ignored cancellation", got, err)
	}
	if len(d.entries) == 0 || len(d.entries) == 20 || d.entries["new"] != nil || len(d.gate) != 0 {
		t.Fatal("sweep cancellation did not preserve partial cleanup/target state")
	}
	checkHeap(t, d)
	acquire(t, d, "new", "a", time.Hour, true)
	if len(d.entries) != 1 {
		t.Fatal("next operation did not finish cleanup")
	}
}
