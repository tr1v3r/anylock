package memory

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// All occupancy checks use a TTL much longer than the bounded workload. They
// test live ownership, NOT callback mutual exclusion after expiry. Driver calls
// are deliberately concurrent; the atomics below only observe occupancy.
const workloadTTL = time.Hour

const (
	apiAcquire = iota
	apiRenew
	apiRelease
	apiCount
)

type callMetrics struct {
	Attempts         uint64     `json:"attempts"`
	Success          uint64     `json:"success"`
	Contention       uint64     `json:"contention_or_absent_or_wrong_owner"`
	ExpectedCancel   uint64     `json:"expected_cancel_or_deadline"`
	UnexpectedErrors uint64     `json:"unexpected_errors"`
	Latency          [64]uint64 `json:"latency_log2_ns_buckets"`
}

// One recorder belongs to each worker: no telemetry lock serializes API calls,
// and memory stays O(workers * APIs * 64), independent of the run duration/QPS.
type workerMetrics struct {
	Calls      [apiCount]callMetrics
	Violations uint64
}

func (m *workerMetrics) call(d *Driver, ctx context.Context, api int, key, owner string) (bool, error) {
	started := time.Now()
	var ok bool
	var err error
	switch api {
	case apiAcquire:
		ok, err = d.TryAcquire(ctx, key, owner, workloadTTL)
	case apiRenew:
		ok, err = d.Renew(ctx, key, owner, workloadTTL)
	case apiRelease:
		ok, err = d.Release(ctx, key, owner)
	}
	c := &m.Calls[api]
	c.Attempts++
	bucket := bits.Len64(uint64(time.Since(started)))
	if bucket > 63 {
		bucket = 63
	}
	c.Latency[bucket]++
	observedErr := requestError(ctx)
	switch {
	case err == nil && observedErr != nil:
		// The driver may have returned nil before the recorder observes a
		// canceled context. Conservatively discard that locally late outcome,
		// without claiming driver cancellation, rollback, or live ownership.
		c.ExpectedCancel++
		return false, observedErr
	case err == nil && ok:
		c.Success++
	case err == nil:
		c.Contention++
	case (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && observedErr != nil:
		// A final context check can fail AFTER committing. Never infer rollback.
		c.ExpectedCancel++
	default:
		c.UnexpectedErrors++
	}
	return ok, err
}

func (m *workerMetrics) expect(d *Driver, ctx context.Context, api int, key, owner string, want bool) {
	ok, err := m.call(d, ctx, api, key, owner)
	if err == nil && ok != want {
		m.Violations++
	}
}

// rounds == 0 runs until ctx ends; otherwise each worker executes fixed rounds.
// Callers bound ctx to <= 5min, far below workloadTTL. A private fixed-seed PRNG
// chooses each worker's keys and renew count; scheduling is intentionally free.
func runMixedWorkload(ctx context.Context, d *Driver, workers, keys, rounds int) []workerMetrics {
	occupancy := make([]atomic.Int64, keys)
	keyNames := make([]string, keys)
	for i := range keyNames {
		keyNames[i] = "work:" + strconv.Itoa(i)
	}
	metrics := make([]workerMetrics, workers)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for worker := range workers {
		go func() {
			defer done.Done()
			random := rand.New(rand.NewPCG(uint64(worker)+1, 0xace51))
			m := &metrics[worker]
			ready.Done()
			<-start
			for round := 0; (rounds == 0 || round < rounds) && requestError(ctx) == nil; round++ {
				index := random.IntN(keys)
				key := keyNames[index]
				// Never reuse an owner, including after a failed/ambiguous acquire.
				owner := strconv.Itoa(worker) + ":" + strconv.Itoa(round)
				ok, err := m.call(d, ctx, apiAcquire, key, owner)
				if err != nil {
					continue
				}
				if !ok {
					m.expect(d, ctx, apiRenew, key, owner, false)
					m.expect(d, ctx, apiRelease, key, owner, false)
					continue
				}
				token := int64(worker) + 1
				if !occupancy[index].CompareAndSwap(0, token) {
					m.Violations++
				}
				for n := random.IntN(3) + 1; n > 0; n-- {
					m.expect(d, ctx, apiRenew, key, owner, true)
				}
				m.expect(d, ctx, apiRenew, key, "stale:"+owner, false)
				m.expect(d, ctx, apiRelease, key, "stale:"+owner, false)
				// Clear BEFORE releasing: a successor may acquire immediately.
				if !occupancy[index].CompareAndSwap(token, 0) {
					m.Violations++
				}
				m.expect(d, ctx, apiRelease, key, owner, true)
				// This previously valid owner must not affect a successor.
				m.expect(d, ctx, apiRenew, key, owner, false)
				m.expect(d, ctx, apiRelease, key, owner, false)
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return metrics
}

func mergeMetrics(workers []workerMetrics) workerMetrics {
	var total workerMetrics
	for _, worker := range workers {
		total.Violations += worker.Violations
		for api, c := range worker.Calls {
			out := &total.Calls[api]
			out.Attempts += c.Attempts
			out.Success += c.Success
			out.Contention += c.Contention
			out.ExpectedCancel += c.ExpectedCancel
			out.UnexpectedErrors += c.UnexpectedErrors
			for i, count := range c.Latency {
				out.Latency[i] += count
			}
		}
	}
	return total
}

func TestBoundedMixedConcurrency(t *testing.T) {
	for _, keys := range []int{1, 64} {
		t.Run(fmt.Sprintf("keys=%d", keys), func(t *testing.T) {
			d := New()
			// An anchor prevents empty-container disposal from hiding tombstones.
			acquire(t, d, "anchor", "anchor-owner", workloadTTL, true)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			total := mergeMetrics(runMixedWorkload(ctx, d, 64, keys, 64))
			if ctx.Err() != nil || total.Violations != 0 {
				t.Fatalf("bounded workload: context=%v ownership violations=%d", ctx.Err(), total.Violations)
			}
			for api, c := range total.Calls {
				if c.Success == 0 || c.UnexpectedErrors != 0 || c.ExpectedCancel != 0 {
					t.Fatalf("API %d: %+v", api, c)
				}
			}
			checkHeap(t, d)
			if len(d.entries) != 1 || len(d.expiry) != 1 {
				t.Fatal("churn accumulated map/heap entries", len(d.entries), len(d.expiry))
			}
			for _, e := range d.expiry[len(d.expiry):cap(d.expiry)] {
				if e != nil {
					t.Fatal("removed heap slot retains an entry")
				}
			}
			release(t, d, "anchor", "anchor-owner", true)
			if d.entries != nil || d.expiry != nil {
				t.Fatal("empty churned store retains containers")
			}
		})
	}
}

func TestBoundedSimultaneousContenders(t *testing.T) {
	for _, contenders := range []int{64, 256, 1024} {
		t.Run(strconv.Itoa(contenders), func(t *testing.T) {
			d := New()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			var winners atomic.Int32
			ready.Add(contenders)
			done.Add(contenders)
			for worker := range contenders {
				go func() {
					defer done.Done()
					ready.Done()
					<-start
					ok, err := d.TryAcquire(ctx, "hot", strconv.Itoa(worker), workloadTTL)
					if err != nil {
						t.Error(err)
					}
					if ok {
						winners.Add(1)
					}
				}()
			}
			ready.Wait()
			close(start)
			done.Wait()
			if winners.Load() != 1 {
				t.Fatal("expected exactly one winner", winners.Load())
			}
			checkHeap(t, d)
			release(t, d, "hot", d.entries["hot"].owner, true)
		})
	}
}

// This context signals that enter has evaluated Done with the gate unavailable.
// Unlike a goroutine-start barrier, this proves callers reached the gate wait.
type observedGateContext struct {
	context.Context
	once sync.Once
	seen chan struct{}
}

func (c *observedGateContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.seen) })
	return c.Context.Done()
}

func TestBoundedManyCanceledGateWaiters(t *testing.T) {
	d := New()
	acquire(t, d, "hot", "owner", workloadTTL, true)
	original := d.entries["hot"].expires
	d.gate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const waiters = 256
	results := make(chan error, waiters)
	var done sync.WaitGroup
	done.Add(waiters)
	joined := make(chan struct{})
	go func() {
		done.Wait()
		close(joined)
	}()
	defer func() {
		cancel()
		// Release the test-held token at most once. A broken implementation
		// may already have consumed it: never block or repeatedly drain tokens
		// now owned by workers. Joining must also be bounded on failure.
		select {
		case <-d.gate:
		default:
		}
		join, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-join.Done():
			t.Error("gate waiters failed to join after bounded teardown")
		}
	}()
	seen := make([]chan struct{}, waiters)
	for i := range waiters {
		seen[i] = make(chan struct{})
		operation := &observedGateContext{Context: ctx, seen: seen[i]}
		go func() {
			defer done.Done()
			var err error
			switch i % apiCount {
			case apiAcquire:
				_, err = d.TryAcquire(operation, "other", strconv.Itoa(i), workloadTTL)
			case apiRenew:
				_, err = d.Renew(operation, "hot", "owner", workloadTTL)
			case apiRelease:
				_, err = d.Release(operation, "hot", "owner")
			}
			results <- err
		}()
	}
	for _, arrived := range seen {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("workers failed to reach held gate")
		}
	}
	cancel()
	cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for range waiters {
		select {
		case err := <-results:
			if !errors.Is(err, context.Canceled) {
				t.Error("gate waiter did not report cancellation", err)
			}
		case <-cleanup.Done():
			t.Fatal("canceled gate waiters did not terminate")
		}
	}
	if len(d.gate) != 1 || len(d.entries) != 1 || d.entries["hot"].expires != original {
		t.Fatal("canceled waiters changed ownership or stole the gate")
	}
	checkHeap(t, d)
	// Also exercise teardown with an already-empty gate, without a broken
	// production implementation or scheduler-dependent fault injection.
	d.leave()
}

func TestBoundedBatchExpirySweep(t *testing.T) {
	const keys, workers = 1024, 64
	d := New()
	keyNames := make([]string, keys)
	for i := range keyNames {
		keyNames[i] = "batch:" + strconv.Itoa(i)
		acquire(t, d, keyNames[i], "expired-owner", workloadTTL, true)
	}
	oldMap := d.entries
	oldBacking := d.expiry[:cap(d.expiry)]
	// All workers are quiescent. The first subsequent real operation must
	// sweep this entire expired batch, even when targeting an unrelated key.
	d.gate <- struct{}{}
	for _, e := range d.expiry {
		e.expires = 0
	}
	heap.Init(&d.expiry)
	d.leave()
	checkHeap(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for worker := range workers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			// Race unrelated operations and successor acquires against the
			// same sweeping gate; no wrapper serializes the driver calls.
			ok, err := d.Release(ctx, "unrelated", "nobody")
			if ok || err != nil {
				t.Error("unrelated sweep operation", ok, err)
			}
			for index := worker; index < keys; index += workers {
				key := keyNames[index]
				owner := "successor:" + strconv.Itoa(index)
				ok, err = d.TryAcquire(ctx, key, owner, workloadTTL)
				if !ok || err != nil {
					t.Error("batch successor failed", key, ok, err)
				}
				ok, err = d.Renew(ctx, key, "expired-owner", workloadTTL)
				if ok || err != nil {
					t.Error("expired batch owner renewed successor", key, ok, err)
				}
				ok, err = d.Release(ctx, key, "expired-owner")
				if ok || err != nil {
					t.Error("expired batch owner removed successor", key, ok, err)
				}
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	if ctx.Err() != nil || len(d.entries) != keys || len(d.expiry) != keys || len(oldMap) != 0 {
		t.Fatal("batch sweep leaked expired entries or lost successors", ctx.Err(), len(d.entries), len(d.expiry))
	}
	for _, e := range oldBacking {
		if e != nil {
			t.Fatal("expired heap backing retained an entry")
		}
	}
	checkHeap(t, d)
	for index, key := range keyNames {
		e := d.entries[key]
		if e == nil || e.owner != "successor:"+strconv.Itoa(index) {
			t.Fatal("batch successor ownership changed", key)
		}
		release(t, d, key, e.owner, true)
	}
	if d.entries != nil || d.expiry != nil {
		t.Fatal("batch successor cleanup retained containers")
	}
}

func TestBoundedControlledExpiryTurnover(t *testing.T) {
	d := New()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	acquire(t, d, "hot", "initial", workloadTTL, true)
	for generation := range 8 {
		stale := d.entries["hot"].owner
		// No workers are running. Hold the actual driver gate while changing
		// private state; no production clock hook or concurrent clock mutation.
		d.gate <- struct{}{}
		d.entries["hot"].expires = 0
		heap.Fix(&d.expiry, d.entries["hot"].index)
		d.leave()
		start := make(chan struct{})
		var ready, done sync.WaitGroup
		var winners atomic.Int32
		ready.Add(64)
		for contender := range 64 {
			done.Add(1)
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				owner := fmt.Sprintf("g%d:o%d", generation, contender)
				ok, err := d.TryAcquire(ctx, "hot", owner, workloadTTL)
				if err != nil {
					t.Error(err)
				}
				if ok {
					winners.Add(1)
				}
				for range 4 {
					ok, err = d.Renew(ctx, "hot", stale, workloadTTL)
					if ok || err != nil {
						t.Error("expired owner renewed successor", ok, err)
					}
					ok, err = d.Release(ctx, "hot", stale)
					if ok || err != nil {
						t.Error("expired owner released successor", ok, err)
					}
				}
			}()
		}
		ready.Wait()
		close(start)
		done.Wait()
		if winners.Load() != 1 || len(d.entries) != 1 || len(d.expiry) != 1 {
			t.Fatal("expiry turnover lost single ownership", generation, winners.Load())
		}
		checkHeap(t, d)
	}
	release(t, d, "hot", d.entries["hot"].owner, true)
	if d.entries != nil || d.expiry != nil {
		t.Fatal("turnover retained storage")
	}
}
