//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/tr1v3r/anylock"
)

func realFencedPostgres(t *testing.T) *postgresFixture {
	t.Helper()
	f := realPostgres(t)
	sequence := "public." + f.table + "_fences"
	d, err := New(f.db, f.prefix, WithTable("public."+f.table), WithFenceSequence(sequence))
	if err != nil {
		t.Fatal(err)
	}
	f.driver = d
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, operationErr := f.db.ExecContext(ctx, "DROP SEQUENCE IF EXISTS "+d.sequence); operationErr != nil {
			t.Error(operationErr)
		}
	})
	if operationErr := d.EnsureFencingSchema(f.ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	return f
}

func TestIntegrationPostgresFencingLifecycle(t *testing.T) {
	f := realFencedPostgres(t)
	locker, err := anylock.New(f.driver, anylock.WithFencing())
	if err != nil {
		t.Fatal(err)
	}
	first, err := locker.TryLock(f.ctx, "key", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })
	token, enabled := first.FencingToken()
	if !enabled || token == 0 {
		t.Fatal(token, enabled)
	}
	if busy, operationErr := f.driver.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "key", Owner: "intruder", TTL: time.Hour, Fencing: true}); operationErr != nil || busy.Acquired || busy.FencingToken != 0 {
		t.Fatal(busy, operationErr)
	}
	if operationErr := first.Renew(f.ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	if next, _ := first.FencingToken(); next != token {
		t.Fatal("renew changed token")
	}
	if operationErr := first.Release(f.ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	// A second Driver shares the same lock and sequence domains.
	other := f.anotherDriver(t, f.prefix)
	other.sequence = f.driver.sequence
	second, err := other.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "key", Owner: "successor", TTL: time.Hour, Fencing: true})
	if err != nil || !second.Acquired || second.FencingToken <= token {
		t.Fatal(second, err)
	}
	if ok, operationErr := f.driver.Release(f.ctx, "key", "not-owner"); ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	// Purging all lease rows must not reset the independent sequence.
	if _, operationErr := f.db.ExecContext(f.ctx, "DELETE FROM "+f.quotedTable()); operationErr != nil {
		t.Fatal(operationErr)
	}
	third, err := other.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "key", Owner: "after-purge", TTL: time.Hour, Fencing: true})
	if err != nil || third.FencingToken <= second.FencingToken {
		t.Fatal(third, err)
	}
}

func TestIntegrationPostgresFenceAllocatedAfterRowLock(t *testing.T) {
	f := realFencedPostgres(t)
	tx, pid, _ := f.holdRow(t, "blocked", time.Hour)
	type reply struct {
		result anylock.AcquireResult
		err    error
	}
	finished := make(chan reply, 1)
	go func() {
		result, err := f.driver.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "blocked", Owner: "successor", TTL: time.Hour, Fencing: true})
		finished <- reply{result, err}
	}()
	f.waitBlocked(t, pid)
	// Allocation before ON CONFLICT would already have consumed a sequence value.
	var called bool
	if operationErr := f.db.QueryRowContext(f.ctx, "SELECT is_called FROM "+f.driver.sequence).Scan(&called); operationErr != nil {
		t.Fatal(operationErr)
	}
	if called {
		t.Fatal("row-lock waiter allocated its token before acquiring ownership")
	}
	// Install a predecessor's token while still holding the row lock. The waiter
	// must allocate AFTER this, once expiry and the row lock let it take ownership.
	var previous int64
	if operationErr := tx.QueryRowContext(f.ctx, "SELECT nextval($1::regclass)", f.driver.sequence).Scan(&previous); operationErr != nil {
		t.Fatal(operationErr)
	}
	if _, operationErr := tx.ExecContext(f.ctx, "UPDATE "+f.quotedTable()+" SET expires_at = clock_timestamp() - interval '1 second' WHERE key=$1", f.prefix+"blocked"); operationErr != nil {
		t.Fatal(operationErr)
	}
	if operationErr := tx.Commit(); operationErr != nil {
		t.Fatal(operationErr)
	}
	select {
	case r := <-finished:
		if r.err != nil || !r.result.Acquired || r.result.FencingToken <= uint64(previous) {
			t.Fatal(r.result, r.err, previous)
		}
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
}

func TestIntegrationPostgresFenceExhaustionRollsBackOwnership(t *testing.T) {
	f := realFencedPostgres(t)
	if _, operationErr := f.db.ExecContext(f.ctx, "SELECT setval($1::regclass,$2,true)", f.driver.sequence, int64(math.MaxInt64)); operationErr != nil {
		t.Fatal(operationErr)
	}
	result, err := f.driver.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "overflow", Owner: "owner", TTL: time.Hour, Fencing: true})
	if err == nil || result.Acquired || result.FencingToken != 0 {
		t.Fatal(result, err)
	}
	f.requireAbsent(t, "overflow")
}

func TestIntegrationPostgresFencedSuccessorSequence(t *testing.T) {
	f := realFencedPostgres(t)
	var previous uint64
	for i := range 20 {
		owner := fmt.Sprint(i)
		result, err := f.driver.TryAcquire(f.ctx, anylock.AcquireRequest{Key: "key", Owner: owner, TTL: time.Hour, Fencing: true})
		if err != nil || !result.Acquired || result.FencingToken <= previous {
			t.Fatal(result, err, previous)
		}
		previous = result.FencingToken
		if ok, operationErr := f.driver.Release(f.ctx, "key", owner); !ok || operationErr != nil {
			t.Fatal(ok, operationErr)
		}
	}
}

func TestIntegrationPostgresConcurrentFencedAcquisition(t *testing.T) {
	f := realFencedPostgres(t)
	d, ctx := f.driver, f.ctx
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
