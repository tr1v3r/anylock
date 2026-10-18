//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math"
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
	token, enabled := first.Fence()
	if !enabled || token == 0 {
		t.Fatal(token, enabled)
	}
	if busy, operationErr := f.driver.TryAcquireFenced(f.ctx, "key", "intruder", time.Hour); operationErr != nil || busy.Acquired || busy.Fence != 0 {
		t.Fatal(busy, operationErr)
	}
	if operationErr := first.Renew(f.ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	if next, _ := first.Fence(); next != token {
		t.Fatal("renew changed token")
	}
	if operationErr := first.Release(f.ctx); operationErr != nil {
		t.Fatal(operationErr)
	}
	// A second Driver shares the same lock and sequence domains.
	other := f.anotherDriver(t, f.prefix)
	other.sequence = f.driver.sequence
	second, err := other.TryAcquireFenced(f.ctx, "key", "successor", time.Hour)
	if err != nil || !second.Acquired || second.Fence <= token {
		t.Fatal(second, err)
	}
	if ok, operationErr := f.driver.Release(f.ctx, "key", "not-owner"); ok || operationErr != nil {
		t.Fatal(ok, operationErr)
	}
	// Purging all lease rows must not reset the independent sequence.
	if _, operationErr := f.db.ExecContext(f.ctx, "DELETE FROM "+f.quotedTable()); operationErr != nil {
		t.Fatal(operationErr)
	}
	third, err := other.TryAcquireFenced(f.ctx, "key", "after-purge", time.Hour)
	if err != nil || third.Fence <= second.Fence {
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
		result, err := f.driver.TryAcquireFenced(f.ctx, "blocked", "successor", time.Hour)
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
		if r.err != nil || !r.result.Acquired || r.result.Fence <= uint64(previous) {
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
	result, err := f.driver.TryAcquireFenced(f.ctx, "overflow", "owner", time.Hour)
	if err == nil || result.Acquired || result.Fence != 0 {
		t.Fatal(result, err)
	}
	f.requireAbsent(t, "overflow")
}

func TestIntegrationPostgresFencedSuccessorSequence(t *testing.T) {
	f := realFencedPostgres(t)
	var previous uint64
	for i := range 20 {
		owner := fmt.Sprint(i)
		result, err := f.driver.TryAcquireFenced(f.ctx, "key", owner, time.Hour)
		if err != nil || !result.Acquired || result.Fence <= previous {
			t.Fatal(result, err, previous)
		}
		previous = result.Fence
		if ok, operationErr := f.driver.Release(f.ctx, "key", owner); !ok || operationErr != nil {
			t.Fatal(ok, operationErr)
		}
	}
}
