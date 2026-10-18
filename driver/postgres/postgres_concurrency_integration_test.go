//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Contenders are goroutines/unique owners, NOT dedicated server connections.
// All share the caller-owned fixture pool, capped at 24 real connections.
func postgresAcquisitionWave(t *testing.T, f *postgresFixture, key, tokenPrefix string, contenders int) string {
	t.Helper()
	type result struct {
		owner string
		ok    bool
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, contenders)
	var ready, workers sync.WaitGroup
	for i := range contenders {
		ready.Add(1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			owner := fmt.Sprintf("%s-%s-contender-%d", key, tokenPrefix, i)
			ready.Done()
			<-start
			// f.ctx has an explicit overall deadline, avoiding the driver's
			// default one-second timeout under a 1024-contender queue.
			ok, err := f.driver.tryAcquire(f.ctx, key, owner, postgresLoadTTL)
			results <- result{owner, ok, err}
		}()
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(results)
	winner, winners := "", 0
	for result := range results {
		if result.err != nil {
			t.Errorf("contender %s: %v", result.owner, result.err)
		}
		if result.ok {
			winner = result.owner
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d contenders, %d max real connections: winners=%d; want exactly one",
			contenders, f.db.Stats().MaxOpenConnections, winners)
	}
	if owner, _ := f.row(t, key); owner != winner {
		t.Fatalf("stored owner=%q; want winner %q", owner, winner)
	}
	t.Logf("contenders=%d pool_max_connections=%d winner=%s", contenders, f.db.Stats().MaxOpenConnections, winner)
	return winner
}

func TestIntegrationPostgresConcurrentMixedChurn(t *testing.T) {
	for _, profile := range []struct {
		name string
		keys int
	}{{"hot", 1}, {"multikey", 16}} {
		t.Run(profile.name, func(t *testing.T) {
			f := realPostgres(t)
			cfg := postgresLoadConfig{workers: 32, keys: profile.keys, iterations: 16}
			report := runPostgresLoad(f.ctx, f.driver, cfg, profile.name)
			checkPostgresLoadReport(t, report, true)
			if report.Totals.ExpectedCancel != 0 {
				t.Error("bounded churn exhausted its overall deadline")
			}
			// All successful owners released; no test worker was canceled and
			// stale/rejected probes must not revive or create a reservation.
			var rows int
			if err := f.db.QueryRowContext(f.ctx, `SELECT count(*) FROM `+f.quotedTable()).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Errorf("churn left %d owned rows; want zero", rows)
			}
		})
	}
}

func TestIntegrationPostgresConcurrentExpiryTakeovers(t *testing.T) {
	f := realPostgres(t)
	owner := "initial-owner"
	ok, err := f.driver.tryAcquire(f.ctx, "expiry", owner, 40*time.Millisecond)
	requireResult(t, true, ok, err)
	for phase := range 3 {
		_, expires := f.row(t, "expiry")
		f.waitExpired(t, expires) // Server-clock condition, never a blind sleep.
		old := owner
		// Keep the physical key stable but use fresh tokens for every takeover.
		owner = postgresAcquisitionWave(t, f, "expiry", fmt.Sprintf("phase-%d", phase), 64)
		_, successorExpiry := f.row(t, "expiry")
		for _, operation := range driverOperations(f.driver)[1:] {
			ok, err := operation.call(f.ctx, "expiry", old, postgresLoadTTL)
			requireResult(t, false, ok, err)
		}
		if actual, unchanged := f.row(t, "expiry"); actual != owner || !unchanged.Equal(successorExpiry) {
			t.Fatalf("expired predecessor damaged successor: owner=%q expiry=%v; want %q %v",
				actual, unchanged, owner, successorExpiry)
		}
		if phase < 2 {
			ok, err := f.driver.Renew(f.ctx, "expiry", owner, 40*time.Millisecond)
			requireResult(t, true, ok, err)
		}
	}
}

func TestIntegrationPostgresConcurrentExpiredOwnerCleanup(t *testing.T) {
	f := realPostgres(t)
	const key = "expired-cleanup"
	ok, err := f.driver.tryAcquire(f.ctx, key, "expired-owner", 40*time.Millisecond)
	requireResult(t, true, ok, err)
	_, expires := f.row(t, key)
	f.waitExpired(t, expires)

	const contenders = 64
	type result struct {
		ok  bool
		err error
	}
	start := make(chan struct{})
	results := make(chan result, contenders)
	var ready, workers sync.WaitGroup
	for range contenders {
		ready.Add(1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			ready.Done()
			<-start
			removed, cleanupErr := f.driver.Release(f.ctx, key, "expired-owner")
			results <- result{removed, cleanupErr}
		}()
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(results)
	deleted := 0
	for result := range results {
		if result.err != nil {
			t.Error(result.err)
		}
		if result.ok {
			deleted++
		}
	}
	if deleted != 1 {
		t.Fatalf("expired row deleted %d times; want exactly one", deleted)
	}
	f.requireAbsent(t, key)
	ok, err = f.driver.Release(f.ctx, key, "expired-owner")
	requireResult(t, false, ok, err)
	winner := postgresAcquisitionWave(t, f, key, "after-cleanup", contenders)
	ok, err = f.driver.Release(f.ctx, key, "expired-owner")
	requireResult(t, false, ok, err)
	if owner, _ := f.row(t, key); owner != winner {
		t.Fatal("expired owner cleanup removed successor", owner, winner)
	}
	ok, err = f.driver.Release(f.ctx, key, winner)
	requireResult(t, true, ok, err)
	f.requireAbsent(t, key)
}

func TestIntegrationPostgresManyExpiredKeys(t *testing.T) {
	f := realPostgres(t)
	const keys = 64
	for i := range keys {
		key := fmt.Sprintf("batch-%d", i)
		ok, err := f.driver.tryAcquire(f.ctx, key, "predecessor", 80*time.Millisecond)
		requireResult(t, true, ok, err)
	}
	var lastExpiry time.Time
	if err := f.db.QueryRowContext(f.ctx, `SELECT max(expires_at) FROM `+f.quotedTable()).Scan(&lastExpiry); err != nil {
		t.Fatal(err)
	}
	f.waitExpired(t, lastExpiry) // All 64 keys are now expired according to the server.
	type result struct {
		index, operation int
		owner            string
		ok               bool
		err              error
	}
	start := make(chan struct{})
	results := make(chan result, keys*4)
	var ready, workers sync.WaitGroup
	for index := range keys {
		for operation := range 4 {
			ready.Add(1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				key := fmt.Sprintf("batch-%d", index)
				owner := fmt.Sprintf("batch-%d-contender-%d", index, operation)
				ready.Done()
				<-start
				var ok bool
				var err error
				switch operation {
				case 0, 1:
					ok, err = f.driver.tryAcquire(f.ctx, key, owner, postgresLoadTTL)
				case 2:
					ok, err = f.driver.Renew(f.ctx, key, "predecessor", postgresLoadTTL)
				case 3:
					ok, err = f.driver.Release(f.ctx, key, "predecessor")
				}
				results <- result{index, operation, owner, ok, err}
			}()
		}
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(results)
	var winners [keys]string
	var counts [keys]int
	for result := range results {
		if result.err != nil {
			t.Error(result.err)
		}
		if result.ok {
			switch result.operation {
			case 0, 1:
				winners[result.index] = result.owner
				counts[result.index]++
			case 2:
				t.Errorf("expired predecessor renewed batch key %d", result.index)
			case 3:
				// Cleanup may delete the predecessor before takeover, but the
				// atomic owner CAS must leave the eventual winner intact.
			}
		}
	}
	for i := range keys {
		if counts[i] != 1 {
			t.Errorf("batch key %d winners=%d; want exactly one", i, counts[i])
		}
		if owner, _ := f.row(t, fmt.Sprintf("batch-%d", i)); owner != winners[i] {
			t.Errorf("batch key %d owner=%q; want %q", i, owner, winners[i])
		}
	}
}

func TestIntegrationPostgresManyCanceledPoolWaiters(t *testing.T) {
	f := realPostgres(t)
	ok, err := f.driver.tryAcquire(f.ctx, "pool", "owner", postgresLoadTTL)
	requireResult(t, true, ok, err)
	_, expires := f.row(t, "pool")
	f.db.SetMaxOpenConns(1)
	conn, err := f.db.Conn(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	const count = 256
	start := make(chan struct{})
	results := make(chan error, count)
	waiting, cancel := context.WithCancel(f.ctx)
	before := f.db.Stats().WaitCount
	var workers sync.WaitGroup
	defer func() {
		// Unblock pool waiters even if cancellation/assertion failed, then join
		// all test workers before the caller-owned fixture is dropped.
		cancel()
		_ = conn.Close()
		workers.Wait()
	}()
	for i := range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			ctx := waiting
			if i%2 == 0 {
				// Half are already canceled; half must actually enter the pool
				// queue before a barrier cancels them together.
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(f.ctx)
				stop()
			}
			op := driverOperations(f.driver)[i%3]
			ok, err := op.call(ctx, "pool", fmt.Sprintf("request-%d", i), postgresLoadTTL)
			if ok || !errors.Is(err, context.Canceled) {
				results <- fmt.Errorf("request %d: %v, %v; want canceled, not ownership", i, ok, err)
			}
		}()
	}
	close(start)
	waitPostgres(t, f.ctx, "128 requests queued behind the sole checked-out connection", func() (bool, error) {
		return f.db.Stats().WaitCount-before >= count/2, nil
	})
	cancel()
	workers.Wait()
	close(results)
	for err := range results {
		t.Error(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if owner, unchanged := f.row(t, "pool"); owner != "owner" || !unchanged.Equal(expires) {
		t.Fatal("pool-only canceled requests changed owner/expiry", owner, unchanged)
	}
	if err := f.db.PingContext(f.ctx); err != nil {
		t.Fatal("caller-owned pool unusable after cancellation wave", err)
	}
}

func TestIntegrationPostgresSustainedStress(t *testing.T) {
	if os.Getenv("ANYLOCK_POSTGRES_STRESS") != "1" {
		t.Skip("set ANYLOCK_POSTGRES_STRESS=1 explicitly for sustained diagnostics")
	}
	cfg, err := parsePostgresStressConfig(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ANYLOCK_POSTGRES_DSN") == "" {
		t.Fatal("ANYLOCK_POSTGRES_STRESS=1 requires ANYLOCK_POSTGRES_DSN")
	}
	for _, profile := range []string{"hot", "multikey"} {
		t.Run(profile, func(t *testing.T) {
			profileConfig := cfg
			if profile == "hot" {
				profileConfig.keys = 1
			}
			f := realPostgresWithin(t, cfg.duration+15*time.Second)
			ctx, cancel := context.WithTimeout(f.ctx, cfg.duration)
			defer cancel()
			report := runPostgresLoad(ctx, f.driver, profileConfig, profile)
			checkPostgresLoadReport(t, report, false)
			// End-of-run cancellation may have committed. Do not release late
			// outcomes or assume rollback; fixture cleanup drops only our table.
		})
	}
}
