package postgres

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	postgresTestConnections = 24
	postgresLoadTTL         = time.Hour // Longer than the entire maximum five-minute workload.
	postgresLoadSeed        = 20261015
)

type postgresLoadConfig struct {
	duration   time.Duration
	workers    int
	keys       int
	iterations int // Nonzero for the mandatory, fixed-operation regression.
}

// Configuration is parsed before opening a fixture, including in server-free tests.
func parsePostgresStressConfig(getenv func(string) string) (postgresLoadConfig, error) {
	cfg := postgresLoadConfig{duration: 2 * time.Second, workers: 64, keys: 32}
	if value := getenv("ANYLOCK_POSTGRES_STRESS_DURATION"); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration < time.Second || duration > 5*time.Minute {
			return cfg, errors.New("ANYLOCK_POSTGRES_STRESS_DURATION must be between 1s and 5m")
		}
		cfg.duration = duration
	}
	for _, field := range []struct {
		name string
		max  int
		dst  *int
	}{
		{"ANYLOCK_POSTGRES_STRESS_CONCURRENCY", 1024, &cfg.workers},
		{"ANYLOCK_POSTGRES_STRESS_KEYS", 4096, &cfg.keys},
	} {
		if value := getenv(field.name); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > field.max {
				return cfg, fmt.Errorf("%s must be an integer between 1 and %d", field.name, field.max)
			}
			*field.dst = n
		}
	}
	return cfg, nil
}

// Fixed 64-bin log2-nanosecond histogram: quantiles are bucket upper bounds,
// not interpolated measurements. Each worker has its own bounded histogram.
type postgresLatencyHistogram [64]uint64

func (h *postgresLatencyHistogram) add(elapsed time.Duration) {
	ns := uint64(max(elapsed, 1))
	h[bits.Len64(ns-1)]++
}

func (h *postgresLatencyHistogram) quantile(percent uint64) time.Duration {
	var count uint64
	for _, n := range h {
		count += n
	}
	if count == 0 {
		return 0
	}
	rank := (count*percent + 99) / 100
	var seen uint64
	for i, n := range h {
		seen += n
		if seen >= rank {
			if i == 63 {
				return time.Duration(1<<63 - 1)
			}
			return time.Duration(uint64(1) << i)
		}
	}
	return 0
}

type postgresOperationCounts struct {
	Attempts         uint64 `json:"attempts"`
	Success          uint64 `json:"success"`
	Contention       uint64 `json:"contention_or_owner_rejection"`
	ExpectedCancel   uint64 `json:"expected_cancel_ambiguous"`
	UnexpectedErrors uint64 `json:"unexpected_errors"`
}

type postgresLoadWorker struct {
	counts     [3]postgresOperationCounts
	latency    postgresLatencyHistogram
	violations uint64
	samples    []string // Capped at four strings per worker; never stores every error/latency.
}

func (w *postgresLoadWorker) problem(format string, args ...any) {
	w.violations++
	if len(w.samples) < 4 {
		w.samples = append(w.samples, fmt.Sprintf(format, args...))
	}
}

// call never interprets a late outcome as ownership and never tries to roll it
// back. Cancellation bounds local observation, not server commit certainty.
func (w *postgresLoadWorker) call(ctx context.Context, d *Driver, op int, key, owner string) (bool, bool) {
	started := time.Now()
	var ok bool
	var err error
	switch op {
	case 0:
		ok, err = d.TryAcquire(ctx, key, owner, postgresLoadTTL)
	case 1:
		ok, err = d.Renew(ctx, key, owner, postgresLoadTTL)
	case 2:
		ok, err = d.Release(ctx, key, owner)
	}
	w.latency.add(time.Since(started))
	count := &w.counts[op]
	count.Attempts++
	ended := requestError(ctx) != nil
	switch {
	case ended && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		count.ExpectedCancel++
		return false, false
	case err != nil:
		count.UnexpectedErrors++
		w.problem("operation %d: %v", op, err)
		return false, false
	case ok:
		count.Success++
	default:
		count.Contention++
	}
	return ok, true
}

type postgresLoadReport struct {
	Profile                 string                             `json:"profile"`
	Seed                    int64                              `json:"seed"`
	Workers                 int                                `json:"contenders"`
	Keys                    int                                `json:"keys"`
	PoolMaxConnections      int                                `json:"pool_max_connections"`
	PoolOpenStart           int                                `json:"pool_open_start"`
	PoolOpenEnd             int                                `json:"pool_open_end"`
	PoolInUseEnd            int                                `json:"pool_in_use_end"`
	PoolIdleEnd             int                                `json:"pool_idle_end"`
	PoolWaitCount           int64                              `json:"pool_wait_count_delta"`
	PoolWaitNanoseconds     int64                              `json:"pool_wait_ns_delta"`
	ElapsedSeconds          float64                            `json:"elapsed_seconds"`
	Operations              map[string]postgresOperationCounts `json:"operations"`
	Totals                  postgresOperationCounts            `json:"totals"`
	ThroughputOperationsSec float64                            `json:"throughput_operations_per_second"`
	LatencyOperationCount   uint64                             `json:"latency_operation_count"`
	LatencyApproximation    string                             `json:"latency_approximation"`
	LatencyP50Microseconds  float64                            `json:"latency_p50_approx_us"`
	LatencyP95Microseconds  float64                            `json:"latency_p95_approx_us"`
	LatencyP99Microseconds  float64                            `json:"latency_p99_approx_us"`
	InvariantViolations     uint64                             `json:"invariant_violations"`
	RequireCompleteMix      bool                               `json:"require_complete_mix"`
	MissingAPIAttempts      []string                           `json:"missing_api_attempts,omitempty"`
	MissingAPISuccesses     []string                           `json:"missing_api_successes,omitempty"`
	ErrorSamples            []string                           `json:"error_samples,omitempty"`
	HeapAllocStartBytes     uint64                             `json:"heap_alloc_start_bytes"`
	HeapAllocEndBytes       uint64                             `json:"heap_alloc_end_bytes"`
	AllocatedBytes          uint64                             `json:"allocated_bytes"`
	GCCycles                uint32                             `json:"gc_cycles"`
	GCPauseNanoseconds      uint64                             `json:"gc_pause_ns"`
	GoroutinesStart         int                                `json:"goroutines_start"`
	GoroutinesEnd           int                                `json:"goroutines_end"`
}

func addPostgresCounts(dst *postgresOperationCounts, src postgresOperationCounts) {
	dst.Attempts += src.Attempts
	dst.Success += src.Success
	dst.Contention += src.Contention
	dst.ExpectedCancel += src.ExpectedCancel
	dst.UnexpectedErrors += src.UnexpectedErrors
}

// Only occupancy is shared between workers. No mutex wraps any driver operation.
// Each owner token is fresh for exactly one attempt; retired tokens are probed
// after release (or a rejected acquisition), concurrently with successor churn.
func runPostgresLoad(ctx context.Context, d *Driver, cfg postgresLoadConfig, profile string) postgresLoadReport {
	poolBefore := d.db.Stats()
	report := postgresLoadReport{
		Profile: profile, Seed: postgresLoadSeed, Workers: cfg.workers, Keys: cfg.keys,
		PoolMaxConnections:   poolBefore.MaxOpenConnections,
		PoolOpenStart:        poolBefore.OpenConnections,
		Operations:           make(map[string]postgresOperationCounts),
		LatencyApproximation: "64 fixed log2-ns buckets; upper bounds (<2x except saturation); all attempted operations including cancellation",
		GoroutinesStart:      runtime.NumGoroutine(),
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	occupancy := make([]atomic.Int32, cfg.keys)
	results := make([]postgresLoadWorker, cfg.workers)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range cfg.workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			w := &results[worker]
			rng := rand.New(rand.NewSource(postgresLoadSeed + int64(worker)))
			<-start
			for iteration := 0; cfg.iterations == 0 || iteration < cfg.iterations; iteration++ {
				if requestError(ctx) != nil {
					return
				}
				index := rng.Intn(cfg.keys)
				key := fmt.Sprintf("load-%d", index)
				owner := fmt.Sprintf("worker-%d-attempt-%d", worker, iteration)
				acquired, certain := w.call(ctx, d, 0, key, owner)
				if !certain {
					return
				}
				if acquired {
					if !occupancy[index].CompareAndSwap(0, 1) {
						w.problem("overlapping long-TTL holders for %s", key)
						return
					}
					runtime.Gosched()
					renewed, certain := w.call(ctx, d, 1, key, owner)
					// Clear BEFORE Release: a real successor may acquire as soon
					// as the SQL release commits, before this caller gets its reply.
					occupancy[index].Store(0)
					if !certain {
						return
					}
					if !renewed {
						w.problem("live owner could not renew %s", key)
						return
					}
					released, certain := w.call(ctx, d, 2, key, owner)
					if !certain {
						return
					}
					if !released {
						w.problem("live owner could not release %s", key)
						return
					}
				}
				for _, op := range []int{1, 2} {
					ok, certain := w.call(ctx, d, op, key, owner)
					if !certain {
						return
					}
					if ok {
						w.problem("retired/rejected owner mutated %s with operation %d", key, op)
						return
					}
				}
			}
		}()
	}
	started := time.Now()
	close(start)
	workers.Wait()
	report.ElapsedSeconds = time.Since(started).Seconds()
	runtime.ReadMemStats(&after)
	report.HeapAllocStartBytes, report.HeapAllocEndBytes = before.HeapAlloc, after.HeapAlloc
	report.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
	report.GCCycles, report.GCPauseNanoseconds = after.NumGC-before.NumGC, after.PauseTotalNs-before.PauseTotalNs
	report.GoroutinesEnd = runtime.NumGoroutine()
	poolAfter := d.db.Stats()
	report.PoolOpenEnd, report.PoolInUseEnd = poolAfter.OpenConnections, poolAfter.InUse
	report.PoolIdleEnd = poolAfter.Idle
	report.PoolWaitCount = poolAfter.WaitCount - poolBefore.WaitCount
	report.PoolWaitNanoseconds = int64(poolAfter.WaitDuration - poolBefore.WaitDuration)
	var histogram postgresLatencyHistogram
	for _, worker := range results {
		for op, name := range []string{"acquire", "renew", "release"} {
			count := report.Operations[name]
			addPostgresCounts(&count, worker.counts[op])
			report.Operations[name] = count
			addPostgresCounts(&report.Totals, worker.counts[op])
		}
		for i, n := range worker.latency {
			histogram[i] += n
			report.LatencyOperationCount += n
		}
		report.InvariantViolations += worker.violations
		for _, sample := range worker.samples {
			if len(report.ErrorSamples) < 8 {
				report.ErrorSamples = append(report.ErrorSamples, sample)
			}
		}
	}
	for i := range occupancy {
		if occupancy[i].Load() != 0 {
			report.InvariantViolations++
		}
	}
	report.ThroughputOperationsSec = float64(report.Totals.Attempts) / report.ElapsedSeconds
	report.LatencyP50Microseconds = float64(histogram.quantile(50)) / 1000
	report.LatencyP95Microseconds = float64(histogram.quantile(95)) / 1000
	report.LatencyP99Microseconds = float64(histogram.quantile(99)) / 1000
	return report
}

// Fixed-round regressions require completed ownership cycles. Sustained runs
// report an incomplete API mix honestly: cancellation during the first matching
// Renew can end the workload before any Release is attempted, which is not a
// driver defect and must not be hidden by issuing canceled calls to pad counts.
func postgresLoadActivity(report *postgresLoadReport, requireCompleteMix bool) error {
	report.RequireCompleteMix = requireCompleteMix
	report.MissingAPIAttempts, report.MissingAPISuccesses = nil, nil
	for _, op := range []string{"acquire", "renew", "release"} {
		if report.Operations[op].Attempts == 0 {
			report.MissingAPIAttempts = append(report.MissingAPIAttempts, op)
		}
		if report.Operations[op].Success == 0 {
			report.MissingAPISuccesses = append(report.MissingAPISuccesses, op)
		}
	}
	if report.Operations["acquire"].Success == 0 {
		return errors.New("no successful acquisition activity")
	}
	if requireCompleteMix && len(report.MissingAPISuccesses) != 0 {
		return fmt.Errorf("bounded workload missing successful APIs: %v", report.MissingAPISuccesses)
	}
	return nil
}

func checkPostgresLoadReport(t *testing.T, report postgresLoadReport, requireCompleteMix bool) {
	t.Helper()
	activityErr := postgresLoadActivity(&report, requireCompleteMix)
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ANYLOCK_POSTGRES_METRICS %s", encoded)
	if report.InvariantViolations != 0 || report.Totals.UnexpectedErrors != 0 {
		t.Errorf("load invariants/errors: %d/%d: %v", report.InvariantViolations,
			report.Totals.UnexpectedErrors, report.ErrorSamples)
	}
	if report.LatencyOperationCount != report.Totals.Attempts || report.Totals.Attempts !=
		report.Totals.Success+report.Totals.Contention+report.Totals.ExpectedCancel+report.Totals.UnexpectedErrors {
		t.Error("metrics outcome/histogram denominator mismatch")
	}
	if activityErr != nil {
		t.Error(activityErr)
	}
}

func TestPostgresStressConfigBounds(t *testing.T) {
	const duration = "ANYLOCK_POSTGRES_STRESS_DURATION"
	const workers = "ANYLOCK_POSTGRES_STRESS_CONCURRENCY"
	const keys = "ANYLOCK_POSTGRES_STRESS_KEYS"
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{duration, "1s", true}, {duration, "5m", true}, {duration, "0", false},
		{duration, "999ms", false}, {duration, "5m1ns", false}, {duration, "invalid", false},
		{workers, "1", true}, {workers, "1024", true}, {workers, "0", false},
		{workers, "1025", false}, {workers, "-1", false}, {workers, "1.5", false},
		{keys, "1", true}, {keys, "4096", true}, {keys, "0", false},
		{keys, "4097", false}, {keys, "99999999999999999999999", false},
	} {
		t.Run(tc.name+"/"+tc.value, func(t *testing.T) {
			_, err := parsePostgresStressConfig(func(name string) string {
				if name == tc.name {
					return tc.value
				}
				return ""
			})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
	cfg, err := parsePostgresStressConfig(func(string) string { return "" })
	if err != nil || cfg.duration != 2*time.Second || cfg.workers != 64 || cfg.keys != 32 {
		t.Fatalf("defaults = %+v, %v", cfg, err)
	}
}

func TestPostgresLoadCancellationBeforeRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var calls atomic.Int64
	db := sql.OpenDB(&contextConnector{exec: func(context.Context) (sqldriver.Result, error) {
		if calls.Add(1) == 2 {
			// The first acquire succeeds. Its matching Renew may commit, but
			// cancellation makes that result ambiguous and stops the worker.
			cancel()
		}
		return sqldriver.RowsAffected(1), nil
	}})
	defer db.Close()
	d, err := New(db, "")
	if err != nil {
		t.Fatal(err)
	}
	report := runPostgresLoad(ctx, d, postgresLoadConfig{workers: 1, keys: 1, iterations: 1}, "cancel-during-renew")
	if calls.Load() != 2 || report.Operations["acquire"].Success != 1 ||
		report.Operations["renew"].ExpectedCancel != 1 || report.Operations["release"].Attempts != 0 {
		t.Fatalf("unexpected cancellation path: transport calls=%d, operations=%+v", calls.Load(), report.Operations)
	}
	optional := report
	if err := postgresLoadActivity(&optional, false); err != nil {
		t.Fatalf("legitimate sustained shutdown rejected: %v", err)
	}
	if len(optional.MissingAPIAttempts) != 1 || optional.MissingAPIAttempts[0] != "release" ||
		len(optional.MissingAPISuccesses) != 2 || optional.MissingAPISuccesses[0] != "renew" ||
		optional.MissingAPISuccesses[1] != "release" || optional.RequireCompleteMix {
		t.Fatalf("incomplete shutdown mix not reported honestly: %+v", optional)
	}
	bounded := report
	if err := postgresLoadActivity(&bounded, true); err == nil || !bounded.RequireCompleteMix {
		t.Fatal("fixed-round regression accepted incomplete ownership cycles")
	}
	// The actual reporter must accept and emit the incomplete sustained mix,
	// not merely the pure activity helper. Counts/histogram/invariants still apply.
	checkPostgresLoadReport(t, report, false)
}

func TestPostgresLoadOutcomeAccounting(t *testing.T) {
	for _, mode := range []string{"success", "contention", "backend error", "late success", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			db := sql.OpenDB(&contextConnector{exec: func(context.Context) (sqldriver.Result, error) {
				switch mode {
				case "contention":
					return sqldriver.RowsAffected(0), nil
				case "backend error":
					return nil, errors.New("backend failed")
				case "late success":
					cancel()
				case "canceled":
					cancel()
					return nil, context.Canceled
				}
				return sqldriver.RowsAffected(1), nil
			}})
			defer db.Close()
			d, err := New(db, "")
			if err != nil {
				t.Fatal(err)
			}
			var worker postgresLoadWorker
			ok, certain := worker.call(ctx, d, 0, "key", "owner")
			want := postgresOperationCounts{Attempts: 1}
			switch mode {
			case "success":
				want.Success = 1
			case "contention":
				want.Contention = 1
			case "backend error":
				want.UnexpectedErrors = 1
			default:
				want.ExpectedCancel = 1
			}
			if worker.counts[0] != want || ok != (mode == "success") ||
				certain != (mode == "success" || mode == "contention") {
				t.Fatalf("outcome=%+v (%v,%v); want %+v", worker.counts[0], ok, certain, want)
			}
			var samples uint64
			for _, n := range worker.latency {
				samples += n
			}
			if samples != 1 {
				t.Fatalf("latency samples=%d; want one attempted operation", samples)
			}
		})
	}
}

func TestPostgresLatencyHistogram(t *testing.T) {
	var h postgresLatencyHistogram
	if h.quantile(99) != 0 {
		t.Fatal("empty histogram has nonzero quantile")
	}
	for i := range 100 {
		h.add(time.Duration(i+1) * time.Microsecond)
	}
	for _, tc := range []struct {
		percent uint64
		want    time.Duration
	}{{50, 65536}, {95, 131072}, {99, 131072}} {
		if got := h.quantile(tc.percent); got != tc.want {
			t.Errorf("p%d = %v; want upper bucket %v", tc.percent, got, tc.want)
		}
	}
	var endpoints postgresLatencyHistogram
	endpoints.add(time.Duration(1<<63 - 1))
	endpoints.add(0)
	if endpoints.quantile(100) != time.Duration(1<<63-1) || endpoints.quantile(1) != 1 {
		t.Fatal("histogram endpoint/saturation mismatch")
	}
}
