package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type stressConfig struct {
	duration time.Duration
	workers  int
	keys     int
}

func parseStressConfig(duration, workers, keys string) (stressConfig, error) {
	config := stressConfig{duration: 10 * time.Second, workers: 64, keys: 64}
	var err error
	if duration != "" {
		config.duration, err = time.ParseDuration(duration)
		if err != nil {
			return config, fmt.Errorf("duration: %w", err)
		}
	}
	if config.duration < time.Second || config.duration > 5*time.Minute {
		return config, fmt.Errorf("duration must be in [1s, 5m]")
	}
	if workers != "" {
		config.workers, err = strconv.Atoi(workers)
		if err != nil {
			return config, fmt.Errorf("workers: %w", err)
		}
	}
	if config.workers < 1 || config.workers > 1024 {
		return config, fmt.Errorf("workers must be in [1, 1024]")
	}
	if keys != "" {
		config.keys, err = strconv.Atoi(keys)
		if err != nil {
			return config, fmt.Errorf("keys: %w", err)
		}
	}
	if config.keys < 1 || config.keys > 4096 {
		return config, fmt.Errorf("keys must be in [1, 4096]")
	}
	return config, nil
}

func TestStressConfigurationBounds(t *testing.T) {
	for _, tc := range []struct {
		name, duration, workers, keys string
		valid                         bool
	}{
		{"defaults", "", "", "", true},
		{"minimum", "1s", "1", "1", true},
		{"maximum", "5m", "1024", "4096", true},
		{"short", "999ms", "1", "1", false},
		{"long", "5m1ns", "1", "1", false},
		{"negative", "-1s", "1", "1", false},
		{"duration syntax", "later", "1", "1", false},
		{"zero workers", "1s", "0", "1", false},
		{"many workers", "1s", "1025", "1", false},
		{"worker syntax", "1s", "1.5", "1", false},
		{"zero keys", "1s", "1", "0", false},
		{"many keys", "1s", "1", "4097", false},
		{"key syntax", "1s", "1", "all", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseStressConfig(tc.duration, tc.workers, tc.keys)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}

type resourceSnapshot struct {
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	Mallocs         uint64 `json:"mallocs"`
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapObjects     uint64 `json:"heap_objects"`
	NumGC           uint32 `json:"gc_cycles"`
	GCPauseNS       uint64 `json:"gc_pause_total_ns"`
	Goroutines      int    `json:"goroutines"`
}

func snapshotResources() resourceSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return resourceSnapshot{
		TotalAllocBytes: m.TotalAlloc, Mallocs: m.Mallocs,
		HeapAllocBytes: m.HeapAlloc, HeapObjects: m.HeapObjects,
		NumGC: m.NumGC, GCPauseNS: m.PauseTotalNs, Goroutines: runtime.NumGoroutine(),
	}
}

type stressAPIMetrics struct {
	callMetrics
	QPS               float64 `json:"attempts_per_second"`
	LatencyP50UpperNS uint64  `json:"latency_p50_quantized_upper_ns"`
	LatencyP95UpperNS uint64  `json:"latency_p95_quantized_upper_ns"`
	LatencyP99UpperNS uint64  `json:"latency_p99_quantized_upper_ns"`
}

// Each bucket i contains durations [2^(i-1), 2^i) ns (bucket 0 includes zero).
// Return the inclusive integer upper bound, NOT a precise sampled percentile.
func latencyQuantile(c callMetrics, percentile uint64) uint64 {
	if c.Attempts == 0 {
		return 0
	}
	target := (c.Attempts*percentile + 99) / 100
	var seen uint64
	for i, count := range c.Latency {
		seen += count
		if seen >= target {
			return (uint64(1) << i) - 1
		}
	}
	return 0
}

func TestWorkloadMetricsActualDeadline(t *testing.T) {
	d := New()
	var m workerMetrics
	ctx := deadlineContext{time.Now().Add(-time.Second)}
	for api := range apiCount {
		m.call(d, ctx, api, "key", "owner")
		c := m.Calls[api]
		if c.Attempts != 1 || c.ExpectedCancel != 1 || c.UnexpectedErrors != 0 {
			t.Fatalf("API %d misclassified actual deadline before Err publication: %+v", api, c)
		}
	}
}

func TestWorkloadMetricsLocallyLateObservation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		api   int
		seed  bool
		owner string
	}{
		{"acquire", apiAcquire, false, "owner"},
		{"renew", apiRenew, true, "owner"},
		{"release", apiRelease, true, "owner"},
		{"contention", apiAcquire, true, "contender"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := New()
			if tc.seed {
				// Renew shortens this to workloadTTL, independent of the clock's
				// sampling resolution; no elapsed-time inequality is assumed.
				acquire(t, d, "key", "owner", 2*workloadTTL, true)
			}
			var original uint64
			if e := d.entries["key"]; e != nil {
				original = e.expires
			}
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			gateWasHeld := false
			ctx := &cancelWhenCommitted{Context: base, cancel: cancel, committed: func() bool {
				if len(d.gate) != 0 {
					gateWasHeld = true
					return false
				}
				// Every driver-side context check still sees a live context.
				// Only the recorder's check AFTER the gate release cancels.
				return gateWasHeld
			}}
			var m workerMetrics
			ok, err := m.call(d, ctx, tc.api, "key", tc.owner)
			c := m.Calls[tc.api]
			if ok || !errors.Is(err, context.Canceled) || !ctx.observed || !gateWasHeld || len(d.gate) != 0 ||
				c.Attempts != 1 || c.ExpectedCancel != 1 || c.Success != 0 || c.Contention != 0 || c.UnexpectedErrors != 0 {
				t.Fatalf("locally late outcome claimed ownership/contention: (%v,%v), %+v", ok, err, c)
			}
			e := d.entries["key"]
			switch tc.name {
			case "acquire":
				if e == nil || e.owner != "owner" {
					t.Fatal("recorder rolled back committed acquisition")
				}
			case "renew":
				if e == nil || e.owner != "owner" || e.expires == original {
					t.Fatal("recorder rolled back committed renewal")
				}
			case "release":
				if e != nil {
					t.Fatal("recorder rolled back committed release")
				}
			case "contention":
				if e == nil || e.owner != "owner" || e.expires != original {
					t.Fatal("late contention observation mutated ownership")
				}
			}
			checkHeap(t, d)
		})
	}
}

func TestWorkloadMetricsUnexpectedErrorAtShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var m workerMetrics
	// Invalid-key validation is a genuine non-context error even at shutdown.
	ok, err := m.call(New(), ctx, apiAcquire, "", "owner")
	c := m.Calls[apiAcquire]
	if ok || err == nil || errors.Is(err, context.Canceled) || c.Attempts != 1 ||
		c.UnexpectedErrors != 1 || c.ExpectedCancel != 0 {
		t.Fatalf("shutdown hid a genuine error: (%v,%v), %+v", ok, err, c)
	}
}

func TestLatencyQuantization(t *testing.T) {
	c := callMetrics{Attempts: 100}
	c.Latency[0], c.Latency[10], c.Latency[20] = 50, 45, 5
	if latencyQuantile(c, 50) != 0 || latencyQuantile(c, 95) != 1023 ||
		latencyQuantile(c, 99) != 1048575 || latencyQuantile(callMetrics{}, 99) != 0 {
		t.Fatal("histogram percentile boundaries changed")
	}
}

type stressReport struct {
	Schema                    int                         `json:"schema"`
	Backend                   string                      `json:"backend"`
	Profile                   string                      `json:"profile"`
	GoVersion                 string                      `json:"go_version"`
	Platform                  string                      `json:"platform"`
	GOMAXPROCS                int                         `json:"gomaxprocs"`
	Workers                   int                         `json:"workers"`
	Keys                      int                         `json:"keys"`
	RequestedDurationNS       int64                       `json:"requested_duration_ns_per_profile"`
	ElapsedNS                 int64                       `json:"elapsed_ns_worker_start_through_join"`
	QPS                       float64                     `json:"api_attempts_per_second"`
	APIs                      map[string]stressAPIMetrics `json:"apis"`
	OwnershipViolations       uint64                      `json:"ownership_violations"`
	Before                    resourceSnapshot            `json:"resources_before"`
	After                     resourceSnapshot            `json:"resources_after_before_cleanup"`
	AllocatedBytes            uint64                      `json:"allocated_bytes_delta"`
	Mallocs                   uint64                      `json:"mallocs_delta"`
	GCCycles                  uint32                      `json:"gc_cycles_delta"`
	GCPauseNS                 uint64                      `json:"gc_pause_ns_delta"`
	SampledPeakHeapAllocBytes uint64                      `json:"sampled_peak_heap_alloc_bytes"`
	SampledPeakGoroutines     uint64                      `json:"sampled_peak_goroutines"`
	Notes                     string                      `json:"notes"`
}

func TestMemoryStress(t *testing.T) {
	switch os.Getenv("ANYLOCK_MEMORY_STRESS") {
	case "":
		t.Skip("opt in with ANYLOCK_MEMORY_STRESS=1; bounded regressions always run")
	case "1":
	default:
		t.Fatal("ANYLOCK_MEMORY_STRESS must be 1 or unset")
	}
	config, err := parseStressConfig(os.Getenv("ANYLOCK_MEMORY_STRESS_DURATION"),
		os.Getenv("ANYLOCK_MEMORY_STRESS_WORKERS"), os.Getenv("ANYLOCK_MEMORY_STRESS_KEYS"))
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []struct {
		name string
		keys int
	}{
		{"hotkey", 1},
		{"multikey", config.keys},
	} {
		t.Run(profile.name, func(t *testing.T) {
			d := New()
			before := snapshotResources()
			var peakHeap, peakGoroutines atomic.Uint64
			peakHeap.Store(before.HeapAllocBytes)
			peakGoroutines.Store(uint64(before.Goroutines))
			stopSampler, samplerDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(samplerDone)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopSampler:
						return
					case <-ticker.C:
						sample := snapshotResources()
						peakHeap.Store(max(peakHeap.Load(), sample.HeapAllocBytes))
						peakGoroutines.Store(max(peakGoroutines.Load(), uint64(sample.Goroutines)))
					}
				}
			}()
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), config.duration)
			workers := runMixedWorkload(ctx, d, config.workers, profile.keys, 0)
			elapsed := time.Since(started)
			cancel()
			total := mergeMetrics(workers)
			close(stopSampler)
			<-samplerDone
			after := snapshotResources()
			report := stressReport{
				Schema: 1, Backend: "memory", Profile: profile.name,
				GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
				GOMAXPROCS: runtime.GOMAXPROCS(0), Workers: config.workers, Keys: profile.keys,
				RequestedDurationNS: int64(config.duration), ElapsedNS: int64(elapsed),
				APIs: make(map[string]stressAPIMetrics), OwnershipViolations: total.Violations,
				Before: before, After: after, AllocatedBytes: after.TotalAllocBytes - before.TotalAllocBytes,
				Mallocs: after.Mallocs - before.Mallocs, GCCycles: after.NumGC - before.NumGC,
				GCPauseNS:                 after.GCPauseNS - before.GCPauseNS,
				SampledPeakHeapAllocBytes: max(peakHeap.Load(), after.HeapAllocBytes),
				SampledPeakGoroutines:     max(peakGoroutines.Load(), uint64(after.Goroutines)),
				Notes: "diagnostic, especially under -race; fixed-seed worker streams, free scheduling; " +
					"all API outcomes included in log2-ns histogram; quantiles are bucket upper bounds; " +
					"QPS denominator includes worker startup/join, excludes cleanup/reporting; " +
					"expected cancel includes nil-error replies first observed after context ends; " +
					"locally discarded late observations do not claim driver cancellation or rollback; " +
					"resources process-global, sampled peaks every 100ms may miss transients; " +
					"shared global driver gate serializes even unrelated keys; no performance floor",
			}
			var attempts uint64
			for api, name := range []string{"acquire", "renew", "release"} {
				c := total.Calls[api]
				attempts += c.Attempts
				report.APIs[name] = stressAPIMetrics{
					callMetrics: c, QPS: float64(c.Attempts) / elapsed.Seconds(),
					LatencyP50UpperNS: latencyQuantile(c, 50), LatencyP95UpperNS: latencyQuantile(c, 95),
					LatencyP99UpperNS: latencyQuantile(c, 99),
				}
			}
			report.QPS = float64(attempts) / elapsed.Seconds()
			data, marshalErr := json.Marshal(report)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			// One JSON object per profile; -v retains it even if a later hard check fails.
			t.Logf("MEMORY_STRESS_JSON %s", data)
			if total.Violations != 0 {
				t.Error("live ownership invariant violations", total.Violations)
			}
			if total.Calls[apiAcquire].Success == 0 {
				t.Error("workload never acquired live ownership")
			}
			for api, c := range total.Calls {
				// A successful acquire may only reach renew/release at shutdown;
				// requiring their success would impose a hidden throughput floor.
				if c.Attempts == 0 || c.UnexpectedErrors != 0 {
					t.Errorf("API %d inactive or unexpected errors: %+v", api, c)
				}
				var observations uint64
				for _, count := range c.Latency {
					observations += count
				}
				if observations != c.Attempts ||
					c.Success+c.Contention+c.ExpectedCancel+c.UnexpectedErrors != c.Attempts {
					t.Errorf("API %d metric accounting mismatch: %+v", api, c)
				}
			}
			checkHeap(t, d) // Workers have joined; private state reads cannot race.
			if len(d.entries) > profile.keys {
				t.Error("resident keys exceeded bounded keyspace")
			}
			// Canceled acquire/renew/release may already have committed. Clean up
			// actual resident owners with a LIVE context, never assume rollback.
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			for key, e := range d.entries {
				ok, releaseErr := d.Release(cleanup, key, e.owner)
				if releaseErr != nil || !ok {
					t.Error("cleanup failed", key, ok, releaseErr)
				}
			}
			if d.entries != nil || d.expiry != nil {
				t.Error("cleanup retained map/heap storage")
			}
		})
	}
}
