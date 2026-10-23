package anylock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestLockBackendErrorsNeverRetry(t *testing.T) {
	failure := errors.New("ambiguous backend failure")
	for _, fenced := range []bool{false, true} {
		for _, acquired := range []bool{false, true} {
			for _, tc := range []struct {
				name string
				err  error
			}{
				{"plain", ErrNotAcquired},
				{"wrapped", fmt.Errorf("backend: %w", ErrNotAcquired)},
				{"joined", errors.Join(failure, ErrNotAcquired)},
			} {
				t.Run(fmt.Sprintf("fenced=%t/acquired=%t/%s", fenced, acquired, tc.name), func(t *testing.T) {
					calls := 0
					driver := acquisitionDriver{Driver: newMemoryDriver(),
						acquire: func(_ context.Context, req AcquireRequest) (AcquireResult, error) {
							calls++
							if req.Fencing != fenced {
								t.Fatal("fencing option was not forwarded")
							}
							result := AcquireResult{Acquired: acquired}
							if calls > 1 {
								// A regression must fail promptly, not hang retrying forever.
								result.Acquired = true
							}
							if result.Acquired && fenced {
								result.FencingToken = 7
							}
							if calls > 1 {
								return result, nil
							}
							return result, tc.err
						},
					}
					options := []Option{WithRetryInterval(time.Millisecond)}
					if fenced {
						options = append(options, WithFencing())
					}
					lease, err := testLocker(t, driver, options...).Lock(context.Background(), "key", time.Hour)
					if lease != nil {
						defer lease.Release(context.Background())
					}
					if calls != 1 || lease != nil || !errors.Is(err, tc.err) {
						t.Fatalf("leaseReturned=%t, error=%v, calls=%d; want nil, original backend error, 1", lease != nil, err, calls)
					}
					if tc.name == "joined" && !errors.Is(err, failure) {
						t.Fatal("joined backend error identity was lost", err)
					}
				})
			}
		}
	}
}

func TestLockCleanupErrorsNeverRetry(t *testing.T) {
	failure := errors.New("cleanup transport failed")
	cleanupFailure := errors.Join(failure, ErrNotAcquired)
	for _, fenced := range []bool{false, true} {
		for _, scenario := range []string{"caller cancellation", "request deadline", "invalid result"} {
			t.Run(fmt.Sprintf("fenced=%t/%s", fenced, scenario), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls, releases := 0, 0
				driver := acquisitionDriver{Driver: hookedDriver{
					release: func(cleanup context.Context, _, _ string) (bool, error) {
						releases++
						if cleanup.Err() != nil {
							t.Fatal("cleanup reused failed request context", cleanup.Err())
						}
						return false, cleanupFailure
					},
				}, acquire: func(request context.Context, req AcquireRequest) (AcquireResult, error) {
					calls++
					if req.Fencing != fenced {
						t.Fatal("fencing option was not forwarded")
					}
					result := AcquireResult{Acquired: true}
					if fenced {
						result.FencingToken = 7
					}
					if calls > 1 {
						return result, nil
					}
					switch scenario {
					case "caller cancellation":
						cancel()
					case "request deadline":
						<-request.Done()
					case "invalid result":
						if fenced {
							result.FencingToken = 0
						} else {
							result.FencingToken = 7
						}
					}
					return result, nil
				}}
				options := []Option{WithRetryInterval(time.Millisecond)}
				if scenario == "request deadline" {
					options = append(options, WithOperationTimeout(10*time.Millisecond))
				}
				if fenced {
					options = append(options, WithFencing())
				}
				lease, err := testLocker(t, driver, options...).Lock(ctx, "key", time.Hour)
				if lease != nil {
					defer lease.Release(context.Background())
				}
				if lease != nil || calls != 1 || releases != 1 || !errors.Is(err, cleanupFailure) || !errors.Is(err, failure) {
					t.Fatalf("leaseReturned=%t, error=%v, calls=%d, releases=%d; want original cleanup failure", lease != nil, err, calls, releases)
				}
				want := ErrInvalidAcquireResult
				if scenario != "invalid result" {
					want = context.Canceled
					if scenario == "request deadline" {
						want = context.DeadlineExceeded
					}
					if !errors.Is(err, ErrLeaseLost) {
						t.Fatal("late acquisition lost lease-loss identity", err)
					}
				}
				if !errors.Is(err, want) {
					t.Fatalf("Lock = %v; want identity %v", err, want)
				}
			})
		}
	}
}

func TestLockStillRetriesContention(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprintf("fenced=%t", fenced), func(t *testing.T) {
			owners := make(map[string]bool)
			driver := acquisitionDriver{Driver: newMemoryDriver(),
				acquire: func(_ context.Context, req AcquireRequest) (AcquireResult, error) {
					if req.Fencing != fenced || req.Owner == "" || owners[req.Owner] {
						t.Fatal("retry changed fencing or reused owner", req)
					}
					owners[req.Owner] = true
					result := AcquireResult{Acquired: len(owners) == 3}
					if result.Acquired && fenced {
						result.FencingToken = 7
					}
					return result, nil
				},
			}
			options := []Option{WithRetryInterval(time.Millisecond)}
			if fenced {
				options = append(options, WithFencing())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			lease, err := testLocker(t, driver, options...).Lock(ctx, "key", time.Hour)
			if lease != nil {
				defer lease.Release(context.Background())
			}
			if lease == nil || err != nil || len(owners) != 3 {
				t.Fatalf("leaseReturned=%t, error=%v, attempts=%d; want successful third attempt", lease != nil, err, len(owners))
			}
			token, enabled := lease.FencingToken()
			if enabled != fenced || (fenced && token != 7) || (!fenced && token != 0) {
				t.Fatal("successful retry lost fencing result", token, enabled)
			}
		})
	}
}
