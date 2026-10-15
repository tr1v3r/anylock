//go:build go1.25

package memory

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func TestSynctestExactNanosecondExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := New()
		acquire(t, d, "k", "a", time.Nanosecond, true)
		acquire(t, d, "k", "a", time.Nanosecond, false)
		if d.entries["k"].expires != 1 {
			t.Fatal("nanosecond TTL was rounded", d.entries["k"].expires)
		}
		time.Sleep(time.Nanosecond)
		if d.ticks() != d.entries["k"].expires {
			t.Fatal("test did not reach exact equality")
		}
		renew(t, d, "k", "a", time.Hour, false)
		release(t, d, "k", "a", false)
		acquire(t, d, "k", "b", time.Hour, true)
		renew(t, d, "k", "a", time.Hour, false)
		release(t, d, "k", "a", false)
		release(t, d, "k", "b", true)
	})
}

func TestSynctestExactTTLArithmetic(t *testing.T) {
	for _, ttl := range []time.Duration{time.Nanosecond, 1234567, time.Duration(math.MaxInt64)} {
		t.Run(ttl.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := New()
				time.Sleep(time.Nanosecond)
				now := d.ticks()
				acquire(t, d, "k", "a", ttl, true)
				if got := d.entries["k"].expires; got != now+uint64(ttl) {
					t.Fatal("acquisition TTL rounded or overflowed", got)
				}
				renew(t, d, "k", "a", ttl, true)
				if got := d.entries["k"].expires; got != now+uint64(ttl) {
					t.Fatal("renewal TTL rounded or overflowed", got)
				}
			})
		})
	}
}

func TestSynctestPostWaitExpiry(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := New()
				acquire(t, d, "k", "a", time.Second, true)
				d.gate <- struct{}{}
				result := make(chan bool, 1)
				go func() {
					var got bool
					var err error
					switch operation {
					case "acquire":
						got, err = d.TryAcquire(context.Background(), "k", "b", time.Hour)
					case "renew":
						got, err = d.Renew(context.Background(), "k", "a", time.Hour)
					case "release":
						got, err = d.Release(context.Background(), "k", "a")
					}
					if err != nil {
						t.Error(err)
					}
					result <- got
				}()
				synctest.Wait()
				time.Sleep(time.Second)
				d.leave()
				if got := <-result; got != (operation == "acquire") {
					t.Fatal("operation used stale pre-wait expiry", operation, got)
				}
				if operation == "acquire" {
					release(t, d, "k", "b", true)
				} else {
					acquire(t, d, "k", "b", time.Hour, true)
				}
			})
		})
	}
}

func TestSynctestPostWaitDeadlineDoesNotMutate(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := New()
				acquire(t, d, "k", "a", time.Hour, true)
				original := d.entries["k"].expires
				d.gate <- struct{}{}
				ctx := deadlineContext{time.Now().Add(time.Second)}
				result := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "acquire":
						_, err = d.TryAcquire(ctx, "new", "b", time.Hour)
					case "renew":
						_, err = d.Renew(ctx, "k", "a", 2*time.Hour)
					case "release":
						_, err = d.Release(ctx, "k", "a")
					}
					result <- err
				}()
				synctest.Wait()
				time.Sleep(time.Second)
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("deadline wait failed", err)
				}
				if len(d.gate) != 1 || d.entries["new"] != nil || d.entries["k"].expires != original {
					t.Fatal("deadline wait mutated ownership or released someone else's gate")
				}
				d.leave()
				// Ready gate plus expired deadline must still reject before mutation.
				if err := d.enter(ctx); !errors.Is(err, context.DeadlineExceeded) || len(d.gate) != 0 {
					t.Fatal("post-wait actual deadline was not rechecked", err)
				}
				release(t, d, "k", "a", true)
			})
		})
	}
}

func TestSynctestIdleStorageSweptByUnrelatedOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := New()
		for _, key := range []string{"a", "b", "c"} {
			acquire(t, d, key, "o", time.Second, true)
		}
		time.Sleep(time.Hour)
		if len(d.entries) != 3 || len(d.expiry) != 3 {
			t.Fatal("idle driver unexpectedly performed maintenance")
		}
		release(t, d, "unrelated", "o", false)
		if d.entries != nil || d.expiry != nil {
			t.Fatal("unrelated operation did not reclaim idle expired storage")
		}
	})
}
