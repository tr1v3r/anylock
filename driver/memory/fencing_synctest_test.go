//go:build go1.25

package memory_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tr1v3r/anylock"
	"github.com/tr1v3r/anylock/driver/memory"
)

func TestSynctestFencingExpiryAndAutoRenew(t *testing.T) {
	for _, automatic := range []bool{false, true} {
		t.Run(map[bool]string{false: "expiry", true: "auto-renew"}[automatic], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := []anylock.Option{anylock.WithFencing()}
				if automatic {
					options = append(options, anylock.WithAutoRenew())
				}
				locker := newLocker(t, memory.New(), options...)
				first, err := locker.TryLock(context.Background(), "key", 9*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer first.Release(context.Background())
				token, enabled := first.FencingToken()
				if !enabled || token == 0 {
					t.Fatal(token, enabled)
				}
				time.Sleep(30 * time.Second)
				synctest.Wait()
				if next, _ := first.FencingToken(); next != token {
					t.Fatal("token changed", next, token)
				}
				if automatic {
					if first.Err() != nil {
						t.Fatal(first.Err())
					}
					if _, operationErr := locker.TryLock(context.Background(), "key", time.Hour); !errors.Is(operationErr, anylock.ErrNotAcquired) {
						t.Fatal(operationErr)
					}
					if operationErr := first.Release(context.Background()); operationErr != nil {
						t.Fatal(operationErr)
					}
				} else if !errors.Is(first.Err(), anylock.ErrLeaseLost) {
					t.Fatal(first.Err())
				}
				second, err := locker.TryLock(context.Background(), "key", time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				defer second.Release(context.Background())
				if next, _ := second.FencingToken(); next <= token {
					t.Fatal(next, token)
				}
				if operationErr := first.Release(context.Background()); operationErr != nil {
					t.Fatal(operationErr)
				}
				if second.Err() != nil {
					t.Fatal("stale release damaged successor", second.Err())
				}
			})
		})
	}
}
