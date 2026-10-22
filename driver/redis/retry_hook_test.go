package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/anylock"
)

type retryPolicyHook func(context.Context, goredis.Cmder) error

func (retryPolicyHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }
func (h retryPolicyHook) ProcessHook(goredis.ProcessHook) goredis.ProcessHook {
	return goredis.ProcessHook(h)
}
func (retryPolicyHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func TestDriverPreservesProcessHooksAndErrors(t *testing.T) {
	for _, kind := range []string{"single", "cluster"} {
		for _, fencing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fencing=%v", kind, fencing), func(t *testing.T) {
				var client interface {
					Client
					AddHook(goredis.Hook)
					Close() error
				}
				if kind == "single" {
					client = goredis.NewClient(&goredis.Options{ContextTimeoutEnabled: true})
				} else {
					client = goredis.NewClusterClient(&goredis.ClusterOptions{ContextTimeoutEnabled: true})
				}
				t.Cleanup(func() { _ = client.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				hookErr := errors.New("hook rejected before network dispatch")
				calls := 0
				client.AddHook(retryPolicyHook(func(got context.Context, cmd goredis.Cmder) error {
					calls++
					if got != ctx || !cmd.NoRetry() {
						t.Error("hook lost request context or no-replay policy", got == ctx, cmd.NoRetry())
					}
					want := "set"
					if fencing {
						want = "eval"
					}
					if cmd.Name() != want {
						t.Error("hook received unexpected command", cmd.Name())
					}
					return hookErr // Do not call next: no socket or Redis server is needed.
				}))
				d, err := New(client, "hooks:")
				if err != nil {
					t.Fatal(err)
				}
				result, err := d.TryAcquire(ctx, anylock.AcquireRequest{
					Key: "key", Owner: "owner", TTL: time.Minute, Fencing: fencing,
				})
				if calls != 1 || result != (anylock.AcquireResult{}) || !errors.Is(err, hookErr) {
					t.Fatal("Process hook or returned error was lost", calls, result, err)
				}
			})
		}
	}
}
