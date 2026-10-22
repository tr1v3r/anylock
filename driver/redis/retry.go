package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// noRetryAdapter preserves the public Client surface while marking only this
// driver's commands. Changing shared client options would affect other users and
// disabling Cluster MaxRedirects would also break safe MOVED/ASK routing.
type noRetryAdapter struct {
	process func(context.Context, goredis.Cmder) error
}

type noRetryCmd struct{ *goredis.Cmd }

func (noRetryCmd) NoRetry() bool { return true }

var (
	_ Client        = noRetryAdapter{}
	_ goredis.Cmder = noRetryCmd{}
)

func (c noRetryAdapter) Do(ctx context.Context, args ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx, args...)
	// Both known clients' public Process methods store their returned error on
	// cmd. Keep that same command for the driver to inspect, including hook errors.
	_ = c.process(ctx, noRetryCmd{cmd})
	return cmd
}

func (c noRetryAdapter) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	command := make([]any, 3+len(keys), 3+len(keys)+len(args))
	command[0], command[1], command[2] = "eval", script, len(keys)
	for i, key := range keys {
		command[3+i] = key
	}
	command = append(command, args...)
	cmd := goredis.NewCmd(ctx, command...)
	if len(keys) > 0 {
		// Match go-redis's Eval helper so Cluster routes on KEYS[1], not script/argc.
		cmd.SetFirstKeyPos(3)
	}
	_ = c.process(ctx, noRetryCmd{cmd})
	return cmd
}
