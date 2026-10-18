package memory

import (
	"context"
	"time"

	"github.com/tr1v3r/anylock"
)

// tryAcquire keeps owner/lifecycle assertions focused on the acquisition outcome.
// Protocol tests call TryAcquire directly. Ordinary acquisition must never leak a token.
func (d *Driver) tryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	result, err := d.TryAcquire(ctx, anylock.AcquireRequest{Key: key, Owner: owner, TTL: ttl})
	if result.FencingToken != 0 {
		panic("ordinary acquisition returned a fence")
	}
	return result.Acquired, err
}
