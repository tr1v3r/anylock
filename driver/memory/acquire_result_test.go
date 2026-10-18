package memory

import (
	"context"
	"time"
)

// tryAcquire adapts the legacy bool-based owner/lifecycle test matrix. Fencing
// tests call the public APIs directly. Ordinary acquisition must never leak a token.
func (d *Driver) tryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	result, err := d.TryAcquire(ctx, key, owner, ttl)
	if result.Fence != 0 {
		panic("ordinary acquisition returned a fence")
	}
	return result.Acquired, err
}
