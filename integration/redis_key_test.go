//go:build integration

package integration_test

import "encoding/hex"

// Mirrors the documented storage format solely for inspecting/injecting faults
// in an isolated namespace. Lockers continue to use logical keys.
func redisLeaseKey(namespaced string) string {
	return "{anylock:" + hex.EncodeToString([]byte(namespaced)) + "}:lease"
}
