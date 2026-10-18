// Package redis implements owner-checked Redis lease operations for package anylock.
//
// Acquiring uses a single SET NX PX command; renewing and releasing use atomic
// owner-checking Lua scripts. Positive TTLs are rounded up to milliseconds.
// Prefixes are prepended verbatim to keys; callers should include any desired
// separator and use a namespace reserved for leases. The lock core generates
// owner tokens and manages lease lifecycle; this driver does neither.
//
// Redis expiry depends on its clock. Clock jumps, pauses, partitions, and lost
// writes during asynchronous replication/failover can invalidate lease safety.
// Fenced acquisition uses Lua and a persistent per-key counter; counter rollback
// breaks fencing. This driver provides neither exactly-once execution nor strong
// consistency. Callers must account for those limits.
// The caller owns the client and its connection resources; the driver never
// closes the client or flushes the database.
//
// Operations check cancellation before calling Redis. They preserve a supplied
// context deadline, or add a one-second deadline when none is supplied. In-flight
// network I/O is bounded by that deadline, not necessarily interrupted immediately
// by cancellation. Custom clients must honor the supplied deadline. Known
// go-redis clients must enable ContextTimeoutEnabled and socket deadline updates.
package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/tr1v3r/anylock"
)

// Client is the minimal Redis command surface used by Driver. Both
// *redis.Client and *redis.ClusterClient implement it. Implementations must be
// safe for concurrent use and honor supplied context deadlines, including during
// network I/O. Cancellation need not immediately interrupt an in-flight command.
type Client interface {
	Do(context.Context, ...any) *goredis.Cmd
	Eval(context.Context, string, []string, ...any) *goredis.Cmd
}

// Driver implements atomic Redis lease operations. Construct it with New;
// its zero value is not usable. Driver is safe for concurrent use when Client is.
type Driver struct {
	client Client
	prefix string
}

var (
	_ anylock.FencingDriver = (*Driver)(nil)
	_ Client                = (*goredis.Client)(nil)
	_ Client                = (*goredis.ClusterClient)(nil)
)

const renewScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`

const releaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
`

// New creates a driver without opening or taking ownership of client resources.
// A nil client, including an interface containing a typed nil, is rejected.
// Known go-redis clients must enable ContextTimeoutEnabled and must not disable
// socket deadline updates (ReadTimeout or WriteTimeout set to -2 before client
// construction). Client options must not be mutated after construction. Custom
// clients must honor supplied context deadlines. An empty prefix is allowed.
func New(client Client, prefix string) (*Driver, error) {
	if isNil(client) {
		return nil, errors.New("anylock/redis: nil client")
	}
	var contextTimeoutEnabled bool
	var readTimeout, writeTimeout time.Duration
	switch concrete := client.(type) {
	case *goredis.Client:
		options := concrete.Options()
		contextTimeoutEnabled = options.ContextTimeoutEnabled
		readTimeout, writeTimeout = options.ReadTimeout, options.WriteTimeout
	case *goredis.ClusterClient:
		options := concrete.Options()
		contextTimeoutEnabled = options.ContextTimeoutEnabled
		readTimeout, writeTimeout = options.ReadTimeout, options.WriteTimeout
	default:
		return &Driver{client: client, prefix: prefix}, nil
	}
	if !contextTimeoutEnabled {
		return nil, errors.New("anylock/redis: go-redis client must enable ContextTimeoutEnabled")
	}
	// go-redis normalizes -2 to -1 for ordinary clients, but cluster options
	// retain -2 until node creation. Both disable socket deadline updates.
	if readTimeout < 0 || writeTimeout < 0 {
		return nil, errors.New("anylock/redis: go-redis client must not disable read or write socket deadlines")
	}
	return &Driver{client: client, prefix: prefix}, nil
}

// TryAcquire atomically creates an absent lease. Acquired false means the key is already
// held, even when owner matches; acquisition is not a renewal operation.
func (d *Driver) TryAcquire(ctx context.Context, key, owner string, ttl time.Duration) (anylock.AcquireResult, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return anylock.AcquireResult{}, err
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
	milliseconds, err := ttlMilliseconds(ttl)
	if err != nil {
		return anylock.AcquireResult{}, err
	}
	cmd := d.client.Do(ctx, "SET", d.prefix+key, owner, "NX", "PX", milliseconds)
	if cmd == nil {
		return anylock.AcquireResult{}, errors.New("anylock/redis: acquire: nil command")
	}
	result, err := cmd.Result()
	if errors.Is(err, goredis.Nil) {
		return anylock.AcquireResult{}, nil
	}
	if err != nil {
		return anylock.AcquireResult{}, fmt.Errorf("anylock/redis: acquire: %w", err)
	}
	if reply, ok := result.(string); !ok || reply != "OK" {
		return anylock.AcquireResult{}, fmt.Errorf("anylock/redis: acquire: unexpected reply %T (%v)", result, result)
	}
	return anylock.AcquireResult{Acquired: true}, nil
}

// Renew atomically extends only an unexpired lease belonging to owner. It never
// creates a lease or revives an expired one. False means ownership was lost.
func (d *Driver) Renew(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
	milliseconds, err := ttlMilliseconds(ttl)
	if err != nil {
		return false, err
	}
	return scriptResult(d.client.Eval(ctx, renewScript, []string{d.prefix + key}, owner, milliseconds), "renew")
}

// Release atomically deletes a stored key only when its owner matches. Redis
// treats expired keys as absent, so false means no matching key remains. A
// successor's key is never removed. The bool reports deletion, not prior validity;
// core Lease.Release treats false as successful cleanup.
func (d *Driver) Release(ctx context.Context, key, owner string) (bool, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return false, err
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
	return scriptResult(d.client.Eval(ctx, releaseScript, []string{d.prefix + key}, owner), "release")
}

func (d *Driver) validate(ctx context.Context, key, owner string) error {
	if d == nil || isNil(d.client) {
		return errors.New("anylock/redis: nil driver or client")
	}
	if ctx == nil {
		return errors.New("anylock/redis: nil context")
	}
	if strings.HasSuffix(key, fenceSuffix) {
		return errors.New("anylock/redis: key uses reserved fencing suffix")
	}
	if key == "" || owner == "" {
		return errors.New("anylock/redis: key and owner must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("anylock/redis: context: %w", err)
	}
	return nil
}

func operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Second)
}

func ttlMilliseconds(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("anylock/redis: TTL must be positive")
	}
	// Divide before rounding: adding time.Millisecond-1 to ttl could overflow.
	milliseconds := int64(ttl / time.Millisecond)
	if ttl%time.Millisecond != 0 {
		milliseconds++
	}
	return milliseconds, nil
}

func scriptResult(cmd *goredis.Cmd, operation string) (bool, error) {
	if cmd == nil {
		return false, fmt.Errorf("anylock/redis: %s: nil command", operation)
	}
	result, err := cmd.Result()
	if err != nil {
		return false, fmt.Errorf("anylock/redis: %s: %w", operation, err)
	}
	if value, ok := result.(int64); ok && (value == 0 || value == 1) {
		return value == 1, nil
	}
	return false, fmt.Errorf("anylock/redis: %s: unexpected reply %T (%v)", operation, result, result)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	default:
		return false
	}
}

// Counter keys are persistent and reserved, including for ordinary acquisitions.
const fenceSuffix = ":__anylock_fence"

// INCR's Lua numeric reply loses precision above 2^53. Fetch the exact decimal
// string with GET instead. INCR exhaustion/errors happen before installing a lease.
const acquireFencedScript = `
if redis.call('EXISTS', KEYS[1]) ~= 0 then return false end
redis.call('INCR', KEYS[2])
local token = redis.call('GET', KEYS[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return token
`

// TryAcquireFenced atomically reserves a key and increments its persistent
// counter. Redis Cluster requires a nonempty hash tag in prefix/key so both keys
// share a slot. Counter loss/rollback (including failover) breaks monotonicity.
func (d *Driver) TryAcquireFenced(
	ctx context.Context, key, owner string, ttl time.Duration,
) (anylock.AcquireResult, error) {
	if err := d.validate(ctx, key, owner); err != nil {
		return anylock.AcquireResult{}, err
	}
	milliseconds, err := ttlMilliseconds(ttl)
	if err != nil {
		return anylock.AcquireResult{}, err
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
	physical := d.prefix + key
	cmd := d.client.Eval(ctx, acquireFencedScript, []string{physical, physical + fenceSuffix}, owner, milliseconds)
	if cmd == nil {
		return anylock.AcquireResult{}, errors.New("anylock/redis: fenced acquire: nil command")
	}
	value, err := cmd.Result()
	if errors.Is(err, goredis.Nil) {
		return anylock.AcquireResult{}, nil
	}
	if err != nil {
		return anylock.AcquireResult{}, fmt.Errorf("anylock/redis: fenced acquire: %w", err)
	}
	text, ok := value.(string)
	if !ok {
		return anylock.AcquireResult{}, errors.New("anylock/redis: fenced acquire: unexpected reply")
	}
	token, err := strconv.ParseUint(text, 10, 63)
	if err != nil || token == 0 {
		return anylock.AcquireResult{}, errors.New("anylock/redis: fenced acquire: invalid token")
	}
	return anylock.AcquireResult{Acquired: true, Fence: token}, nil
}
