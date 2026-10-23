package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

var testContext = context.Background()

type stubClient struct {
	process func(context.Context, goredis.Cmder) error
}

func (c *stubClient) Process(ctx context.Context, cmd goredis.Cmder) error {
	return c.process(ctx, cmd)
}

// A nil map implementing Client exercises nil kinds other than pointers.
type mapClient map[string]string

func (mapClient) Process(context.Context, goredis.Cmder) error { panic("nil client called") }

// replyProcess stores a prepared outcome on the driver-built command, mirroring
// how real clients record results. The concrete wrapper is required because
// Cmder does not expose SetVal.
func replyProcess(value any, err error) func(context.Context, goredis.Cmder) error {
	return func(_ context.Context, cmd goredis.Cmder) error {
		cmd.(noRetryCmd).SetVal(value)
		cmd.SetErr(err)
		return err
	}
}

func newDriver(t *testing.T, addr, prefix string) (*Driver, *goredis.Client) {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{
		Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	driver, err := New(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return driver, client
}

func requireResult(t *testing.T, want bool, got bool, err error) {
	t.Helper()
	if err != nil || got != want {
		t.Fatalf("result = %v, %v; want %v, nil", got, err, want)
	}
}

func TestNewRejectsNilClients(t *testing.T) {
	var plain *goredis.Client
	var cluster *goredis.ClusterClient
	var custom *stubClient
	var mapped mapClient
	for _, client := range []Client{nil, plain, cluster, custom, mapped} {
		if driver, err := New(client, ""); driver != nil || err == nil {
			t.Fatalf("New(%T) = %v, %v; want nil, error", client, driver, err)
		}
	}
	if driver, err := New(&stubClient{}, ""); driver == nil || err != nil {
		t.Fatalf("New(non-nil, empty prefix) = %v, %v", driver, err)
	}
}

func TestNewRequiresDeadlineAwareGoRedisClients(t *testing.T) {
	for _, kind := range []string{"single", "cluster"} {
		for _, tc := range []struct {
			name                  string
			contextTimeoutEnabled bool
			read, write           time.Duration
			wantError             string
		}{
			{name: "default disabled", wantError: "ContextTimeoutEnabled"},
			{name: "enabled", contextTimeoutEnabled: true},
			{name: "disable read deadline", contextTimeoutEnabled: true, read: -2, write: time.Second, wantError: "socket deadlines"},
			{name: "disable write deadline", contextTimeoutEnabled: true, write: -2, wantError: "socket deadlines"},
			{name: "inherit disabled write deadline", contextTimeoutEnabled: true, read: -2, wantError: "socket deadlines"},
			{name: "context-only socket timeout", contextTimeoutEnabled: true, read: -1, write: -1},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				var client Client
				if kind == "single" {
					concrete := goredis.NewClient(&goredis.Options{
						ContextTimeoutEnabled: tc.contextTimeoutEnabled,
						ReadTimeout:           tc.read, WriteTimeout: tc.write,
					})
					t.Cleanup(func() { _ = concrete.Close() })
					client = concrete
				} else {
					concrete := goredis.NewClusterClient(&goredis.ClusterOptions{
						ContextTimeoutEnabled: tc.contextTimeoutEnabled,
						ReadTimeout:           tc.read, WriteTimeout: tc.write,
					})
					t.Cleanup(func() { _ = concrete.Close() })
					client = concrete
				}
				driver, err := New(client, "")
				if tc.wantError == "" {
					if driver == nil || err != nil {
						t.Fatalf("New deadline-aware client = %v, %v", driver, err)
					}
				} else if driver != nil || err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("New unsafe client = %v, %v; want nil, error containing %q", driver, err, tc.wantError)
				}
			})
		}
	}
}

func TestOperationsAddDeadlineWhenAbsent(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(testContext, contextKey{}, "preserved")
	var observed context.Context
	checkContext := func(got context.Context) {
		t.Helper()
		observed = got
		deadline, ok := got.Deadline()
		remaining := time.Until(deadline)
		if !ok || remaining <= 0 || remaining > time.Second {
			t.Errorf("fallback context deadline = %v, %v; remaining %v", deadline, ok, remaining)
		}
		if got.Value(contextKey{}) != "preserved" {
			t.Error("fallback context lost caller value")
		}
	}
	driver, err := New(&stubClient{
		process: func(got context.Context, cmd goredis.Cmder) error {
			checkContext(got)
			value := any(int64(1))
			if cmd.Name() == "set" {
				value = "OK"
			}
			cmd.(noRetryCmd).SetVal(value)
			return nil
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() (bool, error){
		func() (bool, error) { return driver.tryAcquire(ctx, "key", "owner", time.Second) },
		func() (bool, error) { return driver.Renew(ctx, "key", "owner", time.Second) },
		func() (bool, error) { return driver.Release(ctx, "key", "owner") },
	} {
		observed = nil
		ok, callErr := call()
		requireResult(t, true, ok, callErr)
		if observed == nil || !errors.Is(observed.Err(), context.Canceled) {
			t.Fatalf("operation failed to cancel derived deadline context: %v", observed)
		}
	}
	if ctx.Err() != nil {
		t.Fatal("operation canceled caller context")
	}
}

func TestSameKeyContentionMultipleClients(t *testing.T) {
	server := miniredis.RunT(t)
	const clients = 16
	drivers := make([]*Driver, clients)
	for i := range drivers {
		drivers[i], _ = newDriver(t, server.Addr(), "lease:")
	}
	start := make(chan struct{})
	results := make(chan int, clients)
	errorsFound := make(chan error, clients)
	var workers sync.WaitGroup
	for i, driver := range drivers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			ok, err := driver.tryAcquire(testContext, "shared", fmt.Sprintf("owner-%d", i), time.Minute)
			if err != nil {
				errorsFound <- err
			} else if ok {
				results <- i
			}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	winners := 0
	var winner int
	for winner = range results {
		winners++
	}
	if winners != 1 {
		t.Fatalf("winners = %d; want 1", winners)
	}
	owner := fmt.Sprintf("owner-%d", winner)
	ok, err := drivers[winner].tryAcquire(testContext, "shared", owner, time.Hour)
	requireResult(t, false, ok, err)
	if ttl := server.TTL(leaseKey("lease:shared")); ttl != time.Minute {
		t.Fatalf("contending acquisition changed TTL to %v", ttl)
	}
	ok, err = drivers[winner].Release(testContext, "shared", owner)
	requireResult(t, true, ok, err)
	ok, err = drivers[(winner+1)%clients].tryAcquire(testContext, "shared", "successor", time.Minute)
	requireResult(t, true, ok, err)
}

func TestExpirationAndSuccessorRejectStaleOwner(t *testing.T) {
	server := miniredis.RunT(t)
	first, _ := newDriver(t, server.Addr(), "lease:")
	second, client := newDriver(t, server.Addr(), "lease:")
	ok, err := first.tryAcquire(testContext, "key", "old", time.Second)
	requireResult(t, true, ok, err)
	server.FastForward(time.Second)
	ok, err = first.Renew(testContext, "key", "old", time.Hour)
	requireResult(t, false, ok, err)
	ok, err = first.Release(testContext, "key", "old")
	requireResult(t, false, ok, err)
	if server.Exists(leaseKey("lease:key")) {
		t.Fatal("expired lease was revived")
	}
	ok, err = second.tryAcquire(testContext, "key", "new", time.Minute)
	requireResult(t, true, ok, err)
	ok, err = first.Renew(testContext, "key", "old", time.Hour)
	requireResult(t, false, ok, err)
	ok, err = first.Release(testContext, "key", "old")
	requireResult(t, false, ok, err)
	if value, err := client.Get(testContext, leaseKey("lease:key")).Result(); value != "new" || err != nil {
		t.Fatalf("successor = %q, %v", value, err)
	}
	if ttl := server.TTL(leaseKey("lease:key")); ttl != time.Minute {
		t.Fatalf("stale owner changed successor TTL to %v", ttl)
	}
}

func TestRenewalAndNoRevival(t *testing.T) {
	server := miniredis.RunT(t)
	driver, _ := newDriver(t, server.Addr(), "")
	ok, err := driver.Renew(testContext, "missing", "owner", time.Second)
	requireResult(t, false, ok, err)
	ok, err = driver.Release(testContext, "missing", "owner")
	requireResult(t, false, ok, err)
	ok, err = driver.tryAcquire(testContext, "key", "owner", time.Second)
	requireResult(t, true, ok, err)
	server.FastForward(500 * time.Millisecond)
	ok, err = driver.Renew(testContext, "key", "wrong", time.Hour)
	requireResult(t, false, ok, err)
	if ttl := server.TTL(leaseKey("key")); ttl != 500*time.Millisecond {
		t.Fatalf("wrong owner changed TTL to %v", ttl)
	}
	ok, err = driver.Renew(testContext, "key", "owner", 2*time.Second)
	requireResult(t, true, ok, err)
	if ttl := server.TTL(leaseKey("key")); ttl != 2*time.Second {
		t.Fatalf("renewed TTL = %v", ttl)
	}
	server.FastForward(time.Second)
	if !server.Exists(leaseKey("key")) {
		t.Fatal("renewal did not extend lease")
	}
	server.FastForward(time.Second)
	ok, err = driver.Renew(testContext, "key", "owner", time.Second)
	requireResult(t, false, ok, err)
	if server.Exists(leaseKey("key")) {
		t.Fatal("renewal revived expired lease")
	}
	ok, err = driver.tryAcquire(testContext, "key", "owner-2", time.Second)
	requireResult(t, true, ok, err)
	ok, err = driver.Release(testContext, "key", "owner-2")
	requireResult(t, true, ok, err)
	ok, err = driver.Release(testContext, "key", "owner-2")
	requireResult(t, false, ok, err)
	ok, err = driver.Renew(testContext, "key", "owner-2", time.Second)
	requireResult(t, false, ok, err)
}

func TestTTLUpRounding(t *testing.T) {
	for _, ttl := range []time.Duration{time.Nanosecond, time.Millisecond - 1, time.Millisecond, time.Millisecond + 1} {
		t.Run(ttl.String(), func(t *testing.T) {
			server := miniredis.RunT(t)
			driver, _ := newDriver(t, server.Addr(), "ttl:")
			want := time.Duration((int64(ttl)-1)/int64(time.Millisecond)+1) * time.Millisecond
			ok, err := driver.tryAcquire(testContext, "key", "owner", ttl)
			requireResult(t, true, ok, err)
			if got := server.TTL(leaseKey("ttl:key")); got != want {
				t.Fatalf("acquire TTL = %v; want %v", got, want)
			}
			ok, err = driver.Renew(testContext, "key", "owner", ttl)
			requireResult(t, true, ok, err)
			if got := server.TTL(leaseKey("ttl:key")); got != want {
				t.Fatalf("renew TTL = %v; want %v", got, want)
			}
			server.FastForward(want - time.Nanosecond)
			if !server.Exists(leaseKey("ttl:key")) {
				t.Fatal("TTL rounded down")
			}
			server.FastForward(time.Nanosecond)
			if server.Exists(leaseKey("ttl:key")) {
				t.Fatal("lease did not expire")
			}
		})
	}
}

func TestCommandsAndMaxDurationDoNotOverflow(t *testing.T) {
	const maxMilliseconds int64 = 9223372036855
	type contextKey struct{}
	valueContext := context.WithValue(testContext, contextKey{}, "propagated")
	ctx, cancel := context.WithTimeout(valueContext, 5*time.Second)
	defer cancel()
	client := &stubClient{
		process: func(gotContext context.Context, cmd goredis.Cmder) error {
			if gotContext != ctx {
				t.Errorf("%s lost the caller context", cmd.Name())
			}
			args := cmd.Args()
			var want []any
			switch name := cmd.Name(); name {
			case "set":
				want = []any{"SET", leaseKey("prefix:key"), "owner", "NX", "PX", maxMilliseconds}
			case evalCommand:
				switch script := args[1]; script {
				case renewScript:
					want = []any{evalCommand, renewScript, 1, leaseKey("prefix:key"), "owner", maxMilliseconds}
				case releaseScript:
					want = []any{evalCommand, releaseScript, 1, leaseKey("prefix:key"), "owner"}
				default:
					t.Errorf("unexpected script %q", script)
				}
			default:
				t.Errorf("unexpected command %q", name)
			}
			if want != nil && !reflect.DeepEqual(args, want) {
				t.Errorf("%s args = %#v; want %#v", cmd.Name(), args, want)
			}
			if cmd.Name() == "set" {
				cmd.(noRetryCmd).SetVal("OK")
			} else {
				cmd.(noRetryCmd).SetVal(int64(1))
			}
			return nil
		},
	}
	driver, err := New(client, "prefix:")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := driver.tryAcquire(ctx, "key", "owner", time.Duration(math.MaxInt64))
	requireResult(t, true, ok, err)
	ok, err = driver.Renew(ctx, "key", "owner", time.Duration(math.MaxInt64))
	requireResult(t, true, ok, err)
	ok, err = driver.Release(ctx, "key", "owner")
	requireResult(t, true, ok, err)
}

func TestInputValidation(t *testing.T) {
	// Any backend invocation fails the test via nil function panic.
	driver, err := New(&stubClient{}, "prefix:")
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []struct {
		name string
		call func(context.Context, string, string, time.Duration) (bool, error)
	}{
		{"acquire", driver.tryAcquire},
		{"renew", driver.Renew},
		{"release", func(ctx context.Context, key, owner string, _ time.Duration) (bool, error) {
			return driver.Release(ctx, key, owner)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, pair := range [][2]string{{"", "owner"}, {"key", ""}, {"", ""}} {
				if ok, err := operation.call(testContext, pair[0], pair[1], time.Second); ok || err == nil {
					t.Fatalf("invalid key/owner = %v, %v", ok, err)
				}
			}
			if ok, err := operation.call(nil, "key", "owner", time.Second); ok || err == nil {
				t.Fatalf("nil context = %v, %v", ok, err)
			}
			ctx, cancel := context.WithCancel(testContext)
			cancel()
			if ok, err := operation.call(ctx, "key", "owner", time.Second); ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled context = %v, %v", ok, err)
			}
			ctx, cancel = context.WithDeadline(testContext, time.Now().Add(-time.Second))
			defer cancel()
			if ok, err := operation.call(ctx, "key", "owner", time.Second); ok || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expired context = %v, %v", ok, err)
			}
		})
	}
	for _, operation := range []func(context.Context, string, string, time.Duration) (bool, error){driver.tryAcquire, driver.Renew} {
		for _, ttl := range []time.Duration{0, -1, time.Duration(math.MinInt64)} {
			if ok, err := operation(testContext, "key", "owner", ttl); ok || err == nil {
				t.Fatalf("TTL %v = %v, %v", ttl, ok, err)
			}
		}
	}
	for _, driver := range []*Driver{nil, {}, {client: (*stubClient)(nil)}} {
		ok, err := driver.tryAcquire(testContext, "key", "owner", time.Second)
		if ok || err == nil {
			t.Fatalf("nil/zero driver acquire = %v, %v", ok, err)
		}
		ok, err = driver.Renew(testContext, "key", "owner", time.Second)
		if ok || err == nil {
			t.Fatalf("nil/zero driver renew = %v, %v", ok, err)
		}
		ok, err = driver.Release(testContext, "key", "owner")
		if ok || err == nil {
			t.Fatalf("nil/zero driver release = %v, %v", ok, err)
		}
	}
}

func TestMalformedRepliesAndBackendErrors(t *testing.T) {
	backendErr := errors.New("backend failed")
	for _, tc := range []struct {
		name  string
		value any
		err   error
	}{
		{"nil value", nil, nil},
		{"empty string", "", nil},
		{"lowercase ok", "ok", nil},
		{"bytes", []byte("OK"), nil},
		{"integer two", int64(2), nil},
		{"negative integer", int64(-1), nil},
		{"native int", int(1), nil},
		{"float", float64(1), nil},
		{"boolean", true, nil},
		{"array", []any{int64(1)}, nil},
		{"backend error", nil, backendErr},
		{"redis nil", nil, goredis.Nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver, err := New(&stubClient{process: replyProcess(tc.value, tc.err)}, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []struct {
				name string
				call func() (bool, error)
			}{
				{"acquire", func() (bool, error) { return driver.tryAcquire(testContext, "key", "owner", time.Second) }},
				{"renew", func() (bool, error) { return driver.Renew(testContext, "key", "owner", time.Second) }},
				{"release", func() (bool, error) { return driver.Release(testContext, "key", "owner") }},
			} {
				ok, err := operation.call()
				if tc.err == goredis.Nil && operation.name == "acquire" {
					requireResult(t, false, ok, err)
					continue
				}
				if ok || err == nil {
					t.Errorf("%s malformed reply = %v, %v", operation.name, ok, err)
				}
				if tc.err != nil && !errors.Is(err, tc.err) {
					t.Errorf("%s error %v does not wrap %v", operation.name, err, tc.err)
				}
			}
		})
	}
}

func TestIndependentKeysAndNamespaces(t *testing.T) {
	server := miniredis.RunT(t)
	first, client := newDriver(t, server.Addr(), "first:")
	second, _ := newDriver(t, server.Addr(), "second:")
	for _, pair := range []struct {
		driver *Driver
		key    string
	}{{first, "a"}, {first, "b"}, {second, "a"}} {
		ok, err := pair.driver.tryAcquire(testContext, pair.key, "owner", time.Minute)
		requireResult(t, true, ok, err)
	}
	ok, err := first.Release(testContext, "a", "owner")
	requireResult(t, true, ok, err)
	for _, key := range []string{leaseKey("first:b"), leaseKey("second:a")} {
		if value, err := client.Get(testContext, key).Result(); value != "owner" || err != nil {
			t.Errorf("independent key %q = %q, %v", key, value, err)
		}
	}
	// Release must not close the caller's client or remove unrelated keys.
	if err := client.Ping(testContext).Err(); err != nil {
		t.Fatalf("caller client is unusable: %v", err)
	}
}

func TestRedisWrongTypeErrors(t *testing.T) {
	server := miniredis.RunT(t)
	driver, client := newDriver(t, server.Addr(), "")
	if err := client.LPush(testContext, leaseKey("key"), "owner").Err(); err != nil {
		t.Fatal(err)
	}
	ok, err := driver.tryAcquire(testContext, "key", "owner", time.Second)
	requireResult(t, false, ok, err)
	if ok, err := driver.Renew(testContext, "key", "owner", time.Second); ok || err == nil {
		t.Fatalf("renew wrong type = %v, %v", ok, err)
	}
	if ok, err := driver.Release(testContext, "key", "owner"); ok || err == nil {
		t.Fatalf("release wrong type = %v, %v", ok, err)
	}
}
