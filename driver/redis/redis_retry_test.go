package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/anylock"
)

// Drop the first target command's reply only after reading it from the server.
// This proves the mutation committed before the client sees the injected EOF.
// Everything uses freshly allocated loopback servers, never a configured service.
type replyLoss struct {
	command string
	reply   string // Optional complete pipeline reply required before injecting EOF.
	dropped atomic.Bool
	calls   atomic.Int32
}

type replyLossConn struct {
	net.Conn
	fault    *replyLoss
	armed    atomic.Bool
	received []byte // Used only by the connection's reader.
}

func (c *replyLossConn) Write(p []byte) (int, error) {
	marker := fmt.Sprintf("\r\n$%d\r\n%s\r\n", len(c.fault.command), c.fault.command)
	if bytes.Contains(bytes.ToUpper(p), []byte(marker)) {
		c.fault.calls.Add(1)
		c.armed.Store(true)
	}
	return c.Conn.Write(p)
}

func (c *replyLossConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.armed.Load() {
		c.received = append(c.received, p[:n]...)
		complete := c.fault.reply == "" || bytes.Contains(c.received, []byte(c.fault.reply))
		if complete && c.fault.dropped.CompareAndSwap(false, true) {
			c.armed.Store(false)
			_ = c.Conn.Close()
			return 0, io.EOF
		}
	}
	return n, err
}

func (f *replyLoss) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &replyLossConn{Conn: conn, fault: f}, nil
}

func retryClient(t *testing.T, kind, addr string, fault *replyLoss) Client {
	t.Helper()
	if kind == "single" {
		client := goredis.NewClient(&goredis.Options{
			Addr: addr, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
			Dialer: fault.dial,
		})
		t.Cleanup(func() { _ = client.Close() })
		if client.Options().MaxRetries <= 0 {
			t.Fatal("fixture must retain default retries")
		}
		return client
	}
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: []string{addr}, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
		MaxRetries: -1, // Only the default outer MaxRedirects loop may replay.
		Dialer:     fault.dial, ClusterSlots: singleNodeSlots(addr),
	})
	t.Cleanup(func() { _ = client.Close() })
	if client.Options().MaxRedirects <= 0 {
		t.Fatal("fixture must retain cluster outer retries")
	}
	return client
}

func singleNodeSlots(addr string) func(context.Context) ([]goredis.ClusterSlot, error) {
	return func(context.Context) ([]goredis.ClusterSlot, error) {
		return []goredis.ClusterSlot{{Start: 0, End: 16383, Nodes: []goredis.ClusterNode{{Addr: addr}}}}, nil
	}
}

func TestLostMutationReplyIsNotRetried(t *testing.T) {
	for _, kind := range []string{"single", "cluster"} {
		for _, operation := range []string{"acquire", "fenced acquire", "renew", "release"} {
			t.Run(kind+"/"+operation, func(t *testing.T) {
				backend := miniredis.RunT(t)
				fault := &replyLoss{command: "EVAL"}
				if operation == "acquire" {
					fault.command = "SET"
				}
				client := retryClient(t, kind, backend.Addr(), fault)
				d, err := New(client, "retry:")
				if err != nil {
					t.Fatal(err)
				}
				physical := leaseKey("retry:key")
				if operation == "renew" || operation == "release" {
					if setupErr := backend.Set(physical, "owner"); setupErr != nil {
						t.Fatal(setupErr)
					}
					backend.SetTTL(physical, time.Second)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var claimed bool
				switch operation {
				case "acquire", "fenced acquire":
					var result anylock.AcquireResult
					result, err = d.TryAcquire(ctx, anylock.AcquireRequest{
						Key: "key", Owner: "owner", TTL: time.Minute, Fencing: operation == "fenced acquire",
					})
					claimed = result != (anylock.AcquireResult{})
				case "renew":
					claimed, err = d.Renew(ctx, "key", "owner", time.Minute)
				case "release":
					claimed, err = d.Release(ctx, "key", "owner")
				}
				t.Logf("operation=%s dispatched=%d dropped=%v claimed=%v error=%v",
					operation, fault.calls.Load(), fault.dropped.Load(), claimed, err)
				if claimed || !errors.Is(err, io.EOF) || fault.calls.Load() != 1 || !fault.dropped.Load() {
					t.Errorf("lost reply must remain ambiguous EOF without replay: claimed=%v err=%v calls=%d",
						claimed, err, fault.calls.Load())
				}
				// An error must not be mistaken for rollback or provoke cleanup.
				if operation == "release" {
					if backend.Exists(physical) {
						t.Fatal("release did not commit before injected EOF")
					}
				} else if owner, readErr := backend.Get(physical); readErr != nil || owner != "owner" || backend.TTL(physical) != time.Minute {
					t.Fatal("mutation did not commit with expected owner/TTL", owner, readErr, backend.TTL(physical))
				}
				if operation == "fenced acquire" {
					if counter, readErr := backend.Get(counterKey("retry:key")); readErr != nil || counter != "1" {
						t.Fatal("fenced acquisition must increment exactly once", counter, readErr)
					}
				}
			})
		}
	}
}

func TestNewPreservesSharedClientRetries(t *testing.T) {
	for _, kind := range []string{"single", "cluster"} {
		t.Run(kind, func(t *testing.T) {
			backend := miniredis.RunT(t)
			fault := &replyLoss{command: "SET"}
			client := retryClient(t, kind, backend.Addr(), fault)
			if _, err := New(client, "retry:"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Calling the application-owned client directly retains its policy.
			err := client.Do(ctx, "SET", "application-key", "owner", "NX", "PX", 60000).Err()
			if !errors.Is(err, goredis.Nil) || fault.calls.Load() != 2 || !fault.dropped.Load() {
				t.Fatal("New changed retries of unrelated application commands", err, fault.calls.Load())
			}
		})
	}
}

func TestClusterSafeRedirectsRemainEnabled(t *testing.T) {
	for _, redirect := range []string{"MOVED", "ASK"} {
		for _, fencing := range []bool{false, true} {
			for _, loseReply := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/fencing=%v/lost=%v", redirect, fencing, loseReply), func(t *testing.T) {
					testClusterRedirect(t, redirect, fencing, loseReply)
				})
			}
		}
	}
}

func testClusterRedirect(t *testing.T, redirect string, fencing, loseReply bool) {
	t.Helper()
	source, target := miniredis.RunT(t), miniredis.RunT(t)
	var redirected, asking atomic.Int32
	source.Server().SetPreHook(func(peer *server.Peer, command string, _ ...string) bool {
		if strings.EqualFold(command, "SET") || strings.EqualFold(command, "EVAL") {
			redirected.Add(1)
			peer.WriteError(redirect + " 0 " + target.Addr())
			return true
		}
		return false
	})
	target.Server().SetPreHook(func(peer *server.Peer, command string, _ ...string) bool {
		if strings.EqualFold(command, "ASKING") {
			asking.Add(1)
			peer.WriteInline("OK")
			return true
		}
		return false
	})
	fault := &replyLoss{command: "SET", reply: "+OK\r\n"}
	if fencing {
		fault.command, fault.reply = "EVAL", "$1\r\n1\r\n"
	}
	if redirect == "ASK" {
		// Do not drop ASKING's reply: wait until the mutation's reply is read.
		fault.reply = "+OK\r\n" + fault.reply
	}
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: []string{source.Addr()}, ContextTimeoutEnabled: true, Protocol: 2,
		DisableIdentity: true, MaxRetries: -1, ClusterSlots: singleNodeSlots(source.Addr()),
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if loseReply && addr == target.Addr() {
				return fault.dial(ctx, network, addr)
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	d, err := New(client, "redirect:")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := d.TryAcquire(ctx, anylock.AcquireRequest{
		Key: "key", Owner: "owner", TTL: time.Minute, Fencing: fencing,
	})
	if loseReply {
		if result != (anylock.AcquireResult{}) || !errors.Is(err, io.EOF) || !fault.dropped.Load() || fault.calls.Load() != 1 {
			t.Errorf("redirected mutation replayed or lost ambiguity: result=%+v error=%v dropped=%v calls=%d",
				result, err, fault.dropped.Load(), fault.calls.Load())
		}
	} else {
		if err != nil || !result.Acquired {
			t.Fatal("safe redirect was blocked", result, err)
		}
		if fencing && result.FencingToken != 1 || !fencing && result.FencingToken != 0 {
			t.Fatal("redirect changed fencing result", result)
		}
	}
	if redirected.Load() != 1 {
		t.Error("unexpected source replay", redirected.Load())
	}
	if redirect == "ASK" && asking.Load() != 1 {
		t.Error("unexpected ASKING count", asking.Load())
	}
	physical := leaseKey("redirect:key")
	if source.Exists(physical) {
		t.Fatal("redirect source mutated ownership")
	}
	if owner, readErr := target.Get(physical); readErr != nil || owner != "owner" {
		t.Fatal("redirect destination did not acquire", owner, readErr)
	}
	if fencing {
		if counter, readErr := target.Get(counterKey("redirect:key")); readErr != nil || counter != "1" {
			t.Fatal("redirected fenced mutation incremented counter incorrectly", counter, readErr)
		}
	}
}
