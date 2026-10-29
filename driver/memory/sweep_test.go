package memory

import (
	"container/heap"
	"context"
	"fmt"
	"testing"
	"time"
)

// expiredBacklog creates quiescent, already-expired heap nodes. The target's
// later expiry keeps it outside the first cleanup batch without sleeping.
func expiredBacklog(t *testing.T, count int) *Driver {
	t.Helper()
	d := New()
	for i := range count {
		acquire(t, d, fmt.Sprintf("expired:%d", i), "old", time.Hour, true)
	}
	for _, e := range d.expiry {
		e.expires = 0
	}
	heap.Init(&d.expiry)
	return d
}

func TestSweepBudgetPreservesLiveOperations(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release", "contention"} {
		t.Run(operation, func(t *testing.T) {
			d := New()
			acquire(t, d, "live", "owner", time.Hour, true)
			for i := range 3 * sweepBudget {
				acquire(t, d, fmt.Sprintf("expired:%d", i), "old", time.Hour, true)
			}
			for key, e := range d.entries {
				if key != "live" {
					e.expires = 0
				}
			}
			heap.Init(&d.expiry)
			want := 2*sweepBudget + 1
			switch operation {
			case "acquire":
				acquire(t, d, "new", "owner", time.Hour, true)
				want++
			case "renew":
				renew(t, d, "live", "owner", time.Hour, true)
			case "release":
				release(t, d, "live", "owner", true)
				want--
			case "contention":
				acquire(t, d, "live", "other", time.Hour, false)
			}
			if len(d.entries) != want {
				t.Fatalf("cleanup did not respect budget: entries=%d want=%d", len(d.entries), want)
			}
			checkHeap(t, d)
		})
	}
}

func TestExpiredTargetOutsideSweepBudget(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release", "wrong-owner-release"} {
		t.Run(operation, func(t *testing.T) {
			d := expiredBacklog(t, 4*sweepBudget)
			target := d.entries["expired:0"]
			target.expires = d.ticks()
			heap.Fix(&d.expiry, target.index)
			switch operation {
			case "acquire":
				acquire(t, d, target.key, "successor", time.Hour, true)
				if d.entries[target.key] == target || target.index != -1 {
					t.Fatal("acquisition retained the expired owner's record")
				}
				// Both stale operations run while an expired backlog remains.
				renew(t, d, target.key, "old", time.Hour, false)
				release(t, d, target.key, "old", false)
				if d.entries[target.key].owner != "successor" {
					t.Fatal("stale owner affected the successor")
				}
			case "renew":
				renew(t, d, target.key, "old", time.Hour, false)
				if d.entries[target.key] != target || target.expires > d.ticks() {
					t.Fatal("renew revived or unexpectedly swept the target")
				}
			case "release":
				release(t, d, target.key, "old", true)
				if d.entries[target.key] != nil || target.index != -1 {
					t.Fatal("release did not remove its own expired target")
				}
			case "wrong-owner-release":
				release(t, d, target.key, "other", false)
				if d.entries[target.key] != target {
					t.Fatal("wrong owner removed the target outside the cleanup batch")
				}
			}
			checkHeap(t, d)
		})
	}
}

func TestSweepBudgetEventuallyReclaimsIdleBacklog(t *testing.T) {
	const count = 3*sweepBudget + 1
	d := expiredBacklog(t, count)
	backing := d.expiry[:cap(d.expiry)]
	for remaining := count; remaining > 0; {
		ok, err := d.Release(context.Background(), "unrelated", "nobody")
		requireResult(t, ok, err, false)
		remaining = max(0, remaining-sweepBudget)
		if len(d.entries) != remaining {
			t.Fatalf("remaining=%d want=%d", len(d.entries), remaining)
		}
		checkHeap(t, d)
	}
	if d.entries != nil || d.expiry != nil {
		t.Fatal("final batch retained empty containers")
	}
	for _, e := range backing {
		if e != nil {
			t.Fatal("removed heap slot retained an entry")
		}
	}
}
