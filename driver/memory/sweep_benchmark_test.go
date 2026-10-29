package memory

import (
	"container/heap"
	"context"
	"strconv"
	"testing"
	"time"
)

// BenchmarkRenewAfterExpiryBurst measures one live-key renewal after a fresh
// batch expires. Fixture allocation/heap construction is excluded. Use a fixed
// iteration count (for example -benchtime=20x) because each fixture is large.
func BenchmarkRenewAfterExpiryBurst(b *testing.B) {
	for _, count := range []int{10_000, 100_000, 500_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for range b.N {
				d := New()
				d.entries = make(map[string]*entry, count+1)
				d.expiry = make(expiryHeap, 0, count+1)
				for i := range count {
					e := &entry{key: strconv.Itoa(i), owner: "expired", index: i}
					d.entries[e.key] = e
					d.expiry = append(d.expiry, e)
				}
				e := &entry{key: "live", owner: "live-owner", expires: d.ticks() + uint64(time.Hour), index: count}
				d.entries[e.key] = e
				d.expiry = append(d.expiry, e)
				heap.Init(&d.expiry)
				b.StartTimer()
				ok, err := d.Renew(context.Background(), e.key, e.owner, time.Hour)
				b.StopTimer()
				if !ok || err != nil {
					b.Fatal("live ownership lost during expiry cleanup", ok, err)
				}
			}
		})
	}
}
