// Package etcdproposal contains executable design counterexamples, not a driver.
//
// This intentionally small model separates native lease expiry from committed KV
// revocation. It models only the transitions needed for the counterexamples; it
// is not an etcd emulator, transport, clock implementation, or safety proof.
package etcdproposal

import "testing"

type leaseID int

type nativeLease struct {
	ttl     int64
	expires int64
}

type record struct {
	owner string
	lease leaseID
	rev   int64
}

type model struct {
	now       int64
	revision  int64
	nextLease leaseID
	leases    map[leaseID]nativeLease
	keys      map[string]record
}

func newModel() *model {
	return &model{
		leases: make(map[leaseID]nativeLease),
		keys:   make(map[string]record),
	}
}

func (m *model) grant(ttl int64) leaseID {
	if ttl <= 0 {
		panic("model lease TTL must be positive")
	}
	m.nextLease++
	m.leases[m.nextLease] = nativeLease{ttl: ttl, expires: m.now + ttl}
	return m.nextLease
}

func (m *model) live(id leaseID) bool {
	l, exists := m.leases[id]
	return exists && m.now < l.expires
}

// advance expires leases without automatically applying a revocation. This is
// the essential distinction under investigation, not a model of real-time lag.
func (m *model) advance(ticks int64) {
	if ticks < 0 {
		panic("model clock cannot move backward")
	}
	m.now += ticks
}

// put models the relevant etcd checkPut behavior: the attached lease must still
// exist, but its native expiry is not checked by this KV operation.
func (m *model) put(key, owner string, id leaseID) bool {
	if _, exists := m.leases[id]; !exists {
		return false
	}
	m.revision++
	m.keys[key] = record{owner: owner, lease: id, rev: m.revision}
	return true
}

func (m *model) acquire(key, owner string, id leaseID) bool {
	if _, exists := m.keys[key]; exists {
		return false
	}
	return m.put(key, owner, id)
}

// sameSnapshot is deliberately stronger than an owner-value-only comparison.
// It represents the conjunction of owner, modification revision, and lease ID.
func (m *model) sameSnapshot(key string, expected record) bool {
	current, exists := m.keys[key]
	return exists && current == expected
}

func (m *model) casRotate(key string, expected record, replacement leaseID) bool {
	if !m.sameSnapshot(key, expected) {
		return false
	}
	return m.put(key, expected.owner, replacement)
}

func (m *model) casDelete(key string, expected record) bool {
	if !m.sameSnapshot(key, expected) {
		return false
	}
	m.revision++
	delete(m.keys, key)
	return true
}

// revoke is the separate, committed cleanup transition. Scanning the current
// attachments is equivalent for these examples to detaching on every key update:
// keys moved onto a different lease are not deleted by an old lease's revoke.
func (m *model) revoke(id leaseID) {
	for key, current := range m.keys {
		if current.lease == id {
			m.revision++
			delete(m.keys, key)
		}
	}
	delete(m.leases, id)
}

// keepAlive represents a successful, uninterrupted native refresh with the
// original granted TTL. A real already-expired request can wait for revocation;
// this model returns false immediately because that wait is irrelevant here.
// The model does NOT claim this refresh reproduces all server-side wait races.
func (m *model) keepAlive(id leaseID) bool {
	if !m.live(id) {
		return false
	}
	l := m.leases[id]
	l.expires = m.now + l.ttl
	m.leases[id] = l
	return true
}

func mustPut(t *testing.T, m *model, key, owner string, id leaseID) record {
	t.Helper()
	if !m.put(key, owner, id) {
		t.Fatal("test setup: lease missing")
	}
	return m.keys[key]
}

func TestModelExpiredVisibleOwnerCASCanReviveOwnership(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	snapshot := mustPut(t, m, "lock", "owner-a", old)
	m.advance(2)
	if m.live(old) {
		t.Fatal("test setup: old lease must be expired")
	}
	if !m.sameSnapshot("lock", snapshot) {
		t.Fatal("expired native lease must not imply committed key deletion")
	}

	fresh := m.grant(10)
	if !m.casRotate("lock", snapshot, fresh) {
		t.Fatal("counterexample missing: all stored-field comparisons still pass")
	}
	if got := m.keys["lock"]; got.owner != "owner-a" || got.lease != fresh || !m.live(got.lease) {
		t.Fatalf("expected the forbidden revival of expired ownership, got %+v", got)
	}
	// A green test demonstrates the mismatch; this is not compliant renewal.
}

func TestModelExpiredVisibleOwnerCASCanDelete(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	snapshot := mustPut(t, m, "lock", "owner-a", old)
	m.advance(3)
	if m.live(old) {
		t.Fatal("test setup: lease must be expired")
	}
	if !m.casDelete("lock", snapshot) {
		t.Fatal("counterexample missing: CAS release accepts expired ownership")
	}
	if _, exists := m.keys["lock"]; exists {
		t.Fatal("expected key deletion")
	}
}

func TestModelExactCASProtectsSuccessor(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	stale := mustPut(t, m, "lock", "owner-a", old)
	m.advance(2)
	m.revoke(old)
	successor := m.grant(20)
	want := mustPut(t, m, "lock", "owner-b", successor)
	unused := m.grant(7)

	if m.casRotate("lock", stale, unused) {
		t.Fatal("stale owner must not replace successor")
	}
	if m.casDelete("lock", stale) {
		t.Fatal("stale owner must not remove successor")
	}
	m.revoke(old)
	m.revoke(unused)
	if got := m.keys["lock"]; got != want || !m.live(successor) {
		t.Fatalf("successor changed during stale operations/cleanup: got %+v, want %+v", got, want)
	}
}

func TestModelRevokeDetachedOldLeasePreservesRotatedKey(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	snapshot := mustPut(t, m, "lock", "owner-a", old)
	fresh := m.grant(10)
	if !m.casRotate("lock", snapshot, fresh) {
		t.Fatal("test setup: uncontended rotation should pass")
	}
	want := m.keys["lock"]
	m.revoke(old)
	if got := m.keys["lock"]; got != want || !m.live(fresh) {
		t.Fatalf("old lease cleanup changed rotated key: got %+v, want %+v", got, want)
	}
}

func TestModelNativeExpiryDoesNotMakeAcquisitionPredicateAbsent(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	mustPut(t, m, "lock", "owner-a", old)
	attempt := m.grant(10)
	if m.acquire("lock", "owner-a", attempt) {
		t.Fatal("same-owner acquisition must not be reentrant")
	}
	m.advance(2)
	if m.acquire("lock", "owner-b", attempt) {
		t.Fatal("stored key is still occupied even after native expiry")
	}
	m.revoke(old)
	if !m.acquire("lock", "owner-b", attempt) {
		t.Fatal("acquisition should pass after committed revocation")
	}
}

func TestModelKeepAlivePreflightDoesNotMakeLaterCASAtomic(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	snapshot := mustPut(t, m, "lock", "owner-a", old)
	m.advance(1)
	if !m.keepAlive(old) {
		t.Fatal("test setup: preflight occurs while old lease is live")
	}
	// An RPC/queue/lock wait follows the successful preflight. The requested new
	// TTL is different from the native keepalive's original TTL.
	fresh := m.grant(10)
	m.advance(2)
	if m.live(old) || !m.live(fresh) {
		t.Fatal("test setup: old must expire while new lease remains live")
	}
	if !m.casRotate("lock", snapshot, fresh) {
		t.Fatal("counterexample missing: successful preflight did not guard later CAS")
	}
}

func TestModelSubmittedCASCanMutateAfterCallerDeadline(t *testing.T) {
	m := newModel()
	old := m.grant(2)
	snapshot := mustPut(t, m, "lock", "owner-a", old)
	fresh := m.grant(10)
	callerDeadline := m.now + 1
	// The request has been submitted; the caller ceases waiting before the
	// independent state-machine application transition. There is no automatic
	// rollback of an accepted proposal when a client stops waiting.
	m.advance(3)
	if m.now < callerDeadline || m.live(old) {
		t.Fatal("test setup: caller deadline and old expiry must have passed")
	}
	if !m.casRotate("lock", snapshot, fresh) {
		t.Fatal("counterexample missing: local deadline is not a server expiry predicate")
	}
	if m.keys["lock"].lease != fresh {
		t.Fatal("expected forbidden remote mutation despite locally rejected success")
	}
}

func TestModelExactSnapshotDetectsConcurrentSameOwnerRenewal(t *testing.T) {
	m := newModel()
	old := m.grant(10)
	stale := mustPut(t, m, "lock", "owner-a", old)
	other := m.grant(20)
	want := mustPut(t, m, "lock", "owner-a", other)
	fresh := m.grant(30)
	if m.casRotate("lock", stale, fresh) || m.casDelete("lock", stale) {
		t.Fatal("stale incarnation must not pass merely because owner is unchanged")
	}
	if got := m.keys["lock"]; got != want {
		t.Fatalf("concurrent same-owner incarnation changed: got %+v, want %+v", got, want)
	}
	// Such a CAS failure is not proof of ownership loss: a hypothetical driver
	// would need to reread/retry within its existing budget, or return an error.
}

func TestModelPutCanAttachExpiredButNotRevokedLease(t *testing.T) {
	m := newModel()
	id := m.grant(1)
	m.advance(1)
	if m.live(id) {
		t.Fatal("test setup: target lease must be expired")
	}
	if !m.acquire("lock", "owner-a", id) {
		t.Fatal("counterexample missing: lease existence check does not check expiry")
	}
	m.revoke(id)
	if _, exists := m.keys["lock"]; exists {
		t.Fatal("committed revocation must remove the attachment")
	}
}
