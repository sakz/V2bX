package sing

import (
	"sync"
	"testing"

	"github.com/InazumaV/V2bX/common/counter"
)

// newTestSing builds a Sing carrying only the user-mapping and counter state
// touched by the traffic report path; no sing-box instance is needed.
func newTestSing() *Sing {
	return &Sing{
		hookServer: &HookServer{counter: sync.Map{}},
		users: &UserMap{
			uidMap: make(map[string]map[string]int),
		},
		nodeReportMinTrafficBytes: make(map[string]int64),
	}
}

func addTraffic(b *Sing, tag, uuid string, up, down int64) {
	tc := counter.NewTrafficCounter()
	b.hookServer.counter.Store(tag, tc)
	ts := tc.GetCounter(uuid)
	ts.UpCounter.Store(up)
	ts.DownCounter.Store(down)
}

// The same panel user may exist on several nodes of one core. Each node must
// report the traffic it measured under the uid of its own user list.
func TestGetUserTrafficSliceReportsUidOfOwnTag(t *testing.T) {
	b := newTestSing()
	b.users.uidMap["node-a"] = map[string]int{"u1": 11}
	b.users.uidMap["node-b"] = map[string]int{"u1": 22}
	addTraffic(b, "node-a", "u1", 100, 100)
	addTraffic(b, "node-b", "u1", 200, 200)

	trafficA, err := b.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficA) != 1 || trafficA[0].UID != 11 {
		t.Fatalf("node-a: want [u1 uid=11], got %+v", trafficA)
	}

	trafficB, err := b.GetUserTrafficSlice("node-b", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficB) != 1 || trafficB[0].UID != 22 {
		t.Fatalf("node-b: want [u1 uid=22], got %+v", trafficB)
	}
}

// Removing a user from one node's list (what DelUsers does for a tag) must not
// touch the mapping of other nodes sharing the same uuid.
func TestDelUserOnOneTagKeepsOtherTagReports(t *testing.T) {
	b := newTestSing()
	b.users.uidMap["node-a"] = map[string]int{"u1": 11}
	b.users.uidMap["node-b"] = map[string]int{"u1": 22}
	addTraffic(b, "node-a", "u1", 100, 100)
	addTraffic(b, "node-b", "u1", 200, 200)

	// node-b's user list no longer contains u1
	b.users.mapLock.Lock()
	delete(b.users.uidMap["node-b"], "u1")
	b.users.mapLock.Unlock()

	trafficA, err := b.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficA) != 1 || trafficA[0].UID != 11 {
		t.Fatalf("node-a after node-b delete: want [u1 uid=11], got %+v", trafficA)
	}

	trafficB, err := b.GetUserTrafficSlice("node-b", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficB) != 0 {
		t.Fatalf("node-b after delete: want no traffic, got %+v", trafficB)
	}
}

// A counter entry without a user mapping must be dropped, not reported.
func TestGetUserTrafficSliceDropsUnknownUser(t *testing.T) {
	b := newTestSing()
	addTraffic(b, "node-a", "ghost", 100, 100)

	traffic, err := b.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(traffic) != 0 {
		t.Fatalf("want no traffic for unknown user, got %+v", traffic)
	}
}
