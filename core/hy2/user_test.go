package hy2

import (
	"sync"
	"testing"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/counter"
	vCore "github.com/InazumaV/V2bX/core"
)

func newTestHysteria2() *Hysteria2 {
	h := &Hysteria2{
		Hy2nodes: make(map[string]Hysteria2node),
	}
	for _, tag := range []string{"node-a", "node-b"} {
		h.Hy2nodes[tag] = Hysteria2node{
			Tag:           tag,
			TrafficLogger: &HookServer{Tag: tag, Counter: sync.Map{}},
			Auth: &V2bX{
				usersMap: make(map[string]int),
			},
		}
	}
	return h
}

func addHyTraffic(h *Hysteria2, tag, uuid string, up, down int64) {
	hs := h.Hy2nodes[tag].TrafficLogger.(*HookServer)
	tc := counter.NewTrafficCounter()
	hs.Counter.Store(tag, tc)
	ts := tc.GetCounter(uuid)
	ts.UpCounter.Store(up)
	ts.DownCounter.Store(down)
}

// The same panel user may exist on several nodes of one core. Removing the
// user from one node's list must not break auth-state and traffic reporting
// of the other nodes.
func TestDelUsersOnOneNodeDoesNotAffectOtherNodes(t *testing.T) {
	h := newTestHysteria2()
	if _, err := h.AddUsers(&vCore.AddUsersParams{
		Tag:   "node-a",
		Users: []panel.UserInfo{{Id: 11, Uuid: "u1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.AddUsers(&vCore.AddUsersParams{
		Tag:   "node-b",
		Users: []panel.UserInfo{{Id: 22, Uuid: "u1"}},
	}); err != nil {
		t.Fatal(err)
	}
	addHyTraffic(h, "node-a", "u1", 100, 100)
	addHyTraffic(h, "node-b", "u1", 100, 100)

	// panel removes u1 from node-b only
	if err := h.DelUsers([]panel.UserInfo{{Id: 22, Uuid: "u1"}}, "node-b", nil); err != nil {
		t.Fatal(err)
	}

	trafficA, err := h.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficA) != 1 || trafficA[0].UID != 11 {
		t.Fatalf("node-a after node-b delete: want [u1 uid=11], got %+v", trafficA)
	}

	trafficB, err := h.GetUserTrafficSlice("node-b", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficB) != 0 {
		t.Fatalf("node-b after delete: want no traffic, got %+v", trafficB)
	}
}

// Each node must report the traffic it measured under the uid of its own
// user list, even when another node maps the same uuid differently.
func TestGetUserTrafficSliceReportsUidOfOwnNode(t *testing.T) {
	h := newTestHysteria2()
	if _, err := h.AddUsers(&vCore.AddUsersParams{
		Tag:   "node-a",
		Users: []panel.UserInfo{{Id: 11, Uuid: "u1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.AddUsers(&vCore.AddUsersParams{
		Tag:   "node-b",
		Users: []panel.UserInfo{{Id: 22, Uuid: "u1"}},
	}); err != nil {
		t.Fatal(err)
	}
	addHyTraffic(h, "node-a", "u1", 100, 100)
	addHyTraffic(h, "node-b", "u1", 200, 200)

	trafficA, err := h.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficA) != 1 || trafficA[0].UID != 11 {
		t.Fatalf("node-a: want [u1 uid=11], got %+v", trafficA)
	}

	trafficB, err := h.GetUserTrafficSlice("node-b", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(trafficB) != 1 || trafficB[0].UID != 22 {
		t.Fatalf("node-b: want [u1 uid=22], got %+v", trafficB)
	}
}

// A counter entry without a user mapping must be dropped, not reported.
func TestGetUserTrafficSliceDropsUnknownUser(t *testing.T) {
	h := newTestHysteria2()
	addHyTraffic(h, "node-a", "ghost", 100, 100)

	traffic, err := h.GetUserTrafficSlice("node-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(traffic) != 0 {
		t.Fatalf("want no traffic for unknown user, got %+v", traffic)
	}
}

// AddUsers/DelUsers for an unknown tag must fail instead of corrupting state.
func TestUserOpsForUnknownTagError(t *testing.T) {
	h := newTestHysteria2()
	if _, err := h.AddUsers(&vCore.AddUsersParams{
		Tag:   "missing",
		Users: []panel.UserInfo{{Id: 1, Uuid: "u1"}},
	}); err == nil {
		t.Fatal("AddUsers for unknown tag: want error, got nil")
	}
	if err := h.DelUsers([]panel.UserInfo{{Id: 1, Uuid: "u1"}}, "missing", nil); err == nil {
		t.Fatal("DelUsers for unknown tag: want error, got nil")
	}
}
