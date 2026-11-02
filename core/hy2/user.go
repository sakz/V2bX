package hy2

import (
	"errors"
	"net"
	"sync"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/counter"
	vCore "github.com/InazumaV/V2bX/core"
	"github.com/apernet/hysteria/core/v2/server"
)

var _ server.Authenticator = &V2bX{}

type V2bX struct {
	usersMap map[string]int
	mutex    sync.RWMutex
}

func (v *V2bX) Authenticate(addr net.Addr, auth string, tx uint64) (ok bool, id string) {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	if _, exists := v.usersMap[auth]; exists {
		return true, auth
	}
	return false, ""
}

func (h *Hysteria2) AddUsers(p *vCore.AddUsersParams) (added int, err error) {
	n, ok := h.Hy2nodes[p.Tag]
	if !ok {
		return 0, errors.New("the node not found")
	}
	n.Auth.mutex.Lock()
	for i := range p.Users {
		n.Auth.usersMap[p.Users[i].Uuid] = p.Users[i].Id
	}
	n.Auth.mutex.Unlock()
	return len(p.Users), nil
}

func (h *Hysteria2) DelUsers(users []panel.UserInfo, tag string, _ *panel.NodeInfo) error {
	n, ok := h.Hy2nodes[tag]
	if !ok {
		return errors.New("the node not found")
	}
	if v, ok := n.TrafficLogger.(*HookServer).Counter.Load(tag); ok {
		c := v.(*counter.TrafficCounter)
		for i := range users {
			c.Delete(users[i].Uuid)
		}
	}
	n.Auth.mutex.Lock()
	for i := range users {
		delete(n.Auth.usersMap, users[i].Uuid)
	}
	n.Auth.mutex.Unlock()
	return nil
}

func (h *Hysteria2) GetUserTrafficSlice(tag string, reset bool) ([]panel.UserTraffic, error) {
	trafficSlice := make([]panel.UserTraffic, 0)
	n, ok := h.Hy2nodes[tag]
	if !ok {
		return nil, nil
	}
	hook := n.TrafficLogger.(*HookServer)
	n.Auth.mutex.RLock()
	defer n.Auth.mutex.RUnlock()
	if v, ok := hook.Counter.Load(tag); ok {
		c := v.(*counter.TrafficCounter)
		c.Counters.Range(func(key, value interface{}) bool {
			uuid := key.(string)
			traffic := value.(*counter.TrafficStorage)
			up := traffic.UpCounter.Load()
			down := traffic.DownCounter.Load()
			if up+down > hook.ReportMinTrafficBytes {
				if reset {
					traffic.UpCounter.Store(0)
					traffic.DownCounter.Store(0)
				}
				if n.Auth.usersMap[uuid] == 0 {
					c.Delete(uuid)
					return true
				}
				trafficSlice = append(trafficSlice, panel.UserTraffic{
					UID:      n.Auth.usersMap[uuid],
					Upload:   up,
					Download: down,
				})
			}
			return true
		})
		if len(trafficSlice) == 0 {
			return nil, nil
		}
		return trafficSlice, nil
	}
	return nil, nil
}
