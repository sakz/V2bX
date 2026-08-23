package limiter

import (
	"errors"
	"maps"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/format"
	"github.com/InazumaV/V2bX/conf"
	"github.com/juju/ratelimit"
)

var limitLock sync.RWMutex
var limiter map[string]*Limiter

func Init() {
	limiter = map[string]*Limiter{}
}

type Limiter struct {
	DomainRules   []*regexp.Regexp
	ProtocolRules []string
	SpeedLimit    int
	UserOnlineIP  *sync.Map                      // Key: TagUUID, value: {Key: Ip, value: Uid}
	OldUserOnline atomic.Pointer[sync.Map]       // Key: Ip, value: Uid
	UUIDtoUID     atomic.Pointer[map[string]int] // Key: UUID, value: Uid
	UserLimitInfo *sync.Map                      // Key: TagUUID value: UserLimitInfo
	SpeedLimiter  *sync.Map                      // key: TagUUID, value: *ratelimit.Bucket
	AliveList     atomic.Pointer[map[int]int]    // Key: Uid, value: alive_ip
}

type UserLimitInfo struct {
	UID               int
	SpeedLimit        int
	DeviceLimit       int
	DynamicSpeedLimit atomic.Int64
	ExpireTime        atomic.Int64
	OverLimit         atomic.Bool
}

// SetAliveList atomically replaces the alive-ip table (copy-on-write readers).
func (l *Limiter) SetAliveList(alive map[int]int) {
	l.AliveList.Store(&alive)
}

func AddLimiter(tag string, l *conf.LimitConfig, users []panel.UserInfo, aliveList map[int]int) *Limiter {
	info := &Limiter{
		SpeedLimit:    l.SpeedLimit,
		UserOnlineIP:  new(sync.Map),
		UserLimitInfo: new(sync.Map),
		SpeedLimiter:  new(sync.Map),
	}
	info.SetAliveList(aliveList)
	info.OldUserOnline.Store(new(sync.Map))
	uuidmap := make(map[string]int)
	for i := range users {
		uuidmap[users[i].Uuid] = users[i].Id
		userLimit := &UserLimitInfo{}
		userLimit.UID = users[i].Id
		if users[i].SpeedLimit != 0 {
			userLimit.SpeedLimit = users[i].SpeedLimit
		}
		if users[i].DeviceLimit != 0 {
			userLimit.DeviceLimit = users[i].DeviceLimit
		}
		info.UserLimitInfo.Store(format.UserTag(tag, users[i].Uuid), userLimit)
	}
	info.UUIDtoUID.Store(&uuidmap)
	limitLock.Lock()
	limiter[tag] = info
	limitLock.Unlock()
	return info
}

func GetLimiter(tag string) (info *Limiter, err error) {
	limitLock.RLock()
	info, ok := limiter[tag]
	limitLock.RUnlock()
	if !ok {
		return nil, errors.New("not found")
	}
	return info, nil
}

func DeleteLimiter(tag string) {
	limitLock.Lock()
	delete(limiter, tag)
	limitLock.Unlock()
}

// removeKeys returns a copy of m without the given keys.
func removeKeys[V any](m *map[string]V, keys []string) *map[string]V {
	if m == nil {
		return nil
	}
	nm := make(map[string]V, len(*m))
	maps.Copy(nm, *m)
	for _, k := range keys {
		delete(nm, k)
	}
	return &nm
}

// upsertAll returns a copy of m with the given pairs applied.
func upsertAll[V any](m *map[string]V, pairs []panel.UserInfo, val func(panel.UserInfo) V) *map[string]V {
	if m == nil {
		return nil
	}
	nm := make(map[string]V, len(*m)+len(pairs))
	maps.Copy(nm, *m)
	for _, u := range pairs {
		nm[u.Uuid] = val(u)
	}
	return &nm
}

func (l *Limiter) UpdateUser(tag string, added []panel.UserInfo, deleted []panel.UserInfo) {
	delKeys := make([]string, len(deleted))
	for i := range deleted {
		delKeys[i] = deleted[i].Uuid
		key := format.UserTag(tag, delKeys[i])
		l.UserLimitInfo.Delete(key)
		l.UserOnlineIP.Delete(key)
		l.SpeedLimiter.Delete(key)
	}
	if len(delKeys) > 0 {
		if nm := removeKeys(l.UUIDtoUID.Load(), delKeys); nm != nil {
			l.UUIDtoUID.Store(nm)
		}
		if a := l.AliveList.Load(); a != nil {
			na := make(map[int]int, len(*a))
			maps.Copy(na, *a)
			for i := range deleted {
				delete(na, deleted[i].Id)
			}
			l.AliveList.Store(&na)
		}
	}
	if len(added) > 0 {
		addVal := func(u panel.UserInfo) int { return u.Id }
		if nm := upsertAll(l.UUIDtoUID.Load(), added, addVal); nm != nil {
			l.UUIDtoUID.Store(nm)
		}
		for i := range added {
			userLimit := &UserLimitInfo{
				UID: added[i].Id,
			}
			if added[i].SpeedLimit != 0 {
				userLimit.SpeedLimit = added[i].SpeedLimit
			}
			if added[i].DeviceLimit != 0 {
				userLimit.DeviceLimit = added[i].DeviceLimit
			}
			l.UserLimitInfo.Store(format.UserTag(tag, added[i].Uuid), userLimit)
		}
	}
}

func (l *Limiter) UpdateDynamicSpeedLimit(tag, uuid string, limit int, expire time.Time) error {
	if v, ok := l.UserLimitInfo.Load(format.UserTag(tag, uuid)); ok {
		info := v.(*UserLimitInfo)
		info.DynamicSpeedLimit.Store(int64(limit))
		info.ExpireTime.Store(expire.Unix())
	} else {
		return errors.New("not found")
	}
	return nil
}

func (l *Limiter) CheckLimit(taguuid string, ip string, isTcp bool, noSSUDP bool) (Bucket *ratelimit.Bucket, Reject bool) {
	// check if ipv4 mapped ipv6
	ip = strings.TrimPrefix(ip, "::ffff:")

	// Snapshot the previous online table once so lookups stay consistent
	// even if GetOnlineDevice swaps it concurrently.
	oldOnline := l.OldUserOnline.Load()

	// check and gen speed limit Bucket
	nodeLimit := l.SpeedLimit
	userLimit := 0
	deviceLimit := 0
	var uid int
	if v, ok := l.UserLimitInfo.Load(taguuid); ok {
		u := v.(*UserLimitInfo)
		deviceLimit = u.DeviceLimit
		uid = u.UID
		expire := u.ExpireTime.Load()
		if expire < time.Now().Unix() && expire != 0 {
			if u.SpeedLimit != 0 {
				userLimit = u.SpeedLimit
				u.DynamicSpeedLimit.Store(0)
				u.ExpireTime.Store(0)
			} else {
				l.UserLimitInfo.Delete(taguuid)
			}
		} else {
			userLimit = determineSpeedLimit(u.SpeedLimit, int(u.DynamicSpeedLimit.Load()))
		}
	} else {
		return nil, true
	}
	if noSSUDP {
		var aliveIp int
		if alive := l.AliveList.Load(); alive != nil {
			aliveIp = (*alive)[uid]
		}
		// Store online user for device limit
		newipMap := new(sync.Map)
		newipMap.Store(ip, uid)
		if v, loaded := l.UserOnlineIP.LoadOrStore(taguuid, newipMap); loaded {
			oldipMap := v.(*sync.Map)
			// If this is a new ip
			if _, loaded := oldipMap.LoadOrStore(ip, uid); !loaded {
				if v, loaded := oldOnline.Load(ip); loaded {
					if v.(int) == uid {
						oldOnline.Delete(ip)
					}
				} else if deviceLimit > 0 {
					if deviceLimit <= aliveIp {
						oldipMap.Delete(ip)
						return nil, true
					}
				}
			}
		} else if v, ok := oldOnline.Load(ip); ok {
			if v.(int) == uid {
				oldOnline.Delete(ip)
			}
		} else {
			if deviceLimit > 0 {
				if deviceLimit <= aliveIp {
					l.UserOnlineIP.Delete(taguuid)
					return nil, true
				}
			}
		}
	}

	limit := int64(determineSpeedLimit(nodeLimit, userLimit)) * 1000000 / 8 // If you need the Speed limit
	if limit > 0 {
		if v, ok := l.SpeedLimiter.Load(taguuid); ok {
			return v.(*ratelimit.Bucket), false
		}
		Bucket = ratelimit.NewBucketWithQuantum(time.Second, limit, limit) // Byte/s
		if v, loaded := l.SpeedLimiter.LoadOrStore(taguuid, Bucket); loaded {
			return v.(*ratelimit.Bucket), false
		}
		return Bucket, false
	} else {
		return nil, false
	}
}

func (l *Limiter) GetOnlineDevice() (*[]panel.OnlineUser, error) {
	var onlineUser []panel.OnlineUser
	newOld := new(sync.Map)
	l.UserOnlineIP.Range(func(key, value interface{}) bool {
		taguuid := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(key, value interface{}) bool {
			uid := value.(int)
			ip := key.(string)
			newOld.Store(ip, uid)
			onlineUser = append(onlineUser, panel.OnlineUser{UID: uid, IP: ip})
			return true
		})
		l.UserOnlineIP.Delete(taguuid) // Reset online device
		return true
	})
	l.OldUserOnline.Store(newOld)

	return &onlineUser, nil
}

type UserIpList struct {
	Uid    int      `json:"Uid"`
	IpList []string `json:"Ips"`
}
