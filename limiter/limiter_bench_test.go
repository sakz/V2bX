package limiter

import (
	"fmt"
	"testing"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
)

const benchTag = "[https://example.com]-vless:1"

func benchUsers(n int) []panel.UserInfo {
	users := make([]panel.UserInfo, n)
	for i := range users {
		users[i] = panel.UserInfo{
			Id:          i + 1,
			Uuid:        fmt.Sprintf("8f2c1e5a-9b3d-4e6f-a7b8-%012d", i),
			SpeedLimit:  100,
			DeviceLimit: 3,
		}
	}
	return users
}

func benchConf() *conf.LimitConfig {
	return &conf.LimitConfig{SpeedLimit: 0}
}

func init() {
	Init()
}

func BenchmarkCheckLimit10kUsers(b *testing.B) {
	l := AddLimiter(benchTag, benchConf(), benchUsers(10000), map[int]int{1: 3})
	taguuid := benchTag + "|8f2c1e5a-9b3d-4e6f-a7b8-000000000042"
	ip := "::ffff:1.2.3.4"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, reject := l.CheckLimit(taguuid, ip, true, true)
		if reject {
			b.Fatal("unexpected reject")
		}
	}
}

func BenchmarkCheckLimitWithSpeedLimit(b *testing.B) {
	conf := &conf.LimitConfig{SpeedLimit: 100} // Mbps -> bucket path
	l := AddLimiter(benchTag+"sl", conf, benchUsers(10000), nil)
	taguuid := benchTag + "sl|8f2c1e5a-9b3d-4e6f-a7b8-000000000042"
	ip := "1.2.3.4"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket, reject := l.CheckLimit(taguuid, ip, true, true)
		if reject || bucket == nil {
			b.Fatal("unexpected result")
		}
	}
}

func BenchmarkUpdateUser10k(b *testing.B) {
	l := AddLimiter(benchTag, benchConf(), benchUsers(10000), nil)
	added := benchUsers(100)
	deleted := benchUsers(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.UpdateUser(benchTag, added, deleted)
	}
}

func BenchmarkAddLimiter10k(b *testing.B) {
	users := benchUsers(10000)
	alive := map[int]int{1: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DeleteLimiter(benchTag)
		AddLimiter(benchTag, benchConf(), users, alive)
	}
}

func BenchmarkGetOnlineDevice10k(b *testing.B) {
	l := AddLimiter(benchTag, benchConf(), benchUsers(10000), nil)
	taguuid := benchTag + "|8f2c1e5a-9b3d-4e6f-a7b8-000000000042"
	l.CheckLimit(taguuid, "1.2.3.4", true, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = l.GetOnlineDevice()
	}
}
