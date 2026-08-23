package limiter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
)

var panelRulesFixture = panel.Rules{
	Regexp:   []string{`^.*\.example\.com$`},
	Protocol: []string{"bittorrent"},
}

// TestConcurrentAccess exercises the shared state that connection goroutines
// read while the node monitor goroutine mutates. Run with -race.
func TestConcurrentAccess(t *testing.T) {
	users := benchUsers(500)
	l := AddLimiter("racetest", benchConf(), users, map[int]int{1: 3})
	if err := l.UpdateRule(&panelRulesFixture); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// simulate per-connection goroutines
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				taguuid := fmt.Sprintf("racetest|8f2c1e5a-9b3d-4e6f-a7b8-%012d", (w*7+i)%500)
				ip := fmt.Sprintf("10.0.%d.%d", i%256, w)
				_, _ = l.CheckLimit(taguuid, ip, true, true)
				_ = l.CheckDomainRule("example.com")
				i++
			}
		}(w)
	}

	// simulate node monitor task
	for round := 0; round < 20; round++ {
		added := benchUsers(50)
		deleted := benchUsers(50)
		l.UpdateUser("racetest", added, deleted)
		newAlive := make(map[int]int, 500)
		for i := range users {
			newAlive[users[i].Id] = 3
		}
		l.SetAliveList(newAlive) // what node/task.go does on every pull
		_, _ = l.GetOnlineDevice()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
