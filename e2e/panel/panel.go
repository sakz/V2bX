package panel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MockPanel implements the UniProxy API surface used by V2bX with
// configurable user count, latency injection and request counters.
type MockPanel struct {
	Port       int
	Users      int
	PushDelay  time.Duration
	PullDelay  time.Duration
	FailMode   int // 0 ok, 1 = 500
	mu         sync.Mutex
	pushCount  int
	pullCount  int
	aliveCount int
	userBytes  []byte
	nodeBody   []byte
	nodeTag    string
	userTag    string
}

func NewMockPanel(port, users int) *MockPanel {
	return &MockPanel{Port: port, Users: users}
}

func uuidFor(i int) string {
	// Deterministic valid v4-format UUID.
	b := []byte{}
	for len(b) < 16 {
		b = append(b, byte(i&0xff))
		i /= 256
		if i == 0 && len(b) < 16 {
			i = len(b)*7919 + 13
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return strings.Join([]string{h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]}, "-")
}

func (m *MockPanel) buildBodies() {
	type u struct {
		ID          int    `json:"id"`
		UUID        string `json:"uuid"`
		SpeedLimit  int    `json:"speed_limit"`
		DeviceLimit int    `json:"device_limit"`
	}
	users := make([]u, m.Users)
	for i := range users {
		users[i] = u{ID: i + 1, UUID: uuidFor(i)}
	}
	m.userBytes, _ = json.Marshal(map[string]interface{}{"users": users})
	m.nodeBody = []byte(`{
		"host": "127.0.0.1",
		"server_port": NODEPORT,
		"tls": 0,
		"network": "tcp",
		"network_settings": {"header": {"type": "none"}},
		"server_name": "",
		"routes": [],
		"base_config": {"push_interval": 30, "pull_interval": 600}
	}`)
	h := sha256.Sum256(m.userBytes)
	m.userTag = hex.EncodeToString(h[:8])
	hh := sha256.Sum256(m.nodeBody)
	m.nodeTag = hex.EncodeToString(hh[:8])
}

func (m *MockPanel) etag(w http.ResponseWriter, r *http.Request, tag string, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", `"`+tag+`"`)
	if r.Header.Get("If-None-Match") == `"`+tag+`"` {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Write(body)
}

func (m *MockPanel) Handler(nodePort int) http.Handler {
	m.buildBodies()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/server/UniProxy/config", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		d, fail := m.PullDelay, m.FailMode
		m.pullCount++
		m.mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
		if fail == 1 {
			http.Error(w, "fail", 500)
			return
		}
		body := []byte(strings.ReplaceAll(string(m.nodeBody), "NODEPORT", strconv.Itoa(nodePort)))
		m.etag(w, r, m.nodeTag, body)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/user", func(w http.ResponseWriter, r *http.Request) {
		m.etag(w, r, m.userTag, m.userBytes)
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alivelist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"alive":{}}`))
	})
	mux.HandleFunc("/api/v1/server/UniProxy/push", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		d := m.PushDelay
		m.pushCount++
		m.mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/api/v1/server/UniProxy/alive", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.aliveCount++
		m.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/__control", func(w http.ResponseWriter, r *http.Request) {
		var ctl struct {
			PushDelayMs int `json:"push_delay_ms"`
			FailMode    int `json:"fail_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&ctl); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		m.mu.Lock()
		m.PushDelay = time.Duration(ctl.PushDelayMs) * time.Millisecond
		if ctl.FailMode != 0 || ctl.FailMode == 0 && r.URL.Query().Get("clear") == "1" {
			m.FailMode = ctl.FailMode
		}
		m.mu.Unlock()
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/__stats", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]int{
			"push": m.pushCount, "pull": m.pullCount, "alive_posts": m.aliveCount,
		})
	})
	return mux
}

func (m *MockPanel) Serve(nodePort int) error {
	srv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", m.Port), Handler: m.Handler(nodePort)}
	log.Printf("[mockpanel] listening on %d with %d users", m.Port, m.Users)
	return srv.ListenAndServe()
}

func main() {
	port := flag.Int("port", 18081, "listen port")
	users := flag.Int("users", 1000, "user count")
	nodeP := flag.Int("node_port", 17080, "proxied node port")
	flag.Parse()
	m := NewMockPanel(*port, *users)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", *port), m.Handler(*nodeP)))
}

// UUIDFor returns a deterministic valid v4 UUID for user index i.
func UUIDFor(i int) string { return uuidFor(i) }
