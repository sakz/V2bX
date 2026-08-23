package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// Register all xray components so we can embed a real client.
	_ "github.com/InazumaV/V2bX/core/xray/distro/all"
	"github.com/InazumaV/V2bX/e2e/panel"
	xc "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
)

const (
	panelPort   = 18081
	nodePort    = 17080
	varsPort    = 18090
	sinkPort    = 17301
	sourcePort  = 17302
	clientBase  = 17101 // socks ports: clientBase..clientBase+clients-1
	numClients  = 4
)

var gorTotalRe = regexp.MustCompile(`goroutine profile: total (\d+)`)

type MemStats struct {
	HeapAlloc    uint64
	HeapObjects  uint64
	HeapSys      uint64
	HeapIdle     uint64
	HeapReleased uint64
	StackSys     uint64
	Sys          uint64
	NumGC        uint32
	PauseTotalNs uint64
}


func fetchVars() (*MemStats, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/vars", varsPort))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var raw struct {
		Memstats *MemStats `json:"memstats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw.Memstats, nil
}

func fetchGoroutines() (int, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=1", varsPort))
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	m := gorTotalRe.FindSubmatch(b)
	if m == nil {
		return -1, fmt.Errorf("no total in goroutine profile")
	}
	n, _ := strconv.Atoi(string(m[1]))
	return n, nil
}

func procInfo(pid int) (cpuSeconds float64, rssKB int64) {
	out, err := exec.Command("ps", "-o", "cputime=,rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1, -1
	}
	f := strings.Fields(string(out))
	if len(f) < 2 {
		return -1, -1
	}
	rssKB, _ = strconv.ParseInt(f[1], 10, 64)
	parts := strings.Split(f[0], ":")
	for _, p := range parts {
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			continue
		}
		cpuSeconds = cpuSeconds*60 + v
	}
	return
}

type Sample struct {
	T          time.Time `json:"t"`
	RSSKB      int64     `json:"rss_kb"`
	CPUSec     float64   `json:"cpu_sec"`
	Goroutines int       `json:"goroutines"`
	*MemStats
}

func sample(pid int) Sample {
	s := Sample{T: time.Now()}
	s.CPUSec, s.RSSKB = procInfo(pid)
	s.MemStats, _ = fetchVars()
	s.Goroutines, _ = fetchGoroutines()
	return s
}

func waitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", nodePort), time.Second)
		if err == nil {
			c.Close()
			if _, err := fetchVars(); err == nil {
				return nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("v2bx not ready in %s", timeout)
}

func uuidFor(i int) string { return panel.UUIDFor(i) }

func writeConfig(dir string, users int) string {
	type u struct {
		ID          int    `json:"id"`
		UUID        string `json:"uuid"`
		SpeedLimit  int    `json:"speed_limit"`
		DeviceLimit int    `json:"device_limit"`
	}
	list := make([]u, users)
	for i := range list {
		list[i] = u{ID: i + 1, UUID: uuidFor(i)}
	}

	cfg := map[string]interface{}{
		"Log": map[string]string{"Level": "warning"},
		"Cores": []map[string]interface{}{{
			"Type": "xray",
			"Log":  map[string]string{"Level": "warning"},
			"XrayConnectionConfig": map[string]interface{}{
				"handshake": 4, "connIdle": 600, "uplinkOnly": 2,
				"downlinkOnly": 4, "bufferSize": 64,
			},
		}},
		"Nodes": []map[string]interface{}{{
			"Core":                    "xray",
			"ApiHost":                 fmt.Sprintf("http://127.0.0.1:%d", panelPort),
			"ApiKey":                  "benchkey",
			"NodeID":                  1,
			"NodeType":                "vless",
			"Timeout":                 5,
			"ListenIP":                "127.0.0.1",
			"SendIP":                  "127.0.0.1",
			"DeviceOnlineMinTraffic":  200,
			"ReportMinTraffic":        0,
			"XrayOptions":             map[string]interface{}{"DisableSniffing": true},
			"CertConfig":              map[string]interface{}{"CertMode": "none"},
		}},
	}
	b, _ := json.MarshalIndent(cfg, "", " ")
	p := filepath.Join(dir, "config.json")
	os.WriteFile(p, b, 0644)
	return p
}

func spawnV2bX(bin, configPath, logPath string, env ...string) (*exec.Cmd, error) {
	cmd := exec.Command(bin, "server", "--config", configPath, "--watch=false")
	cmd.Env = append(os.Environ(),
		"GOEXPERIMENT=jsonv2",
		fmt.Sprintf("V2BX_PPROF_ADDR=127.0.0.1:%d", varsPort),
	)
	cmd.Env = append(cmd.Env, env...)
	lf, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

// ---- embedded xray clients ----

func startXrayClients(dir string, uuids []string) ([]*xc.Instance, error) {
	insts := make([]*xc.Instance, numClients)
	for i := 0; i < numClients; i++ {
		cfgJSON := fmt.Sprintf(`{
		  "log": {"loglevel": "error"},
		  "inbounds": [{"protocol":"socks","listen":"127.0.0.1","port":%d,"settings":{"auth":"noauth","udp":false}}],
		  "outbounds": [{"protocol":"vless","settings":{"vnext":[{"address":"127.0.0.1","port":%d,"users":[{"id":"%s","encryption":"none"}]}]},"streamSettings":{"network":"tcp"}}]
		}`, clientBase+i, nodePort, uuids[i])
		pb, err := serial.DecodeJSONConfig(strings.NewReader(cfgJSON))
		if err != nil {
			return nil, fmt.Errorf("decode client cfg %d: %w", i, err)
		}
		cfg, err := pb.Build()
		if err != nil {
			return nil, fmt.Errorf("build client cfg %d: %w", i, err)
		}
		inst, err := xc.New(cfg)
		if err != nil {
			return nil, fmt.Errorf("new client core %d: %w", i, err)
		}
		if err := inst.Start(); err != nil {
			return nil, fmt.Errorf("start client core %d: %w", i, err)
		}
		insts[i] = inst
	}
	return insts, nil
}

// ---- sink & source servers ----

var sinkBytes atomic.Int64

func serveSink(ln net.Listener) {
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := c.Read(buf)
					sinkBytes.Add(int64(n))
					if err != nil {
						break
					}
				}
				c.Close()
			}(c)
		}
	}()
}

func serveSource(ln net.Listener) {
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				zeros := make([]byte, 64*1024)
				for {
					if _, err := c.Write(zeros); err != nil {
						return
					}
				}
			}(c)
		}
	}()
}

// ---- SOCKS5 client ----

func socksConnect(socksPort int, target string) (net.Conn, error) {
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := strconv.Atoi(portStr)
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), 3*time.Second)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		c.Close()
		return nil, err
	}
	hello := make([]byte, 2)
	if _, err := io.ReadFull(c, hello); err != nil || hello[1] != 0 {
		c.Close()
		return nil, fmt.Errorf("socks hello failed")
	}
	req := append([]byte{5, 1, 0, 3, byte(len(host))}, []byte(host)...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := c.Write(req); err != nil {
		c.Close()
		return nil, err
	}
	// reply: up to 262 bytes; read fixed header then addr
	rep := make([]byte, 4)
	if _, err := io.ReadFull(c, rep); err != nil || rep[1] != 0 {
		c.Close()
		return nil, fmt.Errorf("socks connect failed: %v", rep)
	}
	switch rep[3] {
	case 1:
		io.ReadFull(c, make([]byte, 6))
	case 3:
		l := make([]byte, 1)
		io.ReadFull(c, l)
		io.ReadFull(c, make([]byte, int(l[0])+2))
	case 4:
		io.ReadFull(c, make([]byte, 18))
	}
	c.SetDeadline(time.Time{})
	return c, nil
}

// ---- load engines ----

var paceMbps = flag.Int("mbps", 0, "if >0, pace aggregate throughput to this many Mbps")

func runDownload(conns, clientIdxBase int, secs int, quotaBytes int64) (bytesMoved int64, wall time.Duration) {
	stop := make(chan struct{})
	var total atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	perConn := quotaBytes / int64(max(conns, 1))
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			socks := clientBase + (idx % numClients)
			c, err := socksConnect(socks, fmt.Sprintf("127.0.0.1:%d", sourcePort))
			if err != nil {
				log.Printf("[load] conn %d dial fail: %v", idx, err)
				return
			}
			defer c.Close()
			buf := make([]byte, 64*1024)
			var got int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.SetReadDeadline(time.Now().Add(10 * time.Second))
				n, err := c.Read(buf)
				got += int64(n)
				total.Add(int64(n))
				if *paceMbps > 0 {
					// aggregate pacing: per-conn share of target Mbps
					shareBps := int64(*paceMbps) * 1e6 / 8 / int64(max(conns, 1)) // bytes/s per conn
					sleepNs := int64(len(buf)) * 1e9 / shareBps                   // 64KB chunk
					time.Sleep(time.Duration(sleepNs))
				}
				if perConn > 0 && got >= perConn {
					return
				}
				if err != nil {
					return
				}
			}
		}(i)
	}
	if secs > 0 {
		time.Sleep(time.Duration(secs) * time.Second)
		close(stop)
	} else {
		wg.Wait() // quota mode: ends by itself
	}
	// grace for counters
	time.Sleep(200 * time.Millisecond)
	wall = time.Since(start)
	return total.Load(), wall
}

func openHeldConns(n int, existing *[]net.Conn) error {
	conns := *existing
	fails := 0
	for len(conns) < n && fails < n/10+50 {
		i := len(conns)
		c, err := socksConnectRetry(clientBase+(i%numClients), fmt.Sprintf("127.0.0.1:%d", sinkPort), 3)
		if err != nil {
			fails++
			time.Sleep(20 * time.Millisecond)
			continue
		}
		conns = append(conns, c)
		if len(conns)%500 == 0 {
			time.Sleep(100 * time.Millisecond) // let the accept queue drain
		}
	}
	*existing = conns
	if len(conns) < n {
		return fmt.Errorf("only %d/%d held", len(conns), n)
	}
	return nil
}

func socksConnectRetry(socksPort int, target string, tries int) (net.Conn, error) {
	var last error
	for i := 0; i < tries; i++ {
		c, err := socksConnect(socksPort, target)
		if err == nil {
			return c, nil
		}
		last = err
		time.Sleep(30 * time.Millisecond)
	}
	return nil, last
}

func closeAll(conns *[]net.Conn) {
	for _, c := range *conns {
		c.Close()
	}
	*conns = nil
}

// ---- control helpers ----

func panelControl(pushDelayMs int) {
	body, _ := json.Marshal(map[string]int{"push_delay_ms": pushDelayMs})
	http.Post(fmt.Sprintf("http://127.0.0.1:%d/__control", panelPort), "application/json", bytes.NewReader(body))
}

func fetchProfile(name, outDir string, sec int) {
	url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/%s", varsPort, name)
	if sec > 0 {
		url += fmt.Sprintf("?seconds=%d", sec)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("[profile] %s: %v", name, err)
		return
	}
	defer resp.Body.Close()
	p := filepath.Join(outDir, name+"_"+strconv.Itoa(sec)+".pprof")
	b, _ := io.ReadAll(resp.Body)
	os.WriteFile(p, b, 0644)
	log.Printf("[profile] saved %s (%d bytes)", p, len(b))
}

func max(a, b int) int { if a > b { return a }; return b }

func main() {
	bin := flag.String("bin", "./v2bx", "path to V2bX binary")
	label := flag.String("label", "opt", "result label")
	scenario := flag.String("scenario", "idle", "idle|conn|tput|slowpanel|profile|userscale")
	usersN := flag.Int("users", 1000, "configured users")
	connsN := flag.Int("conns", 100, "connections for conn/tput scenarios")
	secs := flag.Int("secs", 30, "duration of traffic window")
	outDir := flag.String("out", "results", "output dir")
	childEnv := flag.String("env", "", "extra K=V env for child (repeatable via comma)")
	flag.Parse()

	os.MkdirAll(*outDir, 0755)
	workdir, _ := os.MkdirTemp("", "v2bx-e2e-")
	log.Printf("[%s] workdir %s", *label, workdir)

	// sink/source listeners
	sinkLn, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", sinkPort))
	srcLn, _ := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", sourcePort))
	serveSink(sinkLn)
	serveSource(srcLn)

	// mock panel
	mp := panel.NewMockPanel(panelPort, *usersN)
	_ = mp
	startPanel := func() *http.Server {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", panelPort))
		if err != nil {
			log.Fatalf("panel listen: %v", err)
		}
		srv := &http.Server{Handler: mp.Handler(nodePort)}
		go func() { _ = srv.Serve(ln) }()
		return srv
	}
	panelSrv := startPanel()

	configPath := writeConfig(workdir, *usersN)
	logPath := filepath.Join(*outDir, "v2bx_"+*label+".log")

	env := strings.Split(*childEnv, ",")
	cmd, err := spawnV2bX(*bin, configPath, logPath, env...)
	if err != nil {
		log.Fatalf("spawn: %v", err)
	}
	pid := cmd.Process.Pid
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		panelSrv.Close()
	}()

	if err := waitForReady(90 * time.Second); err != nil {
		b, _ := os.ReadFile(logPath)
		log.Fatalf("not ready: %v\nlog tail:\n%s", err, tail(b, 4000))
	}
	time.Sleep(8 * time.Second) // let user sync settle & first push cycle start

	base := sample(pid)
	log.Printf("[%s] baseline sample: %+v", *label, base)

	result := map[string]interface{}{
		"label":    *label,
		"scenario": *scenario,
		"users":    *usersN,
		"conns":    *connsN,
		"base":     base,
	}

	switch *scenario {
	case "idle":
		time.Sleep(time.Duration(*secs) * time.Second)
		end := sample(pid)
		result["end"] = end

	case "userscale":
		time.Sleep(20 * time.Second)
		var ss []Sample
		for i := 0; i < 5; i++ {
			ss = append(ss, sample(pid))
			time.Sleep(3 * time.Second)
		}
		result["end"] = ss[len(ss)-1]
		result["samples"] = ss

	default:
		// traffic scenarios need embedded xray clients
		uuids := make([]string, numClients)
		for i := range uuids {
			uuids[i] = uuidFor(i)
		}
		clients, err := startXrayClients(workdir, uuids)
		if err != nil {
			log.Fatalf("xray clients: %v", err)
		}
		defer func() {
			for _, c := range clients {
				c.Close()
			}
		}()
	}

	switch *scenario {
	case "conn":
		var held []net.Conn
		if err := openHeldConns(*connsN, &held); err != nil {
			log.Printf("[%s] hold conns incomplete: %v", *label, err)
		}
		result["held"] = len(held)
		time.Sleep(6 * time.Second)
		var ss []Sample
		for i := 0; i < 3; i++ {
			ss = append(ss, sample(pid))
			time.Sleep(2 * time.Second)
		}
		result["end"] = ss[len(ss)-1]
		result["samples"] = ss
		closeAll(&held)
		time.Sleep(3 * time.Second)
		afterClose := sample(pid)
		result["after_close"] = afterClose

	case "tput":
		cpuStart := base.CPUSec
		bytes_, wall := runDownload(*connsN, 0, *secs, 0)
		end := sample(pid)
		cpuSec := end.CPUSec - cpuStart
		gbps := float64(bytes_) * 8 / wall.Seconds() / 1e9
		result["end"] = end
		result["bytes"] = bytes_
		result["wall_s"] = wall.Seconds()
		result["gbps"] = gbps
		result["cpu_sec"] = cpuSec
		result["cpu_per_gbps_s"] = cpuSec / gbps
		result["alloc_rate_mb_s"] = float64(end.HeapAlloc) / 1e6 // snapshot only

	case "slowpanel":
		var held []net.Conn
		openHeldConns(50, &held)
		// window 1: normal panel
		b1, w1 := runDownload(16, 0, 15, 0)
		mid1 := sample(pid)
		// window 2: panel push delayed 5s
		panelControl(5000)
		b2, w2 := runDownload(16, 0, 15, 0)
		mid2 := sample(pid)
		// window 3: panel fully unreachable
		failCtl, _ := json.Marshal(map[string]int{"push_delay_ms": 0})
		http.Post(fmt.Sprintf("http://127.0.0.1:%d/__control", panelPort), "application/json", bytes.NewReader(failCtl))
		panelSrv.Close()
		b3, w3 := runDownload(16, 0, 15, 0)
		mid3 := sample(pid)
		time.Sleep(35 * time.Second) // allow retries to pile up if any
		final := sample(pid)
		result["windows"] = []map[string]interface{}{
			{"phase": "normal", "mbps": float64(b1) * 8 / w1.Seconds() / 1e6},
			{"phase": "push_delay_5s", "mbps": float64(b2) * 8 / w2.Seconds() / 1e6},
			{"phase": "panel_down", "mbps": float64(b3) * 8 / w3.Seconds() / 1e6},
		}
		result["samples"] = []Sample{base, mid1, mid2, mid3, final}
		result["goroutines_final"] = final.Goroutines
		closeAll(&held)
		// restart panel for stats
		panelSrv = startPanel()

	case "profile":
		// throughput while collecting profiles
		stopCh := make(chan struct{})
		go func() {
			runDownload(16, 0, *secs+10, 0)
			close(stopCh)
		}()
		time.Sleep(3 * time.Second)
		fetchProfile("heap", *outDir, 0)
		fetchProfile("allocs", *outDir, 0)
		fetchProfile("goroutine", *outDir, 0)
		fetchProfile("mutex", *outDir, 0)
		fetchProfile("block", *outDir, 0)
		fetchProfile("profile", *outDir, min(*secs, 20))
		<-stopCh
		end := sample(pid)
		result["end"] = end
	}

	// persist JSON result
	rb, _ := json.MarshalIndent(result, "", " ")
	envTag := strings.ReplaceAll(*childEnv, ",", ";")
	oname := filepath.Join(*outDir, fmt.Sprintf("r_%s_%s_u%d_c%d_%s.json", *scenario, *label, *usersN, *connsN, envTag))
	os.WriteFile(oname, rb, 0644)
	log.Printf("[%s/%s] wrote %s", *label, *scenario, oname)
	runtime.GC()
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[len(b)-n:])
}

func min(a, b int) int { if a < b { return a }; return b }
