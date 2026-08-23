package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/InazumaV/V2bX/e2e/panel"
)

func main() {
	port := flag.Int("port", 18081, "listen port")
	users := flag.Int("users", 1000, "user count")
	nodeP := flag.Int("node_port", 17080, "proxied node port")
	flag.Parse()
	m := panel.NewMockPanel(*port, *users)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", *port), m.Handler(*nodeP)))
}
