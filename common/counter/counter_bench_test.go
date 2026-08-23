package counter

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func benchUUID(i int) string {
	return "8f2c1e5a-9b3d-4e6f-a7b8-" + strconv.Itoa(i)
}

func BenchmarkTrafficRxTx(b *testing.B) {
	c := NewTrafficCounter()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := benchUUID(i % 1000)
		c.Rx(id, 1400)
		c.Tx(id, 1400)
	}
}

func BenchmarkTrafficGetCounter(b *testing.B) {
	c := NewTrafficCounter()
	for i := 0; i < 1000; i++ {
		c.GetCounter(benchUUID(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetCounter(benchUUID(i % 1000))
	}
}

func BenchmarkConnCounterReadWrite(b *testing.B) {
	c := NewTrafficCounter()
	s := c.GetCounter("bench-user")
	conn := NewConnCounter(&nopConn{}, s)
	buf := make([]byte, 1400)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = conn.Read(buf)
		_, _ = conn.Write(buf)
	}
}

// nopConn is a minimal net.Conn used to measure wrapper overhead only.
type nopConn struct{}

func (nopConn) Read(b []byte) (int, error)         { return len(b), nil }
func (nopConn) Write(b []byte) (int, error)        { return len(b), nil }
func (nopConn) Close() error                       { return nil }
func (nopConn) LocalAddr() net.Addr                { return nopAddr{} }
func (nopConn) RemoteAddr() net.Addr               { return nopAddr{} }
func (nopConn) SetDeadline(t time.Time) error      { return nil }
func (nopConn) SetReadDeadline(t time.Time) error  { return nil }
func (nopConn) SetWriteDeadline(t time.Time) error { return nil }

type nopAddr struct{}

func (nopAddr) Network() string { return "nop" }
func (nopAddr) String() string  { return "127.0.0.1:0" }
