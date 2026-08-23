package format

import "testing"

func BenchmarkUserTag(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = UserTag("[https://example.com]-vless:1", "8f2c1e5a-9b3d-4e6f-a7b8-c9d0e1f2a3b4")
	}
}
