package dispatcher

import (
	"regexp"
	"sync"
	"testing"
)

const benchPattern = ".*\\.(googlevideo|youtube|ytimg)\\.com$"

var benchDomain = "rr4---sn-uxa-xxxx.googlevideo.com"

// Baseline: what shouldOverride pays today per connection when an
// ExcludeForDomain entry is a regexp pattern (regexp.Compile per call).
func BenchmarkRegexpCompilePerConn(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		re, err := regexp.Compile(benchPattern)
		if err != nil {
			b.Fatal(err)
		}
		if !re.MatchString(benchDomain) {
			b.Fatal("expected match")
		}
	}
}

// What a cached version costs.
func BenchmarkRegexpCached(b *testing.B) {
	var cache sync.Map
	cache.Store(benchPattern, regexp.MustCompile(benchPattern))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := cache.Load(benchPattern)
		if !ok {
			b.Fatal("not cached")
		}
		if !v.(*regexp.Regexp).MatchString(benchDomain) {
			b.Fatal("expected match")
		}
	}
}
