package tsdb

import (
	"testing"
	"time"
)

func TestCached(t *testing.T) {
	cached := newCached(4)

	for i := uint64(0); i < 12; i++ {
		cached.Update(&DBValue{i, nil})
	}

	for i := uint64(0); i < 8; i++ {
		if _, ok := cached.Get(i); ok {
			t.Fatalf("ring buff override check error :%d", i)
		}
	}

	for i := uint64(8); i < 12; i++ {
		if _, ok := cached.Get(i); !ok {
			t.Fatalf("ring buff check error :%d", i)
		}
	}
}

var cached = newCached(4)

func init() {
	for i := 0; i < 10; i++ {

		go func() {
			i := uint64(0)
			for _ = range time.Tick(10 * time.Millisecond) {

				cached.Update(&DBValue{i, nil})

				i++
			}
		}()

	}
}

func BenchmarkCachedUpdate(t *testing.B) {
	for i := 0; i < t.N; i++ {
		cached.Update(&DBValue{uint64(i), nil})
	}
}

func BenchmarkCachedGet(t *testing.B) {
	for i := 0; i < t.N; i++ {
		cached.Get(uint64(i))
	}
}
