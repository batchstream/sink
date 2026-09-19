package gateway

import (
	"fmt"
	"testing"
)

func BenchmarkAffinityRoute(b *testing.B) {
	for _, replicas := range []int{1, 3, 16, 64, 256} {
		b.Run(fmt.Sprintf("replicas-%d", replicas), func(b *testing.B) {
			routes := make([]Route, replicas)
			for i := range routes {
				routes[i].endpoint = fmt.Sprintf("10.0.%d.%d:8080", i/256, i%256)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = affinityRoute("sink://catalog/products/items/s:example-record", routes)
			}
		})
	}
}
