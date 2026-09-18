package capacity

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Run explicitly; regular correctness tests do not depend on timing rankings.
func TestBurstExperiment(t *testing.T) {
	if os.Getenv("SINK_MEMORY_EXPERIMENT") != "1" {
		t.Skip("opt-in load experiment")
	}
	profiles := []struct {
		name     string
		input    int64
		response int64
		mixed    bool
	}{
		{"small", 2 << 10, 8 << 10, false},
		{"mixed", 128 << 10, 3 << 20, true},
		{"large-completion", 512 << 10, 3 << 20, false},
	}
	fmt.Println("profile,burst_percent,accepted,completed,response_failed,elapsed_ms,peak_managed_mib")
	for _, profile := range profiles {
		for _, percent := range []int{5, 10, 15, 20, 25} {
			for range 3 {
				runtime.GC()
				opts := Options{Bytes: 32 << 20, BurstPercent: percent, WaitTimeout: 20 * time.Millisecond}
				p, err := New(opts)
				if err != nil {
					t.Fatal(err)
				}
				var started, done sync.WaitGroup
				var accepted, completed, failed atomic.Int64
				var peak atomic.Int64
				gate := make(chan struct{})
				begin := time.Now()
				for i := range 96 {
					started.Add(1)
					done.Go(func() {
						l := p.NewOwner().NewLease()
						if l.Grow(context.Background(), profile.input, Request) != nil {
							started.Done()
							return
						}
						accepted.Add(1)
						for {
							old := peak.Load()
							used := p.Used()
							if used <= old || peak.CompareAndSwap(old, used) {
								break
							}
						}
						input := make([]byte, int(profile.input))
						input[len(input)-1] = 1
						started.Done()
						<-gate
						response := profile.response
						if profile.mixed && i%10 != 0 {
							response = 8 << 10
						}
						if l.Grow(context.Background(), response, Response) == nil {
							buffer := make([]byte, int(response))
							buffer[len(buffer)-1] = 2
							for {
								old := peak.Load()
								used := p.Used()
								if used <= old || peak.CompareAndSwap(old, used) {
									break
								}
							}
							time.Sleep(200 * time.Microsecond)
							runtime.KeepAlive(buffer)
							completed.Add(1)
						} else {
							failed.Add(1)
						}
						runtime.KeepAlive(input)
						l.Close()
					})
				}
				started.Wait()
				close(gate)
				done.Wait()
				fmt.Printf("%s,%d,%d,%d,%d,%.3f,%.3f\n", profile.name, percent, accepted.Load(), completed.Load(), failed.Load(), float64(time.Since(begin).Microseconds())/1000, float64(peak.Load())/(1<<20))
				assertEmpty(t, p)
			}
		}
	}
}
