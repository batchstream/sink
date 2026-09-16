//go:build integration

package gateway

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc"
)

func TestGatewayDNSWithdrawalDrainBoundary(t *testing.T) {
	for _, scenario := range []string{"withdraw-before-stop", "stop-before-refresh", "dns-outage-during-stop", "stale-cache-during-stop", "scale-to-zero"} {
		t.Run(scenario, func(t *testing.T) {
			first, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = first.Close() })
			_, port, err := net.SplitHostPort(first.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			second, err := net.Listen("tcp6", net.JoinHostPort("::1", port))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = second.Close() })
			var servers []*grpc.Server
			var backends []*controlledEngine
			for _, listener := range []net.Listener{first, second} {
				server := grpc.NewServer()
				backend := &controlledEngine{store: "a"}
				forward.RegisterEngineServer(server, backend)
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(server.Stop)
				servers = append(servers, server)
				backends = append(backends, backend)
			}
			dnsAddress, state := startScalingDNS(t)
			fixture := fixtureEngine{store: "a", target: "dns://" + dnsAddress + "/" + net.JoinHostPort("engine.test", port)}
			gateway := testGateway(t, 4096, fixture)
			// All initial requests go to the first Engine. A stable key makes a
			// stale owner deterministic, rather than sampling random round robin.
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "drain-key", false)}}
			attempts, successes, failures := 0, 0, 0
			write := func() bool {
				attempts++
				ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
				defer cancel()
				response, err := gateway.Write(ctx, request)
				if err == nil && response.Results[0].Status == sink.WriteStatus_WRITE_STATUS_APPLIED {
					successes++
					return true
				}
				failures++
				if err == nil && response.Results[0].GetFailure().GetRetryable() && scenario != "scale-to-zero" {
					t.Fatal("unknown mutation outcome was advertised as safe to replay")
				}
				return false
			}
			if !write() {
				t.Fatal("initial write failed")
			}
			switch scenario {
			case "withdraw-before-stop":
				state.stage.Store(2)
				deadline := time.Now().Add(5 * time.Second)
				for backends[1].calls.Load() == 0 {
					if !write() || time.Now().After(deadline) {
						t.Fatal("failed to drain before stopping")
					}
					time.Sleep(10 * time.Millisecond)
				}
			case "dns-outage-during-stop":
				state.stage.Store(3)
			case "scale-to-zero":
				state.stage.Store(4)
				servers[1].Stop()
			}
			servers[0].GracefulStop()
			if scenario != "withdraw-before-stop" {
				if write() {
					t.Fatal("stale endpoint unexpectedly accepted a new RPC after shutdown")
				}
			}
			if scenario == "stop-before-refresh" {
				// Publish the replacement only after observing the failed call.
				// Scheduler delays must not let a refresh hide this boundary.
				state.stage.Store(2)
			}
			if scenario == "dns-outage-during-stop" || scenario == "stale-cache-during-stop" || scenario == "scale-to-zero" {
				deadline := time.Now().Add(1200 * time.Millisecond)
				for time.Now().Before(deadline) {
					if write() {
						t.Fatal("unavailable membership unexpectedly accepted work")
					}
					time.Sleep(20 * time.Millisecond)
				}
				if scenario == "scale-to-zero" {
					// Restore the second Engine at a new process on the same endpoint.
					listener, err := net.Listen("tcp6", second.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					server := grpc.NewServer()
					forward.RegisterEngineServer(server, backends[1])
					go func() { _ = server.Serve(listener) }()
					t.Cleanup(server.Stop)
				}
				state.stage.Store(2)
			}
			deadline := time.Now().Add(10 * time.Second)
			for backends[1].calls.Load() == 0 {
				write()
				if time.Now().After(deadline) {
					t.Fatal("did not recover after DNS membership recovery")
				}
				time.Sleep(20 * time.Millisecond)
			}
			for range 100 {
				if !write() {
					t.Fatal("write failed after recovered membership")
				}
			}
			calls := int(backends[0].calls.Load() + backends[1].calls.Load())
			if calls != successes || attempts != successes+failures {
				t.Fatalf("hidden replay or lost reply: calls=%d successes=%d failures=%d attempts=%d", calls, successes, failures, attempts)
			}
			if scenario == "withdraw-before-stop" && failures != 0 {
				t.Fatal("sufficient drain lost requests")
			}
			t.Log(fmt.Sprintf("attempts=%d successes=%d expected_failures=%d backend_calls=%d", attempts, successes, failures, calls))
		})
	}
}
