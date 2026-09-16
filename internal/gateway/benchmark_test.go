package gateway

import (
	"fmt"
	"net"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// This measures the additional local routing hop with the same Engine/batcher.
// It is a latency/allocation baseline, not a deployment capacity qualification.
func BenchmarkGatewaySmallPut(b *testing.B) {
	engine := testEngine(b, "a", 4096)
	gateway := testGateway(b, 4096, engine)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	server := grpc.NewServer()
	sink.RegisterSinkServer(server, gateway)
	go func() { _ = server.Serve(listener) }()
	b.Cleanup(server.Stop)
	for _, target := range []struct{ name, address string }{{"direct", engine.target}, {"gateway", listener.Addr().String()}} {
		b.Run(target.name, func(b *testing.B) {
			conn, err := grpc.NewClient(target.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()
			client := sink.NewSinkClient(conn)
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false)}}
			if _, err := client.Write(b.Context(), request); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				response, err := client.Write(b.Context(), request)
				if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					b.Fatalf("write: %v %v", response, err)
				}
			}
		})
	}
}
