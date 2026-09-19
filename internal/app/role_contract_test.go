package app

import (
	"context"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type deadlineReadStorage struct {
	storage.Storage
	seen    chan context.Context
	release chan struct{}
}

func (s *deadlineReadStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.seen <- ctx
	select {
	case <-s.release:
		return s.Storage.Read(ctx, req)
	case <-ctx.Done():
		var empty storage.ReadResponse
		return empty, ctx.Err()
	}
}

func TestEngineOnlyAcceptsForwardingAndHonorsGatewayContext(t *testing.T) {
	shared := "name: primary\nstorage: {driver: mongodb, mongodb: {uri: 'mongodb://127.0.0.1:1'}}"
	loaded, err := config.Decode(strings.NewReader("mode: engine\ngrpc: {address: '127.0.0.1:0'}"), strings.NewReader(shared))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := newMemory(loaded)
	if err != nil {
		t.Fatal(err)
	}
	backend := &deadlineReadStorage{Storage: memory.New(), seen: make(chan context.Context, 2), release: make(chan struct{})}
	engine := &Application{config: loaded, memory: pool, storage: backend}
	core, err := engine.newService(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.configureServer(core, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	go engine.grpcServer.Serve(engine.listener)
	engineConn, err := grpc.NewClient(engine.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer engineConn.Close()
	direct := sink.NewSinkClient(engineConn)
	empty := &sink.ReadRequest{}
	if _, err := direct.Read(t.Context(), empty); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Engine exposes public RPC: %v", err)
	}
	component := "mode: gateway\ngrpc: {address: '127.0.0.1:0'}\nhealth: {address: '127.0.0.1:0'}\nrequest: {max_operations: 2000}\nforwarding:\n  routes: [{store: primary, target: '" + engine.listener.Addr().String() + "', tls: {insecure: true}}]"
	gatewayConfig, err := config.Decode(strings.NewReader(component), nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Config: gatewayConfig}
	gateway, err := New(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gateway.Close)
	go gateway.grpcServer.Serve(gateway.listener)
	conn, err := grpc.NewClient(gateway.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := sink.NewSinkClient(conn)
	addr := &sink.RecordAddress{Uri: "sink://primary/db/items/s:key"}
	operation := &sink.ReadOperation{Address: addr}
	request := &sink.ReadRequest{}
	for range 1500 {
		request.Operations = append(request.Operations, operation)
	}
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Read(caller, request); done <- err }()
	var observed context.Context
	select {
	case observed = <-backend.seen:
	case <-time.After(5 * time.Second):
		t.Fatal("Gateway request did not reach Engine")
	}
	if deadline, ok := observed.Deadline(); ok {
		t.Fatalf("hidden execution deadline: %v", deadline)
	}
	cancel()
	select {
	case <-observed.Done():
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not reach storage")
	}
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation result: %v", err)
	}
	close(backend.release)
	// More than 1000 operations in one Store is valid when Gateway permits it.
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	response, err := client.Read(ctx, request)
	if err != nil || len(response.GetResults()) != 1500 {
		t.Fatalf("Engine reintroduced an operation cap: count=%d err=%v", len(response.GetResults()), err)
	}
}
