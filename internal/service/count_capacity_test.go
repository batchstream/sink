package service_test

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countCapacityStorage struct {
	nativeFixtureStorage
	entered chan storage.NativeRequest
	release chan struct{}
}

func (s *countCapacityStorage) Count(ctx context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	s.entered <- req.Request
	select {
	case <-s.release:
	case <-ctx.Done():
		var empty storage.CountResponse
		return empty, ctx.Err()
	}
	result := storage.CountResponse{Count: 123}
	return result, nil
}

func TestCountReservesBoundedResponsesAndReleasesCapacity(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/bson"} {
		t.Run(contentType, func(t *testing.T) {
			fixture := nativeFixtureStorage{Store: memory.New()}
			backend := &countCapacityStorage{nativeFixtureStorage: fixture, entered: make(chan storage.NativeRequest, 33), release: make(chan struct{})}
			luaOpts := merge.LuaOptions{}
			engine, err := merge.NewLuaEngine(luaOpts)
			if err != nil {
				t.Fatal(err)
			}
			opts := service.Options{Storage: backend, Lua: engine, StoreNames: []string{"primary"}, AdmissionWait: 10 * time.Millisecond}
			server, err := service.New(opts)
			if err != nil {
				t.Fatal(err)
			}
			command := &sink.Command{Store: "primary", Method: "POST", Path: "/products/_search", ContentType: contentType}
			want := 32 // Search counts can use every per-store request slot.
			if contentType == "application/bson" {
				fields := bson.D{{Key: "find", Value: "products"}}
				payload, err := bson.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				command.Method, command.Path = "", ""
				command.Namespace, command.Payload = "catalog", payload
				want = 5 // MongoDB still reserves its 48 MiB driver wire ceiling.
			}
			request := &sink.CountRequest{Command: command}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			finished := make(chan error, want)
			for range want {
				go func() { _, err := server.Count(ctx, request); finished <- err }()
				select {
				case admitted := <-backend.entered:
					if admitted.MaxBytes != 256<<10 {
						t.Fatalf("backend did not receive the Count buffer limit: %d", admitted.MaxBytes)
					}
				case err := <-finished:
					t.Fatalf("Count unnecessarily exhausted capacity: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if _, err := server.Count(ctx, request); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("Count bypassed concurrency or driver buffer limits: %v", err)
			}
			close(backend.release)
			for range want {
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
			}
			response, err := server.Count(ctx, request)
			if err != nil || response.GetCount() != 123 {
				t.Fatalf("Count capacity was not released: %v, %v", response, err)
			}
		})
	}
}

func TestCountRetainsSmallerConfiguredResponseBudget(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.counts = make(chan storage.CountRequest, 1)
	request := &sink.CountRequest{Command: nativeSearchRequest().Command}
	if _, err := client.Count(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if request := <-backend.counts; request.Request.MaxBytes != 4096 {
		t.Fatalf("Count expanded the configured response budget: %d", request.Request.MaxBytes)
	}
}
