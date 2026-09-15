package service

import (
	"testing"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPublishAndExecutionEnforceIndependentStoreRequestLimits(t *testing.T) {
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	for _, executionLimit := range []int{1, 2} {
		publishLimit := 3 - executionLimit
		opts := Options{Storage: memory.New(), Lua: lua, StoreNames: []string{"primary"}, MaxStoreRequests: executionLimit, MaxPublishStoreRequests: publishLimit}
		server, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, publish := range []bool{false, true} {
			limit := executionLimit
			if publish {
				limit = publishLimit
			}
			request := admissionRequest{encodedBytes: 1, stores: []string{"primary"}, publish: publish}
			for range limit {
				_, release, err := server.admitRequest(t.Context(), request)
				if err != nil {
					t.Fatalf("publish=%v rejected before its own limit: %v", publish, err)
				}
				t.Cleanup(release)
			}
			_, release, err := server.admitRequest(t.Context(), request)
			if release != nil {
				release()
			}
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("publish=%v exceeded its own store limit: %v", publish, err)
			}
		}
	}
}
