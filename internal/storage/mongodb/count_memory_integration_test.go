//go:build integration

package mongodb_test

import (
	"testing"
	"time"

	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCountFindReleasesConsumedPageMemory(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := make([]any, 10000)
	for i := range documents {
		doc := bson.D{{Key: "number", Value: i}}
		documents[i] = doc
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "$where", Value: "this.number >= 0"}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}}
	native := mongoNativeRequest(t, fixture.database, find)
	request := storage.CountRequest{Request: native}
	control, err := fixture.store.Count(t.Context(), request)
	if err != nil || control.Count != uint64(len(documents)) {
		t.Fatalf("control: count=%d err=%v", control.Count, err)
	}
	options := capacity.Options{Bytes: 50 << 20, BurstPercent: 10, WaitTimeout: 20 * time.Millisecond}
	pool, err := capacity.New(options)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	defer scope.Release()
	ctx := capacity.WithScope(t.Context(), scope)
	result, err := fixture.store.Count(ctx, request)
	t.Logf("controlCount=%d managedCount=%d poolLimit=%d retainedAfterDriverRelease=%d err=%v", control.Count, result.Count, pool.Limit(), pool.Used(), err)
	if err != nil || result.Count != control.Count {
		t.Fatalf("bounded-page Count accumulated discarded output and failed")
	}
	if pool.Used() != 0 {
		t.Fatalf("completed count retained internal pages: %d", pool.Used())
	}
	request.Request.MaxBytes = 256
	result, err = fixture.store.Count(ctx, request)
	if err != nil || result.Count != control.Count || pool.Used() != 0 {
		t.Fatalf("byte-limited count pages were not released: count=%d retained=%d err=%v", result.Count, pool.Used(), err)
	}
}
