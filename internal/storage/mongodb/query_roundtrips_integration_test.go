//go:build integration

package mongodb_test

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoQueryFetchesLookaheadInFirstBatch(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := []any{}
	for index := range 3 {
		document := bson.D{{Key: "_id", Value: index}}
		documents = append(documents, document)
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	var queries, more atomic.Int32
	monitor := &event.CommandMonitor{Started: func(_ context.Context, command *event.CommandStartedEvent) {
		switch command.CommandName {
		case "find", "aggregate":
			queries.Add(1)
		case "getMore":
			more.Add(1)
		}
	}}
	clientOptions := options.Client().ApplyURI(os.Getenv(mongodbTestURI)).SetMonitor(monitor)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	storeOptions := mongodb.Options{Store: "primary"}
	backend, err := mongodb.New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	find := bson.D{{Key: "find", Value: "documents"}}
	aggregate := bson.D{{Key: "aggregate", Value: "documents"}, {Key: "pipeline", Value: bson.A{}}}
	for _, command := range []bson.D{find, aggregate} {
		for _, offset := range []int64{0, 1, 2, 3} {
			queries.Store(0)
			more.Store(0)
			native := mongoNativeRequest(t, fixture.database, command)
			request := storage.QueryRequest{Request: native, Offset: offset, PageSize: 2, Sort: []storage.SortField{{Field: "_id"}}}
			page, err := backend.Query(t.Context(), request)
			if err != nil || len(page.Documents) != min(2, 3-int(offset)) || page.HasMore != (offset == 0) {
				t.Fatalf("%s offset=%d page=%+v error=%v", command[0].Key, offset, page, err)
			}
			for index, document := range page.Documents {
				if got := bson.Raw(document.Payload).Lookup("_id").AsInt64(); got != offset+int64(index) {
					t.Fatalf("wrong document: %s", document.Payload)
				}
			}
			if queries.Load() != 1 || more.Load() != 0 {
				t.Fatalf("%s offset=%d required %d queries and %d getMore calls", command[0].Key, offset, queries.Load(), more.Load())
			}
		}
	}
	// A first batch split by MongoDB's byte limit must still fetch the
	// lookahead, without charging it to the returned-document budget.
	filter := bson.D{}
	fields := bson.D{{Key: "large", Value: strings.Repeat("x", 6<<20)}}
	update := bson.D{{Key: "$set", Value: fields}}
	if _, err := fixture.collection.UpdateMany(t.Context(), filter, update); err != nil {
		t.Fatal(err)
	}
	for _, command := range []bson.D{find, aggregate} {
		queries.Store(0)
		more.Store(0)
		native := mongoNativeRequest(t, fixture.database, command)
		native.MaxBytes = 13 << 20
		request := storage.QueryRequest{Request: native, PageSize: 2, Sort: []storage.SortField{{Field: "_id"}}}
		page, err := backend.Query(t.Context(), request)
		if err != nil || len(page.Documents) != 2 || !page.HasMore || queries.Load() != 1 || more.Load() != 1 {
			t.Fatalf("%s byte-limited page: documents=%d more=%t queries=%d getMore=%d error=%v",
				command[0].Key, len(page.Documents), page.HasMore, queries.Load(), more.Load(), err)
		}
	}
}
