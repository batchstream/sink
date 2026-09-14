package mongodb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

func TestClientBulkDiscoveryCannotUndoCommandRejection(t *testing.T) {
	hello := bson.D{{Key: "ok", Value: 1}, {Key: "maxWireVersion", Value: 25}}
	rejected := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 59}, {Key: "errmsg", Value: "no such command: bulkWrite"}}
	unmatched := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "nModified", Value: 0}}
	deployment := drivertest.NewMockDeployment(hello, hello, rejected, unmatched, unmatched)
	started := make(chan struct{})
	release := make(chan struct{})
	var hellos, bulks, updates atomic.Int32
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, command *event.CommandStartedEvent) {
			switch command.CommandName {
			case "bulkWrite":
				bulks.Add(1)
			case "update":
				updates.Add(1)
			}
		},
		Succeeded: func(ctx context.Context, command *event.CommandSucceededEvent) {
			if command.CommandName == "hello" && hellos.Add(1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		},
	}
	clientOptions := options.Client().SetMonitor(monitor)
	// The driver's wire fixture keeps this concurrency regression offline.
	//lint:ignore SA1019 Deployment is required by the v2 driver's offline wire fixture.
	clientOptions.Deployment = deployment
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	storeOptions := Options{Store: "primary", MaxConcurrentWrites: 1, MaxConcurrentGroups: 1}
	store, err := New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	discovered := make(chan bool, 1)
	go func() { discovered <- store.supportsClientBulk(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("capability discovery did not start")
	}

	// A newer probe succeeds, but the actual write command is unavailable.
	// Keep the first probe suspended until the legacy fallback has finished.
	request := storage.WriteRequest{}
	value := bson.D{{Key: "value", Value: 1}}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"first", "second"} {
		address := storage.Address{Store: "primary", Namespace: "test", Dataset: "documents", Key: storage.Key{Type: "string", Data: []byte(key)}}
		document := storage.Document{Encoding: storage.DocumentEncodingBSON, Payload: payload}
		precondition := storage.Precondition{Kind: storage.PreconditionRecordExists}
		operation := storage.WriteOperation{Address: address, Document: document, Precondition: precondition}
		request.Operations = append(request.Operations, operation)
	}
	written, err := store.Write(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusPreconditionFailed {
			t.Fatalf("legacy fallback failed: %+v", result)
		}
	}
	if bulks.Load() != 1 || updates.Load() != 2 || store.clientBulkCapability.Load() != clientBulkUnavailable {
		t.Fatalf("command rejection did not disable bulk writes: bulks=%d updates=%d capability=%d", bulks.Load(), updates.Load(), store.clientBulkCapability.Load())
	}
	close(release)
	select {
	case supported := <-discovered:
		if supported || store.clientBulkCapability.Load() != clientBulkUnavailable {
			t.Fatal("stale hello response re-enabled a rejected bulkWrite command")
		}
	case <-ctx.Done():
		t.Fatal("stale discovery did not finish")
	}
}
