package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/testuri"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

func newReadWireFixture(t testing.TB) (*Store, *drivertest.MockDeployment) {
	t.Helper()
	deployment := drivertest.NewMockDeployment()
	clientOptions := options.Client()
	//lint:ignore SA1019 Deployment is required by the v2 driver's offline wire fixture.
	clientOptions.Deployment = deployment
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	storeOptions := Options{Store: "primary"}
	store, err := New(client, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	return store, deployment
}

func readWireReply(document bson.D) bson.D {
	cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "test.documents"}, {Key: "firstBatch", Value: bson.A{document}}}
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
	return reply
}

func TestMongoReadResultsOwnTheirDocuments(t *testing.T) {
	for _, rejectFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("reject-first-%t", rejectFirst), func(t *testing.T) {
			store, deployment := newReadWireFixture(t)
			revision := []byte{1, 2, 3}
			metadata := bson.D{{Key: "revision", Value: bson.Binary{Data: revision}}}
			document := bson.D{{Key: "_id", Value: "one"}, {Key: "text", Value: "value"}, {Key: defaultMetadataField, Value: metadata}}
			reply := readWireReply(document)
			key := storage.Key{Type: "string", Data: []byte("one")}
			address := testuri.Address("primary", []string{"test", "documents"}, key)
			operation := storage.ReadOperation{Address: address}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{operation, operation, operation}}
			if rejectFirst {
				request.Operations[0].Budget = storage.NewReadBudget(1)
			}
			deployment.AddResponses(reply)
			first, err := store.Read(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			start := 0
			if rejectFirst {
				if first.Results[0].Status != storage.ReadStatusFailed || len(first.Results[0].Document.Payload) != 0 {
					t.Fatalf("read ignored the first operation's budget: %+v", first.Results[0])
				}
				start = 1
			}
			for _, result := range first.Results[start:] {
				if result.Status != storage.ReadStatusFound || !bytes.Equal(result.Revision.Data, revision) {
					t.Fatalf("read result: %+v", result)
				}
			}
			retained := first.Results[start+1]
			original := bytes.Clone(retained.Document.Payload)
			first.Results[start].Document.Payload[0] = 0
			first.Results[start].Revision.Data[0] = 9
			if !bytes.Equal(retained.Document.Payload, original) || !bytes.Equal(retained.Revision.Data, revision) {
				t.Fatal("duplicate reads share mutable storage")
			}
			deployment.AddResponses(reply)
			second, err := store.Read(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(second.Results[start].Document.Payload, original) || !bytes.Equal(second.Results[start].Revision.Data, revision) {
				t.Fatal("later read reused mutated bytes")
			}
			second.Results[start].Document.Payload[0] = 0
			second.Results[start].Revision.Data[0] = 9
			if !bytes.Equal(retained.Document.Payload, original) || !bytes.Equal(retained.Revision.Data, revision) {
				t.Fatal("later read modified a retained earlier result")
			}
		})
	}
}

func BenchmarkMongoReadDocument(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes-%d", size), func(b *testing.B) {
			store, deployment := newReadWireFixture(b)
			document := bson.D{{Key: "_id", Value: "one"}, {Key: "text", Value: strings.Repeat("x", size)}}
			reply := readWireReply(document)
			key := storage.Key{Type: "string", Data: []byte("one")}
			address := testuri.Address("primary", []string{"test", "documents"}, key)
			operation := storage.ReadOperation{Address: address}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				deployment.AddResponses(reply)
				response, err := store.Read(b.Context(), request)
				if err != nil || response.Results[0].Status != storage.ReadStatusFound {
					b.Fatalf("read: %+v %v", response, err)
				}
			}
		})
	}
}
