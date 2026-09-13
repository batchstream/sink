//go:build integration

package mongodb_test

import (
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestRecordOperationsKeepByteIdentityWithCollectionCollation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	ctx := t.Context()
	collation := &options.Collation{Locale: "en", Strength: 2}
	create := options.CreateCollection().SetCollation(collation)
	if err := fixture.client.Database(fixture.database).CreateCollection(ctx, "documents", create); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"ABC", "OTHER"} {
		document := bson.D{{Key: "_id", Value: id}, {Key: "value", Value: "original"}}
		if _, err := fixture.collection.InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
	}
	read := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: fixture.address("abc")}, {Address: fixture.address("ABC")}}}
	response, err := fixture.store.Read(ctx, read)
	if err != nil || response.Results[0].Status != storage.ReadStatusNotFound || response.Results[1].Status != storage.ReadStatusFound {
		t.Fatalf("read=%+v err=%v", response, err)
	}
	tests := []struct {
		name      string
		condition storage.PreconditionKind
		count     int
		size      int
	}{
		{name: "replace single", condition: storage.PreconditionRecordExists, count: 1},
		{name: "replace bulk", condition: storage.PreconditionRecordExists, count: 2},
		{name: "replace pipeline", condition: storage.PreconditionRecordExists, count: 2, size: 20 << 10},
		{name: "legacy CAS single", condition: storage.PreconditionRevisionAbsent, count: 1},
		{name: "legacy CAS bulk", condition: storage.PreconditionRevisionAbsent, count: 2},
		{name: "legacy CAS pipeline", condition: storage.PreconditionRevisionAbsent, count: 2, size: 20 << 10},
		{name: "upsert and duplicate retry", condition: storage.PreconditionNone, count: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := storage.WriteRequest{}
			for _, key := range []string{"abc", "other"}[:test.count] {
				value := bson.D{{Key: "value", Value: "wrong"}, {Key: "padding", Value: strings.Repeat("x", test.size)}}
				document := bsonStorageDocument(t, value)
				condition := storage.Precondition{Kind: test.condition}
				operation := storage.WriteOperation{Address: fixture.address(key), Document: document, Precondition: condition}
				request.Operations = append(request.Operations, operation)
			}
			written, err := fixture.store.Write(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range written.Results {
				if result.Status == storage.WriteStatusApplied {
					t.Fatalf("different-case write applied: %+v", result)
				}
				if test.condition != storage.PreconditionNone && result.Status != storage.WriteStatusPreconditionFailed {
					t.Fatalf("different-case record must not match a precondition: %+v", result)
				}
			}
		})
	}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{{Address: fixture.address("abc")}}}
	deleted, err := fixture.store.Delete(ctx, deleteRequest)
	if err != nil || deleted.Results[0].Status != storage.DeleteStatusApplied {
		t.Fatalf("delete=%+v err=%v", deleted, err)
	}
	filter := bson.D{{Key: "value", Value: "original"}}
	count, err := fixture.collection.CountDocuments(ctx, filter)
	if err != nil || count != 2 {
		t.Fatalf("different-case operations changed documents: count=%d err=%v", count, err)
	}
	// Native commands keep the caller's database semantics, including default
	// collation. Only the storage-independent record API fixes binary identity.
	update := bson.D{{Key: "q", Value: bson.D{{Key: "_id", Value: "abc"}}},
		{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "native", Value: true}}}}}}
	command := bson.D{{Key: "update", Value: "documents"}, {Key: "updates", Value: bson.A{update}}}
	native := mongoNativeRequest(t, fixture.database, command)
	updated, err := fixture.store.Execute(ctx, native)
	if err != nil || !updated.Success || bson.Raw(updated.Payload).Lookup("n").AsInt64() != 1 {
		t.Fatalf("native collation changed: response=%+v err=%v", updated, err)
	}
	deleteRequest.Operations[0].Address = fixture.address("ABC")
	deleted, err = fixture.store.Delete(ctx, deleteRequest)
	if err != nil || deleted.Results[0].Status != storage.DeleteStatusApplied {
		t.Fatalf("exact delete=%+v err=%v", deleted, err)
	}
	emptyFilter := bson.D{}
	count, err = fixture.collection.CountDocuments(ctx, emptyFilter)
	if err != nil || count != 1 {
		t.Fatalf("exact delete count=%d err=%v", count, err)
	}
}
