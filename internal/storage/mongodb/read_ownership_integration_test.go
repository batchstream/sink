//go:build integration

package mongodb_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoReadOwnershipAcrossCursorBatches(t *testing.T) {
	fixture := newIntegrationFixture(t)
	writes := storage.WriteRequest{}
	reads := storage.ReadRequest{}
	// More than the default first cursor batch exercises getMore and buffer reuse.
	for i := range 110 {
		address := fixture.address(fmt.Sprintf("document-%03d", i))
		value := bson.D{{Key: "value", Value: int32(i)}}
		document := bsonStorageDocument(t, value)
		write := storage.WriteOperation{Address: address, Document: document}
		writes.Operations = append(writes.Operations, write)
		read := storage.ReadOperation{Address: address}
		reads.Operations = append(reads.Operations, read)
	}
	written, err := fixture.store.Write(t.Context(), writes)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatalf("write: %+v", result)
		}
	}
	reads.Operations = append(reads.Operations, reads.Operations[0])
	first, err := fixture.store.Read(t.Context(), reads)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range first.Results {
		if result.Status != storage.ReadStatusFound {
			t.Fatalf("read %d: %+v", i, result)
		}
		want := i % len(writes.Operations)
		if bson.Raw(result.Document.Payload).Lookup("value").Int32() != int32(want) || !bytes.Equal(result.Revision.Data, written.Results[want].Revision.Data) {
			t.Fatalf("read %d changed across cursor batches: %+v", i, result)
		}
	}
	retained := first.Results[len(first.Results)-1]
	original := bytes.Clone(retained.Document.Payload)
	revision := bytes.Clone(retained.Revision.Data)
	first.Results[0].Document.Payload[0] = 0
	first.Results[0].Revision.Data[0] ^= 1
	if !bytes.Equal(retained.Document.Payload, original) || !bytes.Equal(retained.Revision.Data, revision) {
		t.Fatal("duplicate reads share mutable storage")
	}
	second, err := fixture.store.Read(t.Context(), reads)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second.Results[0].Document.Payload, original) || !bytes.Equal(second.Results[0].Revision.Data, revision) {
		t.Fatal("later read reused mutated bytes")
	}
	second.Results[0].Document.Payload[0] = 0
	second.Results[0].Revision.Data[0] ^= 1
	if !bytes.Equal(retained.Document.Payload, original) || !bytes.Equal(retained.Revision.Data, revision) {
		t.Fatal("later read modified a retained earlier result")
	}
}
