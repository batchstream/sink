//go:build integration

package search_test

import (
	"net/http"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestSearchStorageLifecycleThroughIndexAlias(t *testing.T) {
	fixture := newIntegrationFixture(t)
	alias := fixture.index + "-alias"
	status, body := fixture.request(t, http.MethodPut, "/"+fixture.index+"/_alias/"+alias, nil)
	if status != http.StatusOK {
		t.Fatalf("create alias returned HTTP %d: %s", status, body)
	}
	address := fixture.datasetAddress(alias, "record")
	readOperation := storage.ReadOperation{Address: address}
	readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{readOperation}}
	missing, err := fixture.store.Read(t.Context(), readRequest)
	if err != nil || missing.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read(missing alias record) = %+v, %v", missing, err)
	}

	for _, kind := range []storage.PreconditionKind{storage.PreconditionRecordNotExists, storage.PreconditionRecordExists} {
		condition := storage.Precondition{Kind: kind}
		document := jsonStorageDocument(`{"value":"created"}`)
		if kind == storage.PreconditionRecordExists {
			document = jsonStorageDocument(`{"value":"replaced"}`)
		}
		operation := storage.WriteOperation{Address: address, Document: document, Precondition: condition}
		request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
		written, err := fixture.store.Write(t.Context(), request)
		if err != nil || written.Results[0].Status != storage.WriteStatusApplied {
			t.Fatalf("Write(alias, precondition %v) = %+v, %v", kind, written, err)
		}
		read, err := fixture.store.Read(t.Context(), readRequest)
		if err != nil || read.Results[0].Status != storage.ReadStatusFound {
			t.Fatalf("Read(alias, precondition %v) = %+v, %v", kind, read, err)
		}
		assertJSONEqual(t, read.Results[0].Document.Payload, document.Payload)
	}

	deleteOperation := storage.DeleteOperation{Address: address}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{deleteOperation}}
	for range 2 {
		deleted, err := fixture.store.Delete(t.Context(), deleteRequest)
		if err != nil || deleted.Results[0].Status != storage.DeleteStatusApplied {
			t.Fatalf("Delete(alias) = %+v, %v", deleted, err)
		}
	}
	missing, err = fixture.store.Read(t.Context(), readRequest)
	if err != nil || missing.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read(deleted alias record) = %+v, %v", missing, err)
	}
}
