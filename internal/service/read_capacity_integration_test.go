//go:build integration

package service

import (
	"encoding/json"
	"fmt"
	"github.com/liran/sink/internal/capacity"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
)

func TestReadMicrobatchStorageWorkingSet(t *testing.T) {
	for _, driver := range []string{"mongodb", "opensearch"} {
		t.Run(driver, func(t *testing.T) {
			fixture := newSyncCapacityFixture(t, driver)
			backend := &readCapacityStorage{Storage: fixture.backend, maximum: 2048}
			server := completionServer(t, backend)
			server.server.maxReadBytes = 2048
			memoryOptions := capacity.Options{Bytes: 128 << 20, BurstPercent: 10, WaitTimeout: time.Second}
			pool, err := capacity.New(memoryOptions)
			if err != nil {
				t.Fatal(err)
			}
			server.server.memory = pool
			seed := storage.WriteRequest{}
			calls := make([]*batchCall[*sink.ReadRequest, *sink.ReadResponse], 12)
			for index := range calls {
				key := fmt.Sprintf("record-%d", index)
				scope := pool.NewScope()
				ctx := capacity.WithScope(t.Context(), scope)
				if err := scope.Admit(ctx, 4096); err != nil {
					t.Fatal(err)
				}
				call := readCapacityCall(ctx, key)
				address := call.request.Operations[0].Address
				address.Uri = fixture.recordURI(address)
				converted, err := protocol.ParseAddress(address)
				if err != nil {
					t.Fatal(err)
				}
				value := map[string]any{"value": index, "padding": strings.Repeat("x", 700)}
				wireDocument := syncCapacityDocument(t, fixture.encoding, value)
				document := storage.Document{Encoding: storage.DocumentEncoding(wireDocument.Encoding), Payload: wireDocument.Payload}
				operation := storage.WriteOperation{Address: converted, Document: document}
				seed.Operations = append(seed.Operations, operation)
				calls[index] = call
			}
			written, err := fixture.backend.Write(t.Context(), seed)
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range written.Results {
				if result.Status != storage.WriteStatusApplied {
					t.Fatal(result)
				}
			}
			server.executeReads(t.Context(), calls)
			for index, call := range calls {
				result := awaitCompletion(t, call.result)
				if result.err != nil || result.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
					t.Fatalf("record %d: %v, %v", index, result.response, result.err)
				}
				if result.response.Results[0].Document.Encoding != fixture.encoding {
					t.Fatal("lost encoding")
				}
				var value struct {
					Value int `json:"value" bson:"value"`
				}
				document := result.response.Results[0].Document
				if fixture.encoding == sink.DocumentEncoding_DOCUMENT_ENCODING_BSON {
					err = bson.Unmarshal(document.Payload, &value)
				} else {
					err = json.Unmarshal(document.Payload, &value)
				}
				if err != nil || value.Value != index {
					t.Fatalf("caller %d got value %d: %v", index, value.Value, err)
				}
			}
			if backend.reads.Load() != 6 {
				t.Fatalf("expected 6 bounded chunks, got %d", backend.reads.Load())
			}
			if pool.Used() == 0 {
				t.Fatal("response bytes released before original callers finished")
			}
			for _, call := range calls {
				capacity.FromContext(call.ctx).Release()
			}
			if pool.Used() != 0 {
				t.Fatalf("managed bytes leaked: %d", pool.Used())
			}
			if server.server.inFlightBytes != 0 || server.server.inFlightRequests != 0 {
				t.Fatal("read admission leaked")
			}
		})
	}
}
