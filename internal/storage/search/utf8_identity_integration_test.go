//go:build integration

package search_test

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/service"
)

func TestSearchInvalidUTF8CannotOverwriteUnicodeKey(t *testing.T) {
	fixture := newIntegrationFixture(t)
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: fixture.store, Lua: engine}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	codec := protocol.NewVTProtoCodec()
	valid := fixture.sinkAddress("\uFFFD")
	invalid := fixture.sinkAddress(string([]byte{0xff}))
	binary := fixture.sinkAddress("")
	kind := &sink.RecordKey_BytesValue{BytesValue: []byte{0xff}}
	binary.Key.Kind = kind
	for i, address := range []*sink.RecordAddress{valid, invalid, binary} {
		value := `{"value":"original"}`
		if i > 0 {
			value = `{"value":"other"}`
		}
		put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: sinkDocument(value)}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Address: address, Action: action}
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
		encoded, err := codec.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var decoded sink.WriteRequest
		err = codec.Unmarshal(encoded, &decoded)
		encoded.Free()
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.Write(t.Context(), &decoded)
		if err != nil {
			t.Fatal(err)
		}
		result := response.Results[0]
		if i == 1 {
			if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
				t.Fatalf("invalid string key accepted: %v", result)
			}
		} else if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("valid key rejected: %v", result)
		}
	}
	remove := &sink.DeleteOperation{Address: invalid}
	deleteRequest := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{remove}}
	deleted, err := server.Delete(t.Context(), deleteRequest)
	if err != nil || deleted.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
		t.Fatalf("invalid delete accepted: response=%v error=%v", deleted, err)
	}
	for i, address := range []*sink.RecordAddress{valid, binary} {
		operation := &sink.ReadOperation{Address: address}
		request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
		read, err := server.Read(t.Context(), request)
		if err != nil || read.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND {
			t.Fatalf("record lost: response=%v error=%v", read, err)
		}
		value := `{"value":"original"}`
		if i == 1 {
			value = `{"value":"other"}`
		}
		assertJSONEqual(t, read.Results[0].GetDocument().GetPayload(), []byte(value))
	}
}
