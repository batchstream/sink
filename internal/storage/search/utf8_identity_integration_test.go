//go:build integration

package search_test

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liran/sink/internal/testuri"

	"github.com/liran/sink-go/uri"

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
	opts := service.Options{BoundStore: "primary", Storage: fixture.store, Lua: engine}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	codec := protocol.NewVTProtoCodec()
	valid := fixture.sinkAddress("\uFFFD")
	invalid := fixture.sinkAddress("invalid")
	invalid.Uri = strings.Replace(invalid.Uri, "s:invalid", "s:%FF", 1)
	binary := fixture.sinkAddress("")
	kind := uri.BytesKey([]byte{0xff})
	binary.Uri = testuri.WithKey(binary.GetUri(), kind)
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
		if i == 1 {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid URI accepted: %v %v", response, err)
			}
			continue
		}
		if err != nil || response.Results[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("valid key rejected: %v %v", response, err)
		}

	}
	remove := &sink.DeleteOperation{Address: invalid}
	deleteRequest := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{remove}}
	deleted, err := server.Delete(t.Context(), deleteRequest)
	if status.Code(err) != codes.InvalidArgument {
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
