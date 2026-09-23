package protocol_test

import (
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/batchstream/sink-protocol/uri"
	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/testuri"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSinkServiceContract(t *testing.T) {
	service := sink.File_sink_sink_proto.Services().ByName(protoreflect.Name("Sink"))
	if service == nil {
		t.Fatal("Sink service descriptor is missing")
	}

	methods := service.Methods()
	methodNames := make([]string, 0, methods.Len())
	for index := range methods.Len() {
		methodNames = append(methodNames, string(methods.Get(index).Name()))
	}

	want := []string{"Read", "Write", "Delete", "Execute", "Query", "Count", "Scan"}
	if !slices.Equal(methodNames, want) {
		t.Fatalf("Sink methods = %v, want %v", methodNames, want)
	}
}

func TestRecordAddressContainsOnlyCanonicalURI(t *testing.T) {
	message := sink.File_sink_sink_proto.Messages().ByName("RecordAddress")
	if message == nil || message.Fields().Len() != 1 || message.Fields().Get(0).Name() != "uri" {
		t.Fatal("RecordAddress must contain only uri")
	}
}

func TestNativeCommandUsesResourceURI(t *testing.T) {
	message := sink.File_sink_sink_proto.Messages().ByName("Command")
	want := []protoreflect.Name{"uri", "method", "path", "query", "headers", "content_type", "payload"}
	if message == nil || message.Fields().Len() != len(want) {
		t.Fatal("Command fields do not match the resource URI contract")
	}
	for index, name := range want {
		if message.Fields().Get(index).Name() != name {
			t.Fatalf("Command field %d must be %s", index, name)
		}
	}
}

func TestWriteRequestVTRoundTripPreservesActions(t *testing.T) {
	key := uri.StringKey("record-1")
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"catalog", "products"}, key)}
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(`{"created_at":"2026-08-29T04:34:56Z","value":1}`),
	}
	put := &sink.PutOperation{
		Document: document,
		Mode:     sink.WriteMode_WRITE_MODE_UPSERT,
	}
	putOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Put{Put: put},
	}

	source := []byte("return function(current, incoming) return incoming end")
	digest := sha256.Sum256(source)
	programReference := &sink.LuaProgram{
		Sha256: digest[:],
	}
	fullProgram := &sink.LuaProgram{Source: source, Sha256: digest[:]}
	merge := &sink.MergeOperation{
		IncomingDocument: document,
		LuaProgram:       programReference,
	}
	mergeOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Merge{Merge: merge},
	}

	request := &sink.WriteRequest{
		CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
		Operations:     []*sink.WriteOperation{putOperation, mergeOperation},
		LuaPrograms:    []*sink.LuaProgram{fullProgram},
	}

	encoded, err := request.MarshalVT()
	if err != nil {
		t.Fatalf("MarshalVT() error = %v", err)
	}

	decoded := &sink.WriteRequest{}
	err = decoded.UnmarshalVT(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVT() error = %v", err)
	}

	if !proto.Equal(decoded, request) {
		t.Fatalf("round trip request = %v, want %v", decoded, request)
	}
	if decoded.GetOperations()[0].GetPut() == nil {
		t.Fatal("first operation did not preserve put action")
	}
	if decoded.GetOperations()[1].GetMerge() == nil {
		t.Fatal("second operation did not preserve merge action")
	}
}

func TestForwardVTRoundTripUsesSharedSinkMessages(t *testing.T) {
	key := uri.StringKey("record-1")
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"catalog", "products"}, key)}
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(`{"value":1}`),
	}
	put := &sink.PutOperation{
		Document: document,
		Mode:     sink.WriteMode_WRITE_MODE_UPSERT,
	}
	operation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Put{Put: put},
	}
	write := &sink.WriteRequest{
		Operations: []*sink.WriteOperation{operation},
	}
	body := &forward.ForwardRequest_Write{Write: write}
	request := &forward.ForwardRequest{Version: 1, Store: "primary", Request: body}

	encoded, err := request.MarshalVT()
	if err != nil {
		t.Fatalf("MarshalVT() error = %v", err)
	}
	decoded := &forward.ForwardRequest{}
	err = decoded.UnmarshalVT(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVT() error = %v", err)
	}
	if !proto.Equal(decoded, request) {
		t.Fatalf("VT round trip = %v, want %v", decoded, request)
	}

	standardEncoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	standardDecoded := &forward.ForwardRequest{}
	err = standardDecoded.UnmarshalVT(standardEncoded)
	if err != nil {
		t.Fatalf("UnmarshalVT(standard protobuf) error = %v", err)
	}
	if !proto.Equal(standardDecoded, request) {
		t.Fatalf("standard protobuf round trip = %v, want %v", standardDecoded, request)
	}
}
