package service_test

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage/memory"
)

func TestVTRecordRequestsRejectInvalidUTF8Identities(t *testing.T) {
	codec := protocol.NewVTProtoCodec()
	for _, field := range []string{"store", "namespace", "dataset", "string key", "opaque type"} {
		t.Run(field, func(t *testing.T) {
			publisher := &recordingPublisher{}
			server := newTestServer(t, memory.New(), publisher)
			address := protoAddress("valid")
			invalid := string([]byte{0xff})
			switch field {
			case "store":
				address.Store = invalid
			case "namespace":
				address.Namespace = invalid
			case "dataset":
				address.Dataset = invalid
			case "string key":
				kind := &sink.RecordKey_StringValue{StringValue: invalid}
				address.Key.Kind = kind
			case "opaque type":
				opaque := &sink.OpaqueValue{Type: invalid, Data: []byte("key")}
				kind := &sink.RecordKey_OpaqueValue{OpaqueValue: opaque}
				address.Key.Kind = kind
			}
			encoded, err := codec.Marshal(address)
			if err != nil {
				t.Fatal(err)
			}
			var decoded sink.RecordAddress
			err = codec.Unmarshal(encoded, &decoded)
			encoded.Free()
			if err != nil {
				t.Fatal(err)
			}
			readOperation := &sink.ReadOperation{Address: &decoded}
			readRequest := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOperation}}
			read, err := server.Read(t.Context(), readRequest)
			if (field == "store" && status.Code(err) != codes.InvalidArgument) || (field != "store" && (err != nil || read.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT)) {
				t.Fatalf("invalid identity was read: response=%v error=%v", read, err)
			}
			for _, mode := range []sink.CompletionMode{sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED} {
				operation := foldingPut("valid", sink.WriteMode_WRITE_MODE_UPSERT, 1)
				operation.Address = &decoded
				request := &sink.WriteRequest{Operations: []*sink.WriteOperation{operation}, CompletionMode: mode}
				written, err := server.Write(t.Context(), request)
				if (field == "store" && status.Code(err) != codes.InvalidArgument) || (field != "store" && (err != nil || written.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT)) {
					t.Fatalf("invalid identity was written: response=%v error=%v", written, err)
				}
				remove := &sink.DeleteOperation{Address: &decoded}
				deleteRequest := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{remove}, CompletionMode: mode}
				deleted, err := server.Delete(t.Context(), deleteRequest)
				if (field == "store" && status.Code(err) != codes.InvalidArgument) || (field != "store" && (err != nil || deleted.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT)) {
					t.Fatalf("invalid identity was deleted: response=%v error=%v", deleted, err)
				}
			}
			if publisher.mutationCount() != 0 {
				t.Fatal("invalid identity reached Kafka")
			}
		})
	}
}
