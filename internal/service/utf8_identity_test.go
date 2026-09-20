package service_test

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestVTRecordRequestsRejectInvalidUTF8Identities(t *testing.T) {
	codec := protocol.NewVTProtoCodec()
	for _, raw := range []string{"sink://primary/catalog/products/s:" + string([]byte{0xff}), "sink://primary/%FF/products/s:key", "sink://primary/catalog//s:key", "sink://primary/catalog/products/s:key?query"} {
		t.Run(raw, func(t *testing.T) {
			publisher := &recordingPublisher{}
			server := newTestServer(t, memory.New(), publisher)
			address := &sink.RecordAddress{Uri: raw}

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
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid identity was read: response=%v error=%v", read, err)
			}
			for _, mode := range []sink.CompletionMode{sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED} {
				operation := foldingPut("valid", sink.WriteMode_WRITE_MODE_UPSERT, 1)
				operation.Address = &decoded
				request := &sink.WriteRequest{Operations: []*sink.WriteOperation{operation}, CompletionMode: mode}
				written, err := server.Write(t.Context(), request)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("invalid identity was written: response=%v error=%v", written, err)
				}
				remove := &sink.DeleteOperation{Address: &decoded}
				deleteRequest := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{remove}, CompletionMode: mode}
				deleted, err := server.Delete(t.Context(), deleteRequest)
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("invalid identity was deleted: response=%v error=%v", deleted, err)
				}
			}
			if publisher.mutationCount() != 0 {
				t.Fatal("invalid identity reached Kafka")
			}
		})
	}
}
