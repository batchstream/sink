package service_test

import (
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestInvalidDocumentFailsBeforeWriteOrPublish(t *testing.T) {
	documents := map[string]*sink.Document{
		// Outer framing is valid, but the embedded document is malformed.
		"BSON":       {Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: []byte{15, 0, 0, 0, 3, 'v', 0, 7, 0, 0, 0, 0x42, 0, 0, 0}},
		"JSON value": {Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte("{\"value\":\"\xff\"}")},
		"JSON keys":  {Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte("{\"\xff\":1,\"\xfe\":2}")},
	}
	for _, mode := range []sink.CompletionMode{sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED} {
		for name, document := range documents {
			t.Run(name+"/"+mode.String(), func(t *testing.T) {
				publisher := &recordingPublisher{}
				server := newTestServer(t, memory.New(), publisher)
				put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
				action := &sink.WriteOperation_Put{Put: put}
				operation := &sink.WriteOperation{Address: protoAddress("invalid"), Action: action}
				request := &sink.WriteRequest{Operations: []*sink.WriteOperation{operation}, CompletionMode: mode}
				response, err := server.Write(t.Context(), request)
				if err != nil || response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
					t.Fatalf("invalid document was not rejected: response=%v error=%v", response, err)
				}
				if publisher.mutationCount() != 0 {
					t.Fatal("invalid document reached Kafka")
				}
				read := &sink.ReadOperation{Address: protoAddress("invalid")}
				readRequest := &sink.ReadRequest{Operations: []*sink.ReadOperation{read}}
				stored, err := server.Read(t.Context(), readRequest)
				if err != nil || stored.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
					t.Fatalf("invalid document reached storage: response=%v error=%v", stored, err)
				}
			})
		}
	}
}
