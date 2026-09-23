package kafka

import (
	"bytes"
	"testing"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/logging"
	"github.com/batchstream/sink/internal/queue"
)

func TestQuarantineDiagnosticIncludesOnlyDocument(t *testing.T) {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":42}`)}
	put := &sink.PutOperation{Document: document}
	body := &sink.WriteOperation_Put{Put: put}
	write := &sink.WriteOperation{Action: body}
	mutation := queue.Mutation{Write: write}
	envelope, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	lazy := quarantinedDocument{envelope: envelope}
	diagnostic, ok := lazy.LogValue().Any().(*logging.FailureBody)
	if !ok || diagnostic.Encoding != "json" || !bytes.Equal(diagnostic.Payload, document.Payload) {
		t.Fatalf("wrong diagnostic: %+v", diagnostic)
	}
	if quarantinedBody([]byte("invalid")) != nil {
		t.Fatal("malformed envelope exported as a document")
	}
}

func TestQuarantineDiagnosticPreservesUnknownEncoding(t *testing.T) {
	document := &sink.Document{Encoding: 42, Payload: []byte{0xff, 0x01}}
	put := &sink.PutOperation{Document: document}
	body := &sink.WriteOperation_Put{Put: put}
	write := &sink.WriteOperation{Action: body}
	mutation := queue.Mutation{Write: write}
	envelope, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := quarantinedBody(envelope)
	if diagnostic == nil || diagnostic.Encoding != "enum:42" || !bytes.Equal(diagnostic.Payload, document.Payload) {
		t.Fatalf("unknown document encoding was lost: %+v", diagnostic)
	}
}
