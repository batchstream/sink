package kafka

import (
	"bytes"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/logging"
	"github.com/liran/sink/internal/queue"
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
