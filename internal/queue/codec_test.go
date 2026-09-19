package queue_test

import (
	"bytes"
	"testing"

	"github.com/liran/sink-go/uri"
	"github.com/liran/sink/internal/testuri"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"google.golang.org/protobuf/proto"
)

func TestMutationCodecRoundTrip(t *testing.T) {
	address := testQueueAddress("record-1")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(`{"created_at":"2026-08-29T04:34:56Z","value":1}`),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	mutation := queue.Mutation{Write: operation}

	encoded, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatalf("MarshalMutation() error = %v", err)
	}
	decoded, err := queue.UnmarshalMutation(encoded)
	if err != nil {
		t.Fatalf("UnmarshalMutation() error = %v", err)
	}
	if decoded.Delete != nil || !proto.Equal(decoded.Write, operation) {
		t.Fatalf("UnmarshalMutation() = %#v", decoded)
	}
}

func TestMutationKeyIsStablePerAddress(t *testing.T) {
	address := testQueueAddress("record-1")
	write := &sink.WriteOperation{Address: address}
	deleteOperation := &sink.DeleteOperation{Address: address}
	writeMutation := queue.Mutation{Write: write}
	deleteMutation := queue.Mutation{Delete: deleteOperation}
	writeKey, err := queue.MutationKey(writeMutation)
	if err != nil {
		t.Fatalf("MutationKey(write) error = %v", err)
	}
	deleteKey, err := queue.MutationKey(deleteMutation)
	if err != nil {
		t.Fatalf("MutationKey(delete) error = %v", err)
	}
	if !bytes.Equal(writeKey, deleteKey) {
		t.Fatalf("write key %x differs from delete key %x", writeKey, deleteKey)
	}
	if string(writeKey) != address.GetUri() {
		t.Fatal("queue key must equal the complete canonical URI")
	}

}

func TestMutationKeyIgnoresUnknownAddressFields(t *testing.T) {
	plain := testQueueAddress("record-1")
	withUnknown := proto.Clone(plain).(*sink.RecordAddress)
	withUnknown.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	plainKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: plain}})
	if err != nil {
		t.Fatal(err)
	}
	unknownKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: withUnknown}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plainKey, unknownKey) {
		t.Fatalf("semantic address key changed: %x != %x", plainKey, unknownKey)
	}
}

func TestMutationKeyRetainsEveryURIPathSegment(t *testing.T) {
	first := testQueueAddress("record-1")
	second := proto.Clone(first).(*sink.RecordAddress)
	second.Uri = testuri.WithSegment(second.GetUri(), 0, "another")
	firstKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: first}})
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := queue.MutationKey(queue.Mutation{Write: &sink.WriteOperation{Address: second}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(firstKey, secondKey) {
		t.Fatalf("distinct URI identities collided: %x != %x", firstKey, secondKey)
	}
}

func testQueueAddress(key string) *sink.RecordAddress {
	recordKey := uri.StringKey(key)
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"logical", "records"}, recordKey)}
	return address
}
