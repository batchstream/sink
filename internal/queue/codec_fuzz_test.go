package queue

import (
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"google.golang.org/protobuf/proto"
)

func FuzzMutationEnvelope(f *testing.F) {
	f.Add([]byte("SNKQ"))
	f.Add([]byte{83, 78, 75, 81, 1, 1})
	f.Add([]byte{83, 78, 75, 81, 1, 2})
	f.Add([]byte{83, 78, 75, 81, 2, 1})
	f.Add([]byte{83, 78, 75, 81, 2, 2})
	address := &sink.RecordAddress{Uri: "sink://primary/logical/records/s:one"}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	write := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	write.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	deleted := &sink.DeleteOperation{Address: address}
	mutations := []Mutation{{Write: write}, {Delete: deleted}}
	for _, mutation := range mutations {
		encoded, err := MarshalMutation(mutation)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			t.Skip()
		}
		mutation, err := UnmarshalMutation(payload)
		if err != nil {
			return
		}
		encoded, err := MarshalMutation(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != MutationSize(mutation) {
			t.Fatal("preflight size differs from envelope size")
		}
		decoded, err := UnmarshalMutation(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(decoded.Write, mutation.Write) || !proto.Equal(decoded.Delete, mutation.Delete) {
			t.Fatal("envelope round trip changed the mutation")
		}
	})
}
