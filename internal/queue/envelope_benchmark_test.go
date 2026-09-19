package queue_test

import (
	"bytes"
	"fmt"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
)

func BenchmarkMutationEnvelope(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes-%d", size), func(b *testing.B) {
			address := testQueueAddress("benchmark")
			document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: bytes.Repeat([]byte("x"), size)}
			put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
			operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
			mutation := queue.Mutation{Write: operation}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				encoded, err := queue.MarshalMutation(mutation)
				if err != nil || len(encoded) != queue.MutationSize(mutation) {
					b.Fatalf("marshal: size=%d err=%v", len(encoded), err)
				}
			}
		})
	}
}
