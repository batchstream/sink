package merge_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkLuaMergeDocuments(b *testing.B) {
	for _, fields := range []int{0, 32} {
		for _, encoding := range []storage.DocumentEncoding{storage.DocumentEncodingJSON, storage.DocumentEncodingBSON} {
			b.Run(fmt.Sprintf("fields=%d/encoding=%d", fields, encoding), func(b *testing.B) {
				value := map[string]any{"count": int64(1), "padding": strings.Repeat("x", 1024)}
				for index := range fields {
					value[fmt.Sprintf("field_%d", index)] = fmt.Sprintf("value_%d", index)
				}
				var payload []byte
				var err error
				if encoding == storage.DocumentEncodingBSON {
					payload, err = bson.Marshal(value)
				} else {
					payload, err = json.Marshal(value)
				}
				if err != nil {
					b.Fatal(err)
				}
				document := storage.Document{Encoding: encoding, Payload: payload}
				source := []byte(`return function(current, incoming)
    incoming.count = current.count + incoming.count
    return incoming
end`)
				options := merge.LuaOptions{}
				merger := compileTestProgram(b, source, options)
				request := merge.Request{Current: &document, Incoming: document, ObservedAt: time.Unix(1, 0)}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := merger.Merge(b.Context(), request); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
