package merge_test

import (
	"errors"
	"testing"

	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
)

func TestLuaMergeRejectsMalformedUTF8JSON(t *testing.T) {
	options := merge.LuaOptions{}
	for _, payload := range []string{"{\"value\":\"\xff\"}", "{\"\xff\":1,\"\xfe\":2}"} {
		for _, current := range []bool{false, true} {
			source := []byte("return function(current, incoming) return incoming end")
			request := merge.Request{Incoming: jsonDocument(payload)}
			want := merge.ErrInvalidIncoming
			if current {
				source = []byte("return function(current, incoming) return current end")
				document := request.Incoming
				request.Current = &document
				request.Incoming = jsonDocument(`{}`)
				want = merge.ErrInvalidCurrent
			}
			merger := compileTestProgram(t, source, options)
			result, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, want) || len(result.Document.Payload) != 0 {
				t.Errorf("malformed JSON was silently changed: result=%s error=%v", result.Document.Payload, err)
			}
		}
	}
}

func TestLuaMergeRejectsMalformedUTF8Results(t *testing.T) {
	for _, encoding := range []storage.DocumentEncoding{storage.DocumentEncodingJSON, storage.DocumentEncodingBSON} {
		for _, body := range []string{`return {value=string.char(255)}`, `return {[string.char(255)]=1, [string.char(254)]=2}`} {
			options := merge.LuaOptions{}
			merger := compileTestProgram(t, []byte("return function(current, incoming) "+body+" end"), options)
			document := storage.Document{Encoding: encoding, Payload: []byte(`{}`)}
			if encoding == storage.DocumentEncodingBSON {
				document.Payload = []byte{5, 0, 0, 0, 0}
			}
			request := merge.Request{Incoming: document}
			result, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, merge.ErrInvalidResult) || len(result.Document.Payload) != 0 {
				t.Errorf("invalid UTF-8 result escaped the bridge: encoding=%d result=%q error=%v", encoding, result.Document.Payload, err)
			}
		}
	}
}
