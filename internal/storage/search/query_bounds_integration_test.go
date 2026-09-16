//go:build integration

package search_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestQueryDoesNotFetchLookaheadSourceOrExceedWindow(t *testing.T) {
	fixture := newIntegrationFixture(t)
	settings := []byte(`{"index.max_result_window":2}`)
	code, body := fixture.request(t, http.MethodPut, "/"+fixture.index+"/_settings", settings)
	if code != http.StatusOK {
		t.Fatalf("settings HTTP %d: %s", code, body)
	}
	documents := []string{`{"order":1,"value":"small"}`, fmt.Sprintf(`{"order":2,"value":"%s"}`, strings.Repeat("x", 8192))}
	for index, document := range documents {
		path := fmt.Sprintf("/%s/_doc/%d?refresh=true", fixture.index, index)
		code, body := fixture.request(t, http.MethodPut, path, []byte(document))
		if code != http.StatusCreated {
			t.Fatalf("insert HTTP %d: %s", code, body)
		}
	}
	command := storage.NativeRequest{URI: "sink://primary", Method: "POST", Path: "/" + fixture.index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"sort":["order"],"track_total_hits":false}`), MaxBytes: 4096}
	request := storage.QueryRequest{Request: command, PageSize: 1}
	page, err := fixture.store.Query(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || !page.HasMore {
		t.Fatalf("small first page rejected by large lookahead: %+v err=%v", page, err)
	}
	request.Offset = 1
	request.Request.MaxBytes = 32 << 10
	page, err = fixture.store.Query(t.Context(), request)
	if err != nil || len(page.Documents) != 1 || page.HasMore {
		t.Fatalf("full final page at window boundary: %+v err=%v", page, err)
	}
	request.Offset = 0
	request.PageSize = 2
	page, err = fixture.store.Query(t.Context(), request)
	if err != nil || len(page.Documents) != 2 || page.HasMore {
		t.Fatalf("whole window: %+v err=%v", page, err)
	}
}
