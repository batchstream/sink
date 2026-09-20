package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/storage"
)

func TestQueryEmitsBeforeHTTPResponseCompletes(t *testing.T) {
	first := make(chan struct{})
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"hits":{"hits":[{"_source":{"value":1}}`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, `,{"_source":{"value":2}}],"total":{"value":2,"relation":"eq"}},"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0}}`)
	}))
	defer backend.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	native := storage.NativeRequest{URI: "sink://primary/items", MaxBytes: 4096}
	count := 0
	request := storage.QueryRequest{Request: native, PageSize: 2}
	request.Emit = func(document storage.Document) error {
		count++
		if count == 1 {
			close(first)
		}
		return nil
	}
	done := make(chan error, 1)
	go func() {
		response, err := store.Query(ctx, request)
		if len(response.Documents) != 0 || response.HasMore {
			err = errors.New("streaming backend retained the page")
		}
		done <- err
	}()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("query buffered the entire HTTP response")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count=%d", count)
	}
}

func TestQueryLateMetadataFailureNeverCompletesOrReplays(t *testing.T) {
	for _, suffix := range []string{
		`,"timed_out":true,"_shards":{"total":1,"successful":1,"failed":0}}`,
		`,"timed_out":false,"_shards":{"total":2,"successful":1,"failed":1}}`,
		`,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0}} {}`,
	} {
		t.Run(fmt.Sprint(len(suffix)), func(t *testing.T) {
			calls := 0
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				_, _ = io.WriteString(w, `{"hits":{"hits":[{"_source":{"x":1}}],"total":{"value":1,"relation":"eq"}}`+suffix)
			}))
			defer backend.Close()
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL, backend.URL}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			request := storage.QueryRequest{Request: storage.NativeRequest{URI: "sink://primary/items", MaxBytes: 4096}, PageSize: 1}
			emitted := 0
			request.Emit = func(storage.Document) error { emitted++; return nil }
			_, err = store.Query(t.Context(), request)
			if err == nil || emitted != 1 || calls != 1 {
				t.Fatalf("late failure replay/commit: emitted=%d calls=%d err=%v", emitted, calls, err)
			}
		})
	}
}

func TestSearchStreamRejectsMalformedAndDuplicateMetadata(t *testing.T) {
	for _, body := range []string{`{"hits":{"hits":[],"hits":[]}}`, `{"hits":[]}`, `{"hits":{"hits":[]}},`, strings.Repeat("x", 3)} {
		_, err := decodeSearchPage(strings.NewReader(body), func(json.RawMessage) error { return nil })
		if err == nil {
			t.Fatalf("accepted malformed page %s", body)
		}
	}
}
