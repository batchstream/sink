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

func TestSearchStreamDecodesMetadataAndDiscardsUnknownFields(t *testing.T) {
	body := `{"unknown":{"nested":[null,true,1e300,{"value":[1,2]}]},"_scroll_id":"cursor","terminated_early":false,"timed_out":false,"_clusters":{"total":1,"successful":1,"skipped":0},"_shards":{"total":1,"successful":1,"failed":0},"hits":{"max_score":null,"extra":[{"ignored":true}],"total":{"value":2,"relation":"eq"},"hits":[{"sort":[1]},{"sort":[2]}]}}`
	var hits []string
	page, err := decodeSearchPage(strings.NewReader(body), func(hit json.RawMessage) error {
		hits = append(hits, string(hit))
		return nil
	})
	if err != nil || len(hits) != 2 || hits[0] != `{"sort":[1]}` || hits[1] != `{"sort":[2]}` {
		t.Fatalf("streamed hits=%v err=%v", hits, err)
	}
	if page.ScrollID != "cursor" || page.Clusters == nil || page.Shards == nil || page.TimedOut == nil || *page.TimedOut || page.TerminatedEarly {
		t.Fatalf("stream lost metadata: %+v", page)
	}
	if len(page.Hits.Hits) != 2 || page.Hits.Hits[0] != nil || page.Hits.Hits[1] != nil {
		t.Fatal("decoder retained delivered hit payloads")
	}
}

func TestSearchStreamRejectsTruncatedHitsAndUnknownFields(t *testing.T) {
	for _, body := range []string{
		`{"unknown":`, `{"unknown":{"key":`, `{"unknown":{"key":[`,
		`{"unknown":[{"key":`, `{"unknown":[1,}`, `{"unknown":{"key":1]}`,
		`{"hits":{"hits":[{"sort":`, `{"hits":{"hits":[null,`,
		`{"_scroll_id":[]}`, `{"timed_out":[]}`, `{"terminated_early":[]}`,
		`{"_shards":[]}`, `{"_clusters":[]}`, `{"hits":{"total":`,
	} {
		t.Run(body, func(t *testing.T) {
			_, err := decodeSearchPage(strings.NewReader(body), func(json.RawMessage) error { return nil })
			if err == nil {
				t.Fatal("truncated or invalid metadata was accepted")
			}
		})
	}
}

func TestQueryCallbackErrorStopsDeliveryWithoutRetry(t *testing.T) {
	body := `{"hits":{"hits":[{"_source":{"value":1}},{"_source":{"value":2}}],"total":{"value":2,"relation":"eq"}},"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0}}`
	expected := expectedRequest{method: http.MethodPost, path: "/items/_search", rawQuery: "rest_total_hits_as_int=false", statusCode: http.StatusOK, responseBody: body}
	store, handler := newScriptedStore(t, []expectedRequest{expected})
	t.Cleanup(store.Close)
	native := storage.NativeRequest{URI: "sink://primary/items", MaxBytes: 4096}
	request := storage.QueryRequest{Request: native, PageSize: 2}
	canceled := errors.New("consumer stopped")
	emitted := 0
	request.Emit = func(storage.Document) error { emitted++; return canceled }
	page, err := store.Query(t.Context(), request)
	if !errors.Is(err, canceled) || emitted != 1 || len(page.Documents) != 0 || page.HasMore {
		t.Fatalf("callback error lost: page=%+v emitted=%d err=%v", page, emitted, err)
	}
	handler.verify()
}

func TestScanStreamResumesByteLimitedPageWithoutCollecting(t *testing.T) {
	prefix := `{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":`
	first := expectedRequest{method: http.MethodPost, path: "/items/_search", statusCode: http.StatusOK, responseBody: prefix + `[{"sort":[1]},{"sort":[2]},{"sort":[3]}]}}`}
	second := expectedRequest{method: http.MethodPost, path: "/items/_search", statusCode: http.StatusOK, bodyContains: []string{`"search_after":[2]`}, responseBody: prefix + `[{"sort":[3]}]}}`}
	store, handler := newScriptedStore(t, []expectedRequest{first, second})
	t.Cleanup(store.Close)
	native := storage.NativeRequest{URI: "sink://primary/items", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 300}
	request := storage.ScanRequest{Request: native, BatchSize: 3}
	var delivered []string
	request.Emit = func(document storage.Document) error {
		delivered = append(delivered, string(document.Payload))
		return nil
	}
	page, err := store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 0 || len(page.NextCursor) == 0 || len(delivered) != 2 {
		t.Fatalf("first page=%+v delivered=%v err=%v", page, delivered, err)
	}
	request.Cursor = page.NextCursor
	page, err = store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 0 || len(page.NextCursor) != 0 || len(delivered) != 3 || delivered[2] != `{"sort":[3]}` {
		t.Fatalf("resumed page=%+v delivered=%v err=%v", page, delivered, err)
	}
	handler.verify()
}

func TestScanStreamFailuresWithholdCursor(t *testing.T) {
	for _, scenario := range []string{"callback", "extra-hit", "late-metadata", "duplicate-sort"} {
		t.Run(scenario, func(t *testing.T) {
			hits := `[{"sort":[1]},{"sort":[2]}]`
			if scenario == "extra-hit" {
				hits = `[{"sort":[1]},{"sort":[2]},{"sort":[3]}]`
			}
			if scenario == "duplicate-sort" {
				hits = `[{"sort":[1]},{"sort":[1]}]`
			}
			body := `{"hits":{"hits":` + hits + `},"_shards":{"total":1,"successful":1,"failed":0},"timed_out":` + fmt.Sprint(scenario == "late-metadata") + `}`
			expected := expectedRequest{method: http.MethodPost, path: "/items/_search", statusCode: http.StatusOK, responseBody: body}
			store, handler := newScriptedStore(t, []expectedRequest{expected})
			t.Cleanup(store.Close)
			native := storage.NativeRequest{URI: "sink://primary/items", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 4096}
			request := storage.ScanRequest{Request: native, BatchSize: 1}
			stopped := errors.New("consumer stopped")
			emitted := 0
			request.Emit = func(storage.Document) error {
				emitted++
				if scenario == "callback" {
					return stopped
				}
				return nil
			}
			page, err := store.Scan(t.Context(), request)
			if err == nil || len(page.NextCursor) != 0 || len(page.Documents) != 0 || emitted != 1 {
				t.Fatalf("failed stream completed: page=%+v emitted=%d err=%v", page, emitted, err)
			}
			if scenario == "callback" && !errors.Is(err, stopped) {
				t.Fatalf("callback error was replaced: %v", err)
			}
			handler.verify()
		})
	}
}
