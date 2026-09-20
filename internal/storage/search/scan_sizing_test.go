package search

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/sink/internal/storage"
)

func TestScanShrinksOversizedResponseWithoutSkipping(t *testing.T) {
	const total = 9
	var sizes []int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Size  int   `json:"size"`
			After []int `json:"search_after"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		sizes = append(sizes, body.Size)
		start := 0
		if len(body.After) != 0 {
			start = body.After[0]
		}
		hits := make([]json.RawMessage, 0)
		for id := start + 1; id <= min(total, start+body.Size); id++ {
			hit := json.RawMessage(fmt.Sprintf(`{"sort":[%d],"_source":{"value":"%s"}}`, id, strings.Repeat("x", 50<<10)))
			hits = append(hits, hit)
		}
		encoded, err := json.Marshal(hits)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = fmt.Fprintf(w, `{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":%s}}`, encoded)
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.NativeRequest{URI: "sink://search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 64 << 10}
	request := storage.ScanRequest{Request: command, BatchSize: 16}
	for id := 1; id <= total; id++ {
		page, err := store.Scan(t.Context(), request)
		if err != nil || len(page.Documents) != 1 {
			t.Fatalf("page %d: %d documents, %v", id, len(page.Documents), err)
		}
		var hit struct {
			Sort []int `json:"sort"`
		}
		if err := json.Unmarshal(page.Documents[0].Payload, &hit); err != nil {
			t.Fatal(err)
		}
		if len(hit.Sort) != 1 || hit.Sort[0] != id {
			t.Fatalf("skipped or repeated record: %v, want %d", hit.Sort, id)
		}
		if (len(page.NextCursor) != 0) != (id < total) {
			t.Fatalf("incorrect continuation for record %d", id)
		}
		request.Cursor = page.NextCursor
	}
	if len(sizes) < 4 || sizes[0] != 17 || sizes[1] != 9 || sizes[2] != 5 || sizes[3] != 3 {
		t.Fatalf("unexpected shrink sequence: %v", sizes)
	}
	if len(sizes) != 12 {
		t.Fatalf("repeated oversized reads on later pages: %v", sizes)
	}
	for _, size := range sizes[4:] {
		if size != 2 {
			t.Fatalf("later page forgot its one-document capacity: %v", sizes)
		}
	}
	request.Cursor = nil
	request.Request.MaxBytes = 512 << 10
	page, err := store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != total || len(sizes) != 13 || sizes[12] != 17 {
		t.Fatalf("small page budget throttled a larger caller: documents=%d sizes=%v err=%v", len(page.Documents), sizes, err)
	}
}

func TestScanSizeHintsAreBoundedAndExpire(t *testing.T) {
	var hints scanSizeCache
	first := sha256.Sum256([]byte("first"))
	hints.remember(first, 8)
	if hints.suggest(first, 2) != 2 || hints.suggest(first, 100) != 8 {
		t.Fatal("sizing hint exceeded caller limit or was ignored")
	}
	for index := range maxScanSizeHints {
		key := sha256.Sum256(fmt.Appendf(nil, "query-%d", index))
		hints.remember(key, 4)
	}
	if len(hints.entries) != maxScanSizeHints || hints.suggest(first, 100) != 100 {
		t.Fatal("sizing hints retained an unbounded query history")
	}
	hints.remember(first, 8)
	entry := hints.entries[first].Value.(*scanSizeHint)
	entry.expires = time.Now().Add(-time.Second)
	if hints.suggest(first, 100) != 100 {
		t.Fatal("expired hint prevented probing changed document sizes")
	}
}

func TestScanSizingPreservesFailureAndLookaheadChecks(t *testing.T) {
	for _, scenario := range []string{"duplicate", "partial", "single oversized", "http failure", "backend limit"} {
		t.Run(scenario, func(t *testing.T) {
			var sizes []int
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Size int `json:"size"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				sizes = append(sizes, body.Size)
				if scenario == "http failure" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				hits := make([]json.RawMessage, 0, body.Size)
				for id := 1; id <= body.Size; id++ {
					sort := id
					if scenario == "duplicate" {
						sort = 1
					}
					size := 50 << 10
					if scenario == "single oversized" {
						size = 80 << 10
					}
					hit := json.RawMessage(fmt.Sprintf(`{"sort":[%d],"_source":{"value":"%s"}}`, sort, strings.Repeat("x", size)))
					hits = append(hits, hit)
				}
				encoded, err := json.Marshal(hits)
				if err != nil {
					t.Error(err)
					return
				}
				timedOut := scenario == "partial"
				_, _ = fmt.Fprintf(w, `{"timed_out":%t,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":%s}}`, timedOut, encoded)
			})
			backend := httptest.NewServer(handler)
			defer backend.Close()
			opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
			if scenario == "backend limit" {
				opts.MaxResponseSize = 32 << 10
			}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			command := storage.NativeRequest{URI: "sink://search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 64 << 10}
			request := storage.ScanRequest{Request: command, BatchSize: 16}
			page, err := store.Scan(t.Context(), request)
			if err == nil || len(page.Documents) != 0 || len(page.NextCursor) != 0 {
				t.Fatalf("returned partial page: %+v, %v", page, err)
			}
			if scenario == "http failure" && len(sizes) != 1 {
				t.Fatalf("retried backend failure: %v", sizes)
			}
			if scenario == "backend limit" && len(sizes) != 5 {
				t.Fatalf("unbounded shrink retries: %v", sizes)
			}
		})
	}
}
