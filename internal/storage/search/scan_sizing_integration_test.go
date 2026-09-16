//go:build integration

package search_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestSearchScanShrinksLargeResponses(t *testing.T) {
	fixture := newIntegrationFixture(t)
	seed := storage.WriteRequest{WaitUntilVisible: true}
	for index := range 9 {
		document := jsonStorageDocument(fmt.Sprintf(`{"number":%d,"padding":"%s"}`, index, strings.Repeat("x", 50<<10)))
		operation := storage.WriteOperation{Address: fixture.address(fmt.Sprint(index)), Document: document}
		seed.Operations = append(seed.Operations, operation)
	}
	written, err := fixture.store.Write(t.Context(), seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatal(result)
		}
	}
	command := storage.NativeRequest{URI: "sink://primary", Method: "POST", Path: "/" + fixture.index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"sort":["number"]}`), MaxBytes: 64 << 10}
	request := storage.ScanRequest{Request: command, BatchSize: 16}
	for index := range 9 {
		page, err := fixture.store.Scan(t.Context(), request)
		if err != nil || len(page.Documents) != 1 {
			t.Fatalf("page %d: documents=%d, %v", index, len(page.Documents), err)
		}
		var hit struct {
			Source struct {
				Number int `json:"number"`
			} `json:"_source"`
		}
		if err := json.Unmarshal(page.Documents[0].Payload, &hit); err != nil {
			t.Fatal(err)
		}
		if hit.Source.Number != index {
			t.Fatalf("page %d returned record %d", index, hit.Source.Number)
		}
		if (len(page.NextCursor) != 0) != (index < 8) {
			t.Fatalf("wrong cursor on page %d", index)
		}
		request.Cursor = page.NextCursor
	}
	request.Cursor = nil
	request.Projection = &storage.Projection{Fields: []string{"number"}}
	page, err := fixture.store.Scan(t.Context(), request)
	if err != nil || len(page.Documents) != 9 || len(page.NextCursor) != 0 {
		t.Fatalf("projected scan reused unprojected sizing: documents=%d err=%v", len(page.Documents), err)
	}
}

func TestSearchScanProjectionRetainsSortOutsideSource(t *testing.T) {
	fixture := newIntegrationFixture(t)
	seed := storage.WriteRequest{WaitUntilVisible: true}
	for number := range 3 {
		document := jsonStorageDocument(fmt.Sprintf(`{"number":%d,"name":"item","padding":"%s"}`, number, strings.Repeat("x", 32<<10)))
		operation := storage.WriteOperation{Address: fixture.address(fmt.Sprint(number)), Document: document}
		seed.Operations = append(seed.Operations, operation)
	}
	written, err := fixture.store.Write(t.Context(), seed)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range written.Results {
		if result.Status != storage.WriteStatusApplied {
			t.Fatal(result)
		}
	}
	command := storage.NativeRequest{URI: "sink://primary", Method: "POST", Path: "/" + fixture.index + "/_search",
		ContentType: "application/json", Payload: []byte(`{"sort":["number"],"_source":false}`), Query: "_source=false", MaxBytes: 4096}
	for _, exclude := range []bool{false, true} {
		projection := &storage.Projection{Fields: []string{"name"}, Exclude: exclude}
		if exclude {
			projection.Fields = []string{"number", "padding"}
		}
		request := storage.ScanRequest{Request: command, BatchSize: 1, Projection: projection}
		for number := range 3 {
			page, err := fixture.store.Scan(t.Context(), request)
			if err != nil || len(page.Documents) != 1 || (len(page.NextCursor) != 0) != (number < 2) {
				t.Fatalf("page %d: %+v err=%v", number, page, err)
			}
			var hit struct {
				ID     string         `json:"_id"`
				Source map[string]any `json:"_source"`
				Sort   []int          `json:"sort"`
			}
			if err := json.Unmarshal(page.Documents[0].Payload, &hit); err != nil {
				t.Fatal(err)
			}
			if hit.ID != fmt.Sprint(number) || len(hit.Source) != 1 || hit.Source["name"] != "item" || len(hit.Sort) != 1 || hit.Sort[0] != number {
				t.Fatalf("projection or continuation sort lost: %+v", hit)
			}
			request.Cursor = page.NextCursor
		}
	}
}
