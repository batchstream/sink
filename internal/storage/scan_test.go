package storage

import (
	"bytes"
	"testing"
)

func TestScanCursorBindsCommandButAllowsDifferentPageBudgets(t *testing.T) {
	command := NativeRequest{URI: "sink://search/items", Path: "/_search", Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 4096}
	request := ScanRequest{Request: command, BatchSize: 2}
	cursor, err := request.Resume()
	if err != nil {
		t.Fatal(err)
	}
	document := Document{Encoding: DocumentEncodingJSON, Payload: []byte(`{}`)}
	page, err := cursor.Page([]Document{document}, []byte(`["position"]`))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = page.NextCursor
	request.BatchSize = 7
	request.Request.MaxBytes = 8192
	resumed, err := request.Resume()
	if err != nil || !bytes.Equal(resumed.Position, []byte(`["position"]`)) {
		t.Fatalf("resume=%+v err=%v", resumed, err)
	}
	for _, field := range []string{"store", "resource", "query", "payload"} {
		changed := request
		switch field {
		case "store":
			changed.Request.URI = "sink://another/items"
		case "resource":
			changed.Request.URI = "sink://search/another"
		case "query":
			changed.Request.Query = "routing=another"
		case "payload":
			changed.Request.Payload = []byte(`{"sort":["other"]}`)
		}
		if _, err := changed.Resume(); err == nil {
			t.Fatalf("accepted changed %s", field)
		}
	}
	request.Cursor = bytes.Clone(request.Cursor)
	request.Cursor[len(request.Cursor)-1] ^= 1
	if _, err := request.Resume(); err == nil {
		t.Fatal("accepted corrupt cursor")
	}
}

func TestScanCursorBoundsUntrustedInput(t *testing.T) {
	for _, data := range [][]byte{[]byte("broken"), make([]byte, MaxScanCursorBytes+1)} {
		request := ScanRequest{BatchSize: 1, Cursor: data}
		if _, err := request.Resume(); err == nil {
			t.Fatal("accepted invalid cursor")
		}
	}
	request := ScanRequest{BatchSize: 1}
	cursor, err := request.Resume()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Page(nil, make([]byte, MaxScanCursorBytes)); err == nil {
		t.Fatal("created oversized cursor")
	}
}

func TestScanProjectionBindsCursor(t *testing.T) {
	command := NativeRequest{URI: "sink://primary", Payload: []byte(`{"sort":["uid"]}`)}
	request := ScanRequest{Request: command, BatchSize: 1}
	initial, err := request.Resume()
	if err != nil {
		t.Fatal(err)
	}
	page, err := initial.Page(nil, []byte(`[1]`))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = page.NextCursor
	if _, err := request.Resume(); err != nil {
		t.Fatal(err)
	}
	request.Projection = &Projection{Fields: []string{"name"}}
	if _, err := request.Resume(); err == nil {
		t.Fatal("adding a projection to an existing scan was accepted")
	}
	request.Cursor = nil
	cursor, err := request.Resume()
	if err != nil {
		t.Fatal(err)
	}
	page, err = cursor.Page(nil, []byte(`[1]`))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = page.NextCursor
	request.BatchSize = 7
	request.Request.MaxBytes = 8192
	if _, err := request.Resume(); err != nil {
		t.Fatal(err)
	}
	for _, projection := range []*Projection{nil, {}, {Fields: []string{"other"}}, {Fields: []string{"name"}, Exclude: true}} {
		request.Projection = projection
		if _, err := request.Resume(); err == nil {
			t.Fatalf("accepted changed projection: %+v", projection)
		}
	}
}
