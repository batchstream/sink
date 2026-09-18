//go:build integration

package search_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestReadSourceDisabledPreservesSiblingBudget(t *testing.T) {
	fixture := newIntegrationFixture(t)
	index := fixture.index + "-source-disabled"
	settings := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"_source":{"enabled":false}}}`)
	status, body := fixture.request(t, http.MethodPut, "/"+index, settings)
	if status != http.StatusOK {
		t.Fatalf("create source-disabled index: HTTP %d: %s", status, body)
	}
	t.Cleanup(func() {
		status, body := fixture.request(t, http.MethodDelete, "/"+index, nil)
		if status != http.StatusOK && status != http.StatusNotFound {
			t.Errorf("delete source-disabled index: HTTP %d: %s", status, body)
		}
	})
	const source = `{"value":1}`
	for _, target := range []string{index, fixture.index} {
		status, body := fixture.request(t, http.MethodPut, "/"+target+"/_doc/record", []byte(source))
		if status != http.StatusCreated {
			t.Fatalf("create record in %s: HTTP %d: %s", target, status, body)
		}
	}
	request := storage.ReadRequest{
		Operations: []storage.ReadOperation{
			{Address: fixture.datasetAddress(index, "record")},
			{Address: fixture.address("record")},
		},
		Budget: storage.NewReadBudget(len(source) + 128),
	}
	response, err := fixture.store.Read(t.Context(), request)
	if err != nil || len(response.Results) != 2 {
		t.Fatalf("Read() = %+v, %v", response, err)
	}
	failed := response.Results[0]
	if failed.Status != storage.ReadStatusFailed || failed.Err == nil || !strings.Contains(failed.Err.Error(), "no _source") {
		t.Fatalf("source-disabled record: %+v", failed)
	}
	good := response.Results[1]
	if good.Status != storage.ReadStatusFound || string(good.Document.Payload) != source {
		t.Fatalf("source-disabled record consumed sibling budget: %+v", good)
	}
}
