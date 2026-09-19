package search

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestReadFailuresPreserveDocumentBudget(t *testing.T) {
	tests := []struct {
		name   string
		fields string
		want   string
	}{
		{name: "missing source", fields: `"found":true,"_seq_no":0,"_primary_term":1`, want: "no _source"},
		{name: "null source", fields: `"found":true,"_source":null,"_seq_no":0,"_primary_term":1`, want: "no _source"},
		{name: "missing revision", fields: `"found":true,"_source":{}`, want: "no sequence number"},
		{name: "invalid revision", fields: `"found":true,"_source":{},"_seq_no":-1,"_primary_term":1`, want: "invalid sequence number"},
		{name: "missing found", fields: `"_source":{},"_seq_no":0,"_primary_term":1`, want: "no found flag"},
		{name: "backend error", fields: `"found":true,"error":{"type":"no_shard_available_action_exception","reason":"shard unavailable"}`, want: "shard unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, scope := range []string{"request", "operation", "exhausted"} {
				t.Run(scope, func(t *testing.T) {
					const source = `{"value":1}`
					const charge = len(source) + 128
					payload := fmt.Sprintf(`{"docs":[{"_index":"legacy-records","_id":"bad",%s},{"_index":"legacy-records","_id":"good","found":true,"_source":%s,"_seq_no":0,"_primary_term":1}]}`, test.fields, source)
					expected := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: http.StatusOK, responseBody: payload}
					store, handler := newScriptedStore(t, []expectedRequest{expected})
					t.Cleanup(store.Close)
					var charged int
					maximum := charge
					if scope == "exhausted" {
						maximum = 0
					}
					budget := storage.NewTrackedReadBudget(maximum, func(used int) { charged = used })
					request := storage.ReadRequest{
						Operations: []storage.ReadOperation{{Address: testAddress("bad")}, {Address: testAddress("good")}},
						Budget:     budget,
					}
					if scope == "operation" {
						request.Budget = storage.NewTrackedReadBudget(0, nil)
						for index := range request.Operations {
							request.Operations[index].Budget = budget
						}
					}
					response, err := store.Read(t.Context(), request)
					if err != nil || len(response.Results) != 2 {
						t.Fatalf("Read() = %+v, %v", response, err)
					}
					failed := response.Results[0]
					if failed.Status != storage.ReadStatusFailed || failed.Err == nil || !strings.Contains(failed.Err.Error(), test.want) {
						t.Fatalf("failed document lost its original error: %+v", failed)
					}
					if len(failed.Document.Payload) != 0 || len(failed.Revision.Data) != 0 {
						t.Fatalf("failed document retains a payload or revision: %+v", failed)
					}
					good := response.Results[1]
					if scope == "exhausted" {
						code, _ := storage.ErrorDetails(good.Err)
						if good.Status != storage.ReadStatusFailed || code != storage.ErrorCodeResourceExhausted || charged != 0 || len(good.Document.Payload) != 0 || len(good.Revision.Data) != 0 {
							t.Fatalf("exhausted budget retained a document: %+v, charged %d", good, charged)
						}
					} else if good.Status != storage.ReadStatusFound || string(good.Document.Payload) != source || charged != charge {
						t.Fatalf("failed document consumed its sibling's budget: %+v, charged %d", good, charged)
					}
					handler.verify()
				})
			}
		})
	}
}
