package search

import (
	"net/http"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestMultiGetRejectsUnidentifiedAndMisorderedResults(t *testing.T) {
	for _, member := range []string{
		`{"_index":"legacy-records","_id":"other","found":true,"_source":{},"_seq_no":0,"_primary_term":1}`,
		`{"_index":"legacy-records","_id":"other","found":false}`,
		`{"_index":"legacy-records","found":false}`,
		`{"_id":"record","found":false}`,
		`{"_index":"legacy-records","_id":"other","error":{"type":"index_not_found_exception"}}`,
	} {
		t.Run(member, func(t *testing.T) {
			reply := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: http.StatusOK,
				responseBody: `{"docs":[` + member + `]}`}
			assertStorageFailure(t, "read", reply, true)
			requests := []expectedRequest{reply}
			store, handler := newScriptedStore(t, requests)
			defer handler.verify()
			condition := storage.Precondition{Kind: storage.PreconditionRecordExists}
			operation := storage.WriteOperation{Address: testAddress("record"), Document: testDocument(`{}`), Precondition: condition}
			request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			response, err := store.Write(t.Context(), request)
			if err != nil || response.Results[0].Status != storage.WriteStatusFailed {
				t.Fatalf("Replace accepted an unidentified snapshot: %+v %v", response, err)
			}
			if _, retryable := storage.ErrorDetails(response.Results[0].Err); !retryable {
				t.Fatal("unidentified snapshot must remain retryable")
			}
		})
	}
	// A correct result count does not prove that results retain request order.
	reply := expectedRequest{method: http.MethodPost, path: "/_mget", statusCode: http.StatusOK,
		responseBody: `{"docs":[{"_index":"legacy-records","_id":"second","found":false},{"_index":"legacy-records","_id":"first","found":false}]}`}
	requests := []expectedRequest{reply}
	store, handler := newScriptedStore(t, requests)
	defer handler.verify()
	first := storage.ReadOperation{Address: testAddress("first")}
	second := storage.ReadOperation{Address: testAddress("second")}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{first, second}}
	response, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != storage.ReadStatusFailed {
			t.Fatalf("reordered result acknowledged as absence: %+v", result)
		}
	}
}

func TestBulkRejectsUnidentifiedAndWrongActionResults(t *testing.T) {
	for _, operation := range []string{"write", "delete"} {
		for _, member := range []string{
			`"update":{"_index":"legacy-records","_id":"record","status":200,"_seq_no":0,"_primary_term":1}`,
			`"%s":{"_index":"legacy-records","_id":"other","status":200,"_seq_no":0,"_primary_term":1}`,
			`"%s":{"_index":"legacy-records","status":200,"_seq_no":0,"_primary_term":1}`,
			`"%s":{"_id":"record","status":200,"_seq_no":0,"_primary_term":1}`,
			`"%s":{"_index":"legacy-records","_id":"other","status":409,"error":{"type":"version_conflict_engine_exception"}}`,
			`"%s":{"_index":"legacy-records","_id":"other","status":400,"error":{"type":"mapper_parsing_exception"}}`,
		} {
			t.Run(operation+"/"+member, func(t *testing.T) {
				action := "index"
				if operation == "delete" {
					action = "delete"
				}
				member = strings.ReplaceAll(member, "%s", action)
				reply := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: http.StatusOK,
					responseBody: `{"items":[{` + member + `}]}`}
				assertStorageFailure(t, operation, reply, true)
			})
		}
	}
}

func TestSearchIdentityValidationAcceptsConcreteIndexForAlias(t *testing.T) {
	requests := []expectedRequest{
		{method: http.MethodPost, path: "/_mget", statusCode: http.StatusOK,
			responseBody: `{"docs":[{"_index":"concrete-000001","_id":"record","found":false}]}`},
		{method: http.MethodPost, path: "/_bulk", statusCode: http.StatusOK,
			responseBody: `{"items":[{"create":{"_index":"concrete-000001","_id":"record","status":201,"_seq_no":0,"_primary_term":1}}]}`},
		{method: http.MethodPost, path: "/_bulk", statusCode: http.StatusOK,
			responseBody: `{"items":[{"delete":{"_index":"concrete-000001","_id":"record","status":404}}]}`},
	}
	store, handler := newScriptedStore(t, requests)
	defer handler.verify()
	address := testAddress("record")
	read := storage.ReadOperation{Address: address}
	readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
	readResponse, err := store.Read(t.Context(), readRequest)
	if err != nil || readResponse.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("alias read failed: %+v %v", readResponse, err)
	}
	condition := storage.Precondition{Kind: storage.PreconditionRecordNotExists}
	write := storage.WriteOperation{Address: address, Document: testDocument(`{}`), Precondition: condition}
	writeRequest := storage.WriteRequest{Operations: []storage.WriteOperation{write}}
	writeResponse, err := store.Write(t.Context(), writeRequest)
	if err != nil || writeResponse.Results[0].Status != storage.WriteStatusApplied {
		t.Fatalf("alias create failed: %+v %v", writeResponse, err)
	}
	deletion := storage.DeleteOperation{Address: address}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{deletion}}
	deleteResponse, err := store.Delete(t.Context(), deleteRequest)
	if err != nil || deleteResponse.Results[0].Status != storage.DeleteStatusApplied {
		t.Fatalf("alias delete failed: %+v %v", deleteResponse, err)
	}
}

func TestBulkValidatesAllResultsBeforeAcknowledging(t *testing.T) {
	reply := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: http.StatusOK,
		responseBody: `{"items":[{"create":{"_index":"legacy-records","_id":"first","status":201,"_seq_no":0,"_primary_term":1}},{"index":{"_index":"legacy-records","_id":"other","status":400,"error":{"type":"mapper_parsing_exception"}}}]}`}
	requests := []expectedRequest{reply}
	store, handler := newScriptedStore(t, requests)
	defer handler.verify()
	condition := storage.Precondition{Kind: storage.PreconditionRecordNotExists}
	first := storage.WriteOperation{Address: testAddress("first"), Document: testDocument(`{}`), Precondition: condition}
	second := storage.WriteOperation{Address: testAddress("second"), Document: testDocument(`{}`)}
	request := storage.WriteRequest{Operations: []storage.WriteOperation{first, second}}
	response, err := store.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		_, retryable := storage.ErrorDetails(result.Err)
		if result.Status != storage.WriteStatusFailed || !retryable {
			t.Fatalf("untrusted bulk reply acknowledged or quarantined: %+v", result)
		}
	}
}
