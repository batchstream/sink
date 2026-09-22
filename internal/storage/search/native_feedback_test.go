package search

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/batchstream/sink/internal/storage"
)

func TestNativeFeedbackPreservesHTTPRejection(t *testing.T) {
	for _, code := range []int{400, 409, 429, 503, 504} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			reply := expectedRequest{method: http.MethodGet, path: "/", statusCode: code, responseBody: `{"error":"injected"}`}
			store, handler := newScriptedStore(t, []expectedRequest{reply})
			defer handler.verify()
			request := storage.NativeRequest{URI: "sink://search", Method: http.MethodGet, Path: "/", MaxBytes: 4096}
			// The scripted helper owns the Store identity.
			request.URI = "sink://" + store.logicalStore
			response, err := store.Execute(t.Context(), request)
			if err != nil || response.StatusCode != code || string(response.Payload) != reply.responseBody || response.Success {
				t.Fatalf("native response changed: %+v %v", response, err)
			}
			_, retryable := storage.ErrorDetails(response.Failure)
			if retryable != (code == 429 || code >= 500) {
				t.Fatalf("incorrect feedback for HTTP %d: %v", code, response.Failure)
			}
		})
	}
}

func TestNativeBulkFeedbackFromHTTP200(t *testing.T) {
	for _, code := range []int{400, 409, 429, 503} {
		body := fmt.Sprintf(`{"errors":true,"items":[{"index":{"status":%d}}]}`, code)
		reply := expectedRequest{method: http.MethodPost, path: "/_bulk", statusCode: 200, responseBody: body}
		store, handler := newScriptedStore(t, []expectedRequest{reply})
		request := storage.NativeRequest{URI: "sink://" + store.logicalStore, Method: http.MethodPost, Path: "/_bulk", MaxBytes: 4096}
		response, err := store.Execute(t.Context(), request)
		handler.verify()
		if err != nil || !response.Success || string(response.Payload) != body {
			t.Fatalf("native envelope changed: %+v %v", response, err)
		}
		_, retryable := storage.ErrorDetails(response.Failure)
		if retryable != (code == 429 || code >= 500) {
			t.Fatalf("native bulk item %d: %v", code, response.Failure)
		}
	}
}
