package search

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/worker"
)

func TestEmptySearchKeyIsPermanentWithoutBackendAccess(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("empty key reached backend") })
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := Options{Driver: DriverElasticsearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.Key{Type: "string"}
	address := storage.Address{Store: "search", Namespace: "test", Dataset: "documents", Key: key}
	read := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: address}}}
	readResult, err := store.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest := storage.DeleteRequest{Operations: []storage.DeleteOperation{{Address: address}}}
	deleted, err := store.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []error{readResult.Results[0].Err, deleted.Results[0].Err} {
		code, retryable := storage.ErrorDetails(failure)
		if code != storage.ErrorCodeInvalidArgument || retryable {
			t.Fatalf("invalid key classification: %v", failure)
		}
	}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := service.Options{Storage: store, Lua: lua}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	kind := &sink.RecordKey_StringValue{StringValue: ""}
	wireKey := &sink.RecordKey{Kind: kind}
	wireAddress := &sink.RecordAddress{Store: "search", Namespace: "test", Dataset: "documents", Key: wireKey}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
	put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: document}
	action := &sink.WriteOperation_Put{Put: put}
	operation := &sink.WriteOperation{Address: wireAddress, Action: action}
	mutation := queue.Mutation{Write: operation}
	err = processor.Handle(t.Context(), mutation)
	var failure *worker.ApplyError
	if !errors.As(err, &failure) || failure.Retryable() {
		t.Fatalf("empty key must be eligible for quarantine: %v", err)
	}
}
