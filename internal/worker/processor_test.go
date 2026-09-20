package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/sink-go/uri"
	"github.com/batchstream/sink/internal/testuri"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/queue"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"github.com/batchstream/sink/internal/worker"
)

func TestProcessorAppliesWriteAndDeleteSynchronously(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	serverOptions := service.Options{BoundStore: "primary", Storage: store, Lua: luaEngine}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	address := processorAddress()
	document := processorDocument(`{"value":"value"}`)
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_CREATE}
	writeOperation := &sink.WriteOperation{
		Address: address,
		Action:  &sink.WriteOperation_Put{Put: put},
	}
	writeMutation := queue.Mutation{Write: writeOperation}
	if err := processor.Handle(context.Background(), writeMutation); err != nil {
		t.Fatalf("Handle(write) error = %v", err)
	}

	readRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddress()}},
	}
	read, err := store.Read(context.Background(), readRequest)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusFound {
		t.Fatalf("Read() status = %v", read.Results[0].Status)
	}

	duplicateErr := processor.Handle(context.Background(), writeMutation)
	var applyError *worker.ApplyError
	if !errors.As(duplicateErr, &applyError) || applyError.Retryable() {
		t.Fatalf("Handle(duplicate create) error = %v", duplicateErr)
	}

	deleteOperation := &sink.DeleteOperation{Address: address}
	deleteMutation := queue.Mutation{Delete: deleteOperation}
	if err := processor.Handle(context.Background(), deleteMutation); err != nil {
		t.Fatalf("Handle(delete) error = %v", err)
	}
	read, err = store.Read(context.Background(), readRequest)
	if err != nil {
		t.Fatalf("Read(after delete) error = %v", err)
	}
	if read.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("Read(after delete) status = %v", read.Results[0].Status)
	}
}

func TestProcessorBatchPreservesMixedMutationOrderPerRecord(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatalf("NewLuaEngine() error = %v", err)
	}
	serverOptions := service.Options{BoundStore: "primary", Storage: store, Lua: luaEngine}
	server, err := service.New(serverOptions)
	if err != nil {
		t.Fatalf("service.New() error = %v", err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}

	firstAddress := processorAddressFor("first")
	secondAddress := processorAddressFor("second")
	mutations := []queue.Mutation{
		{Write: processorPut(firstAddress, "before")},
		{Write: processorPut(secondAddress, "independent")},
		{Delete: &sink.DeleteOperation{Address: firstAddress}},
		{Write: processorPut(firstAddress, "after")},
	}
	results := processor.HandleBatch(context.Background(), mutations)
	for index, result := range results {
		if result != nil {
			t.Fatalf("HandleBatch() result[%d] = %v", index, result)
		}
	}

	firstRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddressFor("first")}},
	}
	firstRead, err := store.Read(context.Background(), firstRequest)
	if err != nil {
		t.Fatalf("Read(first) error = %v", err)
	}
	if got := string(firstRead.Results[0].Document.Payload); got != `{"value":"after"}` {
		t.Fatalf("Read(first) document = %q, want after", got)
	}
	secondRequest := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddressFor("second")}},
	}
	secondRead, err := store.Read(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("Read(second) error = %v", err)
	}
	if got := string(secondRead.Results[0].Document.Payload); got != `{"value":"independent"}` {
		t.Fatalf("Read(second) document = %q, want independent", got)
	}
}

func TestProcessorUsesCanonicalURIForOrdering(t *testing.T) {
	store := memory.New()
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	serviceOptions := service.Options{BoundStore: "primary", Storage: store, Lua: engine}
	server, err := service.New(serviceOptions)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	first := processorAddressFor("same-record")
	second := processorAddressFor("same-record")
	mutations := []queue.Mutation{
		{Write: processorPut(first, "first")},
		{Write: processorPut(first, "second")},
		{Write: processorPut(second, "third")},
	}
	for index, result := range processor.HandleBatch(t.Context(), mutations) {
		if result != nil {
			t.Fatalf("HandleBatch() result[%d] = %v", index, result)
		}
	}
	request := storage.ReadRequest{
		Operations: []storage.ReadOperation{{Address: processorStorageAddressFor("same-record")}},
	}
	read, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(read.Results[0].Document.Payload); got != `{"value":"third"}` {
		t.Fatalf("physical record = %s, want final queued value", got)
	}
}

func processorAddress() *sink.RecordAddress {
	return processorAddressFor("record-1")
}

func processorAddressFor(value string) *sink.RecordAddress {
	key := uri.StringKey(value)
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"logical", "records"}, key)}
	return address
}

func processorStorageAddress() storage.Address {
	return processorStorageAddressFor("record-1")
}

func processorStorageAddressFor(value string) storage.Address {
	address := testuri.Address("primary", []string{"logical", "records"}, storage.Key{Type: "string", Data: []byte(value)})
	return address
}

func processorPut(address *sink.RecordAddress, value string) *sink.WriteOperation {
	document := processorDocument(`{"value":"` + value + `"}`)
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	return operation
}

func processorDocument(value string) *sink.Document {
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  []byte(value),
	}
	return document
}
