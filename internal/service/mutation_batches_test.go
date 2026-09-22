package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/batchstream/sink-go/uri"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/testuri"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type completionEvent struct {
	method  string
	visible bool
	keys    []string
}
type completionStorage struct {
	storage.Storage
	events  chan completionEvent
	blocked string
	release chan struct{}
}

func (s *completionStorage) observe(ctx context.Context, event completionEvent) error {
	s.events <- event
	for _, key := range event.keys {
		if key == "archive" && event.visible {
			return errors.New("archive must not wait for refresh")
		}
		if key == s.blocked && event.visible {
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func (s *completionStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	event := completionEvent{method: "Write", visible: req.WaitUntilVisible}
	for _, op := range req.Operations {
		event.keys = append(event.keys, string(testuri.Key(op.Address).Data))
	}
	if err := s.observe(ctx, event); err != nil {
		var response storage.WriteResponse
		return response, err
	}
	return s.Storage.Write(ctx, req)
}
func (s *completionStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	event := completionEvent{method: "Delete", visible: req.WaitUntilVisible}
	for _, op := range req.Operations {
		event.keys = append(event.keys, string(testuri.Key(op.Address).Data))
	}
	if err := s.observe(ctx, event); err != nil {
		var response storage.DeleteResponse
		return response, err
	}
	return s.Storage.Delete(ctx, req)
}
func completionServer(t *testing.T, backend storage.Storage) *BatchingServer {
	t.Helper()
	opts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(opts)
	if err != nil {
		t.Fatal(err)
	}
	coreOpts := Options{BoundStore: "primary", Storage: backend, Lua: lua}
	core, err := New(coreOpts)
	if err != nil {
		t.Fatal(err)
	}
	batchOpts := BatchingOptions{MaxWait: time.Millisecond}
	server, err := NewBatchingServer(core, batchOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}
func completionAddress(key string) *sink.RecordAddress {
	value := uri.StringKey(key)
	recordKey := value
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"catalog", "products"}, recordKey)}
	return address
}
func completionPut(key string, value int) *sink.WriteOperation {
	doc := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: fmt.Appendf(nil, `{"value":%d}`, value)}
	put := &sink.PutOperation{Document: doc, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	action := &sink.WriteOperation_Put{Put: put}
	op := &sink.WriteOperation{Address: completionAddress(key), Action: action}
	return op
}
func completionMerge(key string, value int) *sink.WriteOperation {
	doc := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: fmt.Appendf(nil, `{"value":%d}`, value)}
	program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) return {value=(current and current.value or 0)+incoming.value} end`)}
	mutation := &sink.MergeOperation{IncomingDocument: doc, LuaProgram: program}
	action := &sink.WriteOperation_Merge{Merge: mutation}
	op := &sink.WriteOperation{Address: completionAddress(key), Action: action}
	return op
}
func completionWriteCall(ctx context.Context, mode sink.CompletionMode, ops ...*sink.WriteOperation) *batchCall[*sink.WriteRequest, *sink.WriteResponse] {
	req := &sink.WriteRequest{CompletionMode: mode, Operations: ops}
	call := &batchCall[*sink.WriteRequest, *sink.WriteResponse]{ctx: ctx, request: req, operationCount: len(ops), result: make(chan batchResult[*sink.WriteResponse], 1)}
	return call
}
func completionDeleteCall(ctx context.Context, mode sink.CompletionMode, key string) *batchCall[*sink.DeleteRequest, *sink.DeleteResponse] {
	op := &sink.DeleteOperation{Address: completionAddress(key)}
	req := &sink.DeleteRequest{CompletionMode: mode, Operations: []*sink.DeleteOperation{op}}
	call := &batchCall[*sink.DeleteRequest, *sink.DeleteResponse]{ctx: ctx, request: req, operationCount: 1, result: make(chan batchResult[*sink.DeleteResponse], 1)}
	return call
}
func awaitCompletion[T any](t *testing.T, results <-chan T) T {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("mutation did not finish")
		var empty T
		return empty
	}
}
func assertCompletionWrites(t *testing.T, calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]) {
	t.Helper()
	for _, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.response.Results) != len(call.request.Operations) {
			t.Fatal("lost response boundary")
		}
		for i, r := range result.response.Results {
			if r.OperationIndex != uint32(i) || r.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("invalid result %v", r)
			}
		}
	}
}
func TestMixedWriteCompletionDoesNotWaitForUnrelatedRefresh(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10), blocked: "product", release: make(chan struct{})}
	server := completionServer(t, backend)
	visible := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionMerge("product", 1))
	applied := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("archive", 1))
	queueCompletionCall(t, server.writes, visible)
	queueCompletionCall(t, server.writes, applied)
	defer func() {
		close(backend.release)
		if result := awaitCompletion(t, visible.result); result.err != nil {
			t.Error(result.err)
		}
	}()
	result := awaitCompletion(t, applied.result)
	if result.err != nil || result.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("archive write: %+v", result)
	}
	select {
	case <-visible.result:
		t.Fatal("visible request returned before refresh")
	default:
	}
	first := awaitCompletion(t, backend.events)
	second := awaitCompletion(t, backend.events)
	if first.visible == second.visible {
		t.Fatal("completion modes were combined")
	}
}
func TestMixedDeleteCompletionDoesNotWaitForUnrelatedRefresh(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10), blocked: "product", release: make(chan struct{})}
	server := completionServer(t, backend)
	visible := completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, "product")
	applied := completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "archive")
	queueCompletionCall(t, server.deletes, visible)
	queueCompletionCall(t, server.deletes, applied)
	defer func() {
		close(backend.release)
		if result := awaitCompletion(t, visible.result); result.err != nil {
			t.Error(result.err)
		}
	}()
	result := awaitCompletion(t, applied.result)
	if result.err != nil || result.response.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
		t.Fatalf("archive delete: %+v", result)
	}
	select {
	case <-visible.result:
		t.Fatal("visible delete returned before refresh")
	default:
	}
}
func TestMutationDispatchPreserveSameRecordPutMergeOrderAndFolding(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10)}
	server := completionServer(t, backend)
	applied := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	visible := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{
		completionWriteCall(t.Context(), applied, completionMerge("hot", 1)),
		completionWriteCall(t.Context(), visible, completionMerge("independent", 1)),
		completionWriteCall(t.Context(), applied, completionMerge("hot", 2)),
		completionWriteCall(t.Context(), visible, completionPut("hot", 10)),
		completionWriteCall(t.Context(), applied, completionMerge("hot", 3), completionMerge("other", 1)),
		completionWriteCall(t.Context(), applied, completionMerge("hot", 4)),
	}
	prepareDispatchCalls(calls, backend)
	pending := calls
	for len(pending) > 0 {
		selected, remaining, _ := server.writes.selectReady(pending, nil)
		server.executeWrites(t.Context(), selected)
		pending = remaining
	}
	assertCompletionWrites(t, calls)
	address, err := protocol.ParseAddress(completionAddress("hot"))
	if err != nil {
		t.Fatal(err)
	}
	read := storage.ReadOperation{Address: address}
	req := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
	response, err := backend.Read(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(response.Results[0].Document.Payload, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Value != 17 {
		t.Fatalf("same-record order lost: %d", doc.Value)
	}
	var hot []bool
	for len(backend.events) > 0 {
		e := <-backend.events
		for _, key := range e.keys {
			if key == "hot" {
				hot = append(hot, e.visible)
			}
		}
	}
	if !reflect.DeepEqual(hot, []bool{false, true, false}) {
		t.Fatalf("folding/order/visibility: %v", hot)
	}
}
func TestMutationDispatchTrackEveryRecordInMultiOperationRPC(t *testing.T) {
	applied := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	visible := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{
		completionWriteCall(t.Context(), applied, completionPut("a", 1)),
		completionWriteCall(t.Context(), visible, completionPut("a", 2), completionPut("b", 2)),
		completionWriteCall(t.Context(), applied, completionPut("b", 3)),
		completionWriteCall(t.Context(), visible, completionPut("a", 4)),
	}
	prepareDispatchCalls(calls, memory.New())
	batcher := &requestBatcher[*sink.WriteRequest, *sink.WriteResponse]{maxOperations: 100, maxBytes: 1 << 20}
	pending := calls
	for _, want := range [][]*batchCall[*sink.WriteRequest, *sink.WriteResponse]{calls[:1], {calls[1], calls[3]}, calls[2:3]} {
		selected, remaining, _ := batcher.selectReady(pending, nil)
		if !reflect.DeepEqual(selected, want) {
			t.Fatalf("wrong dependency selection: %v, want %v", selected, want)
		}
		pending = remaining
	}
	if len(pending) != 0 {
		t.Fatal("dispatcher left pending calls")
	}
}
func TestCompletionGroupCancellationDoesNotRetainOtherGroupsContext(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10), blocked: "product", release: make(chan struct{})}
	defer close(backend.release)
	server := completionServer(t, backend)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	visible := completionWriteCall(ctx, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionMerge("product", 1))
	applied := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("archive", 1))
	queueCompletionCall(t, server.writes, visible)
	queueCompletionCall(t, server.writes, applied)
	awaitCompletion(t, backend.events)
	awaitCompletion(t, backend.events)
	cancel()
	if r := awaitCompletion(t, visible.result); r.err == nil {
		t.Fatal("cancelled refresh returned success")
	}
	if r := awaitCompletion(t, applied.result); r.err != nil {
		t.Fatal(r.err)
	}
}
func TestMutationDispatchSkipCancelledDependentCalls(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10), blocked: "hot", release: make(chan struct{})}
	server := completionServer(t, backend)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, completionPut("hot", 1))
	next := completionWriteCall(ctx, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("hot", 2))
	queueCompletionCall(t, server.writes, first)
	queueCompletionCall(t, server.writes, next)
	awaitCompletion(t, backend.events)
	cancel()
	close(backend.release)
	if r := awaitCompletion(t, first.result); r.err != nil {
		t.Fatal(r.err)
	}
	if r := awaitCompletion(t, next.result); status.Code(r.err) != codes.Canceled {
		t.Fatalf("cancelled successor: %v", r.err)
	}
	if len(backend.events) != 0 {
		t.Fatal("cancelled successor reached storage")
	}
}

func TestMutationDispatchKeepFullRecordAddressesSeparate(t *testing.T) {
	for _, difference := range []string{"namespace", "dataset", "key_type", "same"} {
		t.Run(difference, func(t *testing.T) {
			first := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("same", 1))
			operation := completionPut("same", 2)
			switch difference {
			case "namespace":
				operation.Address.Uri = testuri.WithSegment(operation.Address.GetUri(), 0, "another")
			case "dataset":
				operation.Address.Uri = testuri.WithSegment(operation.Address.GetUri(), -2, "another")
			case "key_type":
				kind := uri.BytesKey([]byte("same"))
				operation.Address.Uri = testuri.WithKey(operation.Address.GetUri(), kind)
			}
			second := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, operation)
			calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second}
			prepareDispatchCalls(calls, memory.New())
			active := map[recordIdentity]int{first.records[0]: 1}
			batcher := &requestBatcher[*sink.WriteRequest, *sink.WriteResponse]{maxOperations: 100, maxBytes: 1 << 20}
			selected, _, _ := batcher.selectReady(calls[1:], active)
			want := 1
			if difference == "same" {
				want = 0
			}
			if len(selected) != want {
				t.Fatalf("%s: %d selected, want %d", difference, len(selected), want)
			}
		})
	}
}

func TestDeleteCompletionChangesPreserveRecordOrder(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 10)}
	server := completionServer(t, backend)
	calls := []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]{
		completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "same"),
		completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, "same"),
		completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "same"),
	}
	for _, call := range calls {
		queueCompletionCall(t, server.deletes, call)
	}
	for _, call := range calls {
		r := awaitCompletion(t, call.result)
		if r.err != nil || len(r.response.Results) != 1 || r.response.Results[0].Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatalf("delete result: %+v", r)
		}
	}
	for _, want := range []bool{false, true, false} {
		if e := awaitCompletion(t, backend.events); e.visible != want {
			t.Fatalf("delete ordering: %+v", e)
		}
	}
}

func prepareDispatchCalls(calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse], backend storage.Storage) {
	for _, call := range calls {
		call.records = mutationRequestRecords(call.request)
		call.partition = mutationRequestPartition[*sink.WriteOperation](call.request, backend)
		call.encodedBytes = call.request.SizeVT()
	}
}
