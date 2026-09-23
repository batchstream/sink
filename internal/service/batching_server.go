package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	sinkmetrics "github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBatchMaxWait             = 2 * time.Millisecond
	defaultBatchMaxOperations       = 32
	defaultBatchMaxBytes            = 16 << 20
	defaultBatchMaxQueuedOperations = 10_000
	defaultBatchMaxQueuedBytes      = 128 << 20
)

type BatchingOptions struct {
	MaxWait             time.Duration
	MaxOperations       int
	MaxBytes            int
	MaxQueuedOperations int
	MaxQueuedBytes      int
	Metrics             *sinkmetrics.Metrics
}

type BatchingServer struct {
	server  *Server
	reads   *requestBatcher[*sink.ReadRequest, *sink.ReadResponse]
	writes  *requestBatcher[*sink.WriteRequest, *sink.WriteResponse]
	deletes *requestBatcher[*sink.DeleteRequest, *sink.DeleteResponse]
}

func NewBatchingServer(server *Server, opts BatchingOptions) (*BatchingServer, error) {
	if server == nil {
		return nil, errors.New("create synchronous batching server: server is required")
	}
	normalized, err := normalizeBatchingOptions(server, opts)
	if err != nil {
		return nil, err
	}
	batching := &BatchingServer{server: server}

	readOptions := requestBatcherOptions[*sink.ReadRequest, *sink.ReadResponse]{
		Admission:           server.admission,
		Unlimited:           true,
		Method:              "Read",
		MaxWait:             normalized.MaxWait,
		MaxOperations:       normalized.MaxOperations,
		MaxBytes:            normalized.MaxBytes,
		MaxQueuedOperations: normalized.MaxQueuedOperations,
		MaxQueuedBytes:      normalized.MaxQueuedBytes,
		Execute:             batching.executeReads,
		Metrics:             normalized.Metrics,
	}
	readOptions.Store = server.boundStore
	batching.reads = newRequestBatcher(readOptions)

	writeOptions := requestBatcherOptions[*sink.WriteRequest, *sink.WriteResponse]{
		Admission: server.admission,
		Unlimited: true,
		Records: func(request *sink.WriteRequest) []recordIdentity {
			return mutationRequestRecords(request)
		},
		Partition: func(request *sink.WriteRequest) batchPartition {
			return mutationRequestPartition[*sink.WriteOperation](request, server.storage)
		},
		Method:              "Write",
		MaxWait:             normalized.MaxWait,
		MaxOperations:       normalized.MaxOperations,
		MaxBytes:            normalized.MaxBytes,
		MaxQueuedOperations: normalized.MaxQueuedOperations,
		MaxQueuedBytes:      normalized.MaxQueuedBytes,
		Execute:             batching.executeWrites,
		Metrics:             normalized.Metrics,
	}
	writeOptions.Store = server.boundStore
	batching.writes = newRequestBatcher(writeOptions)

	deleteOptions := requestBatcherOptions[*sink.DeleteRequest, *sink.DeleteResponse]{
		Admission: server.admission,
		Unlimited: true,
		Records: func(request *sink.DeleteRequest) []recordIdentity {
			return mutationRequestRecords(request)
		},
		Partition: func(request *sink.DeleteRequest) batchPartition {
			return mutationRequestPartition[*sink.DeleteOperation](request, server.storage)
		},
		Method:              "Delete",
		MaxWait:             normalized.MaxWait,
		MaxOperations:       normalized.MaxOperations,
		MaxBytes:            normalized.MaxBytes,
		MaxQueuedOperations: normalized.MaxQueuedOperations,
		MaxQueuedBytes:      normalized.MaxQueuedBytes,
		Execute:             batching.executeDeletes,
		Metrics:             normalized.Metrics,
	}
	deleteOptions.Store = server.boundStore
	batching.deletes = newRequestBatcher(deleteOptions)
	return batching, nil
}

func normalizeBatchingOptions(server *Server, opts BatchingOptions) (BatchingOptions, error) {
	if opts.MaxWait < 0 || opts.MaxOperations < 0 || opts.MaxBytes < 0 ||
		opts.MaxQueuedOperations < 0 || opts.MaxQueuedBytes < 0 {
		var empty BatchingOptions
		return empty, errors.New("create synchronous batching server: limits cannot be negative")
	}
	if opts.MaxWait == 0 {
		opts.MaxWait = defaultBatchMaxWait
	}
	if opts.MaxOperations == 0 {
		opts.MaxOperations = defaultBatchMaxOperations
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = defaultBatchMaxBytes
	}
	if opts.MaxQueuedOperations == 0 {
		opts.MaxQueuedOperations = max(defaultBatchMaxQueuedOperations, opts.MaxOperations)
	}
	if opts.MaxQueuedBytes == 0 {
		opts.MaxQueuedBytes = max(defaultBatchMaxQueuedBytes, opts.MaxBytes)
	}
	if opts.MaxOperations > server.maxOperations {
		var empty BatchingOptions
		return empty, fmt.Errorf("create synchronous batching server: max operations %d exceeds server limit %d", opts.MaxOperations, server.maxOperations)
	}
	if opts.MaxQueuedOperations < opts.MaxOperations {
		var empty BatchingOptions
		return empty, errors.New("create synchronous batching server: queued operation limit must cover one batch")
	}
	if opts.MaxQueuedBytes < opts.MaxBytes {
		var empty BatchingOptions
		return empty, errors.New("create synchronous batching server: queued byte limit must be at least the batch byte limit")
	}
	return opts, nil
}

func (s *BatchingServer) Close() {
	s.reads.stop()
	s.writes.stop()
	s.deletes.stop()
	s.reads.wait()
	s.writes.wait()
	s.deletes.wait()
}

func (s *BatchingServer) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	if err := protocol.CheckStore(req, s.server.boundStore); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.server.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	return s.reads.Submit(ctx, req, len(req.GetOperations()), req.SizeVT())
}

func (s *BatchingServer) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if err := protocol.CheckStore(req, s.server.boundStore); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "write request must contain operations")
	}
	if err := s.server.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "write request has an invalid completion mode")
	}
	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return s.server.Write(ctx, req)
	}
	programs, err := parseLuaPrograms(req.GetLuaPrograms())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "write request Lua programs: %v", err)
	}
	normalized := normalizeSynchronousWriteRequest(req, programs)
	return s.writes.Submit(ctx, normalized, len(normalized.GetOperations()), normalized.SizeVT())
}

func normalizeSynchronousWriteRequest(req *sink.WriteRequest, programs luaPrograms) *sink.WriteRequest {
	operations := make([]*sink.WriteOperation, len(req.GetOperations()))
	for index, operation := range req.GetOperations() {
		operations[index] = normalizeSynchronousWriteOperation(operation, programs)
	}
	normalized := &sink.WriteRequest{
		CompletionMode: req.GetCompletionMode(),
		Operations:     operations,
	}
	return normalized
}

func normalizeSynchronousWriteOperation(operation *sink.WriteOperation, programs luaPrograms) *sink.WriteOperation {
	if operation == nil || operation.GetMerge() == nil || operation.GetMerge().GetLuaProgram() == nil {
		return operation
	}
	programReference := operation.GetMerge().GetLuaProgram()
	if len(programReference.GetSource()) > 0 || len(programReference.GetSha256()) != sha256.Size {
		return operation
	}
	var digest [sha256.Size]byte
	copy(digest[:], programReference.GetSha256())
	resolved, ok := programs[digest]
	if !ok {
		return operation
	}
	// Resolve only against the original RPC. Removing the declaration list from
	// the combined request prevents one caller from satisfying another caller's
	// otherwise undeclared digest reference.
	clonedMessage := proto.Clone(operation)
	cloned, clonedOK := clonedMessage.(*sink.WriteOperation)
	if !clonedOK {
		return operation
	}
	program := &sink.LuaProgram{
		Source: resolved.Source,
		Sha256: resolved.SHA256,
	}
	cloned.GetMerge().LuaProgram = program
	return cloned
}

func (s *BatchingServer) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	if err := protocol.CheckStore(req, s.server.boundStore); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "delete request must contain operations")
	}
	if err := s.server.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "delete request has an invalid completion mode")
	}
	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return s.server.Delete(ctx, req)
	}
	return s.deletes.Submit(ctx, req, len(req.GetOperations()), req.SizeVT())
}

func (s *BatchingServer) executeReads(ctx context.Context, calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse]) {
	calls = liveMutationCalls(calls)
	if len(calls) == 0 {
		return
	}
	operations := make([]*sink.ReadOperation, 0, totalReadOperations(calls))
	budgets := &responseGroups{}
	for _, call := range calls {
		operations = append(operations, call.request.GetOperations()...)
		budgets.addContext(call.ctx, len(call.request.GetOperations()))
	}
	request := &sink.ReadRequest{Operations: operations}
	execution, cancel := batchExecutionContext(ctx, calls, 0)
	defer cancel()
	response, err := s.server.read(execution, request, budgets)
	splitReadResponse(calls, response, err)
	if response != nil {
		clear(response.Results)
	}
}

func totalReadOperations(calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse]) int {
	total := 0
	for _, call := range calls {
		total += len(call.request.GetOperations())
	}
	return total
}

func splitReadResponse(
	calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse],
	response *sink.ReadResponse,
	err error,
) {
	if err == nil && (response == nil || len(response.Results) != totalReadOperations(calls)) {
		err = status.Error(codes.Internal, "batched read returned an invalid result count")
	}
	if err != nil {
		for _, call := range calls {
			completeCall(call, (*sink.ReadResponse)(nil), err)
		}
		return
	}
	offset := 0
	for _, call := range calls {
		count := len(call.request.GetOperations())
		// Each caller owns its result slice; a slow receiver must not keep
		// other callers' payloads alive through a shared backing array.
		results := append([]*sink.ReadResult(nil), response.Results[offset:offset+count]...)
		for index, result := range results {
			result.OperationIndex = uint32(index)
		}
		split := &sink.ReadResponse{Results: results}
		completeCall(call, split, nil)
		offset += count
	}
}

// The dispatcher supplies one resource/completion partition and owns ordering.
func (s *BatchingServer) executeWrites(ctx context.Context, calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]) {
	calls = liveMutationCalls(calls)
	if len(calls) == 0 {
		return
	}
	request := combinedWriteRequest(calls)
	budgets := &responseGroups{}
	for _, call := range calls {
		budgets.addContext(call.ctx, len(call.request.GetOperations()))
	}
	execution, cancel := batchExecutionContext(ctx, calls, 0)
	defer cancel()
	completion := newWriteCompletion(calls, s.server.maxReadBytes)
	response, err := s.server.write(execution, request, budgets, completion)
	completion.finish(response, err)
}

func combinedWriteRequest(calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]) *sink.WriteRequest {
	operations := make([]*sink.WriteOperation, 0, totalWriteOperations(calls))
	for _, call := range calls {
		operations = append(operations, call.request.GetOperations()...)
	}
	request := &sink.WriteRequest{
		CompletionMode: calls[0].request.GetCompletionMode(),
		Operations:     operations,
	}
	return request
}

func totalWriteOperations(calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]) int {
	total := 0
	for _, call := range calls {
		total += len(call.request.GetOperations())
	}
	return total
}

// The dispatcher supplies one resource/completion partition and owns ordering.
func (s *BatchingServer) executeDeletes(ctx context.Context, calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]) {
	calls = liveMutationCalls(calls)
	if len(calls) == 0 {
		return
	}
	request := combinedDeleteRequest(calls)
	budgets := &responseGroups{}
	for _, call := range calls {
		budgets.addContext(call.ctx, len(call.request.GetOperations()))
	}
	execution, cancel := batchExecutionContext(ctx, calls, 0)
	defer cancel()
	response, err := s.server.delete(execution, request, budgets)
	splitDeleteResponse(calls, response, err)
}

func combinedDeleteRequest(calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]) *sink.DeleteRequest {
	operations := make([]*sink.DeleteOperation, 0, totalDeleteOperations(calls))
	for _, call := range calls {
		operations = append(operations, call.request.GetOperations()...)
	}
	request := &sink.DeleteRequest{
		CompletionMode: calls[0].request.GetCompletionMode(),
		Operations:     operations,
	}
	return request
}

func totalDeleteOperations(calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]) int {
	total := 0
	for _, call := range calls {
		total += len(call.request.GetOperations())
	}
	return total
}

func splitDeleteResponse(
	calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse],
	response *sink.DeleteResponse,
	err error,
) {
	if err != nil {
		for _, call := range calls {
			completeCall(call, (*sink.DeleteResponse)(nil), err)
		}
		return
	}
	if response == nil || len(response.GetResults()) != totalDeleteOperations(calls) {
		splitErr := status.Error(codes.Internal, "batched delete returned an invalid result count")
		for _, call := range calls {
			completeCall(call, (*sink.DeleteResponse)(nil), splitErr)
		}
		return
	}
	offset := 0
	for _, call := range calls {
		count := len(call.request.GetOperations())
		results := append([]*sink.DeleteResult(nil), response.GetResults()[offset:offset+count]...)
		for index, result := range results {
			result.OperationIndex = uint32(index)
		}
		split := &sink.DeleteResponse{Results: results}
		completeCall(call, split, nil)
		offset += count
	}
}
