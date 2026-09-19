package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/protocol"
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
	sink.UnimplementedSinkServer

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
		MaxConcurrent:       server.maxInFlightRequests,
		Unlimited:           server.memory != nil,
		Method:              "Read",
		MaxWait:             normalized.MaxWait,
		MaxOperations:       normalized.MaxOperations,
		MaxBytes:            normalized.MaxBytes,
		MaxQueuedOperations: normalized.MaxQueuedOperations,
		MaxQueuedBytes:      normalized.MaxQueuedBytes,
		Execute:             batching.executeReads,
		Metrics:             normalized.Metrics,
		ExecutionTimeout:    server.requestTimeout,
	}
	readOptions.Store = server.boundStore
	batching.reads = newRequestBatcher(readOptions)

	writeOptions := requestBatcherOptions[*sink.WriteRequest, *sink.WriteResponse]{
		MaxConcurrent: server.maxInFlightRequests,
		Unlimited:     server.memory != nil,
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
		ExecutionTimeout:    server.requestTimeout,
	}
	writeOptions.Store = server.boundStore
	batching.writes = newRequestBatcher(writeOptions)

	deleteOptions := requestBatcherOptions[*sink.DeleteRequest, *sink.DeleteResponse]{
		MaxConcurrent: server.maxInFlightRequests,
		Unlimited:     server.memory != nil,
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
		ExecutionTimeout:    server.requestTimeout,
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
	ctx, cancel := executionContext(ctx, s.server.requestTimeout)
	defer cancel()
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.server.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	ctx, release, err := s.server.beginBatchMemory(ctx, req)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.reads.Submit(ctx, req, len(req.GetOperations()), req.SizeVT())
}

func (s *BatchingServer) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if err := protocol.CheckStore(req, s.server.boundStore); err != nil {
		return nil, err
	}
	ctx, cancel := executionContext(ctx, s.server.requestTimeout)
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
	ctx, release, err := s.server.beginBatchMemory(ctx, normalized)
	if err != nil {
		return nil, err
	}
	defer release()
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
	ctx, cancel := executionContext(ctx, s.server.requestTimeout)
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
	ctx, release, err := s.server.beginBatchMemory(ctx, req)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.deletes.Submit(ctx, req, len(req.GetOperations()), req.SizeVT())
}

func (s *BatchingServer) executeReads(
	ctx context.Context,
	calls []*batchCall[*sink.ReadRequest, *sink.ReadResponse],
) {
	calls = liveMutationCalls(calls)
	limit := len(calls)
	for len(calls) > 0 {
		count := 0
		bytes := 2 * s.server.maxReadBytes
		if s.server.memory != nil {
			bytes = 0
		}
		byteLimit := s.server.maxInFlightBytes
		if s.server.memory != nil {
			byteLimit = int(s.server.memory.Limit())
		}
		for count < min(limit, len(calls)) {
			next := calls[count].request.SizeVT() + failureResponseBytes(len(calls[count].request.GetOperations()))
			if count > 0 && next > byteLimit-bytes {
				break
			}
			bytes += next
			count++
		}
		group := liveMutationCalls(calls[:count])
		tail := calls[count:]
		if len(group) == 0 {
			calls = tail
			continue
		}
		operations := make([]*sink.ReadOperation, 0, totalReadOperations(group))
		budgets := &requestBudgets{}
		for _, call := range group {
			operations = append(operations, call.request.GetOperations()...)
			budgets.addContext(call.ctx, len(call.request.GetOperations()))
		}
		request := &sink.ReadRequest{Operations: operations}
		execution, cancel := batchExecutionContext(ctx, group, s.server.requestTimeout)
		outcome, err := s.server.read(execution, request, budgets)
		cancel()
		deferred := splitReadResponse(group, outcome, err)
		if outcome.response != nil {
			clear(outcome.response.Results)
		}
		if len(deferred) > 0 {
			// Retry whole RPCs in smaller groups. No partial response escapes,
			// and a deferred caller retains its own budget and repeated-key view.
			limit = max(1, len(group)-len(deferred))
		}
		calls = append(deferred, tail...)
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
	outcome readOutcome,
	err error,
) []*batchCall[*sink.ReadRequest, *sink.ReadResponse] {
	response := outcome.response
	if err == nil && (response == nil || len(response.Results) != totalReadOperations(calls) ||
		(len(outcome.deferred) != 0 && len(outcome.deferred) != len(calls))) {
		err = status.Error(codes.Internal, "batched read returned an invalid result count")
	}
	if err != nil {
		for _, call := range calls {
			completeCall(call, (*sink.ReadResponse)(nil), err)
		}
		return nil
	}
	var deferred []*batchCall[*sink.ReadRequest, *sink.ReadResponse]
	offset := 0
	for owner, call := range calls {
		count := len(call.request.GetOperations())
		if len(outcome.deferred) != 0 && outcome.deferred[owner] {
			deferred = append(deferred, call)
		} else {
			// Each caller owns its result slice; a slow receiver must not keep
			// other callers' payloads alive through a shared backing array.
			results := append([]*sink.ReadResult(nil), response.Results[offset:offset+count]...)
			for index, result := range results {
				result.OperationIndex = uint32(index)
			}
			split := &sink.ReadResponse{Results: results}
			completeCall(call, split, nil)
		}
		offset += count
	}
	return deferred
}

func (s *BatchingServer) executeWrites(
	ctx context.Context,
	calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse],
) {
	for _, wave := range planMutationWaves[*sink.WriteOperation](calls) {
		parallel := len(wave.applied) > 0 && len(wave.visible) > 0 &&
			s.server.maxInFlightRequests > 1
		if parallel {
			applied := combinedWriteRequest(wave.applied)
			visible := combinedWriteRequest(wave.visible)
			appliedBytes := s.server.estimateWriteExecution(applied, len(wave.applied), returningBatchCallers(wave.applied)).bytes
			visibleBytes := s.server.estimateWriteExecution(visible, len(wave.visible), returningBatchCallers(wave.visible)).bytes
			byteLimit := s.server.maxInFlightBytes
			if s.server.memory != nil {
				byteLimit = int(s.server.memory.Limit())
			}
			parallel = appliedBytes+visibleBytes <= byteLimit
		}
		var executions sync.WaitGroup
		for _, group := range [][]*batchCall[*sink.WriteRequest, *sink.WriteResponse]{wave.applied, wave.visible} {
			if len(group) == 0 {
				continue
			}
			if parallel {
				executions.Go(func() { s.executeWriteBatch(ctx, group) })
			} else {
				s.executeWriteBatch(ctx, group)
			}
		}
		executions.Wait()
	}
}

func (s *BatchingServer) executeWriteBatch(
	ctx context.Context,
	calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse],
) {
	calls = liveMutationCalls(calls)
	if len(calls) == 0 {
		return
	}
	ctx, cancel := batchExecutionContext(ctx, calls, s.server.requestTimeout)
	defer cancel()
	for start := 0; start < len(calls); {
		byteLimit := s.server.maxInFlightBytes
		if s.server.memory != nil {
			byteLimit = int(s.server.memory.Limit())
		}
		end := len(calls)
		combined := combinedWriteRequest(calls[start:end])
		if s.server.estimateWriteExecution(combined, end-start, returningBatchCallers(calls[start:end])).bytes > byteLimit {
			end = start + 1
		}
		for end < len(calls) {
			next := combinedWriteRequest(calls[start : end+1])
			if s.server.estimateWriteExecution(next, end+1-start, returningBatchCallers(calls[start:end+1])).bytes > byteLimit {
				break
			}
			end++
		}
		group := liveMutationCalls(calls[start:end])
		if len(group) == 0 {
			start = end
			continue
		}
		request := combinedWriteRequest(group)
		budgets := &requestBudgets{}
		for _, call := range group {
			budgets.addContext(call.ctx, len(call.request.GetOperations()))
		}
		execution, executionCancel := batchExecutionContext(ctx, group, s.server.requestTimeout)
		completion := newWriteCompletion(group, s.server.maxReadBytes)
		response, err := s.server.write(execution, request, budgets, completion)
		executionCancel()
		completion.finish(response, err)
		start = end
	}
}

func returningBatchCallers(calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]) int {
	count := 0
	for _, call := range calls {
		if hasWriteReturns(call.request) {
			count++
		}
	}
	return count
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

func (s *BatchingServer) executeDeletes(
	ctx context.Context,
	calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse],
) {
	for _, wave := range planMutationWaves[*sink.DeleteOperation](calls) {
		parallel := len(wave.applied) > 0 && len(wave.visible) > 0 &&
			s.server.maxInFlightRequests > 1
		if parallel {
			applied := combinedDeleteRequest(wave.applied)
			visible := combinedDeleteRequest(wave.visible)
			responseBytes := failureResponseBytes(len(applied.Operations) + len(visible.Operations))
			byteLimit := s.server.maxInFlightBytes
			if s.server.memory != nil {
				byteLimit = int(s.server.memory.Limit())
			}
			parallel = applied.SizeVT()+visible.SizeVT()+responseBytes <= byteLimit
		}
		var executions sync.WaitGroup
		for _, group := range [][]*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]{wave.applied, wave.visible} {
			if len(group) == 0 {
				continue
			}
			if parallel {
				executions.Go(func() { s.executeDeleteBatch(ctx, group) })
			} else {
				s.executeDeleteBatch(ctx, group)
			}
		}
		executions.Wait()
	}
}

func (s *BatchingServer) executeDeleteBatch(
	ctx context.Context,
	calls []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse],
) {
	calls = liveMutationCalls(calls)
	if len(calls) == 0 {
		return
	}
	ctx, cancel := batchExecutionContext(ctx, calls, s.server.requestTimeout)
	defer cancel()
	for len(calls) > 0 {
		count, bytes := 0, 0
		byteLimit := s.server.maxInFlightBytes
		if s.server.memory != nil {
			byteLimit = int(s.server.memory.Limit())
		}
		for count < len(calls) {
			next := calls[count].request.SizeVT() + failureResponseBytes(len(calls[count].request.GetOperations()))
			if count > 0 && next > byteLimit-bytes {
				break
			}
			bytes += next
			count++
		}
		group := liveMutationCalls(calls[:count])
		calls = calls[count:]
		if len(group) == 0 {
			continue
		}
		request := combinedDeleteRequest(group)
		budgets := &requestBudgets{}
		for _, call := range group {
			budgets.addContext(call.ctx, len(call.request.GetOperations()))
		}
		execution, executionCancel := batchExecutionContext(ctx, group, s.server.requestTimeout)
		response, err := s.server.delete(execution, request, budgets)
		executionCancel()
		splitDeleteResponse(group, response, err)
	}
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

// In-process callers have no gRPC stats scope. Give them the same input ownership
// before queuing; Submit keeps a producer reference if the caller cancels.
func (s *Server) beginBatchMemory(ctx context.Context, req interface{ SizeVT() int }) (context.Context, func(), error) {
	if s.memory == nil || capacity.FromContext(ctx) != nil {
		return ctx, func() {}, nil
	}
	scope := s.memory.NewScope()
	if err := scope.Admit(ctx, 2*req.SizeVT()+1024); err != nil {
		scope.Release()
		return ctx, nil, err
	}
	if err := scope.AdmitCompletion(ctx, protocol.CompletionBytes(req)); err != nil {
		scope.Release()
		return ctx, nil, err
	}
	return capacity.WithScope(ctx, scope), scope.Release, nil
}
