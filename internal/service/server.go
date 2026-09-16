// Package service implements the public Sink gRPC service.
package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	boundStore string
	sink.UnimplementedSinkServer
	*admissionPool

	storage          storage.Storage
	lua              *merge.LuaEngine
	publisher        queue.Publisher
	maxOperations    int
	maxMergeAttempts int
	metrics          *sinkmetrics.Metrics
	maxReadBytes     int
	publishAdmission *admissionPool
}

func (s *Server) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	return s.write(ctx, req, contextBudgets(ctx, len(req.GetOperations())), nil)
}

func (s *Server) write(ctx context.Context, req *sink.WriteRequest, budgets *requestBudgets, completion *writeCompletion) (*sink.WriteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "write request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "write request has an invalid completion mode")
	}
	if hasWriteReturns(req) && req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return nil, status.Error(codes.InvalidArgument, "returned write documents require synchronous completion")
	}
	observation := s.newWriteObservation(req)
	defer observation.finish()
	estimate := s.estimateWriteExecution(req, budgets.callerCount(), returningCallerCount(req, budgets))
	reservation := &admissionReservation{}
	admission := admissionRequest{encodedBytes: estimate.bytes, stores: operationStores(req.GetOperations()), wait: budgets != nil, reservation: reservation}
	admission.inputBytes = req.SizeVT() + 128*len(req.GetOperations())
	admission.publish = req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	started := time.Now()
	ctx, release, err := s.admitRequest(ctx, admission)
	observation.phase("admission", started)
	if err != nil {
		return nil, err
	}
	defer release()
	started = time.Now()
	luaPrograms, err := parseLuaPrograms(req.GetLuaPrograms())
	if err != nil {
		observation.phase("parse", started)
		return nil, status.Errorf(codes.InvalidArgument, "write request Lua programs: %v", err)
	}

	response := &sink.WriteResponse{
		Results: make([]*sink.WriteResult, len(req.GetOperations())),
	}
	defer boundResultFailures(response.Results, budgets, s.maxReadBytes)
	operations := make([]parsedWrite, 0, len(req.GetOperations()))
	for index, operation := range req.GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		if err := contextError(ctx); err != nil {
			observation.phase("parse", started)
			return nil, err
		}
		parsed, err := s.parseWrite(ctx, index, operation, luaPrograms)
		if err := contextError(ctx); err != nil {
			observation.phase("parse", started)
			return nil, err
		}
		if err != nil {
			setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			completion.operation(index, result)
			continue
		}
		if parsed.merge != nil {
			parsed.merge.observation = observation
		}
		operations = append(operations, parsed)
	}
	observation.phase("parse", started)

	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		err := s.publishWrites(ctx, operations, response.Results)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	groups := buildWriteGroups(operations)
	memory := &writeMemoryReservation{reservation: reservation, estimate: estimate}
	executionOptions := writeExecutionOptions{
		memory:           memory,
		returns:          newWriteReturns(req, budgets, s.maxReadBytes),
		budgets:          budgets,
		completion:       completion,
		observation:      observation,
		WaitUntilVisible: req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
	}
	err = s.executeWriteGroups(ctx, groups, response.Results, executionOptions)
	if err != nil {
		return nil, err
	}
	return response, nil
}

type luaPrograms map[[sha256.Size]byte]merge.Program

func parseLuaPrograms(programs []*sink.LuaProgram) (luaPrograms, error) {
	if err := protocol.ValidateLuaDeclarations(programs); err != nil {
		return nil, err
	}
	parsed := make(luaPrograms, len(programs))
	for _, program := range programs {
		digest := sha256.Sum256(program.GetSource())
		declared := merge.Program{Source: bytes.Clone(program.GetSource()), SHA256: bytes.Clone(digest[:])}
		parsed[digest] = declared
	}

	return parsed, nil
}

func (s *Server) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	return s.delete(ctx, req, nil)
}

func (s *Server) delete(ctx context.Context, req *sink.DeleteRequest, budgets *requestBudgets) (*sink.DeleteResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "delete request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return nil, status.Error(codes.InvalidArgument, "delete request has an invalid completion mode")
	}
	admission := admissionRequest{encodedBytes: req.SizeVT() + failureResponseBytes(len(req.GetOperations())), stores: operationStores(req.GetOperations()), wait: budgets != nil}
	admission.inputBytes = req.SizeVT() + 128*len(req.GetOperations())
	admission.publish = req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()

	response := &sink.DeleteResponse{
		Results: make([]*sink.DeleteResult, len(req.GetOperations())),
	}
	defer boundResultFailures(response.Results, budgets, s.maxReadBytes)
	storageOperations := make([]storage.DeleteOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	storageIndexes := make([]int, 0, len(req.GetOperations()))
	positions := make(map[recordIdentity]int)
	queueMutations := make([]queue.Mutation, 0, len(req.GetOperations()))

	for index, operation := range req.GetOperations() {
		result := &sink.DeleteResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		address, err := convertAddress(operation.GetAddress())
		if err != nil {
			setDeleteFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		operationIndexes = append(operationIndexes, index)
		if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
			clonedMessage := proto.Clone(operation)
			cloned, ok := clonedMessage.(*sink.DeleteOperation)
			if !ok {
				return nil, status.Error(codes.Internal, "clone asynchronous delete operation")
			}
			mutation := queue.Mutation{Delete: cloned}
			queueMutations = append(queueMutations, mutation)
			continue
		}
		key := s.identityOf(address)
		position, found := positions[key]
		if !found {
			position = len(storageOperations)
			positions[key] = position
			storageOperation := storage.DeleteOperation{Address: address}
			storageOperations = append(storageOperations, storageOperation)
		}
		storageIndexes = append(storageIndexes, position)
	}

	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		err := s.publishDeletes(ctx, queueMutations, operationIndexes, response.Results)
		if err != nil {
			return nil, err
		}
		return response, nil
	}
	if len(storageOperations) == 0 {
		return response, nil
	}

	storageRequest := storage.DeleteRequest{
		Operations:       storageOperations,
		WaitUntilVisible: req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE,
	}
	storageResponse, err := s.storage.Delete(ctx, storageRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "delete records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid delete result count")
	}
	for index, operationIndex := range operationIndexes {
		storageResult := storageResponse.Results[storageIndexes[index]]
		result := response.Results[operationIndex]
		applyDeleteResult(result, storageResult)
	}
	return response, nil
}

func (s *Server) validateOperationCount(count int) error {
	if count <= s.maxOperations {
		return nil
	}
	message := fmt.Sprintf("request contains %d operations; maximum is %d", count, s.maxOperations)
	return status.Error(codes.ResourceExhausted, message)
}

func validCompletionMode(mode sink.CompletionMode) bool {
	return mode == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED ||
		mode == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED ||
		mode == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
}
