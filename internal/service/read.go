package service

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	return s.read(ctx, req, responseGroupsFor(ctx, len(req.GetOperations())))
}

func (s *Server) read(ctx context.Context, req *sink.ReadRequest, budgets *responseGroups) (*sink.ReadResponse, error) {
	if req == nil || len(req.GetOperations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return nil, err
	}

	response := &sink.ReadResponse{
		Results: make([]*sink.ReadResult, len(req.GetOperations())),
	}
	defer boundResultFailures(response.Results, budgets, s.maxReadBytes)
	storageOperations := make([]storage.ReadOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	storageIndexes := make([]int, 0, len(req.GetOperations()))
	positions := make(map[recordIdentity]int)

	for index, operation := range req.GetOperations() {
		result := &sink.ReadResult{OperationIndex: uint32(index)}
		response.Results[index] = result

		address, err := protocol.ParseAddress(operation.GetAddress())
		if err == nil {
			_, err = s.storage.BatchKey(address)
		}
		if err != nil {
			setReadFailure(result, sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, err, false)
			continue
		}
		key := identityOf(address)
		position, found := positions[key]
		if !found {
			position = len(storageOperations)
			positions[key] = position
			storageOperation := storage.ReadOperation{Address: address}
			storageOperations = append(storageOperations, storageOperation)
		}
		operationIndexes = append(operationIndexes, index)
		storageIndexes = append(storageIndexes, position)
	}

	if len(storageOperations) == 0 {
		return response, nil
	}
	budget := storage.NewUnboundedReadBudget()
	storageRequest := storage.ReadRequest{Operations: storageOperations, Budget: budget}
	storageResponse, err := s.storage.Read(ctx, storageRequest)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return nil, status.Error(codes.Internal, "storage returned an invalid read result count")
	}

	defer clear(storageResponse.Results)
	// Charge copies in each original RPC's order, including repeated keys.
	// Check the aggregate output size before allocating any document copies.
	outputBudgets := budgets.fresh(s.maxReadBytes)
	for index, operationIndex := range operationIndexes {
		owner := budgets.owner(operationIndex)
		stored := storageResponse.Results[storageIndexes[index]]
		if stored.Status == storage.ReadStatusFound {
			if err := outputBudgets[owner].Reserve(len(stored.Document.Payload)); err != nil {
				code, retryable := storageFailureDetails(err)
				setReadFailure(response.Results[operationIndex], code, err, retryable)
				continue
			}
		}
	}
	for index, operationIndex := range operationIndexes {
		result := response.Results[operationIndex]
		if result.Status != sink.ReadStatus_READ_STATUS_UNSPECIFIED {
			continue
		}
		stored := storageResponse.Results[storageIndexes[index]]
		applyReadResult(result, stored)
	}
	return response, nil
}
