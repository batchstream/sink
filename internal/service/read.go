package service

import (
	"context"
	"errors"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	outcome, err := s.read(ctx, req, contextBudgets(ctx, len(req.GetOperations())))
	if err != nil {
		return nil, err
	}
	return outcome.response, nil
}

type readOutcome struct {
	response *sink.ReadResponse
	deferred []bool
}

func (s *Server) read(ctx context.Context, req *sink.ReadRequest, budgets *requestBudgets) (readOutcome, error) {
	var outcome readOutcome
	if req == nil || len(req.GetOperations()) == 0 {
		return outcome, status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.GetOperations())); err != nil {
		return outcome, err
	}
	admission := admissionRequest{encodedBytes: req.SizeVT() + failureResponseBytes(len(req.GetOperations())) + 2*s.maxReadBytes, stores: operationStores(req.GetOperations()), wait: budgets != nil}
	admission.inputBytes = req.SizeVT() + 128*len(req.GetOperations())
	admission.resultBytes = failureResponseBytes(len(req.GetOperations()))
	admission.sharedInput = budgets.ownsInput()
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return outcome, err
	}
	defer release()
	if budgets == nil {
		budgets = contextBudgets(ctx, len(req.GetOperations()))
	}
	if budgets != nil {
		budgets.producer = capacity.FromContext(ctx)
	}

	response := &sink.ReadResponse{
		Results: make([]*sink.ReadResult, len(req.GetOperations())),
	}
	defer boundResultFailures(response.Results, budgets, s.maxReadBytes)
	outcome.response = response
	outcome.deferred = make([]bool, budgets.callerCount())
	storageOperations := make([]storage.ReadOperation, 0, len(req.GetOperations()))
	operationIndexes := make([]int, 0, len(req.GetOperations()))
	storageIndexes := make([]int, 0, len(req.GetOperations()))
	owners := make([][]int, 0, len(req.GetOperations()))
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
			owners = append(owners, nil)
		}
		owners[position] = append(owners[position], budgets.owner(index))
		operationIndexes = append(operationIndexes, index)
		storageIndexes = append(storageIndexes, position)
	}

	if len(storageOperations) == 0 {
		return outcome, nil
	}
	snapshotBudgets := budgets.fresh(forwarding.Snapshots, s.maxReadBytes)
	working := storage.NewReadBudget(s.maxReadBytes)
	snapshots := capacity.FromContext(ctx).NewLease()
	defer capacity.Close(snapshots)
	for index := range storageOperations {
		budget := sharedSnapshotBudget(owners[index], snapshotBudgets)
		if budgets.callerCount() > 1 {
			budget = storage.NewWorkingSetReadBudget(budget, working)
		}
		storageOperations[index].Budget = storage.WithMemoryBudget(ctx, budget, snapshots)
	}
	storageRequest := storage.ReadRequest{Operations: storageOperations, Budget: storage.NewReadBudget(s.maxReadBytes)}
	storageResponse, err := s.storage.Read(ctx, storageRequest)
	if err != nil {
		return outcome, status.Errorf(codes.Unavailable, "read records: %v", err)
	}
	if len(storageResponse.Results) != len(storageOperations) {
		return outcome, status.Error(codes.Internal, "storage returned an invalid read result count")
	}

	defer clear(storageResponse.Results)
	for index, operationIndex := range operationIndexes {
		stored := storageResponse.Results[storageIndexes[index]]
		if errors.Is(stored.Err, storage.ErrReadWorkingSetFull) {
			outcome.deferred[budgets.owner(operationIndex)] = true
		}
	}

	// Charge copies in each original RPC's order, including repeated keys.
	// Check the aggregate output size before allocating any document copies.
	outputBudgets := budgets.fresh(forwarding.Outputs, s.maxReadBytes)
	outputBytes := make([]int, budgets.callerCount())
	for index, operationIndex := range operationIndexes {
		owner := budgets.owner(operationIndex)
		if outcome.deferred[owner] {
			continue
		}
		stored := storageResponse.Results[storageIndexes[index]]
		if stored.Status == storage.ReadStatusFound {
			if err := outputBudgets[owner].Reserve(len(stored.Document.Payload)); err != nil {
				code, retryable := storageFailureDetails(err)
				setReadFailure(response.Results[operationIndex], code, err, retryable)
				continue
			}
			outputBytes[owner] += len(stored.Document.Payload) + 128
		}
	}
	remaining := s.maxReadBytes
	for owner, size := range outputBytes {
		if size > remaining {
			outcome.deferred[owner] = true
		} else {
			remaining -= size
		}
	}
	for index, operationIndex := range operationIndexes {
		result := response.Results[operationIndex]
		if outcome.deferred[budgets.owner(operationIndex)] || result.Status != sink.ReadStatus_READ_STATUS_UNSPECIFIED {
			continue
		}
		stored := storageResponse.Results[storageIndexes[index]]
		if stored.Status == storage.ReadStatusFound {
			if err := budgets.output(budgets.owner(operationIndex), len(stored.Document.Payload)+128); err != nil {
				setReadFailure(result, sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED, err, true)
				continue
			}
		}
		applyReadResult(result, stored)
	}
	return outcome, nil
}
