package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/batchstream/sink/internal/protocol"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type parsedWrite struct {
	index    int
	address  storage.Address
	original *sink.WriteOperation
	put      *parsedPut
	merge    *parsedMerge
	identity recordIdentity
}

type parsedPut struct {
	document     storage.Document
	precondition storage.Precondition
}

type parsedMerge struct {
	observation *writeObservation
	incoming    storage.Document
	merger      merge.Merger
	program     merge.Program
	observedAt  time.Time
}

// A group contains the ordered writes for one complete record address.
type writeGroup struct {
	operations []parsedWrite
	merges     int
}

type writeGroupCandidate struct {
	group     writeGroup
	operation storage.WriteOperation
}

type writeExecutionOptions struct {
	returns          *writeReturns
	completion       *writeCompletion
	observation      *writeObservation
	WaitUntilVisible bool
}

func (o writeExecutionOptions) complete(group writeGroup, results []*sink.WriteResult) {
	o.returns.settle(group, results)
	o.completion.group(group, results)
}

func (s *Server) parseWrite(ctx context.Context, index int, operation *sink.WriteOperation, programs luaPrograms) (parsedWrite, error) {
	parsed := parsedWrite{}
	if operation == nil {
		return parsed, errors.New("write operation is required")
	}
	address, err := protocol.ParseAddress(operation.GetAddress())
	if err != nil {
		return parsed, err
	}
	if _, err := s.storage.BatchKey(address); err != nil {
		return parsed, err
	}
	parsed.index = index
	parsed.address = address
	parsed.original = operation
	parsed.identity = identityOf(address)

	switch action := operation.GetAction().(type) {
	case *sink.WriteOperation_Put:
		put, parseErr := parsePut(action.Put)
		if parseErr != nil {
			return parsed, parseErr
		}
		parsed.put = &put
	case *sink.WriteOperation_Merge:
		mergeOperation, parseErr := s.parseMerge(ctx, action.Merge, programs)
		if parseErr != nil {
			return parsed, parseErr
		}
		parsed.merge = &mergeOperation
	default:
		return parsed, errors.New("write action is required")
	}
	return parsed, nil
}

func parsePut(operation *sink.PutOperation) (parsedPut, error) {
	parsed := parsedPut{}
	if operation == nil {
		return parsed, errors.New("put operation is required")
	}
	document, err := convertDocument(operation.GetDocument())
	if err != nil {
		return parsed, err
	}
	parsed.document = document

	switch operation.GetMode() {
	case sink.WriteMode_WRITE_MODE_CREATE:
		parsed.precondition.Kind = storage.PreconditionRecordNotExists
	case sink.WriteMode_WRITE_MODE_REPLACE:
		parsed.precondition.Kind = storage.PreconditionRecordExists
	case sink.WriteMode_WRITE_MODE_UPSERT:
		parsed.precondition.Kind = storage.PreconditionNone
	default:
		return parsed, errors.New("put operation has an invalid write mode")
	}
	return parsed, nil
}

func (s *Server) parseMerge(ctx context.Context, operation *sink.MergeOperation, programs luaPrograms) (parsedMerge, error) {
	parsed := parsedMerge{}
	if operation == nil {
		return parsed, errors.New("merge operation is required")
	}
	incoming, err := convertDocument(operation.GetIncomingDocument())
	if err != nil {
		return parsed, err
	}
	program := operation.GetLuaProgram()
	if program == nil {
		return parsed, errors.New("lua merge program is required")
	}
	mergeProgram, err := resolveLuaProgram(program, programs)
	if err != nil {
		return parsed, err
	}
	merger, err := s.lua.Compile(ctx, mergeProgram)
	if err != nil {
		return parsed, err
	}
	parsed.incoming = incoming
	parsed.merger = merger
	parsed.program = mergeProgram
	parsed.observedAt = time.Now().UTC()
	return parsed, nil
}

func resolveLuaProgram(program *sink.LuaProgram, programs luaPrograms) (merge.Program, error) {
	var resolved merge.Program
	if len(program.GetSource()) > 0 {
		digest := sha256.Sum256(program.GetSource())
		if len(program.GetSha256()) != 0 &&
			(len(program.GetSha256()) != sha256.Size || !bytes.Equal(program.GetSha256(), digest[:])) {
			return resolved, errors.New("lua program SHA-256 digest does not match source")
		}
		resolved.Source = bytes.Clone(program.GetSource())
		resolved.SHA256 = bytes.Clone(digest[:])
		return resolved, nil
	}
	if len(program.GetSha256()) != sha256.Size {
		return resolved, errors.New("lua program source or SHA-256 reference is required")
	}
	digest := [sha256.Size]byte(program.GetSha256())
	declared, ok := programs[digest]
	if !ok {
		return resolved, errors.New("lua program SHA-256 reference was not declared in the write request")
	}
	// Declarations already own their buffers and compiled programs are immutable.
	// Preserve sharing when many operations reference one declaration.
	resolved.Source = declared.Source
	resolved.SHA256 = declared.SHA256
	return resolved, nil
}

func buildWriteGroups(operations []parsedWrite) []writeGroup {
	groups := make([]writeGroup, 0)
	positions := make(map[recordIdentity]int)
	for _, operation := range operations {
		position, found := positions[operation.identity]
		if !found {
			position = len(groups)
			positions[operation.identity] = position
			var group writeGroup
			groups = append(groups, group)
		}
		groups[position].operations = append(groups[position].operations, operation)
		if operation.merge != nil {
			groups[position].merges++
		}
	}
	return groups
}

// Single puts and unconditional put chains do not need a snapshot or a CAS.
func (g writeGroup) directPut() bool {
	if len(g.operations) == 1 {
		return g.operations[0].put != nil
	}
	for _, operation := range g.operations {
		if operation.put == nil || operation.put.precondition.Kind != storage.PreconditionNone {
			return false
		}
	}
	return true
}

func (s *Server) executeWriteGroups(
	ctx context.Context,
	groups []writeGroup,
	results []*sink.WriteResult,
	opts writeExecutionOptions,
) error {
	// A returned operation is its own commit. Consume each surrounding run
	// once, retaining folding on both sides without rescanning the whole tail.
	for len(groups) > 0 {
		wave := make([]writeGroup, 0, len(groups))
		next := make([]writeGroup, 0)
		for _, group := range groups {
			segment, remaining := group.nextCommit()
			wave = append(wave, segment)
			if len(remaining.operations) > 0 {
				next = append(next, remaining)
			}
		}
		err := s.executeWriteWave(ctx, wave, results, opts)
		if err != nil {
			return err
		}
		groups = next
	}
	return nil
}

func (s *Server) executeWriteWave(
	ctx context.Context,
	groups []writeGroup,
	results []*sink.WriteResult,
	opts writeExecutionOptions,
) error {
	candidates := make([]writeGroupCandidate, 0, len(groups))
	conditional := make([]writeGroup, 0, len(groups))
	for _, group := range groups {
		s.metrics.ObserveMergeFold(group.operations[0].address.Store(), group.merges)
		if !group.directPut() {
			conditional = append(conditional, group)
			continue
		}
		operation := group.operations[len(group.operations)-1]
		storageOperation := storage.WriteOperation{
			Address:      operation.address,
			Document:     operation.put.document,
			Precondition: operation.put.precondition,
		}
		candidate := writeGroupCandidate{group: group, operation: storageOperation}
		candidates = append(candidates, candidate)
	}
	_, err := s.commitWriteCandidates(ctx, candidates, results, opts)
	if err != nil {
		return err
	}
	return s.executeConditionalWrites(ctx, conditional, results, opts)
}

func (s *Server) executeConditionalWrites(
	ctx context.Context,
	groups []writeGroup,
	results []*sink.WriteResult,
	opts writeExecutionOptions,
) error {
	pending := groups
	for range s.maxMergeAttempts {
		if len(pending) == 0 {
			return nil
		}
		if err := contextError(ctx); err != nil {
			return err
		}
		next, err := s.executeWriteAttempt(ctx, pending, results, opts)
		if err != nil {
			return err
		}
		for _, group := range next {
			if group.merges > 0 {
				s.metrics.ObserveMergeConflict(group.operations[0].address.Store(), 1)
			}
		}
		pending = next
	}

	for _, group := range pending {
		s.metrics.ObserveMergeExhausted(group.operations[0].address.Store(), group.merges)
		for _, operation := range group.operations {
			result := results[operation.index]
			result.Status = sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
			conflictErr := errors.New("record changed during conditional write")
			result.Failure = newFailure(sink.FailureCode_FAILURE_CODE_CONFLICT, conflictErr, true)
		}
		opts.complete(group, results)
	}
	return nil
}

func (s *Server) executeWriteAttempt(
	ctx context.Context,
	groups []writeGroup,
	results []*sink.WriteResult,
	opts writeExecutionOptions,
) ([]writeGroup, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	read, err := s.readWriteSnapshots(ctx, groups, opts.observation)
	if err != nil {
		return nil, err
	}
	defer clear(read.Results)
	candidates := make([]writeGroupCandidate, 0, len(groups))
	for index, stored := range read.Results {
		group := groups[index]
		candidate, include := prepareWriteGroup(ctx, group, stored, results)
		if include {
			candidates = append(candidates, candidate)
		} else {
			opts.complete(group, results)
		}
	}
	return s.commitWriteCandidates(ctx, candidates, results, opts)
}

func (s *Server) readWriteSnapshots(ctx context.Context, groups []writeGroup, observation *writeObservation) (storage.ReadResponse, error) {
	budget := storage.NewUnboundedReadBudget()
	readOperations := make([]storage.ReadOperation, 0, len(groups))
	for _, group := range groups {
		readOperation := storage.ReadOperation{Address: group.operations[0].address, Budget: budget}
		readOperations = append(readOperations, readOperation)
	}
	readRequest := storage.ReadRequest{Operations: readOperations, Budget: budget}
	started := time.Now()
	readResponse, err := s.storage.Read(ctx, readRequest)
	observation.phase("storage_read", started)
	if err != nil {
		return readResponse, status.Errorf(codes.Unavailable, "read records for conditional write: %v", err)
	}
	if len(readResponse.Results) != len(groups) {
		return readResponse, status.Error(codes.Internal, "storage returned an invalid conditional read result count")
	}
	return readResponse, nil
}

func (s *Server) commitWriteCandidates(ctx context.Context, candidates []writeGroupCandidate, results []*sink.WriteResult, opts writeExecutionOptions) ([]writeGroup, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	writeOperations := make([]storage.WriteOperation, 0, len(candidates))
	ready := candidates[:0]
	for _, candidate := range candidates {
		if err := opts.returns.reserve(candidate.group, candidate.operation.Document); err != nil {
			failure := storage.WriteResult{Status: storage.WriteStatusFailed, Err: err}
			applyWriteGroupResult(candidate.group, results, failure)
			opts.complete(candidate.group, results)
			continue
		}
		writeOperations = append(writeOperations, candidate.operation)
		ready = append(ready, candidate)
	}
	if len(ready) == 0 {
		return nil, nil
	}
	writeRequest := storage.WriteRequest{
		Operations:       writeOperations,
		WaitUntilVisible: opts.WaitUntilVisible,
	}
	started := time.Now()
	writeResponse, err := s.storage.Write(ctx, writeRequest)
	opts.observation.phase("storage_write", started)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "write records: %v", err)
	}
	if len(writeResponse.Results) != len(ready) {
		return nil, status.Error(codes.Internal, "storage returned an invalid write result count")
	}

	next := make([]writeGroup, 0)
	for index, stored := range writeResponse.Results {
		group := ready[index].group
		if stored.Status == storage.WriteStatusPreconditionFailed && !group.directPut() {
			next = append(next, group)
			continue
		}
		applyWriteGroupResult(group, results, stored)
		attachWriteDocument(group, results, ready[index].operation.Document)
		opts.complete(group, results)
	}
	return next, nil
}

func mergeFailureCode(err error) sink.FailureCode {
	switch {
	case errors.Is(err, merge.ErrExecutionDeadline):
		return sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED
	case errors.Is(err, merge.ErrExecutionExhausted):
		return sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
	case errors.Is(err, merge.ErrInvalidProgram), errors.Is(err, merge.ErrInvalidIncoming),
		errors.Is(err, merge.ErrInvalidResult), errors.Is(err, merge.ErrExecution):
		return sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT
	default:
		return sink.FailureCode_FAILURE_CODE_INTERNAL
	}
}
