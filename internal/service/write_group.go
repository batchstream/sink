package service

import (
	"context"
	"errors"
	"time"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/storage"
)

func (g writeGroup) nextCommit() (writeGroup, writeGroup) {
	end := len(g.operations)
	for index, operation := range g.operations {
		if operation.original.GetReturnDocument() {
			end = max(1, index)
			break
		}
	}
	segment := writeGroup{operations: g.operations[:end]}
	for _, operation := range segment.operations {
		if operation.merge != nil {
			segment.merges++
		}
	}
	remaining := writeGroup{operations: g.operations[end:], merges: g.merges - segment.merges}
	return segment, remaining
}

func prepareWriteGroup(ctx context.Context, group writeGroup, stored storage.ReadResult, results []*sink.WriteResult) (writeGroupCandidate, bool) {
	candidate := writeGroupCandidate{group: group}
	// The commit condition belongs to the original snapshot, never to an
	// intermediate document produced by a Put or Merge in this chain.
	candidate.operation.Address = group.operations[0].address
	switch stored.Status {
	case storage.ReadStatusNotFound:
		candidate.operation.Precondition.Kind = storage.PreconditionRecordNotExists
	case storage.ReadStatusFound:
		if len(stored.Revision.Data) == 0 {
			candidate.operation.Precondition.Kind = storage.PreconditionRevisionAbsent
		} else {
			candidate.operation.Precondition.Kind = storage.PreconditionRevisionMatches
			candidate.operation.Precondition.Revision = stored.Revision
		}
	default:
		cause := stored.Err
		if cause == nil {
			cause = errors.New("storage returned an invalid conditional write snapshot")
		}
		failure := storage.WriteResult{Status: storage.WriteStatusFailed, Err: cause}
		applyWriteGroupResult(group, results, failure)
		return candidate, false
	}
	current := stored.Document
	exists := stored.Status == storage.ReadStatusFound
	include := false
	for _, operation := range group.operations {
		// A retry reevaluates failures too: they may depend on an uncommitted
		// predecessor whose outcome changed with the new snapshot.
		result := results[operation.index]
		result.Status = sink.WriteStatus_WRITE_STATUS_UNSPECIFIED
		result.Failure = nil
		result.Document = nil
		if operation.put != nil {
			condition := operation.put.precondition.Kind
			if (condition == storage.PreconditionRecordExists && !exists) ||
				(condition == storage.PreconditionRecordNotExists && exists) {
				failure := storage.WriteResult{Status: storage.WriteStatusPreconditionFailed}
				applyWriteResult(result, failure)
				continue
			}
			current = operation.put.document
		} else {
			request := merge.Request{Incoming: operation.merge.incoming, ObservedAt: operation.merge.observedAt}
			if exists {
				request.Current = &current
			}
			started := time.Now()
			merged, err := operation.merge.merger.Merge(ctx, request)
			operation.merge.observation.phase("lua", started)
			if err != nil {
				setWriteFailure(result, mergeFailureCode(err), err, ctx.Err() != nil)
				continue
			}
			if len(merged.Document.Payload) == 0 {
				err := errors.New("lua merge program returned an invalid document")
				setWriteFailure(result, sink.FailureCode_FAILURE_CODE_INTERNAL, err, false)
				continue
			}
			current = merged.Document
		}
		candidate.operation.Document = current
		exists = true
		include = true
	}
	return candidate, include
}

func applyWriteGroupResult(group writeGroup, results []*sink.WriteResult, stored storage.WriteResult) {
	for _, operation := range group.operations {
		result := results[operation.index]
		result.Document = nil
		// Conditional and Lua failures become final only when their speculative
		// state commits. A failed commit leaves the entire chain unresolved.
		if stored.Status == storage.WriteStatusApplied && result.Failure != nil {
			continue
		}
		result.Failure = nil
		applyWriteResult(result, stored)
	}
}
