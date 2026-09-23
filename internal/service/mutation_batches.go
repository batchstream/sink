package service

import (
	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/storage"
)

type mutationRequest[Operation addressedOperation] interface {
	GetOperations() []Operation
	GetCompletionMode() sink.CompletionMode
}

// Requests spanning datasets retain their RPC boundary but execute alone.
// Partitioning never permits a later same-record request to pass a predecessor.
type batchPartition struct {
	resource string
	mode     sink.CompletionMode
	isolated bool
}

func mutationRequestPartition[Operation addressedOperation, Request mutationRequest[Operation]](request Request, backend storage.Storage) batchPartition {
	partition := batchPartition{mode: request.GetCompletionMode()}
	for index, operation := range request.GetOperations() {
		address, err := protocol.ParseAddress(operation.GetAddress())
		if err != nil {
			partition.isolated = true
			break
		}
		resource, err := backend.BatchKey(address)
		if err != nil {
			partition.isolated = true
			break
		}
		if index == 0 {
			partition.resource = resource
		} else if partition.resource != resource {
			partition.isolated = true
			break
		}
	}
	return partition
}

func liveMutationCalls[Request any, Response any](calls []*batchCall[Request, Response]) []*batchCall[Request, Response] {
	live := make([]*batchCall[Request, Response], 0, len(calls))
	for _, call := range calls {
		if err := contextError(call.ctx); err != nil {
			completeCall(call, emptyResponse[Response](), err)
			continue
		}
		live = append(live, call)
	}
	return live
}

func mutationRequestRecords[Operation addressedOperation, Request mutationRequest[Operation]](
	request Request,
) []recordIdentity {
	records := make([]recordIdentity, 0, len(request.GetOperations()))
	for _, operation := range request.GetOperations() {
		address, err := protocol.ParseAddress(operation.GetAddress())
		if err == nil {
			records = append(records, identityOf(address))
		}
	}
	return records
}
