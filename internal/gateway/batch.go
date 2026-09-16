package gateway

import (
	"context"
	"strings"
	"sync"
	"unicode/utf8"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type storeGroup struct {
	route   Route
	indices []int
}

func (s *Server) records(ctx context.Context, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	addresses, err := s.validateBatch(req)
	if err != nil {
		return nil, err
	}
	ctx, release, err := s.begin(ctx, req)
	if err != nil {
		return nil, err
	}
	defer release()
	view := s.current
	response := emptyResponse(req, len(addresses))
	groups := make([]storeGroup, 0)
	positions := make(map[Route]int)
	resolved := make(map[string][]Route)
	resolutionErrors := make(map[string]error)
	owners := make(map[string]Route)
	for index, raw := range addresses {
		address, parseErr := protocol.ParseAddress(raw)
		if parseErr != nil {
			failRecords(response, []int{index}, status.Error(codes.InvalidArgument, parseErr.Error()), true)
			continue
		}
		store := address.Store()
		route, routeErr := routeFor(view, store)
		if routeErr != nil {
			failRecords(response, []int{index}, routeErr, true)
			continue
		}
		targets, found := resolved[store]
		resolutionErr, failed := resolutionErrors[store]
		if !found && !failed {
			var release func()
			targets, release, resolutionErr = s.pool.destinations(ctx, route)
			if resolutionErr != nil {
				resolutionErrors[store] = resolutionErr
			} else {
				resolved[store] = targets
				defer release()
			}
		}
		if resolutionErr != nil {
			failRecords(response, []int{index}, resolutionErr, true)
			continue
		}
		recordURI := raw.GetUri()
		owner, found := owners[recordURI]
		if !found {
			owner = affinityRoute(recordURI, targets)
			owners[recordURI] = owner
		}
		route = owner
		position, exists := positions[route]
		if !exists {
			position = len(groups)
			positions[route] = position
			group := storeGroup{route: route}
			groups = append(groups, group)
		}
		groups[position].indices = append(groups[position].indices, index)
	}
	remaining := forwarding.FullBudget(s.request.MaxReadBytes)
	run := func(group storeGroup, grant *forward.Budget) *forward.Budget {
		sub := splitRequest(req, group.indices)
		sub.Grant = grant
		result, callErr := s.forward(ctx, group.route, sub)
		// An absent settlement consumes its entire grant, including on a lost reply.
		used := grant
		notStarted := false
		if callErr == nil {
			used = result.GetUsed()
			notStarted = result.GetNotStarted()
			if result.GetCode() != 0 {
				callErr = forwardedError(result)
			}
		}
		if callErr == nil {
			callErr = mergeRecords(response, result, group.indices)
		}
		if callErr != nil {
			failRecords(response, group.indices, callErr, notStarted)
		}
		return used
	}
	if parallelBatch(req) {
		// These operations do not consume document budgets. Bound fanout independently
		// of the number of Stores; requests never create an unbounded goroutine fanout.
		var work sync.WaitGroup
		jobs := make(chan storeGroup)
		for range min(s.config.MaxFanout, len(groups)) {
			work.Go(func() {
				for group := range jobs {
					run(group, forwarding.FullBudget(s.request.MaxReadBytes))
				}
			})
		}
		for _, group := range groups {
			jobs <- group
		}
		close(jobs)
		work.Wait()
	} else {
		for _, group := range groups {
			grant := proto.Clone(remaining).(*forward.Budget)
			used := run(group, grant)
			consume(remaining, used)
		}
	}
	boundFailures(response, s.request.MaxReadBytes, len(addresses))
	return response, nil
}

func (s *Server) validateBatch(req *forward.ForwardRequest) ([]*sink.RecordAddress, error) {
	var addresses []*sink.RecordAddress
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		for _, op := range body.Read.GetOperations() {
			addresses = append(addresses, op.GetAddress())
		}
	case *forward.ForwardRequest_Write:
		mode := body.Write.GetCompletionMode()
		if !validCompletion(mode) {
			return nil, status.Error(codes.InvalidArgument, "invalid completion mode")
		}
		if err := protocol.ValidateLuaDeclarations(body.Write.GetLuaPrograms()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		for _, op := range body.Write.GetOperations() {
			if op.GetReturnDocument() && mode == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
				return nil, status.Error(codes.InvalidArgument, "returned write documents require synchronous completion")
			}
			addresses = append(addresses, op.GetAddress())
		}
	case *forward.ForwardRequest_Delete:
		if !validCompletion(body.Delete.GetCompletionMode()) {
			return nil, status.Error(codes.InvalidArgument, "invalid completion mode")
		}
		for _, op := range body.Delete.GetOperations() {
			addresses = append(addresses, op.GetAddress())
		}
	}
	if len(addresses) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request must contain operations")
	}
	if len(addresses) > s.request.MaxOperations {
		return nil, status.Error(codes.ResourceExhausted, "request exceeds operation limit")
	}
	return addresses, nil
}
func validCompletion(mode sink.CompletionMode) bool {
	return mode >= sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED && mode <= sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE
}
func parallelBatch(req *forward.ForwardRequest) bool {
	if req.GetDelete() != nil {
		return true
	}
	write := req.GetWrite()
	if write == nil {
		return false
	}
	if write.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return true
	}
	for _, op := range write.GetOperations() {
		if op.GetReturnDocument() || op.GetPut() == nil || op.GetPut().GetMode() != sink.WriteMode_WRITE_MODE_UPSERT {
			return false
		}
	}
	return true
}
func splitRequest(req *forward.ForwardRequest, indices []int) *forward.ForwardRequest {
	split := &forward.ForwardRequest{}
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		read := &sink.ReadRequest{}
		for _, i := range indices {
			read.Operations = append(read.Operations, body.Read.Operations[i])
		}
		split.Request = &forward.ForwardRequest_Read{Read: read}
	case *forward.ForwardRequest_Write:
		write := &sink.WriteRequest{CompletionMode: body.Write.CompletionMode, LuaPrograms: body.Write.LuaPrograms}
		for _, i := range indices {
			write.Operations = append(write.Operations, body.Write.Operations[i])
		}
		split.Request = &forward.ForwardRequest_Write{Write: write}
	case *forward.ForwardRequest_Delete:
		deletion := &sink.DeleteRequest{CompletionMode: body.Delete.CompletionMode}
		for _, i := range indices {
			deletion.Operations = append(deletion.Operations, body.Delete.Operations[i])
		}
		split.Request = &forward.ForwardRequest_Delete{Delete: deletion}
	}
	return split
}
func emptyResponse(req *forward.ForwardRequest, count int) *forward.ForwardResponse {
	response := &forward.ForwardResponse{}
	switch req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		read := &sink.ReadResponse{Results: make([]*sink.ReadResult, count)}
		response.Response = &forward.ForwardResponse_Read{Read: read}
	case *forward.ForwardRequest_Write:
		write := &sink.WriteResponse{Results: make([]*sink.WriteResult, count)}
		response.Response = &forward.ForwardResponse_Write{Write: write}
	case *forward.ForwardRequest_Delete:
		deletion := &sink.DeleteResponse{Results: make([]*sink.DeleteResult, count)}
		response.Response = &forward.ForwardResponse_Delete{Delete: deletion}
	}
	return response
}
func failure(err error, retrySafe bool) *sink.Failure {
	code := sink.FailureCode_FAILURE_CODE_INTERNAL
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition:
		code = sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT
	case codes.ResourceExhausted:
		code = sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED
	case codes.Unavailable:
		code = sink.FailureCode_FAILURE_CODE_UNAVAILABLE
	case codes.DeadlineExceeded, codes.Canceled:
		code = sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED
	}
	retryable := retrySafe && (code == sink.FailureCode_FAILURE_CODE_UNAVAILABLE || code == sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || code == sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED)
	message := status.Convert(err).Message()
	if len(message) > 512 {
		message = "Engine request failed; diagnostic exceeded limit"
	}
	result := &sink.Failure{Code: code, Message: message, Retryable: retryable}
	return result
}
func failRecords(response *forward.ForwardResponse, indices []int, err error, notStarted bool) {
	for _, index := range indices {
		failed := failure(err, notStarted || response.GetRead() != nil)
		if !notStarted && response.GetRead() == nil {
			if len(failed.Message) > 480 {
				failed.Message = "mutation outcome is unknown"
			} else {
				failed.Message = "mutation outcome is unknown: " + failed.Message
			}
		}
		switch body := response.GetResponse().(type) {
		case *forward.ForwardResponse_Read:
			result := &sink.ReadResult{OperationIndex: uint32(index), Status: sink.ReadStatus_READ_STATUS_FAILED, Failure: failed}
			body.Read.Results[index] = result
		case *forward.ForwardResponse_Write:
			result := &sink.WriteResult{OperationIndex: uint32(index), Status: sink.WriteStatus_WRITE_STATUS_FAILED, Failure: failed}
			body.Write.Results[index] = result
		case *forward.ForwardResponse_Delete:
			result := &sink.DeleteResult{OperationIndex: uint32(index), Status: sink.DeleteStatus_DELETE_STATUS_FAILED, Failure: failed}
			body.Delete.Results[index] = result
		}
	}
}
func mergeRecords(response, sub *forward.ForwardResponse, indices []int) error {
	switch body := response.GetResponse().(type) {
	case *forward.ForwardResponse_Read:
		return mergeResults(body.Read.Results, sub.GetRead().GetResults(), indices)
	case *forward.ForwardResponse_Write:
		return mergeResults(body.Write.Results, sub.GetWrite().GetResults(), indices)
	case *forward.ForwardResponse_Delete:
		return mergeResults(body.Delete.Results, sub.GetDelete().GetResults(), indices)
	}
	return status.Error(codes.Internal, "unexpected response type")
}

type recordResult interface {
	*sink.ReadResult | *sink.WriteResult | *sink.DeleteResult
	GetOperationIndex() uint32
}

func mergeResults[T recordResult](dest, source []T, indices []int) error {
	if len(source) != len(indices) {
		return status.Error(codes.Internal, "Engine returned an invalid result count")
	}
	seen := make([]bool, len(indices))
	for _, result := range source {
		index := int(result.GetOperationIndex())
		if result == nil || index >= len(indices) || seen[index] || !validResult(result) {
			return status.Error(codes.Internal, "Engine returned an invalid operation index")
		}
		seen[index] = true
	}
	for _, result := range source {
		index := indices[result.GetOperationIndex()]
		switch typed := any(result).(type) {
		case *sink.ReadResult:
			typed.OperationIndex = uint32(index)
		case *sink.WriteResult:
			typed.OperationIndex = uint32(index)
		case *sink.DeleteResult:
			typed.OperationIndex = uint32(index)
		}
		dest[index] = result
	}
	return nil
}

func boundFailures(response *forward.ForwardResponse, maximum, count int) {
	limit := min(512, max(1, maximum/max(1, count)-128))
	failures := make([]*sink.Failure, 0, count)
	for _, result := range response.GetRead().GetResults() {
		failures = append(failures, result.GetFailure())
	}
	for _, result := range response.GetWrite().GetResults() {
		failures = append(failures, result.GetFailure())
	}
	for _, result := range response.GetDelete().GetResults() {
		failures = append(failures, result.GetFailure())
	}
	for _, failure := range failures {
		if failure == nil || len(failure.Message) <= limit {
			continue
		}
		end := limit
		for end > 0 && !utf8.RuneStart(failure.Message[end]) {
			end--
		}
		failure.Message = strings.Clone(failure.Message[:end])
		if failure.Message == "" {
			failure.Message = "?"
		}
	}
}

func validResult[T recordResult](result T) bool {
	var failed bool
	var failure *sink.Failure
	switch value := any(result).(type) {
	case *sink.ReadResult:
		if value.Status < sink.ReadStatus_READ_STATUS_FOUND || value.Status > sink.ReadStatus_READ_STATUS_FAILED {
			return false
		}
		if value.Status == sink.ReadStatus_READ_STATUS_FOUND && value.Document == nil {
			return false
		}
		failed = value.Status == sink.ReadStatus_READ_STATUS_FAILED
		failure = value.Failure
	case *sink.WriteResult:
		if value.Status < sink.WriteStatus_WRITE_STATUS_ACCEPTED || value.Status > sink.WriteStatus_WRITE_STATUS_FAILED {
			return false
		}
		failed = value.Status == sink.WriteStatus_WRITE_STATUS_FAILED || value.Status == sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED
		failure = value.Failure
	case *sink.DeleteResult:
		if value.Status < sink.DeleteStatus_DELETE_STATUS_ACCEPTED || value.Status > sink.DeleteStatus_DELETE_STATUS_FAILED {
			return false
		}
		failed = value.Status == sink.DeleteStatus_DELETE_STATUS_FAILED
		failure = value.Failure
	}
	if !failed {
		return failure == nil
	}
	return failure != nil && failure.Code >= sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT && failure.Code <= sink.FailureCode_FAILURE_CODE_INTERNAL
}
