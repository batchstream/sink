package gateway

import (
	"context"
	"sync"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/protocol"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) streamRecords(ctx context.Context, req *forward.ForwardRequest, emit func(*forward.ForwardResponse) error) error {
	addresses, err := s.validateBatch(req)
	if err != nil {
		return err
	}
	ctx, release, err := s.begin(ctx, req)
	if err != nil {
		return err
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

	// Routing failures contain metadata only and can be sent immediately.
	for index := range addresses {
		var frame *forward.ForwardResponse
		switch body := response.GetResponse().(type) {
		case *forward.ForwardResponse_Read:
			if body.Read.Results[index] != nil {
				read := &sink.ReadResponse{Results: []*sink.ReadResult{body.Read.Results[index]}}
				frame = &forward.ForwardResponse{Response: &forward.ForwardResponse_Read{Read: read}}
			}
		case *forward.ForwardResponse_Write:
			if body.Write.Results[index] != nil {
				write := &sink.WriteResponse{Results: []*sink.WriteResult{body.Write.Results[index]}}
				frame = &forward.ForwardResponse{Response: &forward.ForwardResponse_Write{Write: write}}
			}
		}
		if frame != nil {
			if err := emit(frame); err != nil {
				return err
			}
		}
	}
	work, execution := errgroup.WithContext(ctx)
	work.SetLimit(s.config.MaxFanout)
	var sending sync.Mutex
	for _, group := range groups {
		work.Go(func() error {
			sub := splitRequest(req, group.indices)
			seen := make([]bool, len(group.indices))
			delivered := 0
			var sendErr error
			call := forwardCall{route: group.route, request: sub}
			call.emit = func(frame *forward.ForwardResponse) error {
				var index uint32
				switch req.GetRequest().(type) {
				case *forward.ForwardRequest_Read:
					results := frame.GetRead().GetResults()
					if len(results) != 1 || results[0] == nil || !validResult(results[0]) {
						return status.Error(codes.Internal, "invalid read result frame")
					}
					index = results[0].GetOperationIndex()
					if int(index) >= len(seen) || seen[index] {
						return status.Error(codes.Internal, "duplicate or invalid read result index")
					}
					results[0].OperationIndex = uint32(group.indices[index])
				case *forward.ForwardRequest_Write:
					results := frame.GetWrite().GetResults()
					if len(results) != 1 || results[0] == nil || !validResult(results[0]) {
						return status.Error(codes.Internal, "invalid write result frame")
					}
					index = results[0].GetOperationIndex()
					if int(index) >= len(seen) || seen[index] {
						return status.Error(codes.Internal, "duplicate or invalid write result index")
					}
					results[0].OperationIndex = uint32(group.indices[index])
				}
				sending.Lock()
				err := emit(frame)
				sendErr = err
				sending.Unlock()
				if err != nil {
					return err
				}
				seen[index] = true
				delivered++
				return nil
			}
			notStarted, err := s.forwardEach(execution, call)
			if sendErr != nil {
				return sendErr
			}
			if execution.Err() != nil {
				return status.FromContextError(execution.Err()).Err()
			}
			if err == nil && delivered != len(group.indices) {
				err = status.Error(codes.Internal, "Engine omitted record results")
			}
			if err != nil {
				if delivered == len(group.indices) {
					return err
				}
				// An interrupted Engine stream cannot revoke delivered results.
				// Settle only missing slots and allow unrelated Stores to finish.
				for index, known := range seen {
					if known {
						continue
					}
					failed := emptyResponse(sub, 1)
					failRecords(failed, []int{0}, err, notStarted)
					if result := failed.GetRead(); result != nil {
						result.Results[0].OperationIndex = uint32(group.indices[index])
					}
					if result := failed.GetWrite(); result != nil {
						result.Results[0].OperationIndex = uint32(group.indices[index])
					}
					sending.Lock()
					err := emit(failed)
					sending.Unlock()
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return work.Wait()
}
