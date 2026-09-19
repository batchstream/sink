// Package engine implements the identity-checked private forwarding endpoint.
package engine

import (
	"context"
	"errors"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	memory *capacity.Guard
	forward.UnimplementedEngineServer
	service         sink.SinkServer
	store           string
	maximum         int
	maxRequestBytes int
	metrics         *metrics.Metrics
}
type Options struct {
	Memory          *capacity.Guard
	Metrics         *metrics.Metrics
	Service         sink.SinkServer
	Store           string
	MaxReadBytes    int
	MaxRequestBytes int
}

func New(opts Options) (*Server, error) {
	if opts.Service == nil || opts.Store == "" || opts.MaxReadBytes <= 0 || opts.MaxRequestBytes < 0 {
		return nil, errors.New("engine requires service, Store and a positive byte limit")
	}
	if opts.MaxRequestBytes == 0 {
		opts.MaxRequestBytes = 64 << 20
	}
	server := &Server{memory: opts.Memory, maxRequestBytes: opts.MaxRequestBytes, service: opts.Service, store: opts.Store, maximum: opts.MaxReadBytes, metrics: opts.Metrics}
	return server, nil
}
func (s *Server) Forward(ctx context.Context, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	response := &forward.ForwardResponse{Version: forwarding.Version, Store: s.store, NotStarted: true}
	started := time.Now()
	defer func() { s.metrics.ObserveForward(req, response, time.Since(started)) }()
	tracker := forwarding.NewTracker(req.GetGrant(), s.maximum)
	reject := func(err error) (*forward.ForwardResponse, error) {
		response.Code = uint32(status.Code(err))
		response.Message = status.Convert(err).Message()
		response.StatusDetails = status.Convert(err).Proto().GetDetails()
		response.Used = tracker.Usage()
		return response, nil
	}
	if req.GetVersion() != forwarding.Version {
		return reject(status.Error(codes.FailedPrecondition, "unsupported forwarding protocol version"))
	}
	if req.GetStore() != s.store {
		return reject(status.Error(codes.FailedPrecondition, "Engine identity does not match route"))
	}
	if req.GetGrant() == nil {
		return reject(status.Error(codes.InvalidArgument, "forwarding budget is required"))
	}
	var request any
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		request = body.Read
	case *forward.ForwardRequest_Write:
		request = body.Write
	case *forward.ForwardRequest_Delete:
		request = body.Delete
	case *forward.ForwardRequest_Execute:
		request = body.Execute
	case *forward.ForwardRequest_Query:
		request = body.Query
	case *forward.ForwardRequest_Count:
		request = body.Count
	case *forward.ForwardRequest_Scan:
		request = body.Scan
	default:
		return reject(status.Error(codes.InvalidArgument, "forwarding request is required"))
	}
	if sized, ok := request.(interface{ SizeVT() int }); ok && sized.SizeVT() > s.maxRequestBytes {
		return reject(status.Error(codes.ResourceExhausted, "forwarded request exceeds public message limit"))
	}
	if err := protocol.CheckStore(request, s.store); err != nil {
		return reject(err)
	}
	admitted, admissionErr := s.memory.Admit(ctx)
	if admissionErr != nil {
		return reject(protocol.MemoryAdmissionError(request, admissionErr))
	}
	s.metrics.AdjustInFlight(1)
	defer s.metrics.AdjustInFlight(-1)
	ctx = admitted
	response.NotStarted = false
	ctx = forwarding.WithTracker(ctx, tracker)
	var err error
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		result, callErr := s.service.Read(ctx, body.Read)
		err = callErr
		response.Response = &forward.ForwardResponse_Read{Read: result}
	case *forward.ForwardRequest_Write:
		result, callErr := s.service.Write(ctx, body.Write)
		err = callErr
		response.Response = &forward.ForwardResponse_Write{Write: result}
	case *forward.ForwardRequest_Delete:
		result, callErr := s.service.Delete(ctx, body.Delete)
		err = callErr
		response.Response = &forward.ForwardResponse_Delete{Delete: result}
	case *forward.ForwardRequest_Execute:
		result, callErr := s.service.Execute(ctx, body.Execute)
		err = callErr
		response.Response = &forward.ForwardResponse_Execute{Execute: result}
	case *forward.ForwardRequest_Query:
		result, callErr := s.service.Query(ctx, body.Query)
		err = callErr
		response.Response = &forward.ForwardResponse_Query{Query: result}
	case *forward.ForwardRequest_Count:
		result, callErr := s.service.Count(ctx, body.Count)
		err = callErr
		response.Response = &forward.ForwardResponse_Count{Count: result}
	case *forward.ForwardRequest_Scan:
		result, callErr := s.service.Scan(ctx, body.Scan)
		err = callErr
		response.Response = &forward.ForwardResponse_Scan{Scan: result}
	}
	return reject(err)
}
