// Package engine implements the identity-checked private forwarding endpoint.
package engine

import (
	"errors"
	"time"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/forwarding"
	"github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
func (s *Server) Forward(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ForwardResponse]) (err error) {
	ctx := stream.Context()
	started := time.Now()
	defer func() { s.metrics.ObserveForward(req, status.Code(err), time.Since(started)) }()
	reject := func(err error) error {
		stream.SetTrailer(metadata.Pairs(forwarding.NotStartedTrailer, "true"))
		return err
	}
	if req.GetVersion() != forwarding.Version {
		return reject(status.Error(codes.FailedPrecondition, "unsupported forwarding protocol version"))
	}
	if req.GetStore() != s.store {
		return reject(status.Error(codes.FailedPrecondition, "Engine identity does not match route"))
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
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		output := &resultStream[sink.ReadResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		return s.service.Read(body.Read, output)
	case *forward.ForwardRequest_Write:
		output := &resultStream[sink.WriteResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		return s.service.Write(body.Write, output)
	case *forward.ForwardRequest_Delete:
		output := &resultStream[sink.DeleteResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		result, err := s.service.Delete(ctx, body.Delete)
		if err != nil {
			return err
		}
		return output.Send(result)
	case *forward.ForwardRequest_Execute:
		output := &resultStream[sink.ExecuteResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		result, err := s.service.Execute(ctx, body.Execute)
		if err != nil {
			return err
		}
		return output.Send(result)
	case *forward.ForwardRequest_Query:
		output := &resultStream[sink.QueryResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		return s.service.Query(body.Query, output)
	case *forward.ForwardRequest_Count:
		output := &resultStream[sink.CountResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		result, err := s.service.Count(ctx, body.Count)
		if err != nil {
			return err
		}
		return output.Send(result)
	case *forward.ForwardRequest_Scan:
		output := &resultStream[sink.ScanResponse]{ServerStream: stream, metrics: s.metrics, request: req, ctx: ctx, store: s.store, maximum: s.maximum, send: stream.Send}
		return s.service.Scan(body.Scan, output)
	}
	return status.Error(codes.Internal, "unsupported forwarding request")
}
