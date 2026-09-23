package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/forwarding"
	"github.com/batchstream/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	sink.UnimplementedSinkServer
	config         config.Gateway
	memory         *capacity.Guard
	request        config.Request
	current        *snapshot
	pool           connections
	metrics        *metrics
	nativeSequence atomic.Uint64
}
type Options struct {
	Memory           *capacity.Guard
	Gateway          config.Gateway
	Request          config.Request
	MaxMessageBytes  int
	MaxResponseBytes int
}

func New(opts Options) (*Server, error) {
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = opts.MaxMessageBytes
	}
	opts.Request.MaxReadBytes = opts.MaxResponseBytes

	if opts.Gateway.MaxFanout <= 0 || opts.Gateway.MaxConnections <= 0 || opts.Gateway.IdleTimeout <= 0 || opts.Gateway.DNSRefreshInterval <= 0 || opts.Request.MaxOperations <= 0 || opts.MaxMessageBytes <= 0 || opts.MaxResponseBytes <= 0 {
		return nil, errors.New("gateway limits must be positive")
	}
	initial, err := newSnapshot(opts.Gateway.Routes)
	if err != nil {
		return nil, err
	}
	server := &Server{config: opts.Gateway, request: opts.Request, metrics: newMetrics(), memory: opts.Memory}
	if opts.Memory != nil {
		server.metrics.registry.MustRegister(opts.Memory)
	}
	server.pool.entries = make(map[Route]*connection)
	server.pool.maximum = opts.Gateway.MaxConnections
	server.pool.messageBytes = opts.MaxMessageBytes + forwarding.EnvelopeBytes
	server.pool.idleTimeout = opts.Gateway.IdleTimeout
	server.pool.dnsRefresh = opts.Gateway.DNSRefreshInterval
	server.current = initial
	server.metrics.routes.Set(float64(len(initial.routes)))
	server.metrics.config.WithLabelValues(initial.hash).Set(1)
	return server, nil
}

// Run only reclaims idle downstream connections; configuration is fixed at startup.
func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(min(5*time.Second, s.config.IdleTimeout))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pool.expire()
		}
	}
}
func (s *Server) Close() { s.pool.close() }
func (s *Server) begin(ctx context.Context, req *forward.ForwardRequest) (context.Context, func(), error) {
	admitted, err := s.memory.Admit(ctx)
	if err != nil {
		return ctx, nil, protocol.MemoryAdmissionError(req, err)
	}
	execution, cancel := context.WithCancel(admitted)
	s.metrics.inFlight.Inc()
	release := func() { cancel(); s.metrics.inFlight.Dec() }
	return execution, release, nil
}

// Scalar RPCs carry one typed response and finish with the gRPC status.
func (s *Server) forward(ctx context.Context, route Route, req *forward.ForwardRequest) (*forward.ForwardResponse, bool, error) {
	var response *forward.ForwardResponse
	call := forwardCall{route: route, request: req}
	call.emit = func(frame *forward.ForwardResponse) error {
		if response != nil {
			return status.Error(codes.Internal, "unexpected extra scalar response")
		}
		response = frame
		return nil
	}
	notStarted, err := s.forwardEach(ctx, call)
	if err != nil {
		return nil, notStarted, err
	}
	if response == nil {
		return nil, false, status.Error(codes.Internal, "Engine omitted scalar response")
	}
	return response, false, nil
}
func routeFor(view *snapshot, store string) (Route, error) {
	route, exists := view.routes[store]
	if !exists {
		return route, status.Error(codes.InvalidArgument, "Store is not configured")
	}
	return route, nil
}
