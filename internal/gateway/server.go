package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
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

// Scalar RPCs carry one typed response plus the final settlement.
func (s *Server) forward(ctx context.Context, route Route, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	var response *forward.ForwardResponse
	call := forwardCall{route: route, request: req}
	call.emit = func(frame *forward.ForwardResponse) error {
		if response != nil {
			return status.Error(codes.Internal, "unexpected extra scalar response")
		}
		response = frame
		return nil
	}
	final, err := s.forwardEach(ctx, call)
	if err != nil {
		return nil, err
	}
	if final.GetCode() != 0 {
		return final, nil
	}
	if response == nil {
		return nil, status.Error(codes.Internal, "Engine omitted scalar response")
	}
	response.Used, response.NotStarted = final.Used, final.NotStarted
	return response, nil
}
func validUsage(grant, used *forward.Budget) bool {
	return used.GetReturns() <= grant.GetReturns()
}
func consume(remaining, used *forward.Budget) {
	remaining.Returns -= used.GetReturns()
}
func routeFor(view *snapshot, store string) (Route, error) {
	route, exists := view.routes[store]
	if !exists {
		return route, status.Error(codes.InvalidArgument, "Store is not configured")
	}
	return route, nil
}

func forwardedError(response *forward.ForwardResponse) error {
	encoded := &statuspb.Status{Code: int32(response.GetCode()), Message: response.GetMessage(), Details: response.GetStatusDetails()}
	return status.FromProto(encoded).Err()
}

func localRejection(route Route, code codes.Code, message string) *forward.ForwardResponse {
	usage := &forward.Budget{}
	response := &forward.ForwardResponse{Version: forwarding.Version, Store: route.Store, Used: usage, Code: uint32(code), Message: message, NotStarted: true}
	return response
}

// responseBudget derives public payload capacity from the transport ceiling.
// Result envelopes, revision tokens and bounded failures are reserved before
// forwarding; Store-local working allocations follow process watermark admission.
func (s *Server) responseBudget(operations int) (*forward.Budget, error) {
	overhead := 0
	if operations > 0 {
		if operations > s.request.MaxReadBytes/1280 {
			return nil, status.Error(codes.ResourceExhausted, "result envelopes exceed gRPC send limit")
		}
		overhead = operations * 1280
	}
	bytes := s.request.MaxReadBytes - overhead
	if bytes <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "response budget is exhausted")
	}
	budget := &forward.Budget{Returns: uint64(bytes)}
	return budget, nil
}
