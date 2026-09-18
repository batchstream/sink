package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/forwarding"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	sink.UnimplementedSinkServer
	config         config.Gateway
	memory         *capacity.Pool
	request        config.Request
	current        *snapshot
	pool           connections
	mu             sync.Mutex
	inFlight       int
	bytes          int
	metrics        *metrics
	activeStores   map[string]int
	nativeSequence atomic.Uint64
}
type Options struct {
	Memory          *capacity.Pool
	Gateway         config.Gateway
	Request         config.Request
	MaxMessageBytes int
}

func New(opts Options) (*Server, error) {
	if opts.Gateway.MaxRequests <= 0 || opts.Gateway.MaxRequestsPerStore <= 0 || opts.Gateway.MaxBytes <= 0 || opts.Gateway.MaxFanout <= 0 || opts.Gateway.MaxConnections <= 0 || opts.Gateway.IdleTimeout <= 0 || opts.Gateway.DNSRefreshInterval <= 0 || opts.Request.Timeout <= 0 || opts.Request.MaxOperations <= 0 || opts.Request.MaxReadBytes <= 0 || opts.MaxMessageBytes <= 0 {
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
	server.activeStores = make(map[string]int)
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
	if s.memory != nil {
		return s.beginMemory(ctx, req)
	}
	// Reserve input, forwarding envelopes and bounded response documents together.
	charge := s.reservation(req)
	if err := ctx.Err(); err != nil {
		return ctx, nil, status.FromContextError(err).Err()
	}
	s.mu.Lock()
	if s.inFlight >= s.config.MaxRequests || charge > s.config.MaxBytes-s.bytes {
		s.mu.Unlock()
		s.metrics.rejected.Inc()
		return ctx, nil, status.Error(codes.ResourceExhausted, "Gateway forwarding capacity is occupied")
	}
	s.inFlight++
	s.bytes += charge
	s.metrics.inFlight.Inc()
	s.metrics.bytes.Add(float64(charge))
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.request.Timeout)
	release := func() {
		cancel()
		s.mu.Lock()
		s.inFlight--
		s.bytes -= charge
		s.metrics.inFlight.Dec()
		s.metrics.bytes.Sub(float64(charge))
		s.mu.Unlock()
	}
	return ctx, release, nil
}
func (s *Server) forward(ctx context.Context, route Route, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	if route.endpoint == "" {
		targets, release, err := s.pool.destinations(ctx, route)
		if err != nil {
			return localRejection(route, status.Code(err), status.Convert(err).Message()), nil
		}
		defer release()
		route = targets[(s.nativeSequence.Add(1)-1)%uint64(len(targets))]
	}
	s.mu.Lock()
	if s.memory == nil && s.activeStores[route.Store] >= s.config.MaxRequestsPerStore {
		s.mu.Unlock()
		s.metrics.rejected.Inc()
		return localRejection(route, codes.ResourceExhausted, "Store forwarding capacity is occupied"), nil
	}
	s.activeStores[route.Store]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeStores[route.Store]--
		if s.activeStores[route.Store] == 0 {
			delete(s.activeStores, route.Store)
		}
		s.mu.Unlock()
	}()
	req.Version = forwarding.Version
	req.Store = route.Store
	entry, err := s.pool.acquire(route)
	if err != nil {
		return localRejection(route, status.Code(err), status.Convert(err).Message()), nil
	}
	defer s.pool.release(entry)
	started := time.Now()
	var response *forward.ForwardResponse
	if s.memory != nil {
		response, err = s.forwardStream(ctx, entry, req)
	} else {
		response, err = entry.client.Forward(ctx, req)
	}
	s.metrics.downstream.Observe(time.Since(started).Seconds())
	if err != nil {
		return nil, err
	}
	if response.GetVersion() != forwarding.Version || response.GetStore() != route.Store || response.GetUsed() == nil || response.GetCode() > uint32(codes.Unauthenticated) {
		return nil, status.Error(codes.Internal, "invalid Engine response identity or version")
	}
	if !validUsage(req.GetGrant(), response.GetUsed()) {
		return nil, status.Error(codes.Internal, "invalid Engine budget settlement")
	}
	return response, nil
}
func validUsage(grant, used *forward.Budget) bool {
	return used.GetSnapshots() <= grant.GetSnapshots() && used.GetInputs() <= grant.GetInputs() && used.GetOutputs() <= grant.GetOutputs() && used.GetReturns() <= grant.GetReturns()
}
func consume(remaining, used *forward.Budget) {
	remaining.Snapshots -= used.GetSnapshots()
	remaining.Inputs -= used.GetInputs()
	remaining.Outputs -= used.GetOutputs()
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

func (s *Server) reservation(req *forward.ForwardRequest) int {
	count := len(req.GetRead().GetOperations()) + len(req.GetWrite().GetOperations()) + len(req.GetDelete().GetOperations())
	charge := 2*req.SizeVT() + count*(512+128) + 4096
	documents := req.GetRead() != nil || req.GetExecute() != nil || req.GetQuery() != nil || req.GetScan() != nil
	for _, op := range req.GetWrite().GetOperations() {
		documents = documents || op.GetReturnDocument()
	}
	if documents {
		charge += 2 * s.request.MaxReadBytes
	}
	if req.GetCount() != nil {
		charge += min(s.request.MaxReadBytes, 256<<10)
	}
	return charge
}
func localRejection(route Route, code codes.Code, message string) *forward.ForwardResponse {
	usage := &forward.Budget{}
	response := &forward.ForwardResponse{Version: forwarding.Version, Store: route.Store, Used: usage, Code: uint32(code), Message: message, NotStarted: true}
	return response
}

func (s *Server) beginMemory(ctx context.Context, req *forward.ForwardRequest) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, s.request.Timeout)
	scope := capacity.FromContext(ctx)
	release := func() {}
	if scope == nil {
		scope = s.memory.NewScope()
		ctx = capacity.WithScope(ctx, scope)
		release = scope.Release
		if err := scope.Admit(ctx, 2*req.SizeVT()+1024); err != nil {
			cancel()
			release()
			return ctx, nil, err
		}
	}
	s.metrics.inFlight.Inc()
	done := func() { cancel(); release(); s.metrics.inFlight.Dec() }
	return ctx, done, nil
}
