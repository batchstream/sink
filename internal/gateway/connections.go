package gateway

import (
	"crypto/tls"
	"sync"
	"time"

	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

type connection struct {
	conn      *grpc.ClientConn
	client    forward.EngineClient
	users     int
	idleSince time.Time
}
type connections struct {
	mu           sync.Mutex
	entries      map[Route]*connection
	maximum      int
	messageBytes int
	idleTimeout  time.Duration
	dnsRefresh   time.Duration
	closed       bool
}

func (p *connections) acquire(route Route) (*connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, status.Error(codes.Unavailable, "Gateway is shutting down")
	}
	if existing := p.entries[route]; existing != nil {
		existing.users++
		return existing, nil
	}
	if len(p.entries) >= p.maximum {
		var oldestRoute Route
		var oldest *connection
		for key, entry := range p.entries {
			if entry.users == 0 && (oldest == nil || entry.idleSince.Before(oldest.idleSince)) {
				oldestRoute = key
				oldest = entry
			}
		}
		if oldest == nil {
			return nil, status.Error(codes.ResourceExhausted, "Gateway connection capacity is occupied")
		}
		_ = oldest.conn.Close()
		delete(p.entries, oldestRoute)
	}
	tlsOptions := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: route.TLS.ServerName}
	transport := credentials.NewTLS(tlsOptions)
	if route.TLS.Insecure {
		transport = insecure.NewCredentials()
	}
	codec := protocol.NewVTProtoCodec()
	dns := &refreshingDNSBuilder{Builder: resolver.Get("dns"), interval: p.dnsRefresh}
	conn, err := grpc.NewClient(route.Target, grpc.WithResolvers(dns), grpc.WithTransportCredentials(transport), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`), grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec), grpc.MaxCallRecvMsgSize(p.messageBytes), grpc.MaxCallSendMsgSize(p.messageBytes)))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "cannot create Engine connection")
	}
	entry := &connection{conn: conn, client: forward.NewEngineClient(conn), users: 1}
	p.entries[route] = entry
	return entry, nil
}
func (p *connections) release(entry *connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry.users--
	entry.idleSince = time.Now()
}
func (p *connections) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for route, entry := range p.entries {
		if entry.users == 0 && time.Since(entry.idleSince) >= p.idleTimeout {
			_ = entry.conn.Close()
			delete(p.entries, route)
		}
	}
}
func (p *connections) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for route, entry := range p.entries {
		_ = entry.conn.Close()
		delete(p.entries, route)
	}
}
