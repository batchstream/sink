package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/status"
)

// Discovery owns a DNS view, independently of the lazily opened RPC channels.
// A public request copies one view per Store and retains it through all groups.
type discovery struct {
	resolver.ClientConn
	mu        sync.Mutex
	addresses []string
	err       error
	ready     chan struct{}
	once      sync.Once
	built     chan struct{}
	resolver  resolver.Resolver
	users     int // protected by connections.mu
	idleSince time.Time
	maximum   int
}

func (d *discovery) UpdateState(state resolver.State) error {
	unique := make(map[string]bool)
	for _, endpoint := range state.Endpoints {
		for _, address := range endpoint.Addresses {
			unique[address.Addr] = true
		}
	}
	for _, address := range state.Addresses {
		unique[address.Addr] = true
	}
	if len(unique) > d.maximum {
		err := errors.New("engine DNS result exceeds connection limit")
		d.ReportError(err)
		return err
	}
	addresses := make([]string, 0, len(unique))
	for address := range unique {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" {
			err = errors.New("engine resolver returned an invalid endpoint")
			d.ReportError(err)
			return err
		}
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	d.mu.Lock()
	d.addresses, d.err = addresses, nil
	d.mu.Unlock()
	d.once.Do(func() { close(d.ready) })
	return nil
}

func (d *discovery) ReportError(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
	d.once.Do(func() { close(d.ready) })
}

func (d *discovery) NewAddress(addresses []resolver.Address) {
	state := resolver.State{Addresses: addresses}
	_ = d.UpdateState(state)
}

func (d *discovery) ParseServiceConfig(string) *serviceconfig.ParseResult {
	result := &serviceconfig.ParseResult{Err: errors.New("engine DNS service configurations are disabled")}
	return result
}

func (d *discovery) snapshot(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-d.ready:
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.addresses) == 0 {
		return nil, status.Error(codes.Unavailable, "Store has no resolved Engine endpoints")
	}
	return append([]string(nil), d.addresses...), nil
}

func (d *discovery) close() {
	<-d.built
	if d.resolver != nil {
		d.resolver.Close()
	}
}

func resolveTarget(value string) (resolver.Target, error) {
	var empty resolver.Target
	if !strings.Contains(value, "://") {
		value = "dns:///" + value
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return empty, err
	}
	if parsed.Scheme != "dns" && parsed.Scheme != "passthrough" {
		return empty, errors.New("engine target must use dns or passthrough")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return empty, errors.New("engine target must not contain query, fragment or user information")
	}
	target := resolver.Target{URL: *parsed}
	if target.Endpoint() == "" {
		return empty, errors.New("engine target requires an endpoint")
	}
	if parsed.Scheme == "passthrough" {
		host, port, err := net.SplitHostPort(target.Endpoint())
		_, addressErr := netip.ParseAddr(host)
		if err != nil || addressErr != nil || port == "" || parsed.Host != "" {
			return empty, errors.New("passthrough target requires a literal IP and port; use dns for service discovery")
		}
	}
	return target, nil
}

func (p *connections) discover(route Route) (*discovery, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "Gateway is shutting down")
	}
	if d := p.discoveries[route]; d != nil {
		d.users++
		p.mu.Unlock()
		return d, nil
	}
	var evicted *discovery
	if len(p.discoveries) >= p.maximum {
		var oldest Route
		for key, d := range p.discoveries {
			if d.users == 0 && (evicted == nil || d.idleSince.Before(evicted.idleSince)) {
				oldest, evicted = key, d
			}
		}
		if evicted == nil {
			p.mu.Unlock()
			return nil, status.Error(codes.ResourceExhausted, "Gateway discovery capacity is occupied")
		}
		delete(p.discoveries, oldest)
	}
	if p.discoveries == nil {
		p.discoveries = make(map[Route]*discovery)
	}
	d := &discovery{ready: make(chan struct{}), built: make(chan struct{}), users: 1, maximum: p.maximum}
	p.discoveries[route] = d
	p.mu.Unlock()
	if evicted != nil {
		evicted.close()
	}
	target, err := resolveTarget(route.Target)
	if err == nil {
		builder := resolver.Get(target.URL.Scheme)
		if target.URL.Scheme == "dns" {
			builder = &refreshingDNSBuilder{Builder: builder, interval: p.dnsRefresh}
		}
		options := resolver.BuildOptions{DisableServiceConfig: true}
		d.resolver, err = builder.Build(target, d, options)
	}
	if err != nil {
		d.ReportError(err)
	}
	close(d.built)
	return d, nil
}

func (p *connections) destinations(ctx context.Context, route Route) ([]Route, func(), error) {
	d, err := p.discover(route)
	if err != nil {
		return nil, nil, err
	}
	release := func() { p.mu.Lock(); d.users--; d.idleSince = time.Now(); p.mu.Unlock() }
	addresses, err := d.snapshot(ctx)
	if err != nil {
		release()
		return nil, nil, err
	}
	targets := make([]Route, len(addresses))
	for i, address := range addresses {
		targets[i] = route
		targets[i].endpoint = address
	}
	return targets, release, nil
}

// Rendezvous hashing depends only on canonical identity and endpoint identity,
// never DNS answer order or a process-random hash seed. A removed endpoint only
// moves records that it owned; a new endpoint takes only records it now wins.
func affinityRoute(identity string, routes []Route) Route {
	var owner Route
	var highest uint64
	identityDigest := sha256.Sum256([]byte(identity))
	prefix := append(identityDigest[:], 0)
	for i, route := range routes {
		digest := sha256.Sum256(append(prefix, route.endpoint...))
		score := binary.BigEndian.Uint64(digest[:8])
		if i == 0 || score > highest || score == highest && route.endpoint < owner.endpoint {
			owner, highest = route, score
		}
	}
	return owner
}
