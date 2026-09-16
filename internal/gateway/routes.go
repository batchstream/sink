// Package gateway routes public Sink requests without database or Kafka clients.
package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/liran/sink-go/uri"
	"github.com/liran/sink/internal/config"
)

type TLS struct {
	Insecure   bool   `yaml:"insecure"`
	ServerName string `yaml:"server_name"`
}
type Route struct {
	Store    string `yaml:"store"`
	Target   string `yaml:"target"`
	TLS      TLS    `yaml:"tls"`
	endpoint string
}
type snapshot struct {
	routes map[string]Route
	hash   string
}

func newSnapshot(configured []config.Route) (*snapshot, error) {
	if len(configured) == 0 || len(configured) > 10000 {
		return nil, errors.New("gateway.routes must contain between 1 and 10000 entries")
	}
	routes := make(map[string]Route, len(configured))
	for _, entry := range configured {
		tls := TLS{Insecure: entry.TLS.Insecure, ServerName: entry.TLS.ServerName}
		route := Route{Store: entry.Store, Target: entry.Target, TLS: tls}
		if !validIdentity(route.Store) || route.Target == "" || len(route.Target) > 2048 || strings.TrimSpace(route.Target) != route.Target {
			return nil, errors.New("each route requires valid store and target")
		}
		if _, exists := routes[route.Store]; exists {
			return nil, fmt.Errorf("duplicate Store %q", route.Store)
		}
		if _, err := resolveTarget(route.Target); err != nil {
			return nil, fmt.Errorf("store %q: %w", route.Store, err)
		}
		if route.TLS.Insecure && route.TLS.ServerName != "" {
			return nil, errors.New("insecure routes must not set TLS server_name")
		}
		routes[route.Store] = route
	}
	data, err := json.Marshal(routes)
	if err != nil {
		return nil, fmt.Errorf("encode route snapshot: %w", err)
	}
	digest := sha256.Sum256(data)
	loaded := &snapshot{routes: routes, hash: hex.EncodeToString(digest[:])}
	return loaded, nil
}
func validIdentity(value string) bool { return uri.ValidStore(value) }

// ValidateRoutes checks configured routes offline for the configuration CLI.
func ValidateRoutes(routes []config.Route) error { _, err := newSnapshot(routes); return err }
