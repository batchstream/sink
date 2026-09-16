// Package gateway routes public Sink requests without database or Kafka clients.
package gateway

import (
	"github.com/liran/sink-go/uri"

	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const maxRouteFileBytes = 4 << 20

type TLS struct {
	Insecure   bool   `yaml:"insecure"`
	ServerName string `yaml:"server_name"`
}
type Route struct {
	Store    string `yaml:"store"`
	Target   string `yaml:"target"`
	State    string `yaml:"state"`
	TLS      TLS    `yaml:"tls"`
	endpoint string
}
type routeFile struct {
	Routes []Route `yaml:"routes"`
}
type snapshot struct {
	routes map[string]Route
	hash   string
}

func readRoutes(path string) (*snapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open routes: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxRouteFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read routes: %w", err)
	}
	if len(data) > maxRouteFileBytes {
		return nil, errors.New("routes file exceeds 4 MiB")
	}
	var parsed routeFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode routes: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("routes must contain exactly one YAML document")
	}
	if len(parsed.Routes) == 0 || len(parsed.Routes) > 10000 {
		return nil, errors.New("routes must contain between 1 and 10000 entries")
	}
	routes := make(map[string]Route, len(parsed.Routes))
	for _, route := range parsed.Routes {
		if !validIdentity(route.Store) || route.Target == "" || len(route.Target) > 2048 || strings.TrimSpace(route.Target) != route.Target {
			return nil, errors.New("each route requires valid store and target")
		}
		if _, exists := routes[route.Store]; exists {
			return nil, fmt.Errorf("duplicate Store %q", route.Store)
		}
		if _, err := resolveTarget(route.Target); err != nil {
			return nil, fmt.Errorf("store %q: %w", route.Store, err)
		}
		switch route.State {
		case "":
			route.State = "active"
		case "active", "draining", "disabled":
		default:
			return nil, fmt.Errorf("invalid route state for %q", route.Store)
		}
		if route.TLS.Insecure && route.TLS.ServerName != "" {
			return nil, errors.New("insecure routes must not set TLS server_name")
		}
		routes[route.Store] = route
	}
	digest := sha256.Sum256(data)
	loaded := &snapshot{routes: routes, hash: hex.EncodeToString(digest[:])}
	return loaded, nil
}
func validIdentity(value string) bool { return uri.ValidStore(value) }

// ValidateRoutes checks a route file offline for the configuration CLI.
func ValidateRoutes(path string) error { _, err := readRoutes(path); return err }
