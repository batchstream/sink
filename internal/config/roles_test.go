package config

import (
	"strings"
	"testing"
)

func TestIsolatedRoles(t *testing.T) {
	tests := []struct {
		name, yaml string
		valid      bool
	}{
		{"missing mode", "storage: {name: a}\n", false},
		{"removed server", "mode: server\n", false},
		{"removed all", "mode: all\n", false},
		{"gateway", "mode: gateway\ngateway:\n  routes_file: routes.yaml\n", true},
		{"gateway storage", "mode: gateway\ngateway: {routes_file: routes.yaml}\nstorage: {name: a}\n", false},
		{"engine", "mode: engine\nstorage:\n  name: a\n  database_id: db-a\n  driver: mongodb\n  mongodb: {uri: 'mongodb://localhost:27017'}\n", true},
		{"missing identity", "mode: engine\nstorage:\n  name: a\n  driver: mongodb\n  mongodb: {uri: 'mongodb://localhost:27017'}\n", false},
		{"engine plural", "mode: engine\nstorages:\n  - name: a\n    driver: mongodb\n    mongodb: {uri: 'mongodb://localhost:27017'}\n", false},
		{"worker multiple", "mode: worker\nstorages: [{name: a}, {name: b}]\n", false},
		{"routes role", "mode: server\ngateway: {routes_file: routes.yaml}\n", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Decode(strings.NewReader(test.yaml))
			if (err == nil) != test.valid {
				t.Fatalf("Decode error=%v", err)
			}
		})
	}
}

func TestRejectRemovedStoreSublimits(t *testing.T) {
	for _, extra := range []string{
		"service:\n  execution:\n    max_requests_per_store: 32\n",
		"service:\n  execution:\n    queue:\n      max_requests_per_store: 32\n",
		"service:\n  execution:\n    scan:\n      max_requests_per_store: 32\n",
		"service:\n  publish:\n    max_requests_per_store: 32\n",
		"  limits:\n    max_execution_bytes: 1MiB\n",
	} {
		_, err := Decode(strings.NewReader(minimalStorage + extra))
		if err == nil || !strings.Contains(err.Error(), "field ") {
			t.Fatalf("removed Store sublimit accepted: %v", err)
		}
	}
}
