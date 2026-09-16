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
		{"gateway", "mode: gateway\ngateway:\n  routes: [{store: a, target: 127.0.0.1:1}]\n", true},
		{"gateway storage", "mode: gateway\ngateway: {routes: [{store: a, target: 127.0.0.1:1}]}\nstorage: {name: a}\n", false},
		{"engine", "mode: engine\nstorage:\n  name: a\n  driver: mongodb\n  mongodb: {uri: 'mongodb://localhost:27017'}\n", true},
		{"missing identity", "mode: engine\nstorage:\n  driver: mongodb\n  mongodb: {uri: 'mongodb://localhost:27017'}\n", false},
		{"engine plural", "mode: engine\nstorages:\n  - name: a\n    driver: mongodb\n    mongodb: {uri: 'mongodb://localhost:27017'}\n", false},
		{"worker multiple", "mode: worker\nstorages: [{name: a}, {name: b}]\n", false},
		{"routes role", "mode: server\ngateway: {routes: [{store: a, target: 127.0.0.1:1}]}\n", false},
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

func TestRejectRemovedStoreSettings(t *testing.T) {
	for _, extra := range []string{
		"service:\n  execution:\n    max_requests_per_store: 32\n",
		"service:\n  execution:\n    queue:\n      max_requests_per_store: 32\n",
		"service:\n  execution:\n    scan:\n      max_requests_per_store: 32\n",
		"service:\n  publish:\n    max_requests_per_store: 32\n",
		"  database_id: removed-identity\n",
		"  limits:\n    max_execution_bytes: 1MiB\n",
	} {
		_, err := Decode(strings.NewReader(minimalStorage + extra))
		if err == nil || !strings.Contains(err.Error(), "field ") {
			t.Fatalf("removed Store setting accepted: %v", err)
		}
	}
}

func TestRejectRemovedGatewaySettings(t *testing.T) {
	for _, extra := range []string{
		"  routes_file: routes.yaml\n",
		"  reload_interval: 5s\n",
		"      state: active\n",
	} {
		input := "mode: gateway\ngateway:\n  routes:\n    - store: primary\n      target: 127.0.0.1:8080\n" + extra
		_, err := Decode(strings.NewReader(input))
		if err == nil || !strings.Contains(err.Error(), "field ") {
			t.Fatalf("removed Gateway setting accepted: %v", err)
		}
	}
}

func TestConfigurationSizeIsBounded(t *testing.T) {
	_, err := Decode(strings.NewReader(strings.Repeat(" ", MaxFileBytes+1)))
	if err == nil || !strings.Contains(err.Error(), "exceeds 4 MiB") {
		t.Fatalf("oversized configuration accepted: %v", err)
	}
}

func TestHealthAddressIsIndependentOfPrometheus(t *testing.T) {
	input := minimalStorage + "health: {address: ' 127.0.0.1:8082 '}\nprometheus: {enabled: false, address: '127.0.0.1:9092'}\n"
	loaded, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Health.Address != "127.0.0.1:8082" || loaded.Prometheus.Address != "127.0.0.1:9092" || loaded.Prometheus.Enabled {
		t.Fatalf("health and Prometheus settings are coupled: %#v / %#v", loaded.Health, loaded.Prometheus)
	}
}
