package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestPrometheusListenerRequiresOptInForEveryRole(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	broker, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	routes := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(routes, []byte("routes:\n  - store: primary\n    target: 127.0.0.1:1\n    tls: {insecure: true}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		yaml    string
		enabled bool
		fails   bool
	}{
		{name: "omitted"},
		{name: "address only", yaml: fmt.Sprintf("prometheus: {address: %q}\n", occupied.Addr().String())},
		{name: "disabled", yaml: fmt.Sprintf("prometheus: {enabled: false, address: %q}\n", occupied.Addr().String())},
		{name: "enabled", yaml: "prometheus: {enabled: true, address: '127.0.0.1:0'}\n", enabled: true},
		{name: "enabled occupied address", yaml: fmt.Sprintf("prometheus: {enabled: true, address: %q}\n", occupied.Addr().String()), fails: true},
	}
	for _, mode := range []string{"gateway", "engine", "worker"} {
		base := fmt.Sprintf("mode: %s\ngrpc: {address: '127.0.0.1:0'}\nshutdown_timeout: 1s\n", mode)
		if mode == "gateway" {
			base += fmt.Sprintf("gateway: {routes_file: %q}\n", routes)
		} else {
			base += "storage:\n  name: primary\n  driver: opensearch\n  search: {endpoints: ['http://127.0.0.1:1']}\n"
			if mode == "worker" {
				base += fmt.Sprintf("  kafka:\n    enabled: true\n    brokers: [%q]\n    topic: {name: mutations, replication_factor: 1}\n    consumer: {group_id: workers}\n", broker.ListenAddrs()[0])
			}
		}
		for _, test := range tests {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				loaded, err := config.Decode(strings.NewReader(base + test.yaml))
				if err != nil {
					t.Fatal(err)
				}
				opts := Options{Config: loaded, Version: "prometheus-switch-test"}
				application, err := New(t.Context(), opts)
				if test.fails {
					if err == nil {
						application.Close()
						t.Fatal("enabled listener ignored an occupied address")
					}
					if !strings.Contains(err.Error(), "listen for Prometheus metrics") {
						t.Fatal(err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(application.Close)
				if (application.metricsListener != nil) != test.enabled || (application.metricsServer != nil) != test.enabled {
					t.Fatal("HTTP listener did not honor prometheus.enabled")
				}
				if !test.enabled {
					return
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- application.Run(ctx) }()
				t.Cleanup(func() {
					cancel()
					select {
					case err := <-done:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(5 * time.Second):
						t.Error("application did not stop")
					}
				})
				client := &http.Client{Timeout: 3 * time.Second}
				for _, endpoint := range []string{"/metrics", "/livez", "/readyz"} {
					response, err := client.Get("http://" + application.metricsListener.Addr().String() + endpoint)
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					if readErr != nil {
						t.Fatal(readErr)
					}
					if endpoint == "/readyz" {
						if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
							t.Fatalf("unexpected readiness status: %d", response.StatusCode)
						}
					} else if response.StatusCode != http.StatusOK {
						t.Fatalf("%s: status %d", endpoint, response.StatusCode)
					}
					if endpoint == "/metrics" && !strings.Contains(string(body), "sink_") {
						t.Fatal("enabled metrics endpoint did not export Sink metrics")
					}
				}
			})
		}
	}
}
