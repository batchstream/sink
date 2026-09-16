package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestHealthEndpointsDoNotRequirePrometheus(t *testing.T) {
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
	tests := []struct {
		name          string
		yaml          string
		healthAddress string
		enabled       bool
		failure       string
	}{
		{name: "omitted"},
		{name: "address only", yaml: fmt.Sprintf("prometheus: {address: %q}\n", occupied.Addr().String())},
		{name: "disabled", yaml: fmt.Sprintf("prometheus: {enabled: false, address: %q}\n", occupied.Addr().String())},
		{name: "enabled", yaml: "prometheus: {enabled: true, address: '127.0.0.1:0'}\n", enabled: true},
		{name: "occupied health without metrics", healthAddress: occupied.Addr().String(), failure: "listen for health endpoints"},
		{name: "occupied health with metrics", yaml: "prometheus: {enabled: true, address: '127.0.0.1:0'}\n", healthAddress: occupied.Addr().String(), failure: "listen for health endpoints"},
		{name: "occupied metrics", yaml: fmt.Sprintf("prometheus: {enabled: true, address: %q}\n", occupied.Addr().String()), failure: "listen for Prometheus metrics"},
	}
	for _, mode := range []string{"gateway", "engine", "worker"} {
		base := fmt.Sprintf("mode: %s\ngrpc: {address: '127.0.0.1:0'}\nshutdown_timeout: 1s\n", mode)
		if mode == "gateway" {
			base += "gateway:\n  routes:\n    - store: primary\n      target: 127.0.0.1:1\n      tls: {insecure: true}\n"
		} else {
			base += "storage:\n  name: primary\n  driver: opensearch\n  search: {endpoints: ['http://127.0.0.1:1']}\n"
			if mode == "worker" {
				base += fmt.Sprintf("  kafka:\n    enabled: true\n    brokers: [%q]\n    topic: {name: mutations, replication_factor: 1}\n    consumer: {group_id: workers}\n", broker.ListenAddrs()[0])
			}
		}
		for _, test := range tests {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				address := test.healthAddress
				if address == "" {
					address = "127.0.0.1:0"
				}
				settings := base + test.yaml + fmt.Sprintf("health: {address: %q}\n", address)
				loaded, err := config.Decode(strings.NewReader(settings))
				if err != nil {
					t.Fatal(err)
				}
				opts := Options{Config: loaded, Version: "prometheus-switch-test"}
				application, err := New(t.Context(), opts)
				if test.failure != "" {
					if err == nil {
						application.Close()
						t.Fatal("occupied address was accepted")
					}
					if !strings.Contains(err.Error(), test.failure) {
						t.Fatal(err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(application.Close)
				if application.healthListener == nil || application.healthServer == nil {
					t.Fatal("health listener must start independently of Prometheus")
				}
				if (application.metricsListener != nil) != test.enabled || (application.metricsServer != nil) != test.enabled {
					t.Fatal("metrics listener did not honor prometheus.enabled")
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
				readyStatus := http.StatusOK
				if mode == "worker" {
					readyStatus = http.StatusServiceUnavailable
				}
				healthAddress := application.healthListener.Addr().String()
				type endpointCheck struct {
					address string
					path    string
					status  int
				}
				endpoints := []endpointCheck{
					{address: healthAddress, path: "/livez", status: http.StatusOK},
					{address: healthAddress, path: "/readyz", status: readyStatus},
					{address: healthAddress, path: "/metrics", status: http.StatusNotFound},
				}
				if test.enabled {
					metricsAddress := application.metricsListener.Addr().String()
					if healthAddress == metricsAddress {
						t.Fatal("health and metrics must have separate listeners")
					}
					metrics := endpointCheck{metricsAddress, "/metrics", http.StatusOK}
					live := endpointCheck{metricsAddress, "/livez", http.StatusNotFound}
					ready := endpointCheck{metricsAddress, "/readyz", http.StatusNotFound}
					endpoints = append(endpoints, metrics, live, ready)
				}
				client := &http.Client{Timeout: 5 * time.Second}
				for _, endpoint := range endpoints {
					response, err := client.Get("http://" + endpoint.address + endpoint.path)
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					if readErr != nil {
						t.Fatal(readErr)
					}
					if response.StatusCode != endpoint.status {
						t.Fatalf("%s%s: status %d, want %d", endpoint.address, endpoint.path, response.StatusCode, endpoint.status)
					}
					if endpoint.path == "/metrics" && endpoint.status == http.StatusOK && !strings.Contains(string(body), "sink_") {
						t.Fatal("enabled metrics endpoint did not export Sink metrics")
					}
				}
			})
		}
	}
}
