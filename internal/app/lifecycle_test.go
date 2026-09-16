package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestApplicationModesKeepTheirOwnResources(t *testing.T) {
	for _, mode := range []string{"engine", "worker"} {
		t.Run(mode, func(t *testing.T) {
			broker, err := kfake.NewCluster(kfake.NumBrokers(1))
			if err != nil {
				t.Fatal(err)
			}
			defer broker.Close()
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
			defer backend.Close()
			input := fmt.Sprintf(`mode: %s
grpc:
  address: "127.0.0.1:0"
prometheus:
  enabled: true
  address: "127.0.0.1:0"
storage:
  name: primary
  driver: opensearch
  search:
    endpoints: [%q]
  kafka:
    enabled: true
    brokers: [%q]
    topic:
      name: mutations
      replication_factor: 1
    consumer:
      group_id: workers
shutdown_timeout: 1s
`, mode, backend.URL, broker.ListenAddrs()[0])
			loaded, err := config.Decode(strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			opts := Options{Config: loaded, Version: "lifecycle-test"}
			app, err := New(t.Context(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer app.Close()
			if (app.grpcServer != nil) != (mode != "worker") || (app.publisher != nil) != (mode != "worker") || (app.worker != nil) != (mode == "worker") {
				t.Fatal("mode acquired another role's resources")
			}
			if app.health != nil {
				found := false
				for _, check := range app.healthChecks {
					found = found || check.service == "sink.kafka.primary"
				}
				if !found {
					t.Fatal("Kafka health service name changed")
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- app.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("application did not stop background work")
				}
			}()
			client := &http.Client{Timeout: 3 * time.Second}
			response, err := client.Get("http://" + app.metricsListener.Addr().String() + "/metrics")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), `version="lifecycle-test"`) {
				t.Fatalf("metrics unavailable in %s mode: %v", mode, err)
			}
		})
	}
}

func TestAssemblyFailureReleasesPreviouslyOpenedListener(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	metrics, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := metrics.Addr().String()
	_ = metrics.Close()
	input := fmt.Sprintf(`mode: engine
grpc:
  address: %q
prometheus:
  enabled: true
  address: %q
storage:
  name: primary
  driver: opensearch
  search:
    endpoints: [http://127.0.0.1:1]
`, occupied.Addr().String(), address)
	loaded, err := config.Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Config: loaded, Version: "test"}
	app, err := New(t.Context(), opts)
	if err == nil {
		app.Close()
		t.Fatal("occupied gRPC listener was accepted")
	}
	if !strings.Contains(err.Error(), "listen for gRPC") {
		t.Fatalf("assembly failed before exercising cleanup: %v", err)
	}
	reopened, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed assembly leaked the metrics listener: %v", err)
	}
	_ = reopened.Close()
}
