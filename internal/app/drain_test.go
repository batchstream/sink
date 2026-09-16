package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestHTTPReadinessRejectsClosedRoles(t *testing.T) {
	for _, mode := range []config.Mode{config.ModeGateway, config.ModeEngine, config.ModeWorker} {
		t.Run(string(mode), func(t *testing.T) {
			settings := config.Config{Mode: mode}
			app := &Application{config: settings}
			app.Close()
			for _, endpoint := range []string{"/readyz", "/readyz?service=sink.storage.primary"} {
				request := httptest.NewRequest(http.MethodGet, endpoint, nil)
				response := httptest.NewRecorder()
				app.serveReadiness(response, request)
				if response.Code != http.StatusServiceUnavailable {
					t.Errorf("%s after Close: got %d, want 503", endpoint, response.Code)
				}
			}
		})
	}
}

type drainingRPC struct {
	sink.UnimplementedSinkServer
	entered chan struct{}
	release chan struct{}
}

func (s *drainingRPC) Read(ctx context.Context, _ *sink.ReadRequest) (*sink.ReadResponse, error) {
	close(s.entered)
	select {
	case <-s.release:
		response := &sink.ReadResponse{}
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestShutdownWithdrawsReadinessAndDrainsAcceptedRPC(t *testing.T) {
	for _, finish := range []bool{true, false} {
		name := "graceful"
		if !finish {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			input := `mode: gateway
grpc: {address: "127.0.0.1:0"}
health: {address: "127.0.0.1:0"}
gateway:
  routes:
    - store: primary
      target: 127.0.0.1:1
      tls: {insecure: true}
shutdown_timeout: 300ms
`
			loaded, err := config.Decode(strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			app := &Application{config: loaded}
			if err := app.configureHealth(); err != nil {
				t.Fatal(err)
			}
			defer app.Close()
			backend := &drainingRPC{entered: make(chan struct{}), release: make(chan struct{})}
			if err := app.configureGRPC(backend, nil); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runDone := make(chan error, 1)
			go func() { runDone <- app.Run(ctx) }()
			conn, err := grpc.NewClient(app.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := sink.NewSinkClient(conn)
			call, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer callCancel()
			result := make(chan error, 1)
			go func() {
				request := &sink.ReadRequest{}
				_, err := client.Read(call, request)
				result <- err
			}()
			select {
			case <-backend.entered:
			case <-call.Done():
				t.Fatal("RPC did not enter the server")
			}
			cancel()
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}
			// Run returns before Close drains gRPC; readiness must already be gone.
			httpClient := &http.Client{Timeout: time.Second}
			for endpoint, expected := range map[string]int{"/readyz": 503, "/livez": 200} {
				response, err := httpClient.Get("http://" + app.healthListener.Addr().String() + endpoint)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != expected {
					t.Errorf("%s during drain: got %d, want %d", endpoint, response.StatusCode, expected)
				}
			}
			closed := make(chan struct{})
			go func() { app.Close(); close(closed) }()
			if finish {
				close(backend.release)
			}
			select {
			case err := <-result:
				if (err == nil) != finish {
					t.Errorf("RPC result = %v, graceful = %v", err, finish)
				}
			case <-call.Done():
				t.Fatal("RPC exceeded shutdown deadline")
			}
			select {
			case <-closed:
			case <-call.Done():
				t.Fatal("Close exceeded shutdown deadline")
			}
		})
	}
}
