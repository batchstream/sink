package app

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liran/sink/internal/config"
)

func TestGatewayDoesNotConstructExecutionDependencies(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: gateway\ngrpc: {address: '127.0.0.1:0'}\nhealth: {address: '127.0.0.1:0'}\ngateway:\n  routes:\n    - store: a\n      target: 127.0.0.1:1\n      tls: {insecure: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Config: loaded, Version: "test"}
	application, err := New(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	if application.storage != nil || application.mongoClient != nil || application.publisher != nil || application.worker != nil || application.batchingServer != nil || len(application.healthChecks) > 0 {
		t.Fatal("Gateway initialized execution dependencies")
	}
	request := httptest.NewRequest("GET", "/readyz", nil)
	response := httptest.NewRecorder()
	application.serveReadiness(response, request)
	if response.Code != 200 {
		t.Fatalf("offline Engine made Gateway unready: %d", response.Code)
	}
}
func TestEngineProcessReadinessDoesNotHideSurvivingCapabilities(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: engine\ngrpc: {address: '127.0.0.1:0'}\nhealth: {address: '127.0.0.1:0'}\nstorage:\n  name: a\n  driver: opensearch\n  search: {endpoints: ['http://127.0.0.1:1']}\n"))
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Config: loaded, Version: "test"}
	application, err := New(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	for _, test := range []struct {
		url    string
		status int
	}{{"/readyz", 200}, {"/readyz?service=sink.storage.a", 503}} {
		request := httptest.NewRequest("GET", test.url, nil)
		response := httptest.NewRecorder()
		application.serveReadiness(response, request)
		if response.Code != test.status {
			t.Fatalf("%s: status %d", test.url, response.Code)
		}
	}
}
