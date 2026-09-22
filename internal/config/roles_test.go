package config

import (
	"strings"
	"testing"
)

func TestRoleBoundariesAndRemovedFields(t *testing.T) {
	for _, test := range []struct{ mode, fields string }{
		{"gateway", "consumer: {}"}, {"gateway", "producer: {}"}, {"gateway", "execution: {}"}, {"gateway", "batching: {}"},
		{"engine", "request: {}"}, {"engine", "consumer: {}"}, {"engine", "forwarding: {}"},
		{"worker", "request: {}"}, {"worker", "batching: {}"}, {"worker", "producer: {}"}, {"worker", "grpc: {}"}, {"worker", "grpc: {address: ':8080'}"},
		{"engine", "storage: {}"}, {"engine", "kafka: {}"},
	} {
		t.Run(test.mode+"/"+test.fields, func(t *testing.T) {
			base := "mode: " + test.mode + "\n"
			store := strings.NewReader(kafkaStore)
			if test.mode == "gateway" {
				if _, err := Decode(strings.NewReader(gatewayConfig+test.fields), nil); err == nil {
					t.Fatal("invalid Gateway section accepted")
				}
				return
			}
			if test.mode == "worker" {
				base += "consumer: {group_id: workers}\n"
			}
			if _, err := Decode(strings.NewReader(base+test.fields), store); err == nil {
				t.Fatal("invalid role section accepted")
			}
		})
	}
}

func TestHealthAndMetricsIndependent(t *testing.T) {
	loaded, err := Decode(strings.NewReader(gatewayConfig+"health: {address: '127.0.0.1:8082'}\nprometheus: {enabled: false, address: '127.0.0.1:9092'}"), nil)
	if err != nil || loaded.Health.Address != "127.0.0.1:8082" || loaded.Prometheus.Enabled || loaded.Prometheus.Address != "127.0.0.1:9092" {
		t.Fatalf("listener defaults: %v", err)
	}
}
