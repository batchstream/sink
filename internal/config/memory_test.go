package config

import (
	"strings"
	"testing"
	"time"
)

func TestMemoryConfiguration(t *testing.T) {
	base := "mode: gateway\nforwarding:\n  routes:\n    - store: primary\n      target: 127.0.0.1:8080\n      tls: {insecure: true}\n"
	defaults, err := Decode(strings.NewReader(base), nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Memory.MaxBytes != 0 || defaults.Memory.BurstPercent != 10 || defaults.Memory.WaitTimeout != 2*time.Second {
		t.Fatalf("defaults: %+v", defaults.Memory)
	}
	explicit := base + "memory: {max_bytes: 128MiB, burst_percent: 15, wait_timeout: 500ms}\n"
	loaded, err := Decode(strings.NewReader(explicit), nil)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Memory.MaxBytes != 128<<20 || loaded.Memory.BurstPercent != 15 || loaded.Memory.WaitTimeout != 500*time.Millisecond {
		t.Fatalf("explicit: %+v", loaded.Memory)
	}
	for _, value := range []string{"burst_percent: 0", "burst_percent: 100", "burst_percent: -1", "max_bytes: 0", "max_bytes: 512", "wait_timeout: 0s", "unknown: 1"} {
		if _, err := Decode(strings.NewReader(base+"memory: {"+value+"}\n"), nil); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}
