package config

import (
	"strings"
	"testing"
)

func TestMemoryConfiguration(t *testing.T) {
	base := "mode: gateway\nforwarding:\n  routes:\n    - store: primary\n      target: 127.0.0.1:8080\n      tls: {insecure: true}\n"
	defaults, err := Decode(strings.NewReader(base), nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Memory.MaxBytes != 0 || defaults.Memory.HighWatermarkPercent != 80 || defaults.Memory.LowWatermarkPercent != 70 {
		t.Fatalf("defaults: %+v", defaults.Memory)
	}
	explicit := base + "memory: {max_bytes: 128MiB, high_watermark_percent: 90, low_watermark_percent: 75}\n"
	loaded, err := Decode(strings.NewReader(explicit), nil)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Memory.MaxBytes != 128<<20 || loaded.Memory.HighWatermarkPercent != 90 || loaded.Memory.LowWatermarkPercent != 75 {
		t.Fatalf("explicit: %+v", loaded.Memory)
	}
	for _, value := range []string{"high_watermark_percent: 0", "high_watermark_percent: 100", "high_watermark_percent: -1", "max_bytes: 0", "max_bytes: 512", "low_watermark_percent: 0", "unknown: 1", "high_watermark_percent: 70, low_watermark_percent: 70", "high_watermark_percent: 60"} {
		if _, err := Decode(strings.NewReader(base+"memory: {"+value+"}\n"), nil); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}
