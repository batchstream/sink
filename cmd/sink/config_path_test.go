package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseConfigPath(t *testing.T) {
	args := []string{"--config", "/etc/sink/config.yaml"}
	path, err := parseConfigPath(args)
	if err != nil {
		t.Fatalf("parseConfigPath() error = %v", err)
	}
	if path != "/etc/sink/config.yaml" {
		t.Fatalf("parseConfigPath() = %q", path)
	}
}

func TestParseConfigPathRequiresFlag(t *testing.T) {
	_, err := parseConfigPath(nil)
	if err == nil || err.Error() != "--config is required" {
		t.Fatalf("parseConfigPath() error = %v", err)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
