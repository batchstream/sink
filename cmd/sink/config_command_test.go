package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestConfigCheckDoesNotOpenDependenciesOrPrintValues(t *testing.T) {
	path := writeConfig(t, `mode: all
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://127.0.0.1:1
    kafka:
      enabled: true
      brokers: [127.0.0.1:1]
      topic:
        name: mutations
      consumer:
        group_id: workers
`)
	args := []string{"config", "check", "--config", path}
	var stdout, stderr bytes.Buffer
	if err := executeCommand(args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "Configuration schema and limits are valid.\n" || stderr.Len() != 0 {
		t.Fatalf("unexpected config check output: %q %q", stdout.String(), stderr.String())
	}
}

func TestConfigCheckRejectsOldSchema(t *testing.T) {
	path := writeConfig(t, "shutdown_timeout_seconds: 15\n")
	args := []string{"config", "check", "--config", path}
	var stdout, stderr bytes.Buffer
	err := executeCommand(args, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "shutdown_timeout_seconds") || stdout.Len() != 0 {
		t.Fatalf("invalid configuration reported as valid: %v", err)
	}
}
