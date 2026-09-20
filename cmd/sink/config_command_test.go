package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestConfigCheckDoesNotOpenDependenciesOrPrintValues(t *testing.T) {
	path := writeConfig(t, "mode: engine\n")
	shared := writeConfig(t, "name: primary\nstorage:\n  driver: mongodb\n  mongodb: {uri: 'mongodb://127.0.0.1:1'}\nkafka:\n  enabled: true\n  brokers: ['127.0.0.1:1']\n  topic: {name: mutations}\n")
	args := []string{"config", "check", "--config", path, "--store-config", shared}
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
