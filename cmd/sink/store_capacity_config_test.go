package main

import (
	"strings"
	"testing"
)

func TestStoreExecutionByteConfiguration(t *testing.T) {
	backend := backendConfig{name: "search"}
	for _, limits := range []map[string]int{{"search": 0}, {"search": -1}, {"search": 300 << 20}, {"unknown": 1}} {
		loaded := config{grpcMaxSendBytes: 64 << 20, storages: []backendConfig{backend}}
		file := serviceConfigFile{StoreExecutionBytes: limits}
		err := loaded.loadReliabilityConfig(file)
		if err == nil || !strings.Contains(err.Error(), "service.store_execution_bytes") {
			t.Fatalf("invalid store limit %v: %v", limits, err)
		}
	}
	loaded := config{grpcMaxSendBytes: 64 << 20, storages: []backendConfig{backend}}
	file := serviceConfigFile{StoreExecutionBytes: map[string]int{"search": 128 << 20}}
	if err := loaded.loadReliabilityConfig(file); err != nil || loaded.storeExecutionBytes["search"] != 128<<20 {
		t.Fatalf("store execution limits: %v, %v", loaded.storeExecutionBytes, err)
	}
	file.StoreExecutionBytes["search"] = 1
	if loaded.storeExecutionBytes["search"] != 128<<20 {
		t.Fatal("configuration retained a mutable input map")
	}
}
