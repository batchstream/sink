package main

import (
	"strings"
	"testing"
	"time"
)

func TestScanAdmissionWaitConfiguration(t *testing.T) {
	for _, value := range []int{0, -1, 30001} {
		loaded := config{grpcMaxSendBytes: 64 << 20}
		file := serviceConfigFile{ScanAdmissionWaitMilliseconds: &value}
		err := loaded.loadReliabilityConfig(file)
		if err == nil || !strings.Contains(err.Error(), "service.scan_admission_wait_milliseconds") {
			t.Fatalf("invalid wait %d: %v", value, err)
		}
	}
	loaded := config{grpcMaxSendBytes: 64 << 20}
	file := serviceConfigFile{}
	if err := loaded.loadReliabilityConfig(file); err != nil || loaded.scanAdmissionWait != 2*time.Second {
		t.Fatalf("default wait=%s err=%v", loaded.scanAdmissionWait, err)
	}
	seconds := 1
	file.RequestTimeoutSeconds = &seconds
	if err := loaded.loadReliabilityConfig(file); err != nil || loaded.scanAdmissionWait != time.Second {
		t.Fatalf("page deadline did not cap wait=%s err=%v", loaded.scanAdmissionWait, err)
	}
	wait := 250
	file.ScanAdmissionWaitMilliseconds = &wait
	if err := loaded.loadReliabilityConfig(file); err != nil || loaded.scanAdmissionWait != 250*time.Millisecond {
		t.Fatalf("explicit wait=%s err=%v", loaded.scanAdmissionWait, err)
	}
}
