package service

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestAllNativeMethodsWaitForStoreCapacityWithoutNewDeadline(t *testing.T) {
	for _, method := range []string{"Query", "Count", "Execute", "Scan"} {
		t.Run(method, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				backend := &nativeBoundaryStore{Store: memory.New()}
				batching, controller := admissionServer(t, backend, 1)
				defer batching.Close()
				permit, err := controller.Acquire(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer permit.Release()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				command := &sink.Command{Uri: "sink://primary", Method: "GET", Path: "/_search"}
				result := make(chan error, 1)
				go func() {
					switch method {
					case "Query":
						request := &sink.QueryRequest{Command: command}
						_, err := batching.server.Query(ctx, request)
						result <- err
					case "Count":
						request := &sink.CountRequest{Command: command}
						_, err := batching.Count(ctx, request)
						result <- err
					case "Execute":
						request := &sink.ExecuteRequest{Command: command}
						_, err := batching.Execute(ctx, request)
						result <- err
					case "Scan":
						request := &sink.ScanRequest{Command: command}
						_, err := batching.server.Scan(ctx, request)
						result <- err
					}
				}()
				synctest.Wait()
				time.Sleep(time.Hour)
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("healthy queued request failed before caller cancellation or capacity: %v", err)
				default:
				}
				if backend.calls != 0 {
					t.Fatal("queued request bypassed Store capacity")
				}
				permit.Release()
				if err := <-result; err != nil {
					t.Fatal(err)
				}
				if backend.calls != 1 {
					t.Fatalf("backend calls = %d, want exactly one", backend.calls)
				}
			})
		})
	}
}
