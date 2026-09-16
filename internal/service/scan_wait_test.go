package service

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type waitingScanStorage struct {
	storage.Storage
	storage.NativeStorage
	calls int
	block bool
}

func (s *waitingScanStorage) Scan(ctx context.Context, _ storage.ScanRequest) (storage.ScanResponse, error) {
	s.calls++
	response := storage.ScanResponse{}
	if s.block {
		<-ctx.Done()
		return response, ctx.Err()
	}
	return response, nil
}

func TestScanAdmissionAndExecutionSharePageDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &waitingScanStorage{Storage: memory.New(), block: true}
		server := completionServer(t, backend).server
		server.requestTimeout = 3 * time.Second
		busy := admissionRequest{encodedBytes: server.maxInFlightBytes}
		_, release, err := server.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatal(err)
		}
		command := &sink.Command{Store: "primary", ContentType: "application/json", Payload: []byte(`{}`)}
		request := &sink.ScanRequest{Command: command}
		finished := make(chan error, 1)
		started := time.Now()
		go func() { _, err := server.Scan(t.Context(), request); finished <- err }()
		synctest.Wait()
		if backend.calls != 0 || len(server.admissionWaiters) != 1 {
			t.Fatal("scan did not wait before backend execution")
		}
		time.Sleep(time.Second)
		release()
		synctest.Wait()
		if backend.calls != 1 || len(server.admissionWaiters) != 0 {
			t.Fatal("released capacity did not wake scan")
		}
		if err := <-finished; status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("page deadline: %v", err)
		}
		if elapsed := time.Since(started); elapsed != 3*time.Second {
			t.Fatalf("admission reset the page deadline: %s", elapsed)
		}
		if server.inFlightBytes != 0 || server.scanBytes != 0 || server.inFlightRequests != 0 {
			t.Fatal("deadline leaked execution capacity")
		}
	})
}

func TestScanAdmissionWaitIsBoundedAndCanceled(t *testing.T) {
	for _, mode := range []string{"wait_timeout", "canceled", "page_timeout"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				backend := &waitingScanStorage{Storage: memory.New()}
				server := completionServer(t, backend).server
				busy := admissionRequest{encodedBytes: server.maxInFlightBytes}
				_, release, err := server.admitRequest(t.Context(), busy)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "page_timeout" {
					server.requestTimeout = time.Second
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				command := &sink.Command{Store: "primary"}
				request := &sink.ScanRequest{Command: command}
				finished := make(chan error, 1)
				go func() { _, err := server.Scan(ctx, request); finished <- err }()
				synctest.Wait()
				if mode == "canceled" {
					cancel()
				}
				err = <-finished
				want := codes.ResourceExhausted
				if mode == "canceled" {
					want = codes.Canceled
				} else if mode == "page_timeout" {
					want = codes.DeadlineExceeded
				}
				if status.Code(err) != want {
					t.Fatalf("got %v, want %s", err, want)
				}
				if mode == "wait_timeout" {
					assertScanAdmissionDetail(t, err, "wait_timeout")
				}
				if backend.calls != 0 || len(server.admissionWaiters) != 0 || server.inFlightBytes != busy.encodedBytes {
					t.Fatal("failed scan executed or leaked a reservation")
				}
				release()
				if _, err := server.Scan(t.Context(), request); err != nil || backend.calls != 1 {
					t.Fatalf("scan did not recover: %v", err)
				}
			})
		})
	}
}

func TestScanWaitQueueBounds(t *testing.T) {
	for _, limit := range []string{"requests", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				server := completionServer(t, memory.New()).server
				server.maxInFlightBytes = 100
				server.maxScanRequests = 10
				server.maxScanBytes = 100
				switch limit {
				case "requests":
					server.maxScanRequests = 1
				case "bytes":
					server.maxScanBytes = 15
				}
				busy := admissionRequest{encodedBytes: 100}
				_, release, err := server.admitRequest(t.Context(), busy)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				scan := admissionRequest{encodedBytes: 10, scan: true, wait: true, stores: []string{"primary"}}
				finished := make(chan error, 1)
				go func() { _, _, err := server.admitRequest(ctx, scan); finished <- err }()
				synctest.Wait()
				_, _, err = server.admitRequest(t.Context(), scan)
				assertScanAdmissionDetail(t, err, "queue")
				if len(server.admissionWaiters) != 1 || server.inFlightBytes != 100 {
					t.Fatal("queue cap did not bound retained scans")
				}
				cancel()
				if err := <-finished; status.Code(err) != codes.Canceled {
					t.Fatal(err)
				}
				synctest.Wait()
				if len(server.admissionWaiters) != 0 {
					t.Fatal("canceled scans leaked queue capacity")
				}
			})
		})
	}
}

func TestOversizedScanDoesNotWaitOrAdvertiseRetry(t *testing.T) {
	server := completionServer(t, memory.New()).server
	server.maxInFlightBytes = 100
	server.maxScanBytes = 50
	for _, size := range []int{51, 101} {
		scan := admissionRequest{encodedBytes: size, scan: true, wait: true, stores: []string{"primary"}}
		_, _, err := server.admitRequest(t.Context(), scan)
		failure := status.Convert(err)
		if failure.Code() != codes.ResourceExhausted || len(failure.Details()) != 0 || len(server.admissionWaiters) != 0 {
			t.Fatalf("permanent scan limit advertised retry or queued: %v", err)
		}
	}
}

func TestScanWaitsBehindOlderWriteReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New()).server
		server.maxInFlightBytes = 100
		busy := admissionRequest{encodedBytes: 20}
		_, releaseBusy, err := server.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatal(err)
		}
		write := admissionRequest{encodedBytes: 95, wait: true}
		admitted := make(chan context.CancelFunc, 1)
		go func() {
			_, release, err := server.admitRequest(t.Context(), write)
			if err != nil {
				t.Error(err)
			}
			admitted <- release
		}()
		synctest.Wait()
		scan := admissionRequest{encodedBytes: 10, scan: true, wait: true, stores: []string{"primary"}}
		scanned := make(chan context.CancelFunc, 1)
		go func() {
			_, release, err := server.admitRequest(t.Context(), scan)
			if err != nil {
				t.Error(err)
			}
			scanned <- release
		}()
		synctest.Wait()
		if len(scanned) != 0 || len(server.admissionWaiters) != 2 {
			t.Fatal("scan bypassed an older write despite fairness")
		}
		releaseBusy()
		synctest.Wait()
		releaseWrite := <-admitted
		if releaseWrite == nil || len(scanned) != 0 {
			t.Fatal("older write did not receive capacity first")
		}
		releaseWrite()
		synctest.Wait()
		releaseScan := <-scanned
		if releaseScan == nil {
			t.Fatal("scan failed instead of waiting for capacity")
		}
		releaseScan()
		if server.inFlightBytes != 0 || server.scanBytes != 0 || len(server.admissionWaiters) != 0 {
			t.Fatal("fair admission leaked reservations")
		}
	})
}

func assertScanAdmissionDetail(t *testing.T, err error, reason string) {
	t.Helper()
	failure := status.Convert(err)
	if failure.Code() != codes.ResourceExhausted || len(failure.Details()) != 1 {
		t.Fatalf("missing admission detail: %v", err)
	}
	detail, ok := failure.Details()[0].(*errdetails.ErrorInfo)
	if !ok || detail.GetDomain() != "sink" || detail.GetReason() != "SCAN_ADMISSION_REJECTED" || detail.GetMetadata()["reason"] != reason {
		t.Fatalf("wrong admission detail: %v", detail)
	}
}
