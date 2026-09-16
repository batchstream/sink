package service

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectAdmissionQueuesBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := completionServer(t, memory.New()).server
		core.maxInFlightRequests = 1
		request := admissionRequest{encodedBytes: 64 << 20, inputBytes: 100, stores: []string{"a"}}
		_, release, err := core.admitRequest(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		admitted := make(chan context.CancelFunc, 1)
		go func() {
			_, done, err := core.admitRequest(t.Context(), request)
			if err != nil {
				t.Error(err)
			}
			admitted <- done
		}()
		synctest.Wait()
		if len(admitted) != 0 || core.queuedBytes >= 1024 || core.queuedRequests != 1 || core.inFlightRequests != 1 {
			t.Fatal("waiting request executed or reserved hypothetical output buffers")
		}
		release()
		done := <-admitted
		if done != nil {
			done()
		}
		if core.queuedRequests != 0 || core.queuedBytes != 0 || core.inFlightBytes != 0 {
			t.Fatal("admitted burst leaked queue or execution capacity")
		}
	})
}

func TestDirectAdmissionBoundsQueuedCountAndBytes(t *testing.T) {
	for _, bound := range []string{"requests", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				core := completionServer(t, memory.New()).server
				core.maxInFlightRequests = 1
				switch bound {
				case "requests":
					core.maxQueuedRequests = 1
				case "bytes":
					core.maxQueuedBytes = 500
				}
				request := admissionRequest{encodedBytes: 1, inputBytes: 100, stores: []string{"primary"}}
				_, release, err := core.admitRequest(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				ctx, cancel := context.WithCancel(t.Context())
				finished := make(chan error, 1)
				go func() { _, _, err := core.admitRequest(ctx, request); finished <- err }()
				synctest.Wait()
				_, done, err := core.admitRequest(t.Context(), request)
				if done != nil {
					done()
				}
				if status.Code(err) != codes.ResourceExhausted || core.queuedRequests != 1 {
					t.Fatalf("unbounded admission queue: %v", err)
				}
				cancel()
				if err := <-finished; status.Code(err) != codes.Canceled {
					t.Fatalf("cancellation became a resource failure: %v", err)
				}
				if core.queuedBytes != 0 || core.queuedRequests != 0 || len(core.admissionWaiters) != 0 {
					t.Fatal("canceled request retained queue capacity")
				}
			})
		})
	}
}

func TestDirectAdmissionWaitDeadlineAndImpossibleRequests(t *testing.T) {
	for _, callerDeadline := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			core := completionServer(t, memory.New()).server
			core.maxInFlightBytes = 100
			busy := admissionRequest{encodedBytes: 100}
			_, release, err := core.admitRequest(t.Context(), busy)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			request := admissionRequest{encodedBytes: 101, inputBytes: 1}
			start := time.Now()
			_, _, err = core.admitRequest(t.Context(), request)
			if status.Code(err) != codes.ResourceExhausted || time.Since(start) != 0 {
				t.Fatal("impossible request occupied the waiting queue")
			}
			request.encodedBytes = 50
			ctx := t.Context()
			want := codes.ResourceExhausted
			if callerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
				defer cancel()
				want = codes.DeadlineExceeded
			}
			_, _, err = core.admitRequest(ctx, request)
			if status.Code(err) != want || core.queuedRequests != 0 || core.inFlightBytes != 100 {
				t.Fatalf("deadline outcome or accounting: %v", err)
			}
		})
	}
}

func TestAdmissionHandoffFillsEveryAvailableSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := completionServer(t, memory.New()).server
		core.maxInFlightBytes = 100
		busy := admissionRequest{encodedBytes: 100}
		_, release, err := core.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatal(err)
		}
		admitted := make(chan context.CancelFunc, 16)
		for range 16 {
			go func() {
				request := admissionRequest{encodedBytes: 1, inputBytes: 1}
				_, done, err := core.admitRequest(t.Context(), request)
				if err != nil {
					t.Error(err)
				}
				admitted <- done
			}()
		}
		synctest.Wait()
		release()
		synctest.Wait()
		if len(admitted) != 16 || core.queuedRequests != 0 {
			t.Fatal("handoff stranded runnable waiters without another completion")
		}
		for range 16 {
			if done := <-admitted; done != nil {
				done()
			}
		}
	})
}

func TestDirectAdmissionWaitConsumesRequestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := completionServer(t, memory.New()).server
		core.requestTimeout = time.Second
		core.admissionWait = time.Second
		core.maxInFlightRequests = 1
		busy := admissionRequest{encodedBytes: 1}
		_, release, err := core.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		go func() { time.Sleep(500 * time.Millisecond); release() }()
		request := admissionRequest{encodedBytes: 1, inputBytes: 1}
		execution, done, err := core.admitRequest(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		defer done()
		deadline, ok := execution.Deadline()
		if !ok || !deadline.Equal(started.Add(time.Second)) {
			t.Fatalf("admission reset the request timeout: %v", deadline)
		}
	})
}
