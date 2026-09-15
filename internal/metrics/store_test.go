package metrics_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMixedStoreRPCsCountEachRequestOnceAndAttributeEveryResult(t *testing.T) {
	observed, err := sinkmetrics.New("test", "alpha", "beta")
	if err != nil {
		t.Fatal(err)
	}
	addresses := []*sink.RecordAddress{{Store: "alpha"}, {Store: "beta"}, {Store: "alpha"}, nil, {Store: "untrusted-store"}}
	read := &sink.ReadRequest{}
	write := &sink.WriteRequest{}
	remove := &sink.DeleteRequest{}
	readResponse := &sink.ReadResponse{}
	writeResponse := &sink.WriteResponse{}
	deleteResponse := &sink.DeleteResponse{}
	for index, address := range addresses {
		readOp := &sink.ReadOperation{Address: address}
		writeOp := &sink.WriteOperation{Address: address}
		deleteOp := &sink.DeleteOperation{Address: address}
		read.Operations = append(read.Operations, readOp)
		write.Operations = append(write.Operations, writeOp)
		remove.Operations = append(remove.Operations, deleteOp)
		readResult := &sink.ReadResult{Status: sink.ReadStatus_READ_STATUS_FOUND}
		writeResult := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
		deleteResult := &sink.DeleteResult{Status: sink.DeleteStatus_DELETE_STATUS_APPLIED}
		if index >= 3 {
			readResult.Status = sink.ReadStatus_READ_STATUS_FAILED
			writeResult.Status = sink.WriteStatus_WRITE_STATUS_FAILED
			deleteResult.Status = sink.DeleteStatus_DELETE_STATUS_FAILED
		}
		readResponse.Results = append(readResponse.Results, readResult)
		writeResponse.Results = append(writeResponse.Results, writeResult)
		deleteResponse.Results = append(deleteResponse.Results, deleteResult)
	}
	cases := []struct {
		method     string
		request    any
		response   any
		success    string
		fullMethod string
	}{
		{"Read", read, readResponse, "found", sink.Sink_Read_FullMethodName},
		{"Write", write, writeResponse, "applied", sink.Sink_Write_FullMethodName},
		{"Delete", remove, deleteResponse, "applied", sink.Sink_Delete_FullMethodName},
	}
	for _, tc := range cases {
		info := &grpc.UnaryServerInfo{FullMethod: tc.fullMethod}
		handler := func(context.Context, any) (any, error) { return tc.response, nil }
		_, err := observed.UnaryServerInterceptor()(t.Context(), tc.request, info, handler)
		if err != nil {
			t.Fatal(err)
		}
		body := scrape(t, observed)
		wanted := []string{
			fmt.Sprintf(`sink_grpc_server_requests_total{code="OK",method="%s",store="_multiple"} 1`, tc.method),
			fmt.Sprintf(`sink_grpc_server_request_duration_seconds_count{method="%s",store="_multiple"} 1`, tc.method),
			fmt.Sprintf(`sink_grpc_server_operation_results_total{method="%s",status="%s",store="alpha"} 2`, tc.method, tc.success),
			fmt.Sprintf(`sink_grpc_server_operation_results_total{method="%s",status="%s",store="beta"} 1`, tc.method, tc.success),
			fmt.Sprintf(`sink_grpc_server_operation_results_total{method="%s",status="failed",store="_unconfigured"} 2`, tc.method),
		}
		for _, line := range wanted {
			if !strings.Contains(body, line) {
				t.Errorf("missing metric: %s", line)
			}
		}
		if strings.Contains(body, "untrusted-store") {
			t.Fatal("unknown store escaped metric classification")
		}
	}
}

func TestNativeRPCsAndTransportErrorsKeepStore(t *testing.T) {
	observed, err := sinkmetrics.New("test", "mongo")
	if err != nil {
		t.Fatal(err)
	}
	command := &sink.Command{Store: "mongo"}
	cases := []struct {
		request any
		method  string
	}{
		{&sink.ExecuteRequest{Command: command}, sink.Sink_Execute_FullMethodName},
		{&sink.QueryRequest{Command: command}, sink.Sink_Query_FullMethodName},
		{&sink.CountRequest{Command: command}, sink.Sink_Count_FullMethodName},
		{&sink.ScanRequest{Command: command}, sink.Sink_Scan_FullMethodName},
	}
	for _, tc := range cases {
		info := &grpc.UnaryServerInfo{FullMethod: tc.method}
		handler := func(context.Context, any) (any, error) {
			return nil, status.Error(codes.Unavailable, "backend unavailable")
		}
		_, err := observed.UnaryServerInterceptor()(t.Context(), tc.request, info, handler)
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
	}
	body := scrape(t, observed)
	for _, method := range []string{"Execute", "Query", "Count", "Scan"} {
		line := fmt.Sprintf(`sink_grpc_server_requests_total{code="Unavailable",method="%s",store="mongo"} 1`, method)
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
	if strings.Contains(body, "sink_grpc_server_operation_results_total{") {
		t.Fatal("transport errors must not fabricate operation results")
	}
}

func TestStoreClassificationBoundsUnknownEmptyAndNilRequests(t *testing.T) {
	observed, err := sinkmetrics.New("test", "mongo")
	if err != nil {
		t.Fatal(err)
	}
	var nilRead *sink.ReadRequest
	var nilWrite *sink.WriteRequest
	var nilDelete *sink.DeleteRequest
	var nilExecute *sink.ExecuteRequest
	var nilQuery *sink.QueryRequest
	var nilCount *sink.CountRequest
	var nilScan *sink.ScanRequest
	requests := []any{nil, nilRead, nilWrite, nilDelete, nilExecute, nilQuery, nilCount, nilScan,
		&sink.ReadRequest{}, &sink.ReadRequest{Operations: []*sink.ReadOperation{nil}},
		&sink.WriteRequest{Operations: []*sink.WriteOperation{nil}}, &sink.DeleteRequest{Operations: []*sink.DeleteOperation{nil}}}
	for _, request := range requests {
		if got := observed.RequestStore(request); got != "_unconfigured" {
			t.Errorf("%T: store = %q", request, got)
		}
	}
	for _, stores := range [][]string{{"mongo", "mongo"}, {"mongo", "unknown"}, {"", "mongo"}, {"unknown", "unknown"}, {"first-unknown", "second-unknown"}} {
		request := &sink.WriteRequest{}
		for _, store := range stores {
			address := &sink.RecordAddress{Store: store}
			operation := &sink.WriteOperation{Address: address}
			request.Operations = append(request.Operations, operation)
		}
		want := "_multiple"
		if stores[0] == stores[1] {
			want = "_unconfigured"
			if stores[0] == "mongo" {
				want = "mongo"
			}
		}
		if got := observed.RequestStore(request); got != want {
			t.Errorf("stores %v: got %q, want %q", stores, got, want)
		}
	}
	for _, store := range []string{"mongo", "Mongo", "_multiple", "_unconfigured", ""} {
		stores := []string{store, store}
		want := "_unconfigured"
		if store == "mongo" {
			want = store
		}
		if got := observed.RequestStores(stores); got != want {
			t.Errorf("store %q: got %q, want %q", store, got, want)
		}
	}
	for index := range 50 {
		store := fmt.Sprintf("untrusted-store-%d", index)
		command := &sink.Command{Store: store}
		request := &sink.ExecuteRequest{Command: command}
		info := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Execute_FullMethodName}
		handler := func(context.Context, any) (any, error) {
			response := &sink.ExecuteResponse{Success: false}
			return response, nil
		}
		_, err := observed.UnaryServerInterceptor()(t.Context(), request, info, handler)
		if err != nil {
			t.Fatal(err)
		}
		observed.ObserveKafkaPublish(store, time.Second, 1, 1)
		observed.ObserveKafkaWorker(store, "failed", 1)
		observed.ObserveKafkaRetry(store, 1)
		observed.ObserveKafkaDeadLetter(store, 1)
		observed.ObserveMergeConflict(store, 1)
		observed.ObserveMergeFold(store, 2)
		observed.ObserveMergeExhausted(store, 1)
		observed.AdjustAdmissionPool(store, "execution", 1, 1)
		observed.ObserveAdmissionPoolRejected(store, "execution", "bytes")
		observed.AdjustScanQueue(store, 1, 10)
		observed.ObserveScanAdmissionWait(store, time.Second)
		observed.AdjustStoreExecutionBytes(store, 10)
		observed.AdjustBatchQueue(store, "Write", 1, 1)
		observed.ObserveBatchRejected(store, "Write", "queue_full")
		batch := sinkmetrics.BatchObservation{Store: store, Method: "Write", Reason: "max_wait"}
		observed.ObserveBatch(batch)
		queue := sinkmetrics.RequestQueueObservation{Store: store, Method: "Write", Outcome: "execute"}
		observed.ObserveRequestQueue(queue)
		phase := sinkmetrics.WritePhaseObservation{Store: store, Completion: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Phase: "lua", Duration: 6 * time.Second}
		observed.ObserveWritePhase(phase)
		observed.ObserveWriteRounds(store, 1, 1)
		observed.ObserveWorkerPoll(store, 1)
		observed.ObserveWorkerRecovery(store)
		observed.ObserveWorkerCommitted(store, time.Now())
		observed.ObserveQuarantined(store)
		observed.SetWorkerOffsetGap(store, true)
	}
	body := scrape(t, observed)
	if strings.Contains(body, "untrusted-store-") {
		t.Fatal("unknown store escaped the configured label set")
	}
	if !strings.Contains(body, `sink_grpc_server_operation_results_total{method="Execute",status="failed",store="_unconfigured"} 50`) {
		t.Fatal("unknown stores were not folded into one result series")
	}
}
