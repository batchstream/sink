package protocol_test

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMemoryAdmissionPreservesSafeScanRetryOnlyBeforeExecution(t *testing.T) {
	scan := &sink.ScanRequest{}
	err := protocol.MemoryAdmissionError(scan, capacity.ErrBusy)
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatal("missing pre-execution retry detail")
	}
	detail, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || detail.GetReason() != "SCAN_ADMISSION_REJECTED" {
		t.Fatal(details)
	}
	backendError := status.Error(codes.ResourceExhausted, "backend resource exhausted")
	if len(status.Convert(protocol.MemoryAdmissionError(scan, backendError)).Details()) != 0 {
		t.Fatal("backend failure advertised safe retry")
	}
	write := &sink.WriteRequest{}
	if len(status.Convert(protocol.MemoryAdmissionError(write, capacity.ErrBusy)).Details()) != 0 {
		t.Fatal("write advertised Scan retry")
	}
}
