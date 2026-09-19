package protocol

import (
	"errors"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// MemoryAdmissionError preserves the Scan SDK's safe retry signal, exclusively
// for temporary refusal before invoking the service or forwarding downstream.
func MemoryAdmissionError(message any, err error) error {
	if !errors.Is(err, capacity.ErrBusy) {
		return err
	}
	_, scan := message.(*sink.ScanRequest)
	if forwarded, ok := message.(*forward.ForwardRequest); ok {
		scan = forwarded.GetScan() != nil
	}
	if !scan {
		return err
	}
	detail := &errdetails.ErrorInfo{Domain: "sink", Reason: "SCAN_ADMISSION_REJECTED", Metadata: map[string]string{"pool": "memory", "reason": "busy"}}
	marked, detailErr := status.Convert(err).WithDetails(detail)
	if detailErr != nil {
		return err
	}
	return marked.Err()
}
