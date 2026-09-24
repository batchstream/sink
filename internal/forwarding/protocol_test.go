package forwarding

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorStatusMarkerNormalizesGRPCContextErrors(t *testing.T) {
	failures := []error{
		context.Canceled,
		context.DeadlineExceeded,
		fmt.Errorf("wrapped: %w", context.Canceled),
		errors.New("plain handler failure"),
	}
	for _, failure := range failures {
		t.Run(failure.Error(), func(t *testing.T) {
			wireError := status.FromContextError(failure).Err()
			marker := ErrorStatusMarker(failure)
			if len(marker) != 64 || marker != ErrorStatusMarker(wireError) {
				t.Fatalf("marker does not match gRPC wire status for %v", failure)
			}
		})
	}
}

func TestErrorStatusMarkerIsBoundedAndRequiresEncodableError(t *testing.T) {
	if marker := ErrorStatusMarker(nil); marker != "" {
		t.Fatal("successful calls must not claim an error")
	}
	failure := status.Error(codes.ResourceExhausted, strings.Repeat("diagnostic", 1024))
	if marker := ErrorStatusMarker(failure); len(marker) != 64 {
		t.Fatalf("marker length=%d, want bounded SHA-256 hex", len(marker))
	}
	invalid := status.Error(codes.Internal, string([]byte{0xff}))
	if marker := ErrorStatusMarker(invalid); marker != "" {
		t.Fatal("unencodable error must not establish ownership")
	}
}
