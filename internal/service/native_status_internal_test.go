package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/batchstream/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestNativeStatusBoundsEveryErrorClassification(t *testing.T) {
	message := strings.Repeat("错误\xff", 10000)
	cause := errors.New(message)
	cases := []struct {
		err  error
		code codes.Code
	}{
		{cause, codes.Internal},
		{storage.InvalidArgumentError(cause), codes.InvalidArgument},
		{storage.ResourceExhaustedError(cause), codes.ResourceExhausted},
		{storage.BackendError(cause), codes.Unavailable},
		{fmt.Errorf("%s: %w", message, storage.ErrNativeUnsupported), codes.Unimplemented},
		{fmt.Errorf("%s: %w", message, context.Canceled), codes.Canceled},
		{fmt.Errorf("%s: %w", message, context.DeadlineExceeded), codes.DeadlineExceeded},
		{status.Error(codes.PermissionDenied, message), codes.PermissionDenied},
	}
	for _, test := range cases {
		converted := status.Convert(nativeStatus(test.err))
		if converted.Code() != test.code || len(converted.Message()) == 0 || len(converted.Message()) > maxFailureMessageBytes || !utf8.ValidString(converted.Message()) {
			t.Errorf("code=%s want=%s bytes=%d valid_utf8=%v", converted.Code(), test.code, len(converted.Message()), utf8.ValidString(converted.Message()))
		}
	}
	if err := nativeStatus(nil); err != nil {
		t.Fatal(err)
	}
}

func TestNativeStatusPreservesDetailsAndShortMessages(t *testing.T) {
	detail := &wrapperspb.StringValue{Value: "backend_unavailable"}
	for _, message := range []string{"短错误", strings.Repeat("错误", 1000)} {
		original, err := status.New(codes.Unavailable, message).WithDetails(detail)
		if err != nil {
			t.Fatal(err)
		}
		converted := status.Convert(nativeStatus(original.Err()))
		if converted.Code() != original.Code() || len(converted.Details()) != 1 {
			t.Fatalf("status or details lost: %v", converted)
		}
		actual, ok := converted.Details()[0].(*wrapperspb.StringValue)
		if !ok || !proto.Equal(actual, detail) {
			t.Fatalf("status detail changed: %v", converted.Details())
		}
		if len(message) <= maxFailureMessageBytes && converted.Message() != message {
			t.Fatalf("short message changed: %q", converted.Message())
		}
		if original.Message() != message {
			t.Fatal("original status was mutated")
		}
	}
}
