package service_test

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeRPCKeepsBackendDiagnosticsBounded(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.executeErr = storage.InvalidArgumentError(errors.New(strings.Repeat("错误\xff", 10000)))
	_, err := client.Execute(t.Context(), nativeSearchRequest())
	message := status.Convert(err).Message()
	if status.Code(err) != codes.InvalidArgument || len(message) == 0 || len(message) > 1024 || !utf8.ValidString(message) {
		t.Fatalf("native diagnostic escaped response budget: code=%s bytes=%d valid_utf8=%v", status.Code(err), len(message), utf8.ValidString(message))
	}
}
