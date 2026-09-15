package service_test

import (
	"reflect"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestScanProjectionSurvivesWireAndRejectsInvalidFields(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.scans = make(chan storage.ScanRequest, 1)
	request := &sink.ScanRequest{Command: nativeSearchRequest().Command}
	for _, projection := range []*sink.Projection{nil, {}, {Fields: []string{"name", "nested.value"}}, {Fields: []string{"_id"}, Exclude: true}} {
		request.Projection = projection
		if _, err := client.Scan(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		captured := (<-backend.scans).Projection
		if (captured == nil) != (projection == nil) {
			t.Fatalf("projection presence changed: %+v", captured)
		}
		if projection != nil && (!reflect.DeepEqual(captured.Fields, projection.Fields) || captured.Exclude != projection.Exclude) {
			t.Fatalf("projection lost: %+v", captured)
		}
	}
	for _, fields := range [][]string{{""}, {" "}, {"name", "name"}} {
		request.Projection = &sink.Projection{Fields: fields}
		if _, err := client.Scan(t.Context(), request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid projection accepted: %v", err)
		}
	}
	if len(backend.scans) != 0 {
		t.Fatal("invalid projection reached storage")
	}
}
