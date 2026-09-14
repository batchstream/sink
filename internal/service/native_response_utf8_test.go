package service_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeResponseRejectsInvalidUTF8AcrossCodecs(t *testing.T) {
	for _, field := range []string{"content type", "header name", "header value"} {
		for _, vt := range []bool{false, true} {
			client, backend := nativeRPCFixture(t, false)
			response := &storage.NativeResponse{ContentType: "application/octet-stream", Payload: []byte{0xff}, Success: true, StatusCode: 200}
			switch field {
			case "content type":
				response.ContentType += "\xff"
			case "header name":
				response.Headers = http.Header{"X-Bad\xff": {"value"}}
			case "header value":
				response.Headers = http.Header{"X-Value": {"\xff"}}
			}
			backend.executeResponse = response
			var options []grpc.CallOption
			if vt {
				options = append(options, grpc.ForceCodecV2(protocol.NewVTProtoCodec()))
			}
			_, err := client.Execute(t.Context(), nativeSearchRequest(), options...)
			if status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "native response") {
				t.Errorf("invalid response metadata escaped validation: field=%s vt=%v error=%v", field, vt, err)
			}
		}
	}
}

func TestNativeResponseRetainsUnicodeHeadersAndBinaryPayload(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.executeResponse = &storage.NativeResponse{ContentType: "application/octet-stream", Payload: []byte{0xff, 0xfe}, Success: true, StatusCode: 200,
		Headers: http.Header{"X-Message": {"中文�"}}}
	response, err := client.Execute(t.Context(), nativeSearchRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.GetPayload(), backend.executeResponse.Payload) || len(response.GetHeaders()) != 1 || response.GetHeaders()[0].GetValues()[0] != "中文�" {
		t.Fatalf("valid response changed: %v", response)
	}
}
