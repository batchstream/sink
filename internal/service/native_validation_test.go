package service_test

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeRPCRejectsInvalidUTF8CommandFields(t *testing.T) {
	client, _ := nativeRPCFixture(t, false)
	codec := grpc.ForceCodecV2(protocol.NewVTProtoCodec())
	for _, field := range []string{"URI store", "URI resource", "method", "path", "query", "content type", "header name", "header value"} {
		t.Run(field, func(t *testing.T) {
			command := nativeSearchRequest().Command
			switch field {
			case "URI store":
				command.Uri = "sink://primary\xff/products"
			case "URI resource":
				command.Uri = "sink://primary/data\xff"
			case "method":
				command.Method = "POST\xff"
			case "path":
				command.Path = "/products\xff/_search"
			case "query":
				command.Query = "q=value\xff"
			case "content type":
				command.ContentType = "application/json\xff"
			case "header name", "header value":
				header := &sink.Header{Name: "X-Option", Values: []string{"value"}}
				if field == "header name" {
					header.Name += "\xff"
				} else {
					header.Values[0] += "\xff"
				}
				command.Headers = []*sink.Header{header}
			}
			execute := &sink.ExecuteRequest{Command: command}
			_, executeErr := client.Execute(t.Context(), execute, codec)
			query := &sink.QueryRequest{Command: command}
			_, queryErr := client.Query(t.Context(), query, codec)
			count := &sink.CountRequest{Command: command}
			_, countErr := client.Count(t.Context(), count, codec)
			scan := &sink.ScanRequest{Command: command}
			_, scanErr := client.Scan(t.Context(), scan, codec)
			for method, err := range map[string]error{"Execute": executeErr, "Query": queryErr, "Count": countErr, "Scan": scanErr} {
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("%s accepted invalid UTF-8 %s: %v", method, field, err)
				}
			}
		})
	}
}

func TestQueryRejectsInvalidUTF8FieldsBeforeDispatch(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.queries = make(chan storage.QueryRequest, 2)
	codec := grpc.ForceCodecV2(protocol.NewVTProtoCodec())
	for _, projection := range []bool{false, true} {
		request := &sink.QueryRequest{Command: nativeSearchRequest().Command}
		if projection {
			request.Projection = &sink.Projection{Fields: []string{"field\xff"}}
		} else {
			field := &sink.SortField{Field: "field\xff"}
			request.Sort = []*sink.SortField{field}
		}
		_, err := client.Query(t.Context(), request, codec)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("invalid query field accepted: projection=%v error=%v", projection, err)
		}
	}
	if len(backend.queries) != 0 {
		t.Fatalf("invalid fields reached backend in %d queries", len(backend.queries))
	}
}
