package service_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLargeLuaFailuresPreserveRPCResults(t *testing.T) {
	for _, batching := range []bool{false, true} {
		for _, binary := range []bool{false, true} {
			name := "direct"
			if batching {
				name = "batched"
			}
			if binary {
				name += " binary"
			}
			t.Run(name, func(t *testing.T) {
				luaOptions := merge.LuaOptions{}
				lua, err := merge.NewLuaEngine(luaOptions)
				if err != nil {
					t.Fatal(err)
				}
				opts := service.Options{Storage: memory.New(), Lua: lua, MaxReadBytes: 8 << 10, MaxInFlightBytes: 64 << 10}
				core, err := service.New(opts)
				if err != nil {
					t.Fatal(err)
				}
				var server sink.SinkServer = core
				if batching {
					batchOptions := service.BatchingOptions{StoreNames: []string{"primary"}, MaxWait: time.Millisecond}
					batch, err := service.NewBatchingServer(core, batchOptions)
					if err != nil {
						t.Fatal(err)
					}
					defer batch.Close()
					server = batch
				}
				seed := putWriteOperation("same", strings.Repeat("错误", 600))
				seedRequest := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{seed}}
				seedResponse, err := core.Write(t.Context(), seedRequest)
				if err != nil || seedResponse.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatalf("seed: %v %v", seedResponse, err)
				}
				source := `return function(current, incoming) error(current.value) end`
				if binary {
					source = `return function(current, incoming) error(string.char(255) .. current.value) end`
				}
				request := mergeWriteRequestWithSource("same", "1", source)
				for range 31 {
					request.Operations = append(request.Operations, request.Operations[0])
				}
				request.Operations = append(request.Operations, putWriteOperation("successful", "committed"))
				listener := bufconn.Listen(1 << 20)
				defer listener.Close()
				codec := protocol.NewVTProtoCodec()
				grpcServer := grpc.NewServer(grpc.ForceServerCodecV2(codec), grpc.MaxSendMsgSize(2*opts.MaxReadBytes))
				sink.RegisterSinkServer(grpcServer, server)
				go func() { _ = grpcServer.Serve(listener) }()
				defer grpcServer.Stop()
				dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
				connection, err := grpc.NewClient("passthrough:///failure-budget", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				client := sink.NewSinkClient(connection)
				response, err := client.Write(t.Context(), request)
				if err != nil {
					t.Fatalf("operation failures broke RPC delivery: %v", err)
				}
				if len(response.Results) != 33 || response.SizeVT() > opts.MaxReadBytes {
					t.Fatalf("unexpected response count/size: %d/%d", len(response.Results), response.SizeVT())
				}
				for index, result := range response.Results[:32] {
					failure := result.GetFailure()
					if result.OperationIndex != uint32(index) || result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || failure.GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT || failure.GetRetryable() || failure.GetMessage() == "" || !utf8.ValidString(failure.GetMessage()) {
						t.Fatalf("failure metadata lost: %v", result)
					}
				}
				if response.Results[32].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatal("successful sibling status lost")
				}
				read, err := core.Read(t.Context(), readRequest("successful"))
				if err != nil || string(read.Results[0].GetDocument().GetPayload()) != `{"value":"committed"}` {
					t.Fatalf("successful sibling was not persisted: %v %v", read, err)
				}
			})
		}
	}
}
