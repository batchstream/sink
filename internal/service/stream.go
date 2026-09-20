package service

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamingKey struct{}

// RPCServer adapts batch execution to result streams. It executes one existing
// microbatch at a time and sends outside the shared executor. The execution
// Server retains its batch mutation API for queue processors.
type RPCServer struct {
	sink.UnimplementedSinkServer
	server   *Server
	batching *BatchingServer
}

func (s *Server) RPC() *RPCServer {
	rpc := &RPCServer{server: s}
	return rpc
}
func (s *BatchingServer) RPC() *RPCServer {
	rpc := &RPCServer{server: s.server, batching: s}
	return rpc
}
func (s *RPCServer) Read(req *sink.ReadRequest, stream grpc.ServerStreamingServer[sink.ReadResponse]) error {
	maximum := defaultBatchMaxOperations
	if s.batching != nil {
		maximum = s.batching.reads.maxOperations
	}
	opts := readStreamOptions{maximum: maximum, batching: s.batching, send: stream.Send}
	return s.server.streamRead(stream.Context(), req, opts)
}

type readStreamOptions struct {
	maximum  int
	batching *BatchingServer
	send     func(*sink.ReadResponse) error
}

func (s *Server) streamRead(ctx context.Context, req *sink.ReadRequest, opts readStreamOptions) error {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return err
	}
	if len(req.GetOperations()) == 0 {
		return status.Error(codes.InvalidArgument, "read request must contain operations")
	}
	if err := s.validateOperationCount(len(req.Operations)); err != nil {
		return err
	}
	ctx = context.WithValue(ctx, streamingKey{}, true)
	for start := 0; start < len(req.Operations); start += opts.maximum {
		if err := contextError(ctx); err != nil {
			return err
		}
		end := min(start+opts.maximum, len(req.Operations))
		batch := &sink.ReadRequest{Operations: req.Operations[start:end]}
		var response *sink.ReadResponse
		var err error
		if opts.batching != nil {
			response, err = opts.batching.Read(ctx, batch)
		} else {
			response, err = s.Read(ctx, batch)
		}
		if err != nil {
			return err
		}
		for i, result := range response.Results {
			result.OperationIndex += uint32(start)
			frame := &sink.ReadResponse{Results: []*sink.ReadResult{result}}
			if frame.SizeVT() > s.maxReadBytes {
				return status.Error(codes.ResourceExhausted, "read result exceeds message limit")
			}
			if err := opts.send(frame); err != nil {
				return err
			}
			response.Results[i] = nil
		}
	}
	return nil
}

func (s *RPCServer) Write(req *sink.WriteRequest, stream grpc.ServerStreamingServer[sink.WriteResponse]) error {
	maximum, bytes := defaultBatchMaxOperations, defaultBatchMaxBytes
	if s.batching != nil {
		maximum, bytes = s.batching.writes.maxOperations, s.batching.writes.maxBytes
	}
	opts := writeStreamOptions{maximum: maximum, bytes: bytes, batching: s.batching, send: stream.Send}
	return s.server.streamWrite(stream.Context(), req, opts)
}

type writeStreamOptions struct {
	maximum  int
	bytes    int
	batching *BatchingServer
	send     func(*sink.WriteResponse) error
}

func (s *Server) streamWrite(ctx context.Context, req *sink.WriteRequest, opts writeStreamOptions) error {
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return err
	}
	if len(req.GetOperations()) == 0 {
		return status.Error(codes.InvalidArgument, "write request must contain operations")
	}
	if err := s.validateOperationCount(len(req.Operations)); err != nil {
		return err
	}
	if !validCompletionMode(req.GetCompletionMode()) {
		return status.Error(codes.InvalidArgument, "invalid completion mode")
	}
	if hasWriteReturns(req) && req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return status.Error(codes.InvalidArgument, "returned write documents require synchronous completion")
	}
	// Validate declarations before any mutation can be submitted.
	if err := protocol.ValidateLuaDeclarations(req.GetLuaPrograms()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = context.WithValue(ctx, streamingKey{}, true)
	for start := 0; start < len(req.Operations); {
		if err := contextError(ctx); err != nil {
			return err
		}
		end, size := start, 0
		for end < len(req.Operations) && end-start < opts.maximum {
			next := req.Operations[end].SizeVT()
			if end > start && next > opts.bytes-size {
				break
			}
			size += next
			end++
		}
		batch := &sink.WriteRequest{Operations: req.Operations[start:end], CompletionMode: req.CompletionMode, LuaPrograms: req.LuaPrograms}
		var response *sink.WriteResponse
		var err error
		if opts.batching != nil {
			response, err = opts.batching.Write(ctx, batch)
		} else {
			response, err = s.Write(ctx, batch)
		}
		if err != nil {
			return err
		}
		for i, result := range response.Results {
			result.OperationIndex += uint32(start)
			frame := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
			if frame.SizeVT() > s.maxReadBytes {
				return status.Error(codes.ResourceExhausted, "write result exceeds message limit")
			}
			if err := opts.send(frame); err != nil {
				return err
			}
			response.Results[i] = nil
		}
		start = end
	}
	return nil
}

func (s *RPCServer) Query(req *sink.QueryRequest, stream grpc.ServerStreamingServer[sink.QueryResponse]) error {
	return s.server.query(stream.Context(), req, stream.Send)
}
func (s *RPCServer) Scan(req *sink.ScanRequest, stream grpc.ServerStreamingServer[sink.ScanResponse]) error {
	return s.server.scan(stream.Context(), req, stream.Send)
}
func (s *RPCServer) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	if s.batching != nil {
		return s.batching.Delete(ctx, req)
	}
	return s.server.Delete(ctx, req)
}
func (s *RPCServer) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return s.server.Execute(ctx, req)
}
func (s *RPCServer) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	return s.server.Count(ctx, req)
}
