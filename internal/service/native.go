package service

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"sort"
	"unicode/utf8"

	"github.com/batchstream/sink-go/uri"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nativeRequest(req *sink.Command, maximum int) (storage.NativeRequest, error) {
	request := storage.NativeRequest{MaxBytes: maximum}
	address, err := uri.Parse(req.GetUri())
	if err != nil {
		return request, status.Error(codes.InvalidArgument, "invalid native URI: "+err.Error())
	}
	for _, value := range []string{req.GetMethod(), req.GetPath(), req.GetQuery(), req.GetContentType()} {
		if !utf8.ValidString(value) {
			return request, status.Error(codes.InvalidArgument, "native command fields must contain valid UTF-8")
		}
	}
	if len(req.GetPayload()) > 0 && req.GetContentType() == "" {
		return request, status.Error(codes.InvalidArgument, "native payload requires content_type")
	}
	if req.GetContentType() != "" {
		if _, _, err := mime.ParseMediaType(req.GetContentType()); err != nil {
			return request, status.Error(codes.InvalidArgument, "invalid native content_type")
		}
	}
	headers := make(http.Header)
	for _, header := range req.GetHeaders() {
		if header == nil || header.GetName() == "" || len(header.GetValues()) == 0 {
			return request, status.Error(codes.InvalidArgument, "native header requires a name and values")
		}
		if !utf8.ValidString(header.GetName()) {
			return request, status.Error(codes.InvalidArgument, "native header name must contain valid UTF-8")
		}
		for _, value := range header.GetValues() {
			if !utf8.ValidString(value) {
				return request, status.Error(codes.InvalidArgument, "native header values must contain valid UTF-8")
			}
		}
		headers[header.GetName()] = append(headers[header.GetName()], header.GetValues()...)
	}
	request = storage.NativeRequest{URI: address.String(),
		Method: req.GetMethod(), Path: req.GetPath(), Query: req.GetQuery(), Headers: headers,
		ContentType: req.GetContentType(), Payload: req.GetPayload(), MaxBytes: maximum}
	return request, nil
}

func nativeStatus(err error) error {
	if err == nil {
		return nil
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		code := codes.Internal
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			code = status.FromContextError(err).Code()
		case errors.Is(err, storage.ErrNativeUnsupported):
			code = codes.Unimplemented
		default:
			storageCode, _ := storage.ErrorDetails(err)
			switch storageCode {
			case storage.ErrorCodeInvalidArgument:
				code = codes.InvalidArgument
			case storage.ErrorCodeResourceExhausted:
				code = codes.ResourceExhausted
			case storage.ErrorCodeUnavailable:
				code = codes.Unavailable
			case storage.ErrorCodeDeadlineExceeded:
				code = codes.DeadlineExceeded
			}
		}
		grpcStatus = status.New(code, err.Error())
	}
	// Backend diagnostics travel in gRPC trailers, outside payload size checks.
	// Bound them like ordinary operation failures while retaining status details.
	encoded := grpcStatus.Proto()
	encoded.Message = boundedFailureMessage(encoded.Message, maxFailureMessageBytes)
	return status.FromProto(encoded).Err()
}

func (s *Server) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	maximum := s.maxReadBytes
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return nil, err
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	result, err := backend.Execute(ctx, request)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if !utf8.ValidString(result.ContentType) {
		return nil, status.Error(codes.Internal, "native response content type must contain valid UTF-8")
	}
	response := &sink.ExecuteResponse{ContentType: result.ContentType, Payload: result.Payload,
		Success: result.Success, StatusCode: uint32(result.StatusCode)}
	names := make([]string, 0, len(result.Headers))
	for name := range result.Headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !utf8.ValidString(name) {
			return nil, status.Error(codes.Internal, "native response header names must contain valid UTF-8")
		}
		for _, value := range result.Headers[name] {
			if !utf8.ValidString(value) {
				return nil, status.Error(codes.Internal, "native response header values must contain valid UTF-8")
			}
		}
		header := &sink.Header{Name: name, Values: result.Headers[name]}
		response.Headers = append(response.Headers, header)
	}
	if response.SizeVT() > maximum {
		return nil, status.Error(codes.ResourceExhausted, "native response exceeds byte limit")
	}
	return response, nil
}

func (s *Server) scan(ctx context.Context, req *sink.ScanRequest, send func(*sink.ScanResponse) error) error {
	maximum := s.maxReadBytes
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return err
	}
	maximum = min(maximum, 4<<20)
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return err
	}
	batchSize := int(req.GetBatchSize())
	if batchSize == 0 {
		batchSize = 100
	}
	scan := storage.ScanRequest{Request: request, BatchSize: batchSize, Cursor: req.GetCursor()}
	if projection := req.GetProjection(); projection != nil {
		scan.Projection = &storage.Projection{Fields: projection.GetFields(), Exclude: projection.GetExclude()}
	}
	if err := scan.Projection.Validate(); err != nil {
		return nativeStatus(err)
	}
	if batchSize > 1000 || len(scan.Cursor) > storage.MaxScanCursorBytes {
		return status.Error(codes.InvalidArgument, "scan batch or cursor exceeds its limit")
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nativeStatus(storage.ErrNativeUnsupported)
	}
	if _, err := scan.Resume(); err != nil {
		return nativeStatus(err)
	}
	// Leave room for cursor metadata and protobuf framing inside the page limit.
	scan.Request.MaxBytes = maximum - min(maximum/4, storage.MaxScanCursorBytes) - 128
	if scan.Request.MaxBytes <= 0 {
		return status.Error(codes.ResourceExhausted, "scan page budget is too small")
	}
	count := 0
	scan.Emit = func(document storage.Document) error {
		count++
		if count > batchSize {
			return status.Error(codes.Internal, "backend exceeded scan page size")
		}
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		frame := &sink.ScanResponse{Documents: []*sink.Document{encoded}}
		if frame.SizeVT() > maximum {
			return status.Error(codes.ResourceExhausted, "scan document exceeds message limit")
		}
		return send(frame)
	}
	result, err := backend.Scan(ctx, scan)
	if err != nil {
		return nativeStatus(err)
	}
	if len(result.Documents) != 0 || (len(result.NextCursor) != 0 && count == 0) || len(result.NextCursor) > storage.MaxScanCursorBytes {
		return status.Error(codes.Internal, "backend returned an invalid streaming scan page")
	}
	final := &sink.ScanResponse{Complete: true, NextCursor: result.NextCursor}
	if final.SizeVT() > maximum {
		return status.Error(codes.ResourceExhausted, "scan cursor exceeds message limit")
	}
	return send(final)
}

func (s *BatchingServer) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return s.server.Execute(ctx, req)
}
