package service

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"sort"
	"unicode/utf8"

	"github.com/liran/sink-go/uri"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
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
	maximum := forwarding.FromContext(ctx).Limit(forwarding.Outputs, s.maxReadBytes)
	if maximum <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "native response budget is exhausted")
	}
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
	admission := admissionRequest{encodedBytes: nativeExecutionBytes(req.GetCommand(), request), stores: []string{protocol.CommandStore(req.GetCommand())}}
	admission.inputBytes = req.SizeVT()
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
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

func (s *Server) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	maximum := forwarding.FromContext(ctx).Limit(forwarding.Outputs, s.maxReadBytes)
	if maximum <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "native response budget is exhausted")
	}
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	// Admission and backend execution share one page deadline, even when the
	// caller did not provide one. Admission has an additional, shorter bound.
	ctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	maximum = min(maximum, 4<<20)
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return nil, err
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
		return nil, nativeStatus(err)
	}
	if batchSize > 1000 || len(scan.Cursor) > storage.MaxScanCursorBytes {
		return nil, status.Error(codes.InvalidArgument, "scan batch or cursor exceeds its limit")
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	encodedBytes := nativeExecutionBytes(req.GetCommand(), request) + req.SizeVT() - req.GetCommand().SizeVT()
	mediaType, _, _ := mime.ParseMediaType(request.ContentType)
	if mediaType != "application/bson" {
		encodedBytes += 2 * (storage.ScanBackendBytes(maximum) - maximum)
	}
	admission := admissionRequest{encodedBytes: encodedBytes, inputBytes: req.SizeVT(), stores: []string{protocol.CommandStore(req.GetCommand())}, scan: true, wait: true}
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := scan.Resume(); err != nil {
		return nil, nativeStatus(err)
	}
	// Leave room for cursor metadata and protobuf framing inside the page limit.
	scan.Request.MaxBytes = maximum - min(maximum/4, storage.MaxScanCursorBytes) - 128
	if scan.Request.MaxBytes <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "scan page budget is too small")
	}
	result, err := backend.Scan(ctx, scan)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if len(result.Documents) > batchSize || (len(result.NextCursor) != 0 && len(result.Documents) == 0) {
		return nil, status.Error(codes.Internal, "backend returned an invalid scan page")
	}
	response := &sink.ScanResponse{NextCursor: result.NextCursor}
	for _, document := range result.Documents {
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		response.Documents = append(response.Documents, encoded)
	}
	if response.SizeVT() > maximum {
		return nil, status.Error(codes.ResourceExhausted, "scan response exceeds byte limit")
	}
	return response, nil
}

func nativeExecutionBytes(req *sink.Command, request storage.NativeRequest) int {
	bytes := req.SizeVT() + 16 + 2*request.MaxBytes
	mediaType, _, _ := mime.ParseMediaType(request.ContentType)
	if mediaType == "application/bson" {
		// The driver receives a complete wire message before Sink can enforce
		// its smaller document/page limit. Account for its 48 MiB wire ceiling.
		bytes += 48 << 20
	}
	return bytes
}

func (s *BatchingServer) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return s.server.Execute(ctx, req)
}

func (s *BatchingServer) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	return s.server.Scan(ctx, req)
}
