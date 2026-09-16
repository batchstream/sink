package config

import (
	"errors"
	"math"
	"time"
)

func resolveService(file serviceFile, grpc GRPC, v *validator) Service {
	var loaded Service
	request := &loaded.Request
	request.Timeout = v.duration("service.request.timeout", file.Request.Timeout, 30*time.Second)
	if request.Timeout > 5*time.Minute {
		v.reject(errors.New("service.request.timeout must not exceed 5m"))
	}
	request.MaxOperations = v.integer("service.request.max_operations", file.Request.MaxOperations, 1000)
	request.MaxReadBytes = v.bytes("service.request.max_read_bytes", file.Request.MaxReadBytes, min(32<<20, grpc.MaxSendMessageBytes/2), grpc.MaxSendMessageBytes/2)
	execution := &loaded.Execution
	execution.MaxRequests = v.bounded("service.execution.max_requests", file.Execution.MaxRequests, 128, 10000)
	execution.MaxBytes = v.bytes("service.execution.max_bytes", file.Execution.MaxBytes, 256<<20, 16<<30)
	queue := &execution.Queue
	queue.MaxRequests = v.bounded("service.execution.queue.max_requests", file.Execution.Queue.MaxRequests, 1024, 10000)
	queue.MaxBytes = v.bytes("service.execution.queue.max_bytes", file.Execution.Queue.MaxBytes, min(32<<20, execution.MaxBytes), 16<<30)
	queue.MaxWait = v.duration("service.execution.queue.max_wait", file.Execution.Queue.MaxWait, min(2*time.Second, request.Timeout))
	if queue.MaxWait > request.Timeout {
		v.reject(errors.New("service.execution.queue.max_wait cannot exceed service.request.timeout"))
	}
	scan := &execution.Scan
	scan.MaxRequests = v.bounded("service.execution.scan.max_requests", file.Execution.Scan.MaxRequests, max(1, execution.MaxRequests/2), execution.MaxRequests)
	scan.MaxBytes = v.bytes("service.execution.scan.max_bytes", file.Execution.Scan.MaxBytes, max(1, execution.MaxBytes/2), execution.MaxBytes)
	scan.AdmissionWait = v.duration("service.execution.scan.admission_wait", file.Execution.Scan.AdmissionWait, min(2*time.Second, request.Timeout))
	if scan.AdmissionWait > request.Timeout {
		v.reject(errors.New("service.execution.scan.admission_wait cannot exceed service.request.timeout"))
	}
	loaded.Publish.MaxRequests = v.bounded("service.publish.max_requests", file.Publish.MaxRequests, 32, 10000)
	loaded.Publish.MaxBytes = v.bytes("service.publish.max_bytes", file.Publish.MaxBytes, 256<<20, 16<<30)
	loaded.Batching = resolveBatching(file.Batching, request, grpc, v)
	loaded.Merge.MaxAttempts = v.integer("service.merge.max_attempts", file.Merge.MaxAttempts, 3)
	lua := &loaded.Merge.Lua
	lua.Timeout = v.duration("service.merge.lua.timeout", file.Merge.Lua.Timeout, 100*time.Millisecond)
	lua.MaxSourceBytes = v.bytes("service.merge.lua.max_source_bytes", file.Merge.Lua.MaxSourceBytes, 64<<10, math.MaxInt)
	lua.MaxResultBytes = v.bytes("service.merge.lua.max_result_bytes", file.Merge.Lua.MaxResultBytes, 16<<20, math.MaxInt)
	lua.MaxCachedPrograms = v.integer("service.merge.lua.max_cached_programs", file.Merge.Lua.MaxCachedPrograms, 256)
	lua.MaxInstructions = v.integer("service.merge.lua.max_instructions", file.Merge.Lua.MaxInstructions, 1_000_000)
	return loaded
}

func resolveBatching(file batchingFile, request *Request, grpc GRPC, v *validator) Batching {
	var loaded Batching
	loaded.MaxWait = v.duration("service.batching.max_wait", file.MaxWait, 2*time.Millisecond)
	loaded.MaxOperations = v.integer("service.batching.max_operations", file.MaxOperations, request.MaxOperations)
	loaded.MaxBytes = v.bytes("service.batching.max_bytes", file.MaxBytes, 16<<20, math.MaxInt)
	loaded.Queue.MaxOperations = v.integer("service.batching.queue.max_operations", file.Queue.MaxOperations, max(10_000, request.MaxOperations))
	loaded.Queue.MaxBytes = v.bytes("service.batching.queue.max_bytes", file.Queue.MaxBytes, max(128<<20, grpc.MaxReceiveMessageBytes), math.MaxInt)
	if loaded.MaxOperations > request.MaxOperations {
		v.reject(errors.New("service.batching.max_operations cannot exceed service.request.max_operations"))
	}
	if loaded.Queue.MaxOperations < request.MaxOperations || loaded.Queue.MaxOperations < loaded.MaxOperations {
		v.reject(errors.New("service.batching.queue.max_operations must cover one server request and one batch"))
	}
	if loaded.Queue.MaxBytes < grpc.MaxReceiveMessageBytes || loaded.Queue.MaxBytes < loaded.MaxBytes {
		v.reject(errors.New("service.batching.queue.max_bytes must cover one gRPC request and one batch"))
	}
	return loaded
}
