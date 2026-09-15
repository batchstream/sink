package config

import (
	"errors"
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
	request.MaxReadBytes = v.bounded("service.request.max_read_bytes", file.Request.MaxReadBytes, min(32<<20, grpc.MaxSendMessageBytes/2), grpc.MaxSendMessageBytes/2)
	execution := &loaded.Execution
	execution.MaxRequests = v.bounded("service.execution.max_requests", file.Execution.MaxRequests, 128, 10000)
	execution.MaxBytes = v.bounded("service.execution.max_bytes", file.Execution.MaxBytes, 256<<20, 16<<30)
	execution.MaxRequestsPerStore = v.bounded("service.execution.max_requests_per_store", file.Execution.MaxRequestsPerStore, 32, 10000)
	scan := &execution.Scan
	scan.MaxRequests = v.bounded("service.execution.scan.max_requests", file.Execution.Scan.MaxRequests, max(1, execution.MaxRequests/2), execution.MaxRequests)
	scan.MaxBytes = v.bounded("service.execution.scan.max_bytes", file.Execution.Scan.MaxBytes, max(1, execution.MaxBytes/2), execution.MaxBytes)
	scan.MaxRequestsPerStore = v.bounded("service.execution.scan.max_requests_per_store", file.Execution.Scan.MaxRequestsPerStore, max(1, execution.MaxRequestsPerStore/2), execution.MaxRequestsPerStore)
	scan.AdmissionWait = v.duration("service.execution.scan.admission_wait", file.Execution.Scan.AdmissionWait, min(2*time.Second, request.Timeout))
	if scan.AdmissionWait > request.Timeout {
		v.reject(errors.New("service.execution.scan.admission_wait cannot exceed service.request.timeout"))
	}
	loaded.Publish.MaxRequestsPerStore = v.bounded("service.publish.max_requests_per_store", file.Publish.MaxRequestsPerStore, 32, 10000)
	loaded.Publish.MaxRequests = v.bounded("service.publish.max_requests", file.Publish.MaxRequests, 32, 10000)
	loaded.Publish.MaxBytes = v.bounded("service.publish.max_bytes", file.Publish.MaxBytes, 256<<20, 16<<30)
	loaded.Batching = resolveBatching(file.Batching, request, grpc, v)
	loaded.Merge.MaxAttempts = v.integer("service.merge.max_attempts", file.Merge.MaxAttempts, 3)
	lua := &loaded.Merge.Lua
	lua.Timeout = v.duration("service.merge.lua.timeout", file.Merge.Lua.Timeout, 100*time.Millisecond)
	lua.MaxSourceBytes = v.integer("service.merge.lua.max_source_bytes", file.Merge.Lua.MaxSourceBytes, 64<<10)
	lua.MaxResultBytes = v.integer("service.merge.lua.max_result_bytes", file.Merge.Lua.MaxResultBytes, 16<<20)
	lua.MaxCachedPrograms = v.integer("service.merge.lua.max_cached_programs", file.Merge.Lua.MaxCachedPrograms, 256)
	lua.MaxInstructions = v.integer("service.merge.lua.max_instructions", file.Merge.Lua.MaxInstructions, 1_000_000)
	return loaded
}

func resolveBatching(file batchingFile, request *Request, grpc GRPC, v *validator) Batching {
	var loaded Batching
	loaded.MaxWait = v.duration("service.batching.max_wait", file.MaxWait, 2*time.Millisecond)
	loaded.MaxOperations = v.integer("service.batching.max_operations", file.MaxOperations, request.MaxOperations)
	loaded.MaxBytes = v.integer("service.batching.max_bytes", file.MaxBytes, 16<<20)
	loaded.Queue.MaxOperations = v.integer("service.batching.queue.max_operations", file.Queue.MaxOperations, max(10_000, request.MaxOperations))
	loaded.Queue.MaxBytes = v.integer("service.batching.queue.max_bytes", file.Queue.MaxBytes, max(128<<20, grpc.MaxReceiveMessageBytes))
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
