package config

import (
	"errors"
	"math"
	"time"
)

func resolveService(file configFile, grpc GRPC, v *validator) Service {
	var loaded Service
	if file.Request != nil {
		loaded.Request.MaxOperations = v.integer("request.max_operations", file.Request.MaxOperations, 1000)
	} else if file.Mode == ModeGateway {
		loaded.Request.MaxOperations = 1000
	}
	execution := file.Execution
	if execution == nil {
		execution = &executionFile{}
	}
	loaded.Execution.MaxSnapshotBytes = v.bytes("execution.max_snapshot_bytes", execution.MaxSnapshotBytes, 32<<20, math.MaxInt)
	loaded.Execution.MaxOutputBytes = v.bytes("execution.max_output_bytes", execution.MaxOutputBytes, 32<<20, math.MaxInt)
	mongo := &loaded.Execution.MongoDB
	mongo.MaxConcurrentWrites = v.integer("execution.mongodb.max_concurrent_writes", execution.MongoDB.MaxConcurrentWrites, 64)
	mongo.MaxConcurrentGroups = v.integer("execution.mongodb.max_concurrent_groups", execution.MongoDB.MaxConcurrentGroups, 16)
	loaded.Merge.MaxAttempts = v.integer("execution.merge.max_attempts", execution.Merge.MaxAttempts, 3)
	lua := &loaded.Merge.Lua
	lua.Timeout = v.duration("execution.merge.lua.timeout", execution.Merge.Lua.Timeout, 100*time.Millisecond)
	lua.MaxSourceBytes = v.bytes("execution.merge.lua.max_source_bytes", execution.Merge.Lua.MaxSourceBytes, 64<<10, math.MaxInt)
	lua.MaxResultBytes = v.bytes("execution.merge.lua.max_result_bytes", execution.Merge.Lua.MaxResultBytes, 16<<20, math.MaxInt)
	lua.MaxCachedPrograms = v.integer("execution.merge.lua.max_cached_programs", execution.Merge.Lua.MaxCachedPrograms, 256)
	lua.MaxInstructions = v.integer("execution.merge.lua.max_instructions", execution.Merge.Lua.MaxInstructions, 1_000_000)
	if file.Mode == ModeEngine {
		batching := file.Batching
		if batching == nil {
			batching = &batchingFile{}
		}
		loaded.Batching = resolveBatching(*batching, grpc, v)
	}
	return loaded
}

func resolveBatching(file batchingFile, grpc GRPC, v *validator) Batching {
	var loaded Batching
	loaded.MaxWait = v.duration("batching.max_wait", file.MaxWait, 2*time.Millisecond)
	loaded.MaxOperations = v.integer("batching.max_operations", file.MaxOperations, 1000)
	loaded.MaxBytes = v.bytes("batching.max_bytes", file.MaxBytes, 16<<20, math.MaxInt)
	loaded.Queue.MaxOperations = v.integer("batching.queue.max_operations", file.Queue.MaxOperations, max(10_000, loaded.MaxOperations))
	loaded.Queue.MaxBytes = v.bytes("batching.queue.max_bytes", file.Queue.MaxBytes, max(128<<20, grpc.MaxReceiveMessageBytes), math.MaxInt)
	if loaded.Queue.MaxOperations < loaded.MaxOperations {
		v.reject(errors.New("batching.queue.max_operations must cover one batch"))
	}
	if loaded.Queue.MaxBytes < grpc.MaxReceiveMessageBytes || loaded.Queue.MaxBytes < loaded.MaxBytes {
		v.reject(errors.New("batching.queue.max_bytes must cover one gRPC request and one batch"))
	}
	return loaded
}
