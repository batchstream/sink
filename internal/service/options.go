// Package service implements the public Sink gRPC service.
package service

import (
	"errors"

	"github.com/batchstream/sink/internal/merge"
	sinkmetrics "github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/queue"
	"github.com/batchstream/sink/internal/storage"
)

const defaultMaxMergeAttempts = 3

type Options struct {
	BoundStore       string
	Storage          storage.Storage
	Lua              *merge.LuaEngine
	Publisher        queue.Publisher
	MaxOperations    int
	MaxMergeAttempts int
	MaxReadBytes     int
	Metrics          *sinkmetrics.Metrics
}

func New(opts Options) (*Server, error) {
	if opts.BoundStore == "" {
		return nil, errors.New("create Sink server: bound Store is required")
	}
	if opts.Storage == nil {
		return nil, errors.New("create Sink server: storage is required")
	}
	if opts.Lua == nil {
		return nil, errors.New("create Sink server: Lua merge engine is required")
	}
	if opts.MaxOperations < 0 || opts.MaxMergeAttempts < 0 || opts.MaxReadBytes < 0 {
		return nil, errors.New("create Sink server: limits cannot be negative")
	}
	if opts.MaxOperations == 0 {
		opts.MaxOperations = int(^uint(0) >> 1)
	}
	if opts.MaxMergeAttempts == 0 {
		opts.MaxMergeAttempts = defaultMaxMergeAttempts
	}
	if opts.MaxReadBytes == 0 {
		opts.MaxReadBytes = storage.DefaultMaxReadBytes
	}
	server := &Server{boundStore: opts.BoundStore, storage: opts.Storage, lua: opts.Lua, publisher: opts.Publisher, maxOperations: opts.MaxOperations, maxMergeAttempts: opts.MaxMergeAttempts, metrics: opts.Metrics, maxReadBytes: opts.MaxReadBytes}
	return server, nil
}
