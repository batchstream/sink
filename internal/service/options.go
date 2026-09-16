// Package service implements the public Sink gRPC service.
package service

import (
	"errors"
	"time"

	"github.com/liran/sink/internal/merge"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
)

const (
	defaultMaxOperations    = 1000
	defaultMaxMergeAttempts = 3
)

type Options struct {
	BoundStore           string
	Storage              storage.Storage
	Lua                  *merge.LuaEngine
	Publisher            queue.Publisher
	MaxOperations        int
	MaxMergeAttempts     int
	Metrics              *sinkmetrics.Metrics
	RequestTimeout       time.Duration
	MaxInFlightRequests  int
	MaxInFlightBytes     int
	MaxAdmissionRequests int
	MaxAdmissionBytes    int
	AdmissionWait        time.Duration
	MaxPublishRequests   int
	MaxPublishBytes      int
	MaxReadBytes         int
	MaxScanRequests      int
	MaxScanBytes         int
	ScanAdmissionWait    time.Duration
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
	if opts.MaxOperations < 0 {
		return nil, errors.New("create Sink server: max operations cannot be negative")
	}
	if opts.MaxMergeAttempts < 0 {
		return nil, errors.New("create Sink server: max merge attempts cannot be negative")
	}
	if opts.RequestTimeout < 0 || opts.MaxInFlightRequests < 0 || opts.MaxInFlightBytes < 0 || opts.MaxReadBytes < 0 {
		return nil, errors.New("create Sink server: resource limits cannot be negative")
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = defaultRequestTimeout
	}
	if opts.MaxInFlightRequests == 0 {
		opts.MaxInFlightRequests = 128
	}
	if opts.MaxInFlightBytes == 0 {
		opts.MaxInFlightBytes = 256 << 20
	}
	if opts.MaxReadBytes == 0 {
		opts.MaxReadBytes = storage.DefaultMaxReadBytes
	}
	if opts.MaxAdmissionRequests < 0 || opts.MaxAdmissionBytes < 0 || opts.AdmissionWait < 0 {
		return nil, errors.New("create Sink server: admission queue limits cannot be negative")
	}
	if opts.MaxAdmissionRequests == 0 {
		opts.MaxAdmissionRequests = 1024
	}
	if opts.MaxAdmissionBytes == 0 {
		opts.MaxAdmissionBytes = min(32<<20, opts.MaxInFlightBytes)
	}
	if opts.AdmissionWait == 0 {
		opts.AdmissionWait = 2 * time.Second
	}
	opts.AdmissionWait = min(opts.AdmissionWait, opts.RequestTimeout)
	if opts.MaxPublishRequests < 0 || opts.MaxPublishBytes < 0 {
		return nil, errors.New("create Sink server: publish limits cannot be negative")
	}
	if opts.MaxPublishRequests == 0 {
		opts.MaxPublishRequests = 32
	}
	if opts.MaxPublishBytes == 0 {
		opts.MaxPublishBytes = 256 << 20
	}
	if opts.MaxScanRequests < 0 || opts.MaxScanBytes < 0 || opts.ScanAdmissionWait < 0 {
		return nil, errors.New("create Sink server: scan limits cannot be negative")
	}
	if opts.MaxScanRequests == 0 {
		opts.MaxScanRequests = max(1, opts.MaxInFlightRequests/2)
	}
	if opts.MaxScanBytes == 0 {
		opts.MaxScanBytes = max(1, opts.MaxInFlightBytes/2)
	}
	if opts.ScanAdmissionWait == 0 {
		opts.ScanAdmissionWait = 2 * time.Second
	}
	opts.ScanAdmissionWait = min(opts.ScanAdmissionWait, opts.RequestTimeout)
	if opts.MaxScanRequests > opts.MaxInFlightRequests || opts.MaxScanBytes > opts.MaxInFlightBytes {
		return nil, errors.New("create Sink server: scan limits cannot exceed total limits")
	}

	maxOperations := opts.MaxOperations
	if maxOperations == 0 {
		maxOperations = defaultMaxOperations
	}
	maxMergeAttempts := opts.MaxMergeAttempts
	if maxMergeAttempts == 0 {
		maxMergeAttempts = defaultMaxMergeAttempts
	}

	executionAdmission := &admissionPool{
		name:                "execution",
		metrics:             opts.Metrics,
		requestTimeout:      opts.RequestTimeout,
		maxInFlightRequests: opts.MaxInFlightRequests,
		maxInFlightBytes:    opts.MaxInFlightBytes,
		maxScanRequests:     opts.MaxScanRequests,
		maxScanBytes:        opts.MaxScanBytes,
		scanAdmissionWait:   opts.ScanAdmissionWait,
		maxQueuedRequests:   opts.MaxAdmissionRequests,
		maxQueuedBytes:      opts.MaxAdmissionBytes,
		admissionWait:       opts.AdmissionWait,
	}
	publishAdmission := &admissionPool{
		name:                "publish",
		metrics:             opts.Metrics,
		requestTimeout:      opts.RequestTimeout,
		maxInFlightRequests: opts.MaxPublishRequests,
		maxInFlightBytes:    opts.MaxPublishBytes,
	}
	server := &Server{
		boundStore:       opts.BoundStore,
		storage:          opts.Storage,
		lua:              opts.Lua,
		publisher:        opts.Publisher,
		maxOperations:    maxOperations,
		maxMergeAttempts: maxMergeAttempts,
		metrics:          opts.Metrics,
		maxReadBytes:     opts.MaxReadBytes,
		admissionPool:    executionAdmission,
		publishAdmission: publishAdmission,
	}
	return server, nil
}
