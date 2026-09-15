// Package service implements the public Sink gRPC service.
package service

import (
	"errors"
	"fmt"
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
	Storage                 storage.Storage
	Lua                     *merge.LuaEngine
	Publisher               queue.Publisher
	MaxOperations           int
	MaxMergeAttempts        int
	Metrics                 *sinkmetrics.Metrics
	RequestTimeout          time.Duration
	MaxInFlightRequests     int
	MaxInFlightBytes        int
	MaxPublishRequests      int
	MaxPublishBytes         int
	MaxPublishStoreRequests int
	MaxStoreRequests        int
	MaxReadBytes            int
	MaxScanRequests         int
	MaxScanBytes            int
	MaxStoreScanRequests    int
	ScanAdmissionWait       time.Duration
	StoreExecutionBytes     map[string]int
	StoreNames              []string
}

func New(opts Options) (*Server, error) {
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
	if opts.RequestTimeout < 0 || opts.MaxInFlightRequests < 0 || opts.MaxInFlightBytes < 0 || opts.MaxStoreRequests < 0 || opts.MaxReadBytes < 0 {
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
	if opts.MaxStoreRequests == 0 {
		opts.MaxStoreRequests = 32
	}
	if opts.MaxReadBytes == 0 {
		opts.MaxReadBytes = storage.DefaultMaxReadBytes
	}
	if opts.MaxPublishRequests < 0 || opts.MaxPublishBytes < 0 || opts.MaxPublishStoreRequests < 0 {
		return nil, errors.New("create Sink server: publish limits cannot be negative")
	}
	if opts.MaxPublishRequests == 0 {
		opts.MaxPublishRequests = 32
	}
	if opts.MaxPublishStoreRequests == 0 {
		opts.MaxPublishStoreRequests = opts.MaxStoreRequests
	}
	if opts.MaxPublishBytes == 0 {
		opts.MaxPublishBytes = 256 << 20
	}
	if opts.MaxScanRequests < 0 || opts.MaxScanBytes < 0 || opts.MaxStoreScanRequests < 0 || opts.ScanAdmissionWait < 0 {
		return nil, errors.New("create Sink server: scan limits cannot be negative")
	}
	if opts.MaxScanRequests == 0 {
		opts.MaxScanRequests = max(1, opts.MaxInFlightRequests/2)
	}
	if opts.MaxScanBytes == 0 {
		opts.MaxScanBytes = max(1, opts.MaxInFlightBytes/2)
	}
	if opts.MaxStoreScanRequests == 0 {
		opts.MaxStoreScanRequests = max(1, opts.MaxStoreRequests/2)
	}
	if opts.ScanAdmissionWait == 0 {
		opts.ScanAdmissionWait = 2 * time.Second
	}
	opts.ScanAdmissionWait = min(opts.ScanAdmissionWait, opts.RequestTimeout)
	if opts.MaxScanRequests > opts.MaxInFlightRequests || opts.MaxScanBytes > opts.MaxInFlightBytes || opts.MaxStoreScanRequests > opts.MaxStoreRequests {
		return nil, errors.New("create Sink server: scan limits cannot exceed total limits")
	}
	storeRequests := make(map[string]int, len(opts.StoreNames))
	publishStoreRequests := make(map[string]int, len(opts.StoreNames))
	storeBytes := make(map[string]int, len(opts.StoreNames))
	for _, name := range opts.StoreNames {
		storeRequests[name] = 0
		publishStoreRequests[name] = 0
		storeBytes[name] = 0
	}
	storeLimits := make(map[string]int, len(opts.StoreExecutionBytes))
	for name, limit := range opts.StoreExecutionBytes {
		if _, configured := storeRequests[name]; !configured || limit <= 0 || limit > opts.MaxInFlightBytes {
			return nil, fmt.Errorf("create Sink server: execution byte limit for %q must name a configured store and be between 1 and the global limit", name)
		}
		storeLimits[name] = limit
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
		name:                 "execution",
		metrics:              opts.Metrics,
		requestTimeout:       opts.RequestTimeout,
		maxInFlightRequests:  opts.MaxInFlightRequests,
		maxInFlightBytes:     opts.MaxInFlightBytes,
		maxStoreRequests:     opts.MaxStoreRequests,
		storeRequests:        storeRequests,
		admissionChanged:     make(chan struct{}),
		maxScanRequests:      opts.MaxScanRequests,
		maxScanBytes:         opts.MaxScanBytes,
		maxStoreScanRequests: opts.MaxStoreScanRequests,
		storeScanRequests:    make(map[string]int),
		scanAdmissionWait:    opts.ScanAdmissionWait,
		storeBytes:           storeBytes,
		maxStoreBytes:        storeLimits,
	}
	publishAdmission := &admissionPool{
		name:                "publish",
		metrics:             opts.Metrics,
		requestTimeout:      opts.RequestTimeout,
		maxInFlightRequests: opts.MaxPublishRequests,
		maxInFlightBytes:    opts.MaxPublishBytes,
		maxStoreRequests:    opts.MaxPublishStoreRequests,
		storeRequests:       publishStoreRequests,
		admissionChanged:    make(chan struct{}),
	}
	server := &Server{
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
